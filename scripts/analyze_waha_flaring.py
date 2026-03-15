#!/usr/bin/env python3
"""
Investigate the relationship between Waha hub gas prices and Permian Basin flaring.

Hypothesis: low/negative Waha prices incentivise operators to flare gas rather than
sell it, because pipeline takeaway constraints make the gas worthless or costly to move.

Data sources:
- Waha daily close prices ($/MMBtu) from XLSX
- VIIRS Nightfire (VNF) daily flare detections with radiative heat (MW)
- RRC gas disposition (monthly reported flaring volumes, MCF)

High-signal subset strategy:
- VNF: use daily total radiative heat (MW) across all Permian flares as a proxy for
  instantaneous flaring intensity. Also count of active flares per day.
- Focus on 2021+ where VNF coverage is most consistent and Waha volatility is highest.
- RRC: monthly lease-level flared MCF for monthly-resolution validation.
"""

import duckdb
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import matplotlib.dates as mdates
import numpy as np
from scipy import stats
from pathlib import Path
from datetime import datetime

OUT_DIR = Path("analysis")
OUT_DIR.mkdir(exist_ok=True)

con = duckdb.connect()

# ── Load Waha prices ──────────────────────────────────────────────────────────
con.execute("""
    INSTALL spatial; LOAD spatial;
    CREATE TABLE waha AS
    SELECT
        TRY_CAST(Field1 AS DATE) AS date,
        TRY_CAST(Field2 AS DOUBLE) AS price
    FROM st_read('/Users/louis/Downloads/waha.xlsx')
    WHERE TRY_CAST(Field1 AS DATE) IS NOT NULL
      AND TRY_CAST(Field2 AS DOUBLE) IS NOT NULL
      -- Exclude Winter Storm Uri spike (Feb 12-17 2021, up to $209/MMBtu)
      AND TRY_CAST(Field2 AS DOUBLE) < 50
    ORDER BY date
""")

# ── Load VNF daily aggregates (Permian bbox) ─────────────────────────────────
con.execute("""
    CREATE TABLE vnf_daily AS
    SELECT
        date,
        count(*) AS detections,
        count(DISTINCT flare_id) AS active_flares,
        sum(rh_mw) AS total_rh_mw,
        avg(rh_mw) FILTER (WHERE rh_mw > 0) AS avg_rh_mw
    FROM 'data/vnf.parquet'
    WHERE detected
    GROUP BY date
    ORDER BY date
""")

# ── Load RRC monthly flaring (Permian districts) ─────────────────────────────
# District mapping: 06→6E, 08→7B, 09→7C, 10→08, 11→8A
con.execute("""
    CREATE TABLE rrc_monthly AS
    SELECT
        make_date(TRY_CAST(cycle_year AS INT), TRY_CAST(cycle_month AS INT), 1) AS month,
        sum(lease_gas_dispcd04_vol + lease_csgd_dispcde04_vol) AS flared_mcf,
        sum(lease_gas_total_vol + lease_csgd_total_vol) AS produced_mcf,
        count(DISTINCT lease_no) FILTER (
            WHERE (lease_gas_dispcd04_vol + lease_csgd_dispcde04_vol) > 0
        ) AS leases_flaring
    FROM 'data/gas_disposition.parquet'
    WHERE district_no IN ('06','08','09','10','11')
    GROUP BY 1
    HAVING month IS NOT NULL
    ORDER BY 1
""")

# ── Merge daily: VNF × Waha ──────────────────────────────────────────────────
daily = con.execute("""
    SELECT
        w.date,
        w.price,
        v.active_flares,
        v.total_rh_mw,
        v.detections,
        v.avg_rh_mw
    FROM waha w
    JOIN vnf_daily v ON v.date = w.date
    WHERE w.date >= '2017-01-01'
    ORDER BY w.date
""").fetchdf()

# ── Merge monthly: RRC × Waha ────────────────────────────────────────────────
monthly = con.execute("""
    SELECT
        r.month,
        r.flared_mcf,
        r.produced_mcf,
        r.leases_flaring,
        CASE WHEN r.produced_mcf > 0
             THEN 100.0 * r.flared_mcf / r.produced_mcf
             ELSE NULL END AS flaring_pct,
        w.avg_price,
        w.min_price,
        w.negative_days
    FROM rrc_monthly r
    JOIN (
        SELECT date_trunc('month', date)::DATE AS month,
               avg(price) AS avg_price,
               min(price) AS min_price,
               count(*) FILTER (WHERE price < 0) AS negative_days
        FROM waha
        GROUP BY 1
    ) w ON w.month = r.month
    WHERE r.month >= '2017-01-01'
    ORDER BY r.month
""").fetchdf()

print(f"Daily dataset: {len(daily)} rows, {daily['date'].min()} to {daily['date'].max()}")
print(f"Monthly dataset: {len(monthly)} rows, {monthly['month'].min()} to {monthly['month'].max()}")

# ── Analysis 1: Rolling correlations ─────────────────────────────────────────
# Use 30-day rolling averages to smooth cloud-cover noise in VNF
daily["price_30d"] = daily["price"].rolling(30, center=True).mean()
daily["rh_30d"] = daily["total_rh_mw"].rolling(30, center=True).mean()
daily["flares_30d"] = daily["active_flares"].rolling(30, center=True).mean()

# ── Analysis 2: Price regime buckets ──────────────────────────────────────────
def price_regime(p):
    if p < 0:
        return "Negative (<$0)"
    elif p < 1:
        return "Very low ($0–1)"
    elif p < 2:
        return "Low ($1–2)"
    elif p < 4:
        return "Moderate ($2–4)"
    else:
        return "High (>$4)"

daily["regime"] = daily["price"].apply(price_regime)

regime_order = ["Negative (<$0)", "Very low ($0–1)", "Low ($1–2)", "Moderate ($2–4)", "High (>$4)"]
regime_stats = []
for regime in regime_order:
    subset = daily[daily["regime"] == regime]
    if len(subset) > 0:
        regime_stats.append({
            "regime": regime,
            "n_days": len(subset),
            "avg_rh_mw": subset["total_rh_mw"].mean(),
            "median_rh_mw": subset["total_rh_mw"].median(),
            "avg_flares": subset["active_flares"].mean(),
            "avg_price": subset["price"].mean(),
        })

print("\n── Price Regime Analysis ──")
print(f"{'Regime':<20} {'Days':>6} {'Avg RH (MW)':>12} {'Med RH (MW)':>12} {'Avg Flares':>11} {'Avg Price':>10}")
for r in regime_stats:
    print(f"{r['regime']:<20} {r['n_days']:>6} {r['avg_rh_mw']:>12.1f} {r['median_rh_mw']:>12.1f} {r['avg_flares']:>11.1f} {r['avg_price']:>10.2f}")

# ── Analysis 3: Lagged cross-correlation ─────────────────────────────────────
# Does flaring respond to price with a lag?
print("\n── Lagged Cross-Correlation (price → flaring) ──")
print(f"{'Lag (days)':<12} {'Corr (RH)':>10} {'Corr (flares)':>14}")
valid = daily.dropna(subset=["price", "total_rh_mw", "active_flares"])
for lag in [0, 7, 14, 30, 60, 90]:
    if lag == 0:
        p = valid["price"].values
        rh = valid["total_rh_mw"].values
        fl = valid["active_flares"].values
    else:
        p = valid["price"].values[:-lag]
        rh = valid["total_rh_mw"].values[lag:]
        fl = valid["active_flares"].values[lag:]
    r_rh, pval_rh = stats.pearsonr(p, rh)
    r_fl, pval_fl = stats.pearsonr(p, fl)
    sig_rh = "***" if pval_rh < 0.001 else "**" if pval_rh < 0.01 else "*" if pval_rh < 0.05 else ""
    sig_fl = "***" if pval_fl < 0.001 else "**" if pval_fl < 0.01 else "*" if pval_fl < 0.05 else ""
    print(f"{lag:<12} {r_rh:>9.3f}{sig_rh:<4} {r_fl:>9.3f}{sig_fl:<4}")

# ── Analysis 4: Monthly correlation (RRC reported) ───────────────────────────
valid_m = monthly.dropna(subset=["avg_price", "flared_mcf"])
r_monthly, p_monthly = stats.pearsonr(valid_m["avg_price"], valid_m["flared_mcf"])
print(f"\n── Monthly Correlation (RRC flared MCF vs avg Waha price) ──")
print(f"  r = {r_monthly:.3f}, p = {p_monthly:.4f} (n={len(valid_m)})")

r_pct, p_pct = stats.pearsonr(
    valid_m["avg_price"],
    valid_m["flaring_pct"].fillna(0)
)
print(f"  r (flaring %) = {r_pct:.3f}, p = {p_pct:.4f}")

# ── Analysis 5: Focus on 2021+ (most recent, best data) ─────────────────────
daily_recent = daily[daily["date"] >= "2021-01-01"].copy()
monthly_recent = monthly[monthly["month"] >= "2021-01-01"].copy()

valid_r = daily_recent.dropna(subset=["price", "total_rh_mw"])
r_recent, p_recent = stats.pearsonr(valid_r["price"], valid_r["total_rh_mw"])
print(f"\n── 2021+ Daily Correlation ──")
print(f"  r = {r_recent:.3f}, p = {p_recent:.6f} (n={len(valid_r)})")

# ── Analysis 6: Extreme events — negative price episodes ─────────────────────
print("\n── Negative Price Episodes (consecutive negative days) ──")
neg_episodes = con.execute("""
    WITH ranked AS (
        SELECT w.date, w.price,
               v.active_flares, v.total_rh_mw,
               w.date - ROW_NUMBER() OVER (ORDER BY w.date)::INT * INTERVAL '1 day' AS grp
        FROM waha w
        JOIN vnf_daily v ON v.date = w.date
        WHERE w.price < 0
    )
    SELECT
        min(date)::VARCHAR AS start_date,
        max(date)::VARCHAR AS end_date,
        count(*) AS days,
        round(min(price), 2) AS min_price,
        round(avg(price), 2) AS avg_price,
        round(avg(total_rh_mw), 1) AS avg_rh_mw,
        round(avg(active_flares), 0) AS avg_flares
    FROM ranked
    GROUP BY grp
    HAVING count(*) >= 3
    ORDER BY start_date
""").fetchdf()

if len(neg_episodes) > 0:
    print(neg_episodes.to_string(index=False))
else:
    print("  No episodes of 3+ consecutive negative days with VNF overlap")

# ── Contextual: avg flaring in 30-day windows around negative episodes ───────
print("\n── Flaring Before/During/After Negative Episodes ──")
context = con.execute("""
    WITH neg_periods AS (
        SELECT date FROM waha WHERE price < 0 AND date >= '2021-01-01'
    ),
    context AS (
        SELECT
            CASE
                WHEN v.date IN (SELECT date FROM neg_periods) THEN 'During negative'
                WHEN v.date IN (SELECT date + INTERVAL '1 day' * i FROM neg_periods, generate_series(1,30) t(i)) THEN 'After (1-30d)'
                WHEN v.date IN (SELECT date - INTERVAL '1 day' * i FROM neg_periods, generate_series(1,30) t(i)) THEN 'Before (1-30d)'
                ELSE 'Normal'
            END AS period,
            v.total_rh_mw,
            v.active_flares
        FROM vnf_daily v
        WHERE v.date >= '2021-01-01'
    )
    SELECT period,
           count(*) AS days,
           round(avg(total_rh_mw), 1) AS avg_rh_mw,
           round(avg(active_flares), 0) AS avg_flares
    FROM context
    GROUP BY period
    ORDER BY CASE period
        WHEN 'Before (1-30d)' THEN 1
        WHEN 'During negative' THEN 2
        WHEN 'After (1-30d)' THEN 3
        ELSE 4
    END
""").fetchdf()
print(context.to_string(index=False))


# ══════════════════════════════════════════════════════════════════════════════
# PLOTS
# ══════════════════════════════════════════════════════════════════════════════

plt.style.use("seaborn-v0_8-darkgrid")
fig_kw = dict(figsize=(14, 10), dpi=150)

# ── Plot 1: Dual-axis time series (30d rolling) ──────────────────────────────
fig, (ax1, ax2) = plt.subplots(2, 1, **fig_kw, sharex=True)
fig.suptitle("Waha Gas Prices vs Permian Flaring Intensity", fontsize=14, fontweight="bold")

# Top: price
ax1.plot(daily["date"], daily["price_30d"], color="#2196F3", linewidth=1)
ax1.axhline(0, color="red", linewidth=0.5, linestyle="--", alpha=0.7)
ax1.fill_between(daily["date"], daily["price_30d"], 0,
                  where=daily["price_30d"] < 0, color="red", alpha=0.15)
ax1.set_ylabel("Waha Price ($/MMBtu)\n30-day rolling avg", fontsize=10)
ax1.set_title("Waha Hub Natural Gas Price", fontsize=11)

# Bottom: flaring
ax2.plot(daily["date"], daily["rh_30d"], color="#FF5722", linewidth=1, label="Total RH (MW)")
ax2r = ax2.twinx()
ax2r.plot(daily["date"], daily["flares_30d"], color="#FF9800", linewidth=1, alpha=0.7, label="Active flares")
ax2.set_ylabel("Total Radiative Heat (MW)\n30-day rolling avg", fontsize=10, color="#FF5722")
ax2r.set_ylabel("Active Flare Sites", fontsize=10, color="#FF9800")
ax2.set_title("VIIRS Nightfire Flaring Intensity — Permian Basin", fontsize=11)
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))
ax2.xaxis.set_major_locator(mdates.YearLocator())

# Shade negative price periods
for _, row in neg_episodes.iterrows() if len(neg_episodes) > 0 else []:
    for ax in [ax1, ax2]:
        ax.axvspan(row["start_date"], row["end_date"], color="red", alpha=0.08)

fig.tight_layout()
fig.savefig(OUT_DIR / "01_timeseries.png", bbox_inches="tight")
print(f"\nSaved {OUT_DIR / '01_timeseries.png'}")

# ── Plot 2: Scatter — daily price vs flaring (30d smoothed) ──────────────────
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), dpi=150)
fig.suptitle("Waha Price vs Flaring Intensity (30-day rolling averages)", fontsize=13, fontweight="bold")

valid_scatter = daily.dropna(subset=["price_30d", "rh_30d", "flares_30d"])

# RH scatter
ax1.scatter(valid_scatter["price_30d"], valid_scatter["rh_30d"],
            alpha=0.3, s=8, c="#FF5722")
# Regression line
slope, intercept, r, p, se = stats.linregress(valid_scatter["price_30d"], valid_scatter["rh_30d"])
x_line = np.linspace(valid_scatter["price_30d"].min(), valid_scatter["price_30d"].max(), 100)
ax1.plot(x_line, slope * x_line + intercept, "k--", linewidth=1.5,
         label=f"r={r:.3f}, p={p:.2e}")
ax1.axvline(0, color="red", linewidth=0.5, linestyle="--", alpha=0.5)
ax1.set_xlabel("Waha Price ($/MMBtu)")
ax1.set_ylabel("Total Radiative Heat (MW)")
ax1.legend(fontsize=9)

# Flare count scatter
ax2.scatter(valid_scatter["price_30d"], valid_scatter["flares_30d"],
            alpha=0.3, s=8, c="#FF9800")
slope2, intercept2, r2, p2, se2 = stats.linregress(valid_scatter["price_30d"], valid_scatter["flares_30d"])
ax2.plot(x_line, slope2 * x_line + intercept2, "k--", linewidth=1.5,
         label=f"r={r2:.3f}, p={p2:.2e}")
ax2.axvline(0, color="red", linewidth=0.5, linestyle="--", alpha=0.5)
ax2.set_xlabel("Waha Price ($/MMBtu)")
ax2.set_ylabel("Active Flare Sites")
ax2.legend(fontsize=9)

fig.tight_layout()
fig.savefig(OUT_DIR / "02_scatter_daily.png", bbox_inches="tight")
print(f"Saved {OUT_DIR / '02_scatter_daily.png'}")

# ── Plot 3: Price regime box plots ───────────────────────────────────────────
fig, (ax1, ax2) = plt.subplots(1, 2, figsize=(14, 6), dpi=150)
fig.suptitle("Flaring Intensity by Waha Price Regime", fontsize=13, fontweight="bold")

regime_data_rh = [daily[daily["regime"] == r]["total_rh_mw"].dropna().values for r in regime_order]
regime_data_fl = [daily[daily["regime"] == r]["active_flares"].dropna().values for r in regime_order]
regime_labels = [f"{r}\n(n={len(daily[daily['regime']==r])})" for r in regime_order]

bp1 = ax1.boxplot(regime_data_rh, labels=regime_labels, patch_artist=True, showfliers=False)
colors = ["#d32f2f", "#f57c00", "#fbc02d", "#66bb6a", "#2196f3"]
for patch, color in zip(bp1["boxes"], colors):
    patch.set_facecolor(color)
    patch.set_alpha(0.6)
ax1.set_ylabel("Total Radiative Heat (MW)")
ax1.set_xlabel("Waha Price Regime")

bp2 = ax2.boxplot(regime_data_fl, labels=regime_labels, patch_artist=True, showfliers=False)
for patch, color in zip(bp2["boxes"], colors):
    patch.set_facecolor(color)
    patch.set_alpha(0.6)
ax2.set_ylabel("Active Flare Sites")
ax2.set_xlabel("Waha Price Regime")

fig.tight_layout()
fig.savefig(OUT_DIR / "03_regime_boxplots.png", bbox_inches="tight")
print(f"Saved {OUT_DIR / '03_regime_boxplots.png'}")

# ── Plot 4: Monthly — RRC reported flaring vs Waha ──────────────────────────
fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), dpi=150, sharex=True)
fig.suptitle("RRC Reported Flaring vs Waha Prices (Monthly)", fontsize=14, fontweight="bold")

ax1.bar(monthly["month"], monthly["avg_price"], width=25, color="#2196F3", alpha=0.7)
ax1.axhline(0, color="red", linewidth=0.5, linestyle="--")
ax1.fill_between(monthly["month"], monthly["avg_price"], 0,
                  where=monthly["avg_price"] < 0, color="red", alpha=0.15)
ax1.set_ylabel("Avg Waha Price ($/MMBtu)")
ax1.set_title("Monthly Average Waha Price")

ax2.bar(monthly["month"], monthly["flared_mcf"] / 1e6, width=25, color="#FF5722", alpha=0.7,
        label="Flared (MMCF)")
ax2.set_ylabel("Reported Flared Gas (MMCF)")
ax2.set_title("RRC Reported Permian Gas Flared")
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y"))

fig.tight_layout()
fig.savefig(OUT_DIR / "04_monthly_rrc.png", bbox_inches="tight")
print(f"Saved {OUT_DIR / '04_monthly_rrc.png'}")

# ── Plot 5: Monthly scatter — RRC flaring vs Waha ───────────────────────────
fig, ax = plt.subplots(figsize=(8, 6), dpi=150)
valid_ms = monthly.dropna(subset=["avg_price", "flared_mcf"])
sc = ax.scatter(valid_ms["avg_price"], valid_ms["flared_mcf"] / 1e6,
                c=valid_ms["month"].apply(lambda x: x.year), cmap="viridis",
                alpha=0.7, s=40, edgecolors="white", linewidths=0.5)
slope_m, int_m, r_m, p_m, _ = stats.linregress(valid_ms["avg_price"], valid_ms["flared_mcf"] / 1e6)
x_m = np.linspace(valid_ms["avg_price"].min(), valid_ms["avg_price"].max(), 100)
ax.plot(x_m, slope_m * x_m + int_m, "k--", linewidth=1.5, label=f"r={r_m:.3f}, p={p_m:.4f}")
ax.axvline(0, color="red", linewidth=0.5, linestyle="--", alpha=0.5)
ax.set_xlabel("Avg Monthly Waha Price ($/MMBtu)")
ax.set_ylabel("Reported Flared Gas (MMCF)")
ax.set_title("RRC Reported Flaring vs Waha Price (Monthly)", fontweight="bold")
ax.legend()
plt.colorbar(sc, label="Year")
fig.tight_layout()
fig.savefig(OUT_DIR / "05_monthly_scatter.png", bbox_inches="tight")
print(f"Saved {OUT_DIR / '05_monthly_scatter.png'}")

# ── Plot 6: 2021+ focus with detrending ──────────────────────────────────────
# Detrend flaring (remove linear time trend) to isolate price signal
dr = daily_recent.dropna(subset=["price", "total_rh_mw"]).copy()
dr["day_idx"] = (dr["date"] - dr["date"].min()).dt.days
slope_t, int_t, _, _, _ = stats.linregress(dr["day_idx"], dr["total_rh_mw"])
dr["rh_detrended"] = dr["total_rh_mw"] - (slope_t * dr["day_idx"] + int_t)
dr["rh_detrended_30d"] = dr["rh_detrended"].rolling(30, center=True).mean()
dr["price_30d"] = dr["price"].rolling(30, center=True).mean()

valid_dt = dr.dropna(subset=["price_30d", "rh_detrended_30d"])
r_dt, p_dt = stats.pearsonr(valid_dt["price_30d"], valid_dt["rh_detrended_30d"])

fig, (ax1, ax2) = plt.subplots(2, 1, figsize=(14, 8), dpi=150, sharex=True)
fig.suptitle("2021+ Detrended Analysis: Isolating Price Signal", fontsize=14, fontweight="bold")

ax1.plot(dr["date"], dr["price_30d"], color="#2196F3", linewidth=1.2)
ax1.axhline(0, color="red", linewidth=0.5, linestyle="--")
ax1.fill_between(dr["date"], dr["price_30d"], 0,
                  where=dr["price_30d"] < 0, color="red", alpha=0.15)
ax1.set_ylabel("Waha Price ($/MMBtu)\n30-day avg")
ax1.set_title("Waha Price")

ax2.plot(dr["date"], dr["rh_detrended_30d"], color="#FF5722", linewidth=1.2)
ax2.axhline(0, color="gray", linewidth=0.5, linestyle="--", alpha=0.5)
ax2.set_ylabel("Detrended RH (MW)\n30-day avg")
ax2.set_title(f"Detrended Flaring Intensity (r={r_dt:.3f}, p={p_dt:.2e})")
ax2.xaxis.set_major_formatter(mdates.DateFormatter("%Y-%m"))

fig.tight_layout()
fig.savefig(OUT_DIR / "06_detrended_2021.png", bbox_inches="tight")
print(f"Saved {OUT_DIR / '06_detrended_2021.png'}")

# ── Summary statistics ────────────────────────────────────────────────────────
print("\n" + "=" * 70)
print("SUMMARY OF FINDINGS")
print("=" * 70)

# Overall correlation
valid_all = daily.dropna(subset=["price_30d", "rh_30d"])
r_all, p_all = stats.pearsonr(valid_all["price_30d"], valid_all["rh_30d"])
print(f"\nOverall daily correlation (30d smoothed): r={r_all:.3f}, p={p_all:.2e}")
print(f"2021+ daily correlation (raw):            r={r_recent:.3f}, p={p_recent:.6f}")
print(f"2021+ detrended correlation (30d):        r={r_dt:.3f}, p={p_dt:.2e}")
print(f"Monthly RRC correlation:                  r={r_monthly:.3f}, p={p_monthly:.4f}")

neg_rh = daily[daily["price"] < 0]["total_rh_mw"].mean()
pos_rh = daily[daily["price"] >= 2]["total_rh_mw"].mean()
print(f"\nAvg flaring (RH MW) on negative price days: {neg_rh:.1f}")
print(f"Avg flaring (RH MW) on normal (>=$2) days:  {pos_rh:.1f}")
if pos_rh > 0:
    print(f"Ratio: {neg_rh / pos_rh:.2f}x")

print(f"\nPlots saved to {OUT_DIR.resolve()}/")
print("Done.")
