WORKERS ?= 32
export WORKERS

.PHONY: all db refresh export vendor serve permits permit-details wells vnf plumes r3 clean help

all: db

# --- scrapers ---

permits: data/filings.csv
wells: data/wells.csv data/operators.csv
vnf: data/vnf_profiles/.done
plumes: data/plumes_cm.csv data/plumes_imeo.csv
r3: data/r3_facilities.csv
permit-details: data/permit_details.csv

data/filings.csv:
	uv run scripts/scrape_permits.py

data/raw_html/.done: data/filings.csv
	uv run scripts/scrape_permit_details.py
	@touch $@

data/permit_details.csv data/permit_properties.csv data/flare_locations.csv data/permit_attachments.csv: data/raw_html/.done
	uv run scripts/parse_permit_details.py

data/r3_facilities.csv:
	uv run scripts/fetch_r3.py

data/plumes_cm.csv data/plumes_imeo.csv:
	uv run scripts/fetch_plumes.py

data/.rrc_downloaded:
	uv run scripts/download_rrc.py data
	@touch $@

data/wells.csv data/operators.csv: data/.rrc_downloaded
	uv run scripts/parse_rrc.py data

data/pdq/.done: data/.rrc_downloaded
	mkdir -p data/pdq
	unzip -o data/PDQ_DSV.zip -d data/pdq
	@touch $@

data/vnf_profiles/.done:
	uv run scripts/fetch_vnf.py
	@touch $@

# --- database ---

refresh:
	rm -f data/data.duckdb
	$(MAKE) db

db: data/data.duckdb

data/data.duckdb: data/filings.csv data/wells.csv data/operators.csv data/vnf_profiles/.done data/flare_locations.csv data/permit_details.csv data/permit_properties.csv data/r3_facilities.csv data/plumes_cm.csv data/plumes_imeo.csv data/pdq/.done queries/*.sql
	@rm -f $@
	duckdb $@ < queries/load.sql
	duckdb $@ < queries/rrc.sql
	duckdb $@ < queries/export.sql
	@echo "Database ready: $@"

# --- web app ---

export: data/data.duckdb queries/export.sql
	mkdir -p web/data
	duckdb data/data.duckdb < queries/export.sql

vendor:
	scripts/vendor.sh

serve:
	uv run python -m http.server 8080 -d web

clean:
	rm -f data/data.duckdb data/wells.csv data/operators.csv data/.rrc_downloaded data/r3_facilities.csv data/plumes_cm.csv data/plumes_imeo.csv

help:
	@echo "gaslight — dark flaring analysis for the Permian Basin"
	@echo ""
	@echo "  make db              Full pipeline (load → rrc → export)"
	@echo "  make refresh         Rebuild database from scratch"
	@echo "  make export          Re-export parquets for web app"
	@echo "  make vendor          Download vendored JS dependencies"
	@echo "  make serve           Dev server on :8080"
	@echo ""
	@echo "  make permits         Scrape SWR 32 permit metadata"
	@echo "  make permit-details  Scrape + parse permit detail pages"
	@echo "  make wells           Download + parse RRC well/operator data"
	@echo "  make vnf             Fetch VNF profiles from EOG"
	@echo "  make plumes          Fetch Carbon Mapper + IMEO plumes"
	@echo "  make r3              Fetch RRC R-3 gas processing facilities"
	@echo ""
	@echo "  make clean           Remove derived data"
	@echo "  make help            This message"
