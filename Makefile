WORKERS ?= 32
export WORKERS

.PHONY: all permits rrc vnf db metadata documents download combine clean

all: db

# --- SWR 32 scraper (existing) ---

metadata: data/filings.csv
documents: data/docs.csv
download: data/pdfs/.done
combine: data/swr32_exceptions.csv

data/filings.csv:
	./scrape.sh metadata

data/docs.csv: data/filings.csv
	./scrape.sh documents

data/pdfs/.done: data/docs.csv
	./scrape.sh download
	@touch $@

data/swr32_exceptions.csv: data/filings.csv data/docs.csv
	./scrape.sh combine

# --- dark flaring pipeline ---

permits: data/filings.csv
rrc: data/wells.csv data/operators.csv
vnf: data/vnf_profiles/.done

data/dbf900.ebc.gz data/orf850.ebc.gz:
	uv run scripts/download_rrc.py data

data/wells.csv data/operators.csv: data/dbf900.ebc.gz data/orf850.ebc.gz
	uv run scripts/parse_rrc.py data

data/vnf_profiles/.done:
	uv run scripts/fetch_vnf.py
	@touch $@

# --- database ---

db: data/dark_flaring.duckdb

data/dark_flaring.duckdb: data/filings.csv data/wells.csv data/operators.csv data/vnf_profiles/.done
	@rm -f $@
	duckdb $@ < queries/schema.sql
	duckdb $@ < queries/load.sql
	duckdb $@ < queries/dark_flaring.sql
	@echo "Database ready: $@"

clean:
	rm -f data/dark_flaring.duckdb data/wells.csv data/operators.csv
