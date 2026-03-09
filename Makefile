WORKERS ?= 32
export WORKERS

.PHONY: all build preview data db refresh permits rrc vnf plumes metadata documents download combine clean

all: db

# --- notebook ---

build:
	yarn build

preview:
	yarn preview

data:
	gh release download data-v1 -p dark_flaring.duckdb.gz -D data --clobber
	gunzip -f data/dark_flaring.duckdb.gz

# --- SWR 32 scraper ---

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
plumes: data/plumes_cm.csv data/plumes_imeo.csv

data/plumes_cm.csv data/plumes_imeo.csv:
	uv run scripts/fetch_plumes.py

data/.rrc_downloaded:
	uv run scripts/download_rrc.py data
	@touch $@

data/wells.csv data/operators.csv: data/.rrc_downloaded
	uv run scripts/parse_rrc.py data

data/vnf_profiles/.done:
	uv run scripts/fetch_vnf.py
	@touch $@

# --- database ---

refresh:
	rm -f data/dark_flaring.duckdb
	$(MAKE) db

db: data/dark_flaring.duckdb

data/dark_flaring.duckdb: data/filings.csv data/wells.csv data/operators.csv data/vnf_profiles/.done data/flare_locations.csv data/plumes_cm.csv data/plumes_imeo.csv queries/*.sql
	@rm -f $@
	duckdb $@ < queries/schema.sql
	duckdb $@ < queries/plume_schema.sql
	duckdb $@ < queries/load.sql
	duckdb $@ < queries/load_plumes.sql
	duckdb $@ < queries/dark_flaring.sql
	duckdb $@ < queries/plume_analysis.sql
	@echo "Database ready: $@"

clean:
	rm -f data/dark_flaring.duckdb data/wells.csv data/operators.csv data/.rrc_downloaded data/plumes_cm.csv data/plumes_imeo.csv
	rm -rf docs/.observable/dist
