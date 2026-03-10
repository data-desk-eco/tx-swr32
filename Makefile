WORKERS ?= 32
export WORKERS

.PHONY: all build preview data db refresh permits rrc vnf plumes clean

all: db

# --- notebook ---

build:
	yarn build

preview:
	yarn preview

data:
	gh release download data-v1 -p dark_flaring.duckdb.gz -D data --clobber
	gunzip -f data/dark_flaring.duckdb.gz

# --- scrapers ---

permits: data/filings.csv
rrc: data/wells.csv data/operators.csv
vnf: data/vnf_profiles/.done
plumes: data/plumes_cm.csv data/plumes_imeo.csv

data/filings.csv:
	uv run scripts/scrape_permits.py

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

data/dark_flaring.duckdb: data/filings.csv data/wells.csv data/operators.csv data/vnf_profiles/.done data/flare_locations.csv data/excluded_facilities.csv data/plumes_cm.csv data/plumes_imeo.csv queries/*.sql
	@rm -f $@
	duckdb $@ < queries/schema.sql
	duckdb $@ < queries/load.sql
	duckdb $@ < queries/transform.sql
	duckdb $@ < queries/views.sql
	@echo "Database ready: $@"

clean:
	rm -f data/dark_flaring.duckdb data/wells.csv data/operators.csv data/.rrc_downloaded data/plumes_cm.csv data/plumes_imeo.csv
	rm -rf docs/.observable/dist
