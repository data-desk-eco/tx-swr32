WORKERS ?= 32
export WORKERS

.PHONY: all build preview data db refresh permits permit-details rrc vnf plumes clean map-data map-vendor map-serve

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

permit-details: data/permit_details.csv

data/filings.csv:
	uv run scripts/scrape_permits.py

data/raw_html/.done: data/filings.csv
	uv run scripts/scrape_permit_details.py
	@touch $@

data/permit_details.csv data/permit_properties.csv data/flare_locations.csv data/permit_attachments.csv: data/raw_html/.done
	uv run scripts/parse_permit_details.py

data/plumes_cm.csv data/plumes_imeo.csv:
	uv run scripts/fetch_plumes.py

data/survALLp.shp: data/.rrc_downloaded

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

# --- parquet pre-processing ---

data/vnf.parquet: data/vnf_profiles/.done queries/prep_vnf.sql
	duckdb < queries/prep_vnf.sql

data/gas_disposition.parquet: data/pdq/.done queries/prep_pdq.sql
	duckdb < queries/prep_pdq.sql

# --- database ---

refresh:
	rm -f data/dark_flaring.duckdb
	$(MAKE) db

db: data/dark_flaring.duckdb

data/dark_flaring.duckdb: data/filings.csv data/wells.csv data/operators.csv data/vnf.parquet data/flare_locations.csv data/permit_details.csv data/permit_properties.csv data/excluded_facilities.csv data/plumes_cm.csv data/plumes_imeo.csv data/gas_disposition.parquet data/pdq/.done data/survALLp.shp queries/*.sql
	@rm -f $@
	duckdb $@ < queries/schema.sql
	duckdb $@ < queries/load.sql
	duckdb $@ < queries/transform.sql
	duckdb $@ < queries/views.sql
	@echo "Database ready: $@"

clean:
	rm -f data/dark_flaring.duckdb data/wells.csv data/operators.csv data/.rrc_downloaded data/plumes_cm.csv data/plumes_imeo.csv data/vnf.parquet data/gas_disposition.parquet
	rm -rf docs/.observable/dist

# --- map ---

map-data: data/dark_flaring.duckdb queries/export.sql
	mkdir -p web/data
	duckdb data/dark_flaring.duckdb < queries/export.sql
	@echo "Map data exported to web/data/"

map-vendor:
	scripts/vendor.sh

map-serve:
	python3 -m http.server 8080 -d web
