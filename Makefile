WORKERS ?= 8
export WORKERS

.PHONY: all metadata documents download combine clean

all: combine

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

clean:
	rm -rf data
