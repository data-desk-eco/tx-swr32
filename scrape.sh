#!/usr/bin/env bash
set -euo pipefail

# Texas RRC SWR 32 Exceptions Scraper
# Downloads all PDFs and metadata from the public query tool at:
# https://webapps.rrc.state.tx.us/swr32/publicquery.xhtml
#
# Usage: ./scrape.sh {metadata|documents|download|combine|all}

BASE="https://webapps.rrc.state.tx.us/swr32/publicquery.xhtml"
DPIMG="https://webapps.rrc.state.tx.us/dpimages/r"
DELAY=0.3
WORKERS=${WORKERS:-8}  # parallel workers for Phase 2 & 3
DATA_DIR="data"
PDF_DIR="$DATA_DIR/pdfs"
COOKIES="$DATA_DIR/.cookies"
METADATA_CSV="$DATA_DIR/filings.csv"
DOCS_CSV="$DATA_DIR/docs.csv"
STATE_FILE="$DATA_DIR/.state"
TODAY=$(date +%m/%d/%Y)
TODAY_URL=$(printf '%s' "$TODAY" | sed 's|/|%2F|g')
SEARCH_FROM="01/01/2019"
SEARCH_FROM_URL="01%2F01%2F2019"

mkdir -p "$PDF_DIR"

# --- helpers ---

log() { printf '%s  %s\n' "$(date +%H:%M:%S)" "$*" >&2; }

# Retry a curl command up to 3 times with backoff
curl_retry() {
  local attempt=0
  while [ $attempt -lt 3 ]; do
    if curl --connect-timeout 15 --max-time 60 "$@" ; then
      return 0
    fi
    attempt=$(( attempt + 1 ))
    log "RETRY: curl failed (attempt $attempt/3), waiting ${attempt}s..."
    sleep "$attempt"
  done
  log "ERROR: curl failed after 3 attempts"
  return 1
}

# Check if a response looks valid (not empty, no ViewExpiredException)
response_ok() {
  local f="$1"
  [ -f "$f" ] && [ -s "$f" ] || return 1
  ! grep -q 'ViewExpiredException' "$f" 2>/dev/null
}

get_viewstate() {
  local f="$1" vs=""
  vs=$(grep -o 'name="javax.faces.ViewState"[^/]*' "$f" 2>/dev/null \
    | head -1 | sed 's/.*value="//;s/".*//') || true
  if [ -z "$vs" ]; then
    vs=$(grep -o 'javax.faces.ViewState:0">.*]]' "$f" 2>/dev/null \
      | head -1 | sed 's/.*CDATA\[//;s/]].*//' ) || true
  fi
  echo "$vs"
}

init_session() {
  rm -f "$COOKIES"
  curl_retry -s -c "$COOKIES" "$BASE" -o "$DATA_DIR/.page.html"
  local vs=$(get_viewstate "$DATA_DIR/.page.html")
  if [ -z "$vs" ]; then
    log "WARN: init_session got empty ViewState, retrying..."
    sleep 2
    rm -f "$COOKIES"
    curl_retry -s -c "$COOKIES" "$BASE" -o "$DATA_DIR/.page.html"
    vs=$(get_viewstate "$DATA_DIR/.page.html")
  fi
  echo "$vs"
}

do_search() {
  local vs="$1" filing="${2:-}" date_from="${3:-}" date_to="${4:-}"
  local out="$DATA_DIR/.search.xml"
  local data="javax.faces.partial.ajax=true"
  data="$data&javax.faces.source=pbqueryForm%3AsearchExceptions"
  data="$data&javax.faces.partial.execute=%40all"
  data="$data&javax.faces.partial.render=pbqueryForm%3ApQueryTable"
  data="$data&pbqueryForm%3AsearchExceptions=pbqueryForm%3AsearchExceptions"
  data="$data&pbqueryForm=pbqueryForm"
  data="$data&javax.faces.ViewState=$(printf '%s' "$vs" | sed 's/:/%3A/g')"
  data="$data&pbqueryForm%3AfilingTypeList_focus="
  data="$data&pbqueryForm%3AfilingTypeList_input="
  data="$data&pbqueryForm%3ApermanentException_focus="
  data="$data&pbqueryForm%3ApermanentException_input="
  data="$data&pbqueryForm%3Aswr32h8_focus="
  data="$data&pbqueryForm%3Aswr32h8_input="
  data="$data&pbqueryForm%3ApropertyTypeList_focus="
  data="$data&pbqueryForm%3ApropertyTypeList_input="

  if [ -n "$filing" ]; then
    data="$data&pbqueryForm%3AfilingNumber_input=$filing"
    data="$data&pbqueryForm%3AfilingNumber_hinput=$filing"
  fi
  if [ -n "$date_from" ]; then
    local df_url=$(printf '%s' "$date_from" | sed 's|/|%2F|g')
    local dt_url=$(printf '%s' "$date_to" | sed 's|/|%2F|g')
    data="$data&pbqueryForm%3AsubmittalDateFrom_input=$df_url"
    data="$data&pbqueryForm%3AsubmittalDateTo_input=$dt_url"
  fi

  curl_retry -s -b "$COOKIES" -c "$COOKIES" \
    -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
    -H 'Faces-Request: partial/ajax' \
    -H 'X-Requested-With: XMLHttpRequest' \
    -d "$data" "$BASE" -o "$out"

  echo "$out"
}

do_paginate() {
  local vs="$1" first="$2"
  local out="$DATA_DIR/.paginate.xml"
  local vs_encoded=$(printf '%s' "$vs" | sed 's/:/%3A/g')

  curl_retry -s -b "$COOKIES" -c "$COOKIES" \
    -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
    -H 'Faces-Request: partial/ajax' \
    -H 'X-Requested-With: XMLHttpRequest' \
    -d "javax.faces.partial.ajax=true&javax.faces.source=pbqueryForm%3ApQueryTable&javax.faces.partial.execute=pbqueryForm%3ApQueryTable&javax.faces.partial.render=pbqueryForm%3ApQueryTable&javax.faces.behavior.event=page&javax.faces.partial.event=page&pbqueryForm%3ApQueryTable_pagination=true&pbqueryForm%3ApQueryTable_first=${first}&pbqueryForm%3ApQueryTable_rows=10&pbqueryForm%3ApQueryTable_encodeFeature=true&pbqueryForm%3ApQueryTable_rppDD=10&pbqueryForm=pbqueryForm&pbqueryForm%3AfilingTypeList_focus=&pbqueryForm%3AfilingTypeList_input=&pbqueryForm%3ApermanentException_focus=&pbqueryForm%3ApermanentException_input=&pbqueryForm%3Aswr32h8_focus=&pbqueryForm%3Aswr32h8_input=&pbqueryForm%3ApropertyTypeList_focus=&pbqueryForm%3ApropertyTypeList_input=&pbqueryForm%3AsubmittalDateFrom_input=${SEARCH_FROM_URL}&pbqueryForm%3AsubmittalDateTo_input=${TODAY_URL}&pbqueryForm%3ApQueryTable%3Aj_idt152%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt154%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt156%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt158%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt160%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt162%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt164%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt166%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt168%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt170%3Afilter=&pbqueryForm%3ApQueryTable%3Aj_idt172%3Afilter=&pbqueryForm%3ApQueryTable_selection=&pbqueryForm%3ApQueryTable_resizableColumnState=&javax.faces.ViewState=${vs_encoded}" \
    "$BASE" -o "$out"

  echo "$out"
}

view_detail() {
  local vs="$1" row="$2"
  local out="$DATA_DIR/.detail.html"
  local vs_encoded=$(printf '%s' "$vs" | sed 's/:/%3A/g')

  curl_retry -s -L -b "$COOKIES" -c "$COOKIES" \
    -d "pbqueryForm=pbqueryForm&javax.faces.ViewState=${vs_encoded}&pbqueryForm%3ApQueryTable%3A${row}%3Aj_idt150=pbqueryForm%3ApQueryTable%3A${row}%3Aj_idt150" \
    "$BASE" -o "$out"

  echo "$out"
}

parse_table_rows() {
  local f="$1"
  # Split tags onto separate lines, extract gridcell contents, strip HTML
  sed 's/></>\
</g' "$f" \
    | grep 'role="gridcell"' \
    | sed 's/.*role="gridcell"[^>]*>//;s/<\/td.*//;s/<[^>]*>//g' \
    | sed 's/^[[:space:]]*//;s/[[:space:]]*$//' \
    > "$DATA_DIR/.cells.tmp"

  # 12 cells per row; skip cell 0 (actions button), output cells 1-11
  awk -v OFS='\t' '
    { cells[NR-1] = $0 }
    END {
      for (r = 0; r * 12 + 11 < NR; r++) {
        base = r * 12
        printf "%s", cells[base+1]
        for (c = 2; c <= 11; c++) printf "\t%s", cells[base+c]
        print ""
      }
    }
  ' "$DATA_DIR/.cells.tmp"
}

parse_detail_docs() {
  local f="$1"
  # Pair up doc IDs, filenames, and file types from the attachment table
  local ids_file="$DATA_DIR/.doc_ids.tmp"
  local names_file="$DATA_DIR/.doc_names.tmp"
  local types_file="$DATA_DIR/.doc_types.tmp"

  grep -o 'dpimages[^0-9]*r[^0-9]*[0-9][0-9]*' "$f" \
    | grep -o '[0-9][0-9]*$' > "$ids_file"

  sed -n '/attachmentTable/,/<\/table>/p' "$f" \
    | grep -o 'text-align: left; ">[^<]*' \
    | sed 's/text-align: left; ">//' > "$names_file"

  sed -n '/attachmentTable/,/<\/table>/p' "$f" \
    | grep -o 'text-align: center; width:30%">[^<]*' \
    | sed 's/text-align: center; width:30%">//' \
    | sed '/^$/d' > "$types_file"

  paste "$ids_file" "$names_file" "$types_file" 2>/dev/null || true
}

get_total_records() {
  grep -o 'out of [0-9]* records' "$1" | head -1 | sed 's/out of //;s/ records//'
}

save_state() { echo "$1" > "$STATE_FILE"; }
load_state() { cat "$STATE_FILE" 2>/dev/null || echo ""; }

# --- Phase 1: Collect metadata from search results ---

phase_metadata() {
  log "Phase 1: Collecting metadata from search results"

  if [ ! -f "$METADATA_CSV" ]; then
    printf 'excep_seq\tsubmittal_dt\tfiling_no\tstatus\tfiling_type\toperator_no\toperator_name\tproperty\teffective_dt\texpiration_dt\tfv_district\n' > "$METADATA_CSV"
  fi

  local state=$(load_state)
  local start_page=0
  case "$state" in meta:*) start_page="${state#meta:}" ; log "Resuming from page $start_page" ;; esac

  log "Initializing session..."
  local vs=$(init_session)

  log "Searching ($SEARCH_FROM to $TODAY)..."
  local search_result=$(do_search "$vs" "" "$SEARCH_FROM" "$TODAY")
  vs=$(get_viewstate "$search_result")

  local total=$(get_total_records "$search_result")
  log "Found $total records"

  local total_pages=$(( (total + 9) / 10 ))

  if [ "$start_page" -eq 0 ]; then
    parse_table_rows "$search_result" >> "$METADATA_CSV"
    save_state "meta:1"
    start_page=1
    log "Page 1/$total_pages"
    sleep "$DELAY"
  fi

  local page=0 first=0 fail_count=0
  for page in $(seq "$start_page" $(( total_pages - 1 )) ); do
    first=$(( page * 10 ))
    local page_result=$(do_paginate "$vs" "$first")

    # Validate response; on failure, reinit session and re-search
    if ! response_ok "$page_result"; then
      log "WARN: Bad response on page $((page + 1)), reinitializing session..."
      vs=$(init_session)
      do_search "$vs" "" "$SEARCH_FROM" "$TODAY" > /dev/null
      vs=$(get_viewstate "$DATA_DIR/.search.xml")
      page_result=$(do_paginate "$vs" "$first")
      if ! response_ok "$page_result"; then
        fail_count=$(( fail_count + 1 ))
        if [ $fail_count -ge 5 ]; then
          log "ERROR: Too many consecutive failures, stopping"
          break
        fi
        log "WARN: Still failing, skipping page $((page + 1))"
        sleep 5
        continue
      fi
      fail_count=0
    else
      fail_count=0
    fi

    local new_vs=$(get_viewstate "$page_result")
    [ -n "$new_vs" ] && vs="$new_vs"
    parse_table_rows "$page_result" >> "$METADATA_CSV"
    save_state "meta:$((page + 1))"
    log "Page $((page + 1))/$total_pages"
    sleep "$DELAY"
  done

  # Deduplicate
  local tmp="$DATA_DIR/.dedup.tmp"
  head -1 "$METADATA_CSV" > "$tmp"
  tail -n +2 "$METADATA_CSV" | sort -t'	' -k3,3 -u >> "$tmp"
  mv "$tmp" "$METADATA_CSV"

  local count=$(( $(wc -l < "$METADATA_CSV") - 1 ))
  log "Phase 1 complete: $count unique filings"
  save_state "meta:done"
}

# --- Phase 2: Get document IDs from detail pages (parallel workers) ---

# Single-worker loop: processes filings from a shard file
_documents_worker() {
  local worker_id="$1" shard_file="$2" done_file="$3" out_file="$4"
  local wdir="$DATA_DIR/.w${worker_id}"
  mkdir -p "$wdir"

  # Worker gets its own cookie jar and temp files
  local w_cookies="$wdir/cookies" w_page="$wdir/page.html"
  local w_search="$wdir/search.xml" w_detail="$wdir/detail.html"
  local w_ids="$wdir/ids.tmp" w_names="$wdir/names.tmp" w_types="$wdir/types.tmp"

  # Init worker session
  rm -f "$w_cookies"
  curl_retry -s -c "$w_cookies" "$BASE" -o "$w_page"
  local vs=$(get_viewstate "$w_page")
  local i=0

  while IFS= read -r filing_no; do
    [ -z "$filing_no" ] && continue
    i=$(( i + 1 ))

    # Skip if already done (lockfile claim)
    local lockfile="$DATA_DIR/.lock_${filing_no}"
    if [ -f "$lockfile" ] || ! mkdir "$lockfile" 2>/dev/null; then
      continue
    fi

    # Search by filing number
    local data="javax.faces.partial.ajax=true"
    data="$data&javax.faces.source=pbqueryForm%3AsearchExceptions"
    data="$data&javax.faces.partial.execute=%40all"
    data="$data&javax.faces.partial.render=pbqueryForm%3ApQueryTable"
    data="$data&pbqueryForm%3AsearchExceptions=pbqueryForm%3AsearchExceptions"
    data="$data&pbqueryForm=pbqueryForm"
    data="$data&javax.faces.ViewState=$(printf '%s' "$vs" | sed 's/:/%3A/g')"
    data="$data&pbqueryForm%3AfilingNumber_input=$filing_no"
    data="$data&pbqueryForm%3AfilingNumber_hinput=$filing_no"
    data="$data&pbqueryForm%3AfilingTypeList_focus=&pbqueryForm%3AfilingTypeList_input="
    data="$data&pbqueryForm%3ApermanentException_focus=&pbqueryForm%3ApermanentException_input="
    data="$data&pbqueryForm%3Aswr32h8_focus=&pbqueryForm%3Aswr32h8_input="
    data="$data&pbqueryForm%3ApropertyTypeList_focus=&pbqueryForm%3ApropertyTypeList_input="

    curl_retry -s -b "$w_cookies" -c "$w_cookies" \
      -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
      -H 'Faces-Request: partial/ajax' \
      -H 'X-Requested-With: XMLHttpRequest' \
      -d "$data" "$BASE" -o "$w_search"

    if ! response_ok "$w_search"; then
      rm -f "$w_cookies"
      curl_retry -s -c "$w_cookies" "$BASE" -o "$w_page"
      vs=$(get_viewstate "$w_page")
      curl_retry -s -b "$w_cookies" -c "$w_cookies" \
        -H 'Content-Type: application/x-www-form-urlencoded; charset=UTF-8' \
        -H 'Faces-Request: partial/ajax' \
        -H 'X-Requested-With: XMLHttpRequest' \
        -d "$data" "$BASE" -o "$w_search"
    fi
    vs=$(get_viewstate "$w_search")
    sleep "$DELAY"

    local count=$(get_total_records "$w_search" 2>/dev/null) || count=0
    if [ -z "$count" ] || [ "$count" -eq 0 ] 2>/dev/null; then
      printf '%s\t\t\t\n' "$filing_no" >> "$out_file"
      rm -f "$w_cookies"
      curl_retry -s -c "$w_cookies" "$BASE" -o "$w_page"
      vs=$(get_viewstate "$w_page")
      continue
    fi

    # View detail page
    local vs_enc=$(printf '%s' "$vs" | sed 's/:/%3A/g')
    curl_retry -s -L -b "$w_cookies" -c "$w_cookies" \
      -d "pbqueryForm=pbqueryForm&javax.faces.ViewState=${vs_enc}&pbqueryForm%3ApQueryTable%3A0%3Aj_idt150=pbqueryForm%3ApQueryTable%3A0%3Aj_idt150" \
      "$BASE" -o "$w_detail"

    if ! response_ok "$w_detail"; then
      rm -f "$w_cookies"
      curl_retry -s -c "$w_cookies" "$BASE" -o "$w_page"
      vs=$(get_viewstate "$w_page")
      printf '%s\t\t\t\n' "$filing_no" >> "$out_file"
      continue
    fi
    sleep "$DELAY"

    # Extract documents (worker-local temp files)
    grep -o 'dpimages[^0-9]*r[^0-9]*[0-9][0-9]*' "$w_detail" \
      | grep -o '[0-9][0-9]*$' > "$w_ids"
    sed -n '/attachmentTable/,/<\/table>/p' "$w_detail" \
      | grep -o 'text-align: left; ">[^<]*' \
      | sed 's/text-align: left; ">//' > "$w_names"
    sed -n '/attachmentTable/,/<\/table>/p' "$w_detail" \
      | grep -o 'text-align: center; width:30%">[^<]*' \
      | sed 's/text-align: center; width:30%">//' \
      | sed '/^$/d' > "$w_types"

    local doc_info=$(paste "$w_ids" "$w_names" "$w_types" 2>/dev/null) || true
    if [ -n "$doc_info" ]; then
      echo "$doc_info" | while IFS='	' read -r doc_id filename file_type; do
        printf '%s\t%s\t%s\t%s\n' "$filing_no" "$doc_id" "$filename" "$file_type"
      done >> "$out_file"
    else
      printf '%s\t\t\t\n' "$filing_no" >> "$out_file"
    fi

    # Navigate back
    curl_retry -s -b "$w_cookies" -c "$w_cookies" "$BASE" -o "$w_page"
    vs=$(get_viewstate "$w_page")

    if [ $(( i % 50 )) -eq 0 ]; then
      log "Worker $worker_id: $i processed"
    fi
    if [ $(( i % 200 )) -eq 0 ]; then
      rm -f "$w_cookies"
      curl_retry -s -c "$w_cookies" "$BASE" -o "$w_page"
      vs=$(get_viewstate "$w_page")
    fi
  done < "$shard_file"

  rm -rf "$wdir"
  log "Worker $worker_id finished ($i processed)"
}

phase_documents() {
  log "Phase 2: Collecting document IDs ($WORKERS workers)"

  [ -f "$METADATA_CSV" ] || { log "ERROR: Run 'metadata' first"; exit 1; }

  if [ ! -f "$DOCS_CSV" ]; then
    printf 'filing_no\tdoc_id\tfilename\tfile_type\n' > "$DOCS_CSV"
  fi

  # Get already-done filings
  local done_file="$DATA_DIR/.done_filings.tmp"
  tail -n +2 "$DOCS_CSV" 2>/dev/null | cut -f1 | sort -u > "$done_file"

  # Create lockfiles for already-done filings
  while IFS= read -r fn; do
    [ -n "$fn" ] && mkdir -p "$DATA_DIR/.lock_${fn}" 2>/dev/null
  done < "$done_file"

  # Get remaining filings
  local filings_file="$DATA_DIR/.all_filings.tmp"
  tail -n +2 "$METADATA_CSV" | cut -f3 | sort -u > "$filings_file"
  local total=$(wc -l < "$filings_file" | tr -d ' ')
  local done_count=$(wc -l < "$done_file" | tr -d ' ')
  log "$total total filings, $done_count already done"

  # Split filings into shards for parallel workers
  local lines_per_shard=$(( (total + WORKERS - 1) / WORKERS ))
  split -l "$lines_per_shard" "$filings_file" "$DATA_DIR/.shard_"

  # Launch workers
  local pids="" w=0
  for shard in "$DATA_DIR"/.shard_*; do
    local out="$DATA_DIR/.docs_w${w}.tsv"
    _documents_worker "$w" "$shard" "$done_file" "$out" &
    pids="$pids $!"
    w=$(( w + 1 ))
  done

  log "Launched $w workers, waiting..."
  local failed=0
  for pid in $pids; do
    wait "$pid" || failed=$(( failed + 1 ))
  done
  [ $failed -gt 0 ] && log "WARN: $failed worker(s) had errors"

  # Merge worker outputs into docs.csv
  for f in "$DATA_DIR"/.docs_w*.tsv; do
    [ -f "$f" ] && cat "$f" >> "$DOCS_CSV"
  done

  # Clean up
  rm -f "$DATA_DIR"/.shard_* "$DATA_DIR"/.docs_w*.tsv
  rm -rf "$DATA_DIR"/.lock_*

  local count=$(( $(wc -l < "$DOCS_CSV") - 1 ))
  log "Phase 2 complete: $count document entries"
}

# --- Phase 3: Download all documents (parallel) ---

phase_download() {
  log "Phase 3: Downloading documents ($WORKERS workers)"

  [ -f "$DOCS_CSV" ] || { log "ERROR: Run 'documents' first"; exit 1; }

  # Build download list (skip already-downloaded)
  local dl_list="$DATA_DIR/.downloads.tmp"
  : > "$dl_list"
  tail -n +2 "$DOCS_CSV" | while IFS='	' read -r filing_no doc_id filename file_type; do
    [ -z "$doc_id" ] && continue
    local ext="${filename##*.}"
    [ -z "$ext" ] || [ "$ext" = "$filename" ] && ext="pdf"
    local outfile="$PDF_DIR/${filing_no}_${doc_id}.${ext}"
    [ -f "$outfile" ] && [ -s "$outfile" ] && continue
    printf '%s\t%s\n' "$doc_id" "$outfile" >> "$dl_list"
  done

  local total=$(wc -l < "$dl_list" | tr -d ' ')
  log "$total files to download"
  [ "$total" -eq 0 ] && { log "Nothing to download"; return; }

  # Use xargs for simple parallelism
  cat "$dl_list" | while IFS='	' read -r doc_id outfile; do
    echo "$DPIMG/$doc_id" "$outfile"
  done | xargs -P "$WORKERS" -L 1 sh -c '
    curl --connect-timeout 15 --max-time 120 -s -L -o "$2" "$1" 2>/dev/null
    if [ -f "$2" ] && grep -q "<html" "$2" 2>/dev/null; then rm -f "$2"; fi
  ' _

  rm -f "$dl_list"
  log "Phase 3 complete"
}

# --- Phase 4: Build final combined CSV ---

phase_combine() {
  log "Building combined CSV"

  local combined="$DATA_DIR/swr32_exceptions.csv"

  # Use awk to join metadata with documents (handles empty TSV fields correctly)
  awk -F'\t' -v OFS='\t' -v docs_file="$DOCS_CSV" '
    BEGIN {
      # Load docs into associative arrays keyed by filing_no
      while ((getline line < docs_file) > 0) {
        n = split(line, f, "\t")
        fn = f[1]
        if (fn == "filing_no" || fn == "") continue
        did = f[2]
        if (did == "") continue
        fname = f[3]
        # Determine extension
        ext = fname
        sub(/.*\./, "", ext)
        if (ext == fname || ext == "") ext = "pdf"
        if (doc_ids[fn] != "") {
          doc_ids[fn] = doc_ids[fn] ";"
          pdf_files[fn] = pdf_files[fn] ";"
        }
        doc_ids[fn] = doc_ids[fn] did
        pdf_files[fn] = pdf_files[fn] fn "_" did "." ext
      }
    }
    NR == 1 {
      print $0, "doc_ids", "pdf_files"
      next
    }
    {
      fn = $3
      print $0, doc_ids[fn], pdf_files[fn]
    }
  ' "$METADATA_CSV" > "$combined"

  local count=$(( $(wc -l < "$combined") - 1 ))
  log "Combined CSV: $combined ($count records)"
}

# --- main ---

case "${1:-}" in
  metadata)  phase_metadata ;;
  documents) phase_documents ;;
  download)  phase_download ;;
  combine)   phase_combine ;;
  all)       phase_metadata; phase_documents; phase_download; phase_combine ;;
  *)
    echo "Usage: $0 {metadata|documents|download|combine|all}" >&2
    echo "  metadata   Paginate search results, collect filing metadata" >&2
    echo "  documents  Visit each filing detail page for document IDs" >&2
    echo "  download   Download all documents" >&2
    echo "  combine    Build final CSV joining metadata + documents" >&2
    echo "  all        Run all phases" >&2
    exit 1
    ;;
esac
