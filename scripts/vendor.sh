#!/usr/bin/env bash
set -euo pipefail

VENDOR="web/vendor"
rm -rf "$VENDOR"
mkdir -p "$VENDOR/duckdb" "$VENDOR/fonts"

UA='Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36'

echo "maplibre-gl@5.1.0 ..."
curl -sLo "$VENDOR/maplibre-gl.js"  "https://unpkg.com/maplibre-gl@5.1.0/dist/maplibre-gl.js"
curl -sLo "$VENDOR/maplibre-gl.css" "https://unpkg.com/maplibre-gl@5.1.0/dist/maplibre-gl.css"

echo "duckdb-wasm@1.29.0 ..."
JSDELIVR="https://cdn.jsdelivr.net/npm"
curl -sLo "$VENDOR/duckdb/duckdb-browser.mjs"  "$JSDELIVR/@duckdb/duckdb-wasm@1.29.0/+esm"
curl -sLo "$VENDOR/duckdb/apache-arrow.mjs"     "$JSDELIVR/apache-arrow@17.0.0/+esm"
curl -sLo "$VENDOR/duckdb/tslib.mjs"            "$JSDELIVR/tslib@2.6.3/+esm"
curl -sLo "$VENDOR/duckdb/flatbuffers.mjs"      "$JSDELIVR/flatbuffers@24.3.25/+esm"

UNPKG="https://unpkg.com/@duckdb/duckdb-wasm@1.29.0/dist"
curl -sLo "$VENDOR/duckdb/duckdb-eh.wasm"              "$UNPKG/duckdb-eh.wasm"
curl -sLo "$VENDOR/duckdb/duckdb-browser-eh.worker.js" "$UNPKG/duckdb-browser-eh.worker.js"

echo "inter font ..."
FONTS_CSS=$(curl -sH "User-Agent: $UA" \
  "https://fonts.googleapis.com/css2?family=Inter:ital,opsz,wght@0,14..32,400..500;1,14..32,400..500&display=swap")

python3 -c "
import re, urllib.request, sys

css = sys.stdin.read()
urls = re.findall(r'url\((https://[^)]+\.woff2)\)', css)
local_css = css
for i, url in enumerate(urls):
    fname = f'inter-latin-{i}.woff2'
    urllib.request.urlretrieve(url, f'$VENDOR/fonts/{fname}')
    local_css = local_css.replace(url, fname, 1)

with open('$VENDOR/fonts/inter.css', 'w') as f:
    f.write(local_css)
print(f'  {len(urls)} font files')
" <<< "$FONTS_CSS"

echo ""
echo "Vendored to $VENDOR/:"
du -sh "$VENDOR" | cut -f1
