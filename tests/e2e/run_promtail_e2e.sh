#!/usr/bin/env bash
# Real end-to-end test: a real Promtail container (Grafana's actual log shipper,
# not a synthetic stand-in) tails a log file, parses JSON, promotes the `level`
# field to a Loki label, and pushes to Pierre's real `/loki/api/v1/push` endpoint.
# A synthetic generator writes the log lines, but everything downstream of the
# file is the real collector -> real Pierre binary -> real query API, end to end.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIERRE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"
# Docker Desktop's file sharing on this machine doesn't cover /tmp (bind mounts
# from /tmp silently show up as empty in the container) — must live under a path
# Docker Desktop actually shares, e.g. anywhere under the project tree.
mkdir -p "$SCRIPT_DIR/.tmp"
WORK_DIR="$(mktemp -d "$SCRIPT_DIR/.tmp/run.XXXXXX")"
MARKER="e2e$(date +%s)$$"
LINE_COUNT=50
GEN_DURATION=5

NATIVE_PORT=14400
LOKI_PORT=13200
QUERY_PORT=13201
CONTAINER_NAME="pierre-e2e-promtail-$$"

cleanup() {
  echo "--- cleanup ---"
  docker rm -f "$CONTAINER_NAME" >/dev/null 2>&1 || true
  [[ -n "${PIERRE_PID:-}" ]] && kill "$PIERRE_PID" >/dev/null 2>&1 || true
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

echo "--- work dir: $WORK_DIR ---"
mkdir -p "$WORK_DIR/logs" "$WORK_DIR/promtail" "$WORK_DIR/pierre-data"

# --- Pierre config + binary ---
cat > "$WORK_DIR/pierre.toml" <<EOF
data_dir = "$WORK_DIR/pierre-data"
native_listen_addr = "127.0.0.1:$NATIVE_PORT"
loki_listen_addr = "127.0.0.1:$LOKI_PORT"
query_listen_addr = "127.0.0.1:$QUERY_PORT"
fields = ["level", "job"]
textindex_bucket_duration_secs = 3600
textindex_flush_interval_secs = 1

[[rollup]]
field = "level"
kind = "exact"
EOF

echo "--- building pierre (release) ---"
(cd "$PIERRE_ROOT" && cargo build --release >/dev/null 2>&1)

echo "--- starting pierre ---"
RUST_LOG=info "$PIERRE_ROOT/target/release/pierre" "$WORK_DIR/pierre.toml" > "$WORK_DIR/pierre.log" 2>&1 &
PIERRE_PID=$!

for i in $(seq 1 30); do
  if curl -sf "http://127.0.0.1:$QUERY_PORT/query/logs?start=0&end=1" >/dev/null 2>&1; then
    echo "pierre ready after ${i}00ms"
    break
  fi
  sleep 0.1
done

# --- Promtail config, rendered from template ---
sed "s/__LOKI_PORT__/$LOKI_PORT/" "$SCRIPT_DIR/promtail_config.yaml.template" > "$WORK_DIR/promtail/config.yaml"

# Generate the full log file *before* starting Promtail. Docker Desktop for Mac's
# bind-mount file sharing (gRPC-FUSE/VirtioFS) does not reliably forward inotify
# events from host writes into the container (well-known limitation — FSEvents-to-
# inotify translation is incomplete), so a live tail-while-appending setup misses
# lines non-deterministically. Writing the file first means Promtail's *initial*
# read (open + full scan) does the work instead of relying on live-append events —
# still the real collector discovering, parsing, and shipping a real file end to end.
echo "--- generating $LINE_COUNT log lines with marker $MARKER ---"
python3 "$SCRIPT_DIR/generate_logs.py" "$WORK_DIR/logs/synthetic.log" "$MARKER" "$LINE_COUNT" 0

echo "--- starting promtail container ---"
docker run -d --name "$CONTAINER_NAME" \
  --add-host=host.docker.internal:host-gateway \
  -p 19080:9080 \
  -v "$WORK_DIR/logs:/var/log/synthetic:ro" \
  -v "$WORK_DIR/promtail:/etc/promtail:ro" \
  grafana/promtail:latest -config.file=/etc/promtail/config.yaml >/dev/null

echo "--- waiting for promtail to discover, parse, and ship the file ---"
sleep 10

echo "--- promtail metrics (read/sent/dropped counters) ---"
curl -s http://127.0.0.1:19080/metrics 2>&1 | grep -E "promtail_(read_lines|sent_entries|dropped_entries|files_active)" || echo "metrics endpoint unreachable"

echo "--- promtail container logs (tail) ---"
docker logs "$CONTAINER_NAME" 2>&1 | tail -20

echo "--- raw dump: anything ingested at all? ---"
curl -s "http://127.0.0.1:$QUERY_PORT/query/logs?start=0&end=9223372036854775807" | python3 -c "import json,sys; d=json.load(sys.stdin); print(f'{len(d)} records total'); [print(r['message']) for r in d[:5]]"

echo "--- generated log file (host side) tail ---"
tail -5 "$WORK_DIR/logs/synthetic.log"
wc -l "$WORK_DIR/logs/synthetic.log"

echo "--- querying pierre for marker $MARKER ---"
set +e
RESULT=$(python3 - "$QUERY_PORT" "$MARKER" "$LINE_COUNT" <<'PYEOF'
import sys, urllib.request, json, re

import time

query_port, marker, expected_count = sys.argv[1], sys.argv[2], int(sys.argv[3])
# /query/search bounds how many BM25 time-buckets a query may span (a wide-open
# start=0/end=i64::MAX range would enumerate ~2.5M buckets and hang the server —
# a real bug this e2e test found). Bracket real "now" with a day of margin instead.
now_ns = int(time.time() * 1_000_000_000)
start_ns, end_ns = now_ns - 86_400_000_000_000, now_ns + 86_400_000_000_000
url = f"http://127.0.0.1:{query_port}/query/search?start={start_ns}&end={end_ns}&q={marker}&k={expected_count + 50}"
with urllib.request.urlopen(url, timeout=10) as resp:
    hits = json.loads(resp.read())

seqs = set()
for hit in hits:
    record = hit.get("record")
    if not record:
        continue
    m = re.search(r"seq (\d+)", record["message"])
    if m and marker in record["message"]:
        seqs.add(int(m.group(1)))

expected = set(range(expected_count))
missing = expected - seqs
extra = seqs - expected

print(f"RESULT hits={len(hits)} matched_seqs={len(seqs)} missing={sorted(missing)} extra={sorted(extra)}")
sys.exit(0 if not missing and not extra else 1)
PYEOF
)
STATUS=$?
set -e
echo "$RESULT"

echo "--- querying pierre via loki query_range for level=error ---"
python3 - "$LOKI_PORT" "$MARKER" <<'PYEOF'
import sys, urllib.request, urllib.parse, json

loki_port, marker = sys.argv[1], sys.argv[2]
q = urllib.parse.quote(f'{{level="error"}} |= "{marker}"')
url = f"http://127.0.0.1:{loki_port}/loki/api/v1/query_range?query={q}&start=0&end=9223372036854775807"
with urllib.request.urlopen(url, timeout=10) as resp:
    body = json.loads(resp.read())
n = sum(len(s["values"]) for s in body["data"]["result"])
print(f"loki query_range level=error matches: {n}")
PYEOF

if [[ "$STATUS" -eq 0 ]]; then
  echo "=== E2E PROMTAIL TEST PASSED: all $LINE_COUNT lines shipped end-to-end via real Promtail ==="
  exit 0
else
  echo "=== E2E PROMTAIL TEST FAILED: see missing/extra above ==="
  cat "$WORK_DIR/pierre.log"
  exit 1
fi
