#!/usr/bin/env bash
# Real end-to-end test: the official MCP reference client (`@modelcontextprotocol/
# inspector`'s CLI mode, TypeScript, a genuinely independent implementation from
# Pierre's Rust `rmcp` server) does a real MCP handshake against Pierre's real
# compiled binary over Streamable HTTP, lists the real tool schemas, then calls
# `search_logs` and gets back a record ingested moments earlier over a real HTTP
# request. Same "real external client, not our own code" discipline as
# `run_promtail_e2e.sh` — `tests/mcp_server.rs` already covers the tools with a
# real `rmcp` client, but that's the same SDK the server is built on, so it can't
# catch a cross-implementation wire-format mismatch the way an independent client can.
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
PIERRE_ROOT="$(cd "$SCRIPT_DIR/../.." && pwd)"

if ! command -v npx >/dev/null 2>&1; then
  echo "npx not found — install Node.js to run this test (npx runs the MCP Inspector CLI without a permanent install)." >&2
  exit 1
fi

# Cleared for the rest of this script: at least one sandboxed dev environment
# sets NODE_OPTIONS to a --require preload path that doesn't resolve inside
# npx's own child node processes, breaking every npx/node invocation below
# with an unrelated-looking MODULE_NOT_FOUND. Harmless to clear generally —
# nothing else in this script needs a custom NODE_OPTIONS.
export NODE_OPTIONS=

# The old standalone `npx` package (pre-npm-v7, no scoped-package auto-install
# support) fails on `@modelcontextprotocol/inspector` with a confusing "You must
# supply a command" error instead of a clear version complaint — catch it here.
NPX_MAJOR="$(npx --version | cut -d. -f1)"
if [[ "$NPX_MAJOR" -lt 7 ]]; then
  echo "npx $(npx --version) is too old (need >=7, bundled with Node >=15) to run @modelcontextprotocol/inspector — check for a stale standalone npx earlier in PATH." >&2
  exit 1
fi

mkdir -p "$SCRIPT_DIR/.tmp"
WORK_DIR="$(mktemp -d "$SCRIPT_DIR/.tmp/run.XXXXXX")"
MARKER="mcp-e2e-$(date +%s)-$$"

NATIVE_PORT=14417
QUERY_PORT=13301
ES_BULK_PORT=19300
MCP_PORT=18300

cleanup() {
  echo "--- cleanup ---"
  [[ -n "${PIERRE_PID:-}" ]] && kill "$PIERRE_PID" >/dev/null 2>&1 || true
  rm -rf "$WORK_DIR"
}
trap cleanup EXIT

echo "--- work dir: $WORK_DIR ---"
mkdir -p "$WORK_DIR/pierre-data"

cat > "$WORK_DIR/pierre.toml" <<EOF
data_dir = "$WORK_DIR/pierre-data"
native_listen_addr = "127.0.0.1:$NATIVE_PORT"
loki_listen_addr = "127.0.0.1:13300"
query_listen_addr = "127.0.0.1:$QUERY_PORT"
es_bulk_listen_addr = "127.0.0.1:$ES_BULK_PORT"
syslog_listen_addr = "127.0.0.1:15600"
otlp_grpc_listen_addr = "127.0.0.1:14427"
otlp_http_listen_addr = "127.0.0.1:14418"
mcp_listen_addr = "127.0.0.1:$MCP_PORT"
fields = ["level"]
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

echo "--- ingesting one real record via the ES _bulk endpoint (real HTTP, real NDJSON) ---"
BULK_BODY="$(printf '{"index":{}}\n{"message":"%s payment service unresponsive","level":"error"}\n' "$MARKER")"
curl -sf -X POST "http://127.0.0.1:$ES_BULK_PORT/_bulk" \
  -H "Content-Type: application/x-ndjson" \
  --data-binary "$BULK_BODY" \
  >/dev/null

# search_logs's own bucket-count guard rejects overly wide time ranges — a real,
# narrow "last hour" window is both realistic agent usage and comfortably under it.
read -r START_NS END_NS <<< "$(python3 -c '
import time
now = int(time.time() * 1e9)
print(now - 1800_000_000_000, now + 1800_000_000_000)
')"

INSPECTOR=(npx -y @modelcontextprotocol/inspector@latest --cli "http://127.0.0.1:$MCP_PORT/mcp" --transport http)

echo "--- real independent MCP client: tools/list ---"
TOOLS_JSON="$("${INSPECTOR[@]}" --method tools/list)"
echo "$TOOLS_JSON" | python3 -c "
import json, sys
tools = {t['name'] for t in json.load(sys.stdin)['tools']}
expected = {'search_logs', 'get_context', 'list_streams', 'aggregate', 'find_anomalies'}
missing = expected - tools
if missing:
    print(f'MISSING TOOLS: {missing}', file=sys.stderr)
    sys.exit(1)
print(f'all {len(expected)} expected tools present')
"

echo "--- real independent MCP client: tools/call search_logs ---"
CALL_JSON="$("${INSPECTOR[@]}" --method tools/call --tool-name search_logs \
  --tool-arg "start_ns=$START_NS" --tool-arg "end_ns=$END_NS" --tool-arg "q=$MARKER")"
echo "$CALL_JSON"
echo "$CALL_JSON" | python3 -c "
import json, sys
result = json.load(sys.stdin)
assert result.get('isError') is not True, f'tool call reported an error: {result}'
body = json.loads(result['content'][0]['text'])
hits = body['hits']
assert len(hits) == 1, f'expected exactly 1 hit, got {len(hits)}: {hits}'
assert '$MARKER' in hits[0]['message'], f'marker not found in {hits[0][\"message\"]!r}'
print('search_logs found the real ingested record via a real independent MCP client')
"

echo "--- pierre log tail ---"
tail -20 "$WORK_DIR/pierre.log"

echo "--- PASS ---"
