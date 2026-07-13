# RUNBOOK — demo/pitch deployment on a production or QA VM

Concrete steps for the scenario this was written for: handing the Pierre binary to run on a
VM inside a production or QA network (not your own machine), tee'd from an existing real log
pipeline, pushed with real volume to see how far it goes. See `DECISIONS.md`'s "Path to first
real-world exposure" entry for the reasoning behind every choice below.

## 1. Build the binary

Build it wherever `edgestore` is checked out alongside `pierre` (see `README.md`'s
"Development" section for why) — your dev machine or a CI runner, not the target VM.

```sh
cargo build --release
```

Copy `target/release/pierre` to the VM. It's a single static binary — no other files needed
except the config below.

## 2. Generate an auth token

The VM sits on a production/QA network, not a machine only you can reach — turn auth on.

```sh
openssl rand -hex 32
```

Keep this value; it goes in `auth_tokens` below and in the collector's push config.

## 3. Write `pierre.toml` on the VM

Copy the repo's `pierre.toml` as a starting point and change:

```toml
data_dir = "/var/lib/pierre/data"        # or wherever has disk headroom on the VM
native_listen_addr = "0.0.0.0:4317"      # only if the collector isn't co-located
loki_listen_addr = "0.0.0.0:3100"
query_listen_addr = "0.0.0.0:3101"
fields = ["level", "status", "trace_id", "latency_ms", "path"]  # match what the real pipeline emits

auth_tokens = ["<the token from step 2>"]
```

Leave `[backup]` commented out (the default). This deployment is deliberately ephemeral —
see `DECISIONS.md` — no new durable copy of production/QA log data gets created. If the VM is
torn down after, the data goes with it.

Bind to `0.0.0.0` only if the existing collector runs on a different host from Pierre; if
they're co-located, `127.0.0.1` is enough and keeps the attack surface smaller regardless of
the auth token.

## 4. Point the existing collector at Pierre

Whatever's already shipping logs (Promtail, Alloy, Vector, Fluent Bit) gets a **second**
output added alongside its existing one — Pierre is a tee, not a replacement. Example for
Promtail's config (`clients:` list):

```yaml
clients:
  - url: http://<existing-destination>/loki/api/v1/push   # unchanged
  - url: http://<pierre-vm>:3100/loki/api/v1/push
    bearer_token: "<the token from step 2>"
```

If the collector doesn't support a bearer token in its client config, check its docs for
`authorization`/`headers` — the requirement is just `Authorization: Bearer <token>`.

## 5. Run it and watch the stats line

```sh
RUST_LOG=info ./pierre /path/to/pierre.toml
```

Every 5 seconds, a line like this prints:

```
[pierre::stats] stats: ingest=48213 total, 1024.3 rec/s | rollup dropped=0 | textindex dropped=0
```

That's the live signal for "how far we go" — watch `rec/s` climb with real traffic, and watch
`dropped` for either counter start climbing (rollup/textindex falling behind ingest under
load — not data loss, see `SPEC.md` FR-19, but a real signal the async pipelines are
saturated).

If you're not watching the terminal live, redirect to a file and `tail -f` it, or run under
`systemd` with `journalctl -f -u pierre`.

## 6. Sanity-check before the real push

- `curl http://localhost:3101/query/logs?start=0&end=<now_ns>` with the bearer token — confirms
  the query surface is up and auth is actually enforced (should 401 without the token, 200
  with it).
- Confirm the existing pipeline's original destination is unaffected — Pierre is additive; if
  something goes wrong here, the existing pipeline keeps working regardless.
- Known gaps that don't block this deployment, for context (see `STATUS.md` for the full
  list): no cold-tier BM25 index stripping yet (disk-cost optimization, not correctness), no
  clustering/replica mode. `GET /metrics` (Prometheus text format, same counters as the stats
  log line) is available on the query API port if you want to point a scraper at it instead of
  tailing logs — same auth as the rest of that surface.

## After the event

Tear down the VM (or at minimum stop the process and remove `data_dir`) — nothing here is
meant to persist past the demo. If it goes well and a real pilot follows, that's a new set of
decisions (backup config, retention, longer-lived auth token rotation) — not a continuation of
this ephemeral setup.
