# Diagnostics

One internal module; no alerts, dashboard, external collector, or new dependency. Apply [0002-diagnostics.sql](database/0002-diagnostics.sql) explicitly. It reuses the existing database connection and TLS policy. Missing migrations or database outages do not prevent local logging or agent execution.

## Recorded data

- `api`: route template, GET/POST/other, HTTP status and response-header latency. SSE timing covers stream setup, not its lifetime. `X-Cloudroom-Diagnostic-Id` matches `run_id-sequence`.
- `agent_start`: Cloudroom session ID, initialization success and elapsed milliseconds; not inference response time.
- `agent_exit`: session ID, fixed reason and expected/technical classification from Runtime. Normal turn completion is not a crash.
- `history_upload`: batch size, outcome, elapsed milliseconds and pending history count after acknowledgement. `history_fault` identifies journal read/acknowledgement failure.
- `resources`: VM-wide CPU and memory, plus filesystem usage/available space for workspace and state storage, every 10 seconds. CPU/memory use Linux `/proc`; unsupported or failed samples are `null`, never zero. First CPU sample is unknown. Filesystem sampling uses `/bin/df` with a one-second timeout. These are observations, not resource limits.
- `diagnostics`: cumulative dropped-record, local-write-failure and database-write-failure counts for this process.

Only fixed diagnostic fields and correlation IDs are recorded. No credentials, prompts, native history, filesystem paths, request bodies or raw URLs. History and recovery remain with their existing owners.

## Storage and limits

- `$CLOUDROOM_STATE_DIR/diagnostics.jsonl` and `diagnostics.previous.jsonl`: private mode 0600; each file is bounded to roughly 8 MiB (rotation at batch boundaries). Writes run off the request path, without per-record fsync.
- One-second batches; a 1,024-record ingress queue and a 1,024-record pending database buffer. Overflow is counted. Database calls have a two-second deadline; local writes use the blocking pool. Only one diagnostic database operation runs at once, sharing the existing pool.
- Retries use stable `(store, run_id, sequence)` keys. Pending uploads survive brief outages in memory, not process loss. Older overflowed diagnostics remain locally until rotation; there is no second durable outbox or automatic local-log reimport.
- PostgreSQL retains seven days per store, pruned hourly while the core runs. Stopped cores do not run cleanup. Local logs use size-based retention instead. Shutdown attempts a final flush for up to five seconds.
- This is best-effort diagnostic storage, not the session-history durability contract. Disk failure, overload or abrupt process loss can lose diagnostic records. Check local logs during database failures; database failure counters appear in the next diagnostic summary.

Use owner-isolated database credentials as required by [0001](database/0001-session-records.sql). A `store` label is not an authorization boundary. RLS denies browser-role access by default; never expose database credentials or these SQL queries through an unrestricted web endpoint.

## Inspect

Local files: `jq . "$CLOUDROOM_STATE_DIR/diagnostics.jsonl"`. For older local records, inspect `diagnostics.previous.jsonl` too. These commands require the protected service account's file access.

Run scoped, read-only SQL using the owner's database access (`psql "$CLOUDROOM_DATABASE_URL" -v store="$CLOUDROOM_STORE"`):

```sql
SELECT to_timestamp(timestamp_ms / 1000.0) AS recorded_at, record
FROM cloudroom_diagnostics WHERE store = :'store'
ORDER BY timestamp_ms DESC LIMIT 100;

SELECT record->>'route' AS route, record->>'status' AS status,
       count(*), avg((record->>'duration_ms')::numeric) AS average_ms
FROM cloudroom_diagnostics
WHERE store = :'store' AND record->>'kind' = 'api'
  AND timestamp_ms > (extract(epoch FROM now() - interval '1 hour') * 1000)::bigint
GROUP BY 1, 2;
```

## Verify

Run `cargo test --locked --all-targets` and `python3 tests/core_e2e.py --foundation` for HTTP, real Linux resource sampling, migration recovery, redaction, retention and browser-role denial. Full `tests/core_e2e.py` additionally checks real Codex startup/crash signals and local diagnostics while history storage is unavailable. Use only disposable test resources; these checks do not deploy or migrate production.
