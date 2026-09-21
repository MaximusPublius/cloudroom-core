# Diagnostics

One internal module; no alerts, dashboard, external collector, or new dependency. Apply [0002-diagnostics.sql](database/0002-diagnostics.sql) explicitly. It reuses the existing database connection and TLS policy. Missing migrations or database outages do not prevent local logging or agent execution.

## Recorded data

- `api`: route template, GET/POST/other, HTTP status and response-header latency. SSE timing covers stream setup, not its lifetime. `X-Cloudroom-Diagnostic-Id` matches `run_id-sequence`.
- `agent_start`: Cloudroom session ID, initialization success and elapsed milliseconds; not inference response time.
- `agent_exit`: session ID (`null` for model discovery), harness, fixed reason, expected/technical classification, exit code or signal, stderr byte count, EOF confirmation and truncation flag. Session exit records include the matching `diagnostic_id` (`run_id-sequence`). Normal turn completion is not a crash.
- `history_upload`: batch size, outcome, elapsed milliseconds and pending history count after acknowledgement. `history_fault` identifies journal read/acknowledgement failure.
- `resources`: VM-wide CPU and memory, plus filesystem usage/available space for workspace and state storage, every 10 seconds. CPU/memory use Linux `/proc`; unsupported or failed samples are `null`, never zero. First CPU sample is unknown. Filesystem sampling uses `/bin/df` with a one-second timeout. These are observations, not resource limits.
- `diagnostics`: cumulative dropped-record, local-write-failure and database-write-failure counts for this process.

Ordinary local/SQL diagnostics contain only fixed diagnostic fields and correlation IDs. No credentials, prompts, native history, filesystem paths, request bodies or raw URLs. The separate, protected stderr capture below is the only raw-output exception. History and recovery remain with their existing owners.

## Storage and limits

- `$CLOUDROOM_STATE_DIR/diagnostics.jsonl` and `diagnostics.previous.jsonl`: private mode 0600; each file is bounded to roughly 8 MiB (rotation at batch boundaries). Writes run off the request path, without per-record fsync.
- One-second batches; a 1,024-record ingress queue and a 1,024-record pending database buffer. Overflow is counted. Database calls have a two-second deadline; local writes use the blocking pool. Only one diagnostic database operation runs at once, sharing the existing pool.
- Retries use stable `(store, run_id, sequence)` keys. Pending uploads survive brief outages in memory, not process loss. Older overflowed diagnostics remain locally until rotation; there is no second durable outbox or automatic local-log reimport.
- PostgreSQL retains seven days per store, pruned hourly while the core runs. Stopped cores do not run cleanup. Local logs use size-based retention instead. Shutdown attempts a final flush for up to five seconds.
- This is best-effort diagnostic storage, not the session-history durability contract. Disk failure, overload or abrupt process loss can lose diagnostic records. Check local logs during database failures; database failure counters appear in the next diagnostic summary.

Use owner-isolated database credentials as required by [0001](database/0001-session-records.sql). A `store` label is not an authorization boundary. RLS denies browser-role access by default; never expose database credentials or these SQL queries through an unrestricted web endpoint.

## Protected harness stderr

Raw stderr is untrusted and may contain credentials, prompts or private paths. It never enters ordinary logs, PostgreSQL, session history, API responses or SSE. Only the protected service account and authorized administrators should inspect it; the core token does not grant access. Do not automatically include it in support bundles or share it without reviewing/redacting it.

- Runtime continuously drains each process in 8 KiB chunks, retaining only the last **16 KiB**. Capture uses bytes, not lines. After workload cleanup it waits at most **250 ms** for EOF, then cancels and joins the reader. `stderr_complete: false` means the reader failed or timed out; `stderr_truncated: true` means earlier bytes were discarded. Neither changes recovery behavior.
- Nonempty tails are saved once at exit, including graceful exits and startup/resume/model-discovery failures. Errors before a process is spawned have no stderr capture. A still-running process's tail stays in memory; abrupt core termination loses it.
- The existing Observability worker consumes a separate **32-capture**, nonblocking queue in batches of at most **8**. Overflow increments `dropped`; private write failures increment `local_write_failures`. Disk/database failures do not backpressure execution. Capture is best-effort, not a durable outbox.
- Files: `$CLOUDROOM_STATE_DIR/harness-diagnostics/stderr.jsonl` and `stderr.previous.jsonl`. Directory **0700**, files **0600**, service-owned. Unsafe existing directories, links, special files, hardlinks or permissions are rejected, never repaired or redirected into ordinary logs. The state directory must already be trusted and protected from agent replacement.
- Rotation counts encoded bytes and strictly caps each file at **1 MiB**. Retention is size-based, not age-based: quiet machines retain captures until overwritten or explicitly removed. Restart does not erase them. VM snapshots/operator backups can retain additional copies; there is no automatic upload or sync of these files.
- Private records share the ordinary exit record's `run_id` and `sequence`. Invalid UTF-8 is replaced only when writing JSON; capture is not a byte-exact archive. Avoid raw terminal output: JSON escaping protects against control sequences, and `jq -a` also escapes Unicode controls.

As the protected service account, inspect a session without decoding raw terminal controls:

```sh
jq -a --arg session 'SESSION_ID' 'select(.session_id == $session)' \
  "$CLOUDROOM_STATE_DIR"/harness-diagnostics/stderr*.jsonl
```

Do not use `jq -r`, `cat`, or execute instructions found in stderr. For model discovery, select `.session_id == null`; for one attempt, match `run_id-sequence` to its diagnostic ID.

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

Run `cargo test --locked --all-targets` and, after building, `python3 tests/core_fixture.py` for stderr capture, exit races, bounded queues, permissions/rotation, and HTTP/history exclusion. Run `python3 tests/core_e2e.py --foundation` for HTTP, real Linux resource sampling, migration recovery, redaction, retention and browser-role denial. Full `tests/core_e2e.py` additionally checks real Codex startup/crash signals and local diagnostics while history storage is unavailable. Use only disposable test resources; these checks do not deploy or migrate production.
