# Dashboard API

`GET /v1/dashboard` uses the same per-VM bearer token as the session API. It reads the existing session registry and cached resource sample; it does not query the database, control agents, or collect separate history. Poll every five seconds over the deployment's authenticated HTTPS endpoint.

```json
{
  "version": 1,
  "sampledAt": 1780000000000,
  "runtime": { "ready": true, "configured": true, "version": "0.1.0" },
  "appConnectivity": "unknown",
  "resources": { "cpu": 12.5, "memory": 30, "disk": 20 },
  "resourceBytes": { "memoryUsedBytes": 4800000000, "memoryTotalBytes": 16000000000, "diskAvailableBytes": 80000000000, "diskTotalBytes": 100000000000 },
  "diskThresholds": { "warningBytes": 5000000000, "pauseBytes": 2000000000 },
  "agents": { "working": 0, "queued": 0, "waiting": 1, "failed": 0 },
  "storage": {
    "enabled": true, "level": "normal", "reason": "disk_capacity",
    "workspace_available_bytes": 80000000000, "history_available_bytes": 80000000000,
    "workspace_total_bytes": 100000000000, "history_total_bytes": 100000000000,
    "sampled_at": 1780000000000
  },
  "onboarding": { "localConnected": null, "offlineTaskVerified": null },
  "capabilities": { "settings": false, "updates": false },
  "sessionCount": 1,
  "sessions": [{
    "id": "cr_example", "title": "Codex session", "repository": "example",
    "harness": "codex", "model": null, "state": "waiting",
    "activity": "waiting", "lastActivity": 1780000000000
  }]
}
```

- Timestamps are Unix milliseconds. Resource readings and `sampledAt` are null before measurement. Samples refresh approximately every ten seconds; reads do not refresh their timestamp. Disk means the configured repository filesystem. Its percentage is `used / (used + available)`, excluding Linux-reserved blocks; no available space means 100%.
- `storage` is the same cached safety snapshot returned by `/v1/health`, independent of model discovery. Its level is `normal`, `low_space`, or `blocked`; reasons distinguish `disk_capacity`, `measurement_unavailable`, and `unprotected_test_mode`. Byte counts and `sampled_at` are null until a valid measurement, and after measurement failure or expiry. Clients should display available GB and the actual reason; older cores without this field have unknown storage details. See [disk policy](storage.md) for unchanged thresholds.
- `runtime.ready` means the core is running and storage permits execution. `runtime.configured` means at least 1 harness is configured. Neither verifies inference login or app onboarding. An installed core can run without repositories or harness accounts; new sessions return 409 until agent setup exists.
- Authenticated `GET /v1/ready` returns `{ "ready": true }` with 200 only while storage permits work and both history tables are reachable. Otherwise it returns 503. It creates no test records and does not require agent setup. Unlike the dashboard, this deployment check queries the database.
- Sessions are the latest 1,000 entries in this core's local registry, ordered by last recorded activity. `sessionCount` is the full local count. Saved-only database history and unrelated app/native processes are not included.
- Starting/resuming sessions are `queued`; running/interrupting/closing are `working`; idle or storage-paused sessions are `waiting`; closed sessions are `stopped`; lost/failed processes are `failed`. Unrecognized states stay `unknown`. An idle session is not assumed to be a completed task; its latest failed prompt stays `failed` and an uncertain prompt outcome stays `unknown`.
- Titles are deliberately generic, never derived from prompts. Only repository basenames are returned. Model names come from recorded native identity; missing values remain null. New records retain `timestamp_ms` inside the existing text record format; older records remain readable and unknown historical times stay null. No SQL migration is needed.
- Never return receipts, prompts, transcript text, native paths, process arguments, or credentials. This endpoint is a read-only summary, not session replay.
- Settings and updates are unsupported in this slice. Clients must not call update endpoints without an explicit `capabilities.updates: true`; unsupported settings controls remain disabled. Missing capabilities mean unsupported, not enabled.

## Metric history

`GET /v1/metrics?range=1h` accepts only `1h`, `24h`, `7d`, or `30d` (default `1h`). It uses the same bearer authentication and the configured store's existing diagnostics table. No migration, separate collector, or browser database access is needed.

- Response: `{version:1, range, from, to, bucketMs, points}`. Times are Unix milliseconds. Buckets are 30 seconds, 5 minutes, 30 minutes, or 3 hours respectively, with at most 337 points (241 for 30 days).
- Each point has `at` (bucket start), `sampledAt` (last observation), `gap`, `cpu`/`cpuPeak`, `memory`/`memoryPeak`, `memoryUsedBytes`/`memoryTotalBytes`, `diskAvailableBytes`/`diskTotalBytes`, and nullable `agents` with `working`, `queued`, `waiting`, and `failed` counts.
- CPU/memory are interval averages and peaks. A missing measurement makes that interval's average null. Bytes and counts are the last sample, not averages. Missing buckets are omitted; gaps over 30 seconds set `gap`. Clients must not connect lines across gaps or replace unknown values with zero.
- Counts cover the full local registry, not the dashboard's 1,000-row list. They use the dashboard state definitions above. Waiting includes idle/disk-paused sessions; failed means sessions currently in a failed state, not crashes during the interval. Old samples without agent counts remain null.
- Session Management supplies counts to the existing ten-second resource sampler, independently of browser connections. Resource/agent history has best-effort 30-day retention and survives core restarts when uploaded. Other diagnostics retain seven days. Longer history accumulates after this core update; already deleted samples cannot be backfilled. It is not a durable accounting ledger.
- Reads aggregate in PostgreSQL using the existing store/time index, serialize through one read gate, and cache each range for 30 seconds. Numeric readings are materialized before sorting, avoiding large JSON spills. Queries have a six-second statement deadline and an eight-second overall I/O deadline, within the web client's ten-second request budget. Storage failure returns 503 rather than an empty healthy chart. No raw diagnostic records leave the endpoint.
- Live dashboard readings retain the percentage-only `resources` contract. Optional `resourceBytes` adds absolute capacity; `agents` supplies current full-registry counts; `diskThresholds` exposes the actual configured warning/pause boundaries (null without disk protection). Older cores omit these fields.

Verify with `python3 tests/core_fixture.py` after building. It checks authentication, bounded results, resource sampling, old records, and secret exclusion. Run the consuming app against the real core as well as its fixtures.
