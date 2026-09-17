# Session lifecycle and replay

## Close a session

`POST /v1/sessions/{id}/close` requires the same bearer authentication as other commands:

```json
{"request_id":"close-this-session"}
```

HTTP 202 means the close request is saved locally. Wait for its receipt to become `completed` and the session state to become `closed`. New prompts are rejected once close is accepted. Retrying the same request ID returns its receipt without repeating execution.

Close releases the harness slot and preserves history. Start with a new request ID to create another session; closing never resumes or replaces the old one. A session still initializing returns 409 instead of accepting close prematurely.

Runtime closes Codex's stdin, allowing native cleanup, and continues collecting records until exit. A four-second grace bounds an unresponsive harness; forced or unsuccessful exits produce `unknown`, not a successful close receipt. Protected Linux execution also stops the session's workload cgroup and waits until it has no live descendants. Cleanup failure blocks replacement; it is never treated as a successful close.

Service shutdown signals all harnesses before waiting. Session recording and best-effort diagnostics get a five-second budget. HTTP draining ends after six seconds even if a peer stops reading. Shutdown is distinct from client disconnect, which never stops execution.

## Queued prompts and restart

- A prompt that arrives while a turn is running is saved locally, then acknowledged with an `accepted` receipt. Queued prompts run in acceptance order, one turn at a time. Retrying a request ID returns its receipt; different content for the same ID is rejected.
- Acceptance and queue membership come from one fsynced receipt. Old separate `enqueue` records remain readable without duplicate delivery, including a crash between the old two writes.
- Interrupt stops the current turn only; queued prompts continue afterwards. Close prevents further delivery and marks unrun queued receipts `failed`.
- Normal service shutdown leaves eligible sessions `suspended`. Both normal and abrupt restarts resume the same saved Codex conversation. Deliberately closed sessions stay closed; historical `process_lost`/`failed` sessions are not automatically reopened.
- An unexpected harness exit gets one recovery attempt after confirmed cleanup. A new user request or completed/interrupted turn permits a later attempt. A failed or interrupted resume becomes `process_lost`, and unrun queued receipts become `failed`; there is no restart loop or fresh-conversation fallback.
- Unfinished dispatched work becomes `unknown` on shutdown or `unknown_after_restart` on a crash and is never resent. Queued work waits for the native resume handshake, sufficient storage, and a ready harness. This does not promise continuation of an interrupted task or exactly-once external effects.
- Before admitting execution, protected Linux startup clears only its validated workload subtree and confirms it is empty. Runtime also confirms each session's descendants have exited before recovery. Saved PIDs or similar command text never authorize killing a process. Unprotected local tests merely wait for an old PID to disappear; uncertainty blocks replacement and provides no descendant-containment guarantee.

## Storage and replay

- All locally accepted records are fsynced before subscribers are notified. Native turn events own progress; RPC replies acknowledge delivery or rejection.
- Local replay uses a rebuilt, per-session sequence index. It does not scan other sessions.
- Local and database event pages contain at most 256 records. Continue after the last returned sequence; sequences need not be contiguous within a session.
- Saved-only metadata and the requested latest receipt share one bounded-memory scan, separate from replay pagination. Parsing stays in Rust because native output may contain NUL characters that PostgreSQL JSON processing rejects.
- New native-event records omit the duplicate `data.value` payload on disk and in PostgreSQL. The existing API field is reconstructed from the original frame on reads. Old records are unchanged, including during upload retries. No schema migration is needed.
- Saved-only sessions still do not restore running processes or populate the full receipt map.

## Verify

```sh
cargo fmt --check
CARGO_BUILD_JOBS=1 cargo clippy --locked --all-targets -- -D warnings
CARGO_BUILD_JOBS=1 cargo test --locked --all-targets
CARGO_BUILD_JOBS=1 cargo build --locked
python3 tests/core_fixture.py
python3 tests/core_e2e.py --fixture
python3 tests/core_e2e.py
```

The standalone fixtures use no inference or database. `--fixture` adds isolated PostgreSQL and HTTP lifecycle checks on Linux. The full E2E command uses the configured Codex account and checks actual tool exit before cleanup. Missing resources must fail explicitly; fixtures are not real-inference evidence.
