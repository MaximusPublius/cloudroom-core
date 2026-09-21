# Harness adapters

Cloudroom owns processes, durable requests/queues, recording, replay and recovery. Harnesses own their agent loop, tools, inference, retries and compaction. Runtime's shared process driver handles bounded stdio, shutdown and Linux containment; each adapter owns its protocol and native history.

## Configure

Existing Codex environment variables still work. `CLOUDROOM_HARNESS` selects the default (`codex` when omitted). Configure either or both adapters:

- Codex: `CLOUDROOM_CODEX_BINARY`, `CLOUDROOM_CODEX_HOME`, `CLOUDROOM_CODEX_MODEL`.
- Pi: `CLOUDROOM_PI_BINARY`, `CLOUDROOM_PI_HOME`, `CLOUDROOM_PI_MODEL`, `CLOUDROOM_PI_PROVIDER`.
- `CLOUDROOM_MODEL` remains the fallback when an adapter-specific model is omitted.
- `CLOUDROOM_ACCOUNT_HOME` and the prepared repository remain shared account/workspace settings.

Example Pi-only configuration (in addition to the existing database, token, repository and storage settings):

```sh
CLOUDROOM_HARNESS=pi
CLOUDROOM_PI_BINARY=/usr/local/bin/pi
CLOUDROOM_PI_HOME=/home/cloudroom-agent/.pi/agent
CLOUDROOM_PI_PROVIDER=openai-codex
CLOUDROOM_PI_MODEL=gpt-6-astra
```

Use the official `@earendil-works/pi-coding-agent` **0.85.1**, with its supported Node runtime on `/usr/local/bin:/usr/bin:/bin`. Provision the harness and its account credentials separately; the core does not install packages or perform login. Adapter startup verifies the resolved model/provider, native identity, and bundled context extension. Protocol compatibility must be tested before upgrading Pi.

Pi runs through `--mode rpc`; it is not embedded or forked. Startup networking/telemetry are disabled through Pi's process flags. Existing Pi settings/resources and explicit project-trust decisions still apply. Prepare trusted packages before service startup. The account directory is separate from Cloudroom's protected state directory. Every configured harness home must share the monitored filesystem with the workspace. Neither API/database tokens nor administrative environment variables are inherited by either harness. Pi credentials are available to that agent account; this is not isolation of inference credentials from its own tools.

## API and compatibility

Select a configured harness when creating a session:

```json
{"request_id":"my-session","harness":"pi"}
```

All other session, prompt, interrupt, close, events and SSE endpoints are shared. Session creation accepts a model override and a reasoning level for either harness. Pi maps `none` to `off`, verifies the model supports the requested level, and checks that Pi applied it; unsupported levels fail rather than being silently changed. Capabilities include each harness's default inference provider. Pi advertises `provider_selection: true`; creation accepts an optional `provider` override authenticated in its private Pi home. Omitting it preserves the configured default. Harness selection, model, reasoning and provider are saved with acceptance; changing deployment defaults does not change an existing session's selected model. Explicitly selecting another harness with the same creation request ID conflicts. Legacy receipts without harness metadata mean Codex. Original stored records are not rewritten, and legacy Codex `data.value` is reconstructed on reads inside the adapter boundary.

Codex reasoning is validated against its native `model/list` catalog, not a fixed Cloudroom list. `/v1/capabilities` exposes `harnesses[].models` with each model's `reasoning_levels`; a null catalog means discovery failed, not unsupported models. Discovery creates no Codex thread or turn and is cached for 60 seconds. Pi exposes its adapter's `reasoning_levels`; its model-specific check still happens during startup. Existing accepted requests are deduplicated before model revalidation.

Launch rejections include stable codes such as `invalid_reasoning_effort`, `invalid_model`, and `request_conflict`. `storage_blocked`, `service_stopping`, and `model_catalog_unavailable` are temporary. Clients should classify codes, not assume every HTTP 409 is permanent, and never display arbitrary remote error bodies.

Session responses include `harness`, `provider`, `capabilities` and an adapter-owned `native_cursor`. `current_turn` and `native_offset` remain for legacy Codex readers; they are not portable request/capture identifiers. Follow `current_request` and receipts for execution. Pi emits common text/thinking deltas and item/tool events; `tool_snapshot` replaces previous partial tool output rather than appending it. Raw native frames and native session records are preserved.

## Queue, completion and recovery

There is one durable Cloudroom queue. Pi's memory-only steering/follow-up queue is not used for that backlog. RPC acknowledgement means acceptance, not completion. Pi requests settle only after its full run, including internal retries and compaction; an additional state check guards premature settled signals. Handled extension commands with no agent run are reconciled separately. Interrupt intent is correlated to the active request even when Pi aborts inside a tool without emitting a new assistant message. Interrupt clears Pi's internal continuations first, but preserves Cloudroom's durable future requests.

New Pi sessions use an exclusively created empty file that Pi initializes itself. Startup confirms its header and syncs it before admitting prompts. Resume checks exact identity, workspace, model/provider and supported session version; it never creates a missing file or silently migrates/replaces a conversation. Pi capture resumes by entry ID and the exact recorded boundary, rather than treating a persisted Codex byte offset as portable. Missing/corrupt native history leaves Cloudroom history readable and fails that session's recovery, not unrelated sessions. Uncertain dispatched work is never resent.

A process-generation check rejects old replies/events, and targeted controls are checked again at dispatch. Existing workload reconciliation, bounded recovery and storage gating apply to both adapters.

## Notices, dialogs and shutdown

The Pi adapter supplies one small, bundled extension for context-only notices. It uses Pi's public extension API and never implements a queue or agent loop. `storage_warning_delivery.confirmed` means accepted by the harness, not that the model has consumed it. During active tools, Pi can defer insertion to a safe turn boundary. Kernel disk enforcement does not depend on model cooperation.

Interactive extension dialogs are currently unsupported: the adapter records cancellation and responds `cancelled: true`, never approval. The capability is explicit. Terminal-only extensions do not become remote UI automatically. Images, steering, model-changing commands and arbitrary native RPC forwarding are not added to the HTTP API by this slice.

Pi close clears native queues, cancels direct bash and active work, waits for acknowledgements, then closes stdin and captures remaining history. Both adapters share the bounded escalation and cgroup cleanup path. Forced/uncertain exits are not reported as clean closes. Linux containment, not Unix process groups alone, covers detached descendants. Unprotected test mode provides no descendant-containment guarantee.

## Verify

```sh
cargo fmt --check
CARGO_BUILD_JOBS=1 cargo clippy --locked --all-targets -- -D warnings
CARGO_BUILD_JOBS=1 cargo test --locked --all-targets
CARGO_BUILD_JOBS=1 cargo build --locked
python3 tests/core_fixture.py
python3 tests/pi_http.py
python3 tests/pi_e2e.py
# Disposable Linux + PostgreSQL + configured native credentials:
python3 tests/core_e2e.py --fixture
python3 tests/core_e2e.py
python3 tests/core_e2e.py --harness pi
# Prepared disposable Linux VM and test accounts only:
sudo python3 tests/storage_e2e.py --disposable --mixed
```

`pi_e2e.py` uses disposable files, the real Pi CLI and existing valid account credentials. It tests HTTP execution, interruption, queued continuation, native recording and restart with the database unavailable. It refuses near-expired copied OAuth credentials instead of refreshing them behind the user's back. Linux cgroup/disk enforcement and real PostgreSQL upload require their separate Linux tests; a local pass does not establish deployment readiness. No schema migration is introduced by the adapter change.
