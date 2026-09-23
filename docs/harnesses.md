# Harness adapters

Cloudroom owns processes, durable requests/queues, recording, replay and recovery. Harnesses own their agent loop, tools, inference, retries and compaction. Runtime's shared process driver handles bounded stdio, shutdown and Linux containment; each adapter owns its protocol and native history.

## Configure

Existing Codex environment variables still work. `CLOUDROOM_HARNESS` selects the default (`codex` when omitted). Configure the required adapters:

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

## Claude Code

`src/runtime/claude.rs` integrates the native CLI through stream JSON, without an SDK bridge. Core discovers `CLOUDROOM_ACCOUNT_HOME/.local/bin/claude`, then `/usr/local/bin/claude`, when the account's `.claude` directory exists. Prepare both before restarting core. No Claude-specific service variables are needed. Core resolves the executable path at startup: restart it safely after upgrading the CLI to use the new binary and model catalog. Opus 5.5 requires Claude Code **2.1.280+**. The default model is `sonnet`; session creation can select another native model. `CLOUDROOM_HARNESS=claude-code` selects Claude as the default harness.

### Native subscription login

Use the user's existing Claude subscription. In a terminal on the VM, logged in as the configured agent account, run `claude auth login` and complete Anthropic's own flow. Verify with `claude auth status`. On managed VMs, an administrator can open that shell with `sudo -iu cloudroom-agent`; do not sign in as root or the protected core account. Keep credentials on the VM. Never paste login codes or tokens into Cloudroom or copy them from the laptop.

Create sessions with `"harness":"claude-code"`. Missing login returns `claude_auth_required` before acceptance; verification failures return `claude_auth_unavailable`. The GUI retains the task and offers **Retry start** after native login. This is login guidance, not an embedded OAuth flow or a live subscription-limit check.

The adapter disables native steering, Fast mode, structured questions and hidden native subagents. Delegation uses Cloudroom child sessions through an in-process MCP transport. Hooks/plugins load normally. Startup and context notices use native non-query messages, not inference. Rewind forks conversation history without restoring files; native paths and checkpoints remain adapter-owned.

### Selected skills

Structured prompt `content` can carry skill mentions with UTF-16 `start`/`end` offsets and a `resource` containing `kind: "command"`, `source: "skill"`, `name`, and `origin: "user" | "project"`. Its text blocks, joined with newlines, must match `text`. The Claude harness advertises `skill_mentions: true`; GUI clients must not drop these mentions when using an older core.

Before each send, core loads the selected user/project `SKILL.md` bodies on the execution machine under the agent's file permissions. Tags work anywhere and repeated selections load once. Saved requests stay unchanged. Missing, ambiguous, oversized or unsupported native-feature skills fail before delivery. Bodies are literal instructions, not a replacement for native argument, shell, hook or fork execution; use an untagged leading slash command for those features and namespaced plugin skills.

### Verification boundary

Local probes against Claude Code **2.1.278** verified guarded startup, context-only notices, missing-login failures, exact-identity resume, conversation forks and acknowledged shutdown. HTTP checks used disposable PostgreSQL and logged-out native execution; a separate wire fixture verified child coordination, compaction, Stop and queued continuation. These are not real-inference evidence. Logged-in Linux/GUI acceptance and template activation remain required before claiming deployment readiness. Upgrades must preserve the checked lifecycle, cancellation and hook capabilities.

## Cloud Codex sign-in

The core advertises `codex_auth` and exposes authenticated `GET /v1/accounts/codex`, `POST /v1/accounts/codex/login`, and `POST /v1/accounts/codex/cancel`. Mutations accept `{ "request_id": "unique-id" }`; reuse it after an uncertain response. Cancellation targets that login attempt only.

The core uses Codex's official device-code flow in the configured runtime user's home. Enable device-code login in ChatGPT security settings if OpenAI requires it. The app receives only account status and the temporary verification URL/code, never OAuth tokens. Login traffic is excluded from conversation and diagnostic records. A live account/usage check verifies ChatGPT access; limits and network errors stay distinct from missing authentication. Active Codex work blocks login changes.

New Codex starts without authentication return `codex_auth_required` before acceptance. Clients keep the prompt and offer sign-in, then retry the same start ID. Already accepted work and running sessions are unchanged. Expired login attempts can be restarted; interruption never replays a task. Pi login is separate.

With `codex_auth_import`, authenticated `POST /v1/accounts/codex/import` accepts a file-backed ChatGPT login (64 KiB maximum). It creates `auth.json` only when absent, under the runtime user with mode `0600`, then verifies the account. Existing files, active work, and pending browser sign-ins are preserved. Retries never replace a login; tokens never enter responses or history.

The desktop helper attempts this import before showing sign-in or starting Codex, and discovers logins added after pairing. `CODEX_HOME` is remembered; Keychain-only, missing, or unusable local logins fall back to **Connect Codex**. Import is not continuous credential sync or account switching. Old `auth-codex` sync requests still receive HTTP 410. Upgrade core and desktop together; existing cloud logins remain intact. Each native Codex manages renewal; cross-machine refresh conflicts still require live verification.

## API and compatibility

Select a configured harness when creating a session:

```json
{"request_id":"my-session","harness":"pi"}
```

All other session, prompt, interrupt, close, events and SSE endpoints are shared. Session creation accepts a model override and a reasoning level for either harness. Pi maps `none` to `off`, verifies the model supports the requested level, and checks that Pi applied it; unsupported levels fail rather than being silently changed. Capabilities include each harness's default inference provider. Pi advertises `provider_selection: true`; creation accepts an optional `provider` override authenticated in its private Pi home. Omitting it preserves the configured default. Harness selection, model, reasoning and provider are saved with acceptance; changing deployment defaults does not change an existing session's selected model. Explicitly selecting another harness with the same creation request ID conflicts. Legacy receipts without harness metadata mean Codex. Original stored records are not rewritten, and legacy Codex `data.value` is reconstructed on reads inside the adapter boundary.

Codex reasoning is validated against its native `model/list` catalog, not a fixed Cloudroom list. `/v1/capabilities` exposes `harnesses[].models` with each model's `reasoning_levels`; a null catalog means discovery failed, not unsupported models. Discovery creates no Codex thread or turn and is cached for 60 seconds. Pi exposes its adapter's `reasoning_levels`; its model-specific check still happens during startup. Existing accepted requests are deduplicated before model revalidation.

Launch rejections include stable codes such as `invalid_reasoning_effort`, `invalid_model`, and `request_conflict`. `storage_blocked`, `service_stopping`, and `model_catalog_unavailable` are temporary. Clients should classify codes, not assume every HTTP 409 is permanent, and never display arbitrary remote error bodies.

Session responses include `harness`, `provider`, `capabilities` and an adapter-owned `native_cursor`. `current_turn` and `native_offset` remain for legacy Codex readers; they are not portable request/capture identifiers. Follow `current_request` and receipts for execution. Pi emits common text/thinking deltas and item/tool events; `tool_snapshot` replaces previous partial tool output rather than appending it. Raw native frames and native session records are preserved.

## Command Guard

New Codex, Pi and Claude runs include a small shell-command guard by default. Disable it
for a session with `command_guard_enabled: false` in `POST /v1/sessions`; this
choice survives resume and passes to Cloudroom-managed Pi and Claude children. The guard
runs on the execution machine without the GUI. It never edits personal hooks or
trusts unrelated Codex hooks. If native Codex hooks are explicitly disabled,
guarded startup fails rather than re-enabling other hooks.

Rules cover root/home deletion, disk wipes, hosted repository deletion, and fork
bombs. Blocks return a named reason to the agent. This is regex-based accident
prevention, not a sandbox or protection against arbitrary scripts and tools.
Codex hooks were verified with 0.155.1; Pi uses the version above. Core Codex
startup also requires the system Node runtime already used by Pi. Missing Node
fails startup rather than silently dropping the guard.

`src/runtime/command-guard.mjs` owns the shared rules and hook source. Keep its
GUI copy, `gui/packages/plugin-sdk/src/internal/command-guard-runtime.mjs`,
byte-identical when preparing a release.

## Cursor implementation preview

The core discovers `cursor-agent` in the agent account's `.local/bin` or `/usr/local/bin`, with an existing `.cursor` home. Use `harness: "cursor"` in the API; the GUI provider remains `acp-cursor`. No new Cloudroom service environment variables are required. After upgrading core and the GUI, use **Settings → Cloudroom → Cursor → Manage connection**, or `room cloudroom cursor login --request-id ID` followed by `room cloudroom cursor status`. Browser login runs on the VM, with `NO_OPEN_BROWSER=1` confined to the Cursor login child. Credentials stay in the shared native account home.

The authenticated account API exposes `GET /v1/accounts/cursor` and `POST /v1/accounts/cursor/{login,cancel,key}`. Mutations use `{ "request_id": "ID" }`; `/key` additionally takes `api_key`. Mutations return HTTP 202. An active login is reused, cancellation targets its request ID, and missing authentication rejects session creation before acceptance. Keys are verified with `--list-models` (`status` ignores API keys), saved privately as `.cursor/cloudroom-api-key`, and supplied through `CURSOR_API_KEY` only to Cursor children. The GUI clears its password field after submission; CLI key input uses `room cloudroom cursor key` through stdin, never a key argument. Neither login links nor keys enter conversation records or sync. Close live Cursor sessions before changing their login.

Cursor runs through ACP and shares the process driver, durable queue and history. Steering cancels the active ACP prompt and sends the correction in the same Cloudroom turn. Prompt dispatch is not completion. Reasoning is verified at launch and remains fixed for that session. Manual compaction, rewind, direct image inputs, independent subagent controls and context-only notices are not advertised.

Guarded Cursor starts fail explicitly: permission callbacks do not cover already-allowed commands. Cursor `2026.09.18-9a7762b` ran native safety hooks in print mode but ignored the same hooks in ACP, even with project trust and explicit plugin loading. Unguarded execution requires an explicit `command_guard_enabled: false` request; the core never silently removes this protection.

The adapter records bounded native SQLite snapshot chunks, including metadata and WAL-backed data. The Python helper uses SQLite backup with query-only SQL; read/write opening permits required WAL sidecars after Cursor exits. `cursor-history.py restore ROOT` accepts one complete snapshot's native JSONL on stdin, checks its digest, and refuses to replace an existing session. It restores native history, not workspace files or credentials.

Initial sync discovery includes Cursor skills, global rules and the `notifications`, `hints`, and `suggestNextPrompt` preferences. Existing paired roots are preserved. Credentials, session stores, hooks, plugins and MCP configuration do not sync.

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
python3 tests/cursor_http.py # disposable PostgreSQL required for start-gating checks
python3 tests/pi_e2e.py
# Disposable Linux + PostgreSQL + configured native credentials:
python3 tests/core_e2e.py --fixture
python3 tests/core_e2e.py
python3 tests/core_e2e.py --harness pi
# Prepared disposable Linux VM and test accounts only:
sudo python3 tests/storage_e2e.py --disposable --mixed
```

`pi_e2e.py` uses disposable files, the real Pi CLI and existing valid account credentials. It tests HTTP execution, interruption, queued continuation, native recording and restart with the database unavailable. It refuses near-expired copied OAuth credentials instead of refreshing them behind the user's back. Linux cgroup/disk enforcement and real PostgreSQL upload require their separate Linux tests; a local pass does not establish deployment readiness. No schema migration is introduced by the adapter change.
