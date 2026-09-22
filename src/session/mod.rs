mod children;
mod outbox;
pub use outbox::Journal;
mod history;
mod prompt;

use crate::{
    config::Config,
    observability::{Observability, Signal, elapsed_ms, now_ms},
    runtime,
    workspace::storage::{Guard, Level, Snapshot},
};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::BTreeMap,
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::{Duration, Instant},
};
use tokio::sync::{Semaphore, mpsc, watch};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Record {
    pub sequence: u64,
    pub session_id: String,
    pub kind: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub timestamp_ms: Option<u64>,
    pub data: Value,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub native: Option<String>,
}

impl Record {
    // Preserve the existing wire format without storing params twice alongside the native frame.
    fn for_client(mut self) -> Result<Self> {
        runtime::for_client(&mut self.data, self.native.as_deref())?;
        Ok(self)
    }
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Receipt {
    pub request_id: String,
    pub command: String,
    pub input: Value,
    pub state: String,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub model: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub provider: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub workspace: Option<crate::workspace::Workspace>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

#[derive(Default, Clone, Serialize)]
pub struct Session {
    pub session_id: String,
    pub harness: runtime::Kind,
    pub capabilities: Value,
    pub native_cursor: Value,
    pub provider: Option<String>,
    pub native_id: Option<String>,
    pub native_path: Option<std::path::PathBuf>,
    pub native_offset: u64,
    pub state: String,
    pub current_request: Option<String>,
    pub current_turn: Option<String>,
    pub startup_error: Option<String>,
    #[serde(skip)]
    has_dispatched: bool,
    pub last_sequence: u64,
    pub receipts: BTreeMap<String, Receipt>,
    // Ordered prompts accepted while busy, delivered one turn at a time.
    pub queue: Vec<String>,
    #[serde(default)]
    pub prompts: BTreeMap<String, prompt::PromptState>,
    pub storage_paused: bool,
    pub queue_paused: bool,
    #[serde(default)]
    pub compacting: bool,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub compact_request: Option<String>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub checkpoint: Option<Value>,
    pub rewind_request: Option<String>,
    pub parent_session: Option<String>,
    pub reasoning: Option<String>,
    pub workspace: Option<crate::workspace::Workspace>,
    #[serde(skip)]
    last_activity: Option<u64>,
    #[serde(skip)]
    last_prompt_state: Option<String>,
    #[serde(skip)]
    model: Option<String>,
    #[serde(skip)]
    storage_warned: bool,
    #[serde(skip)]
    handle: Option<runtime::Handle>,
    #[serde(skip)]
    ready: bool,
    #[serde(skip)]
    recovery_attempted: bool,
    #[serde(skip)]
    close_request: Option<String>,
    // Last harness pid, so a later instance can reconcile ownership before resuming.
    #[serde(skip)]
    harness_pid: Option<u32>,
}

impl Session {
    fn resume(&self) -> Option<runtime::Resume> {
        Some(runtime::Resume {
            id: self.native_id.clone()?,
            path: self.native_path.clone()?,
            cursor: self.native_cursor.clone(),
            model: self.model.clone(),
            provider: self.provider.clone(),
            reasoning: self.reasoning.clone(),
        })
    }

    fn can_resume(&self) -> bool {
        self.native_id.is_some()
            && self.native_path.is_some()
            && self.close_request.is_none()
            && !matches!(
                self.state.as_str(),
                "closed" | "process_lost" | "failed" | "resuming" | "rewinding"
            )
    }

    fn waiting_start(&self) -> Option<String> {
        if !matches!(self.state.as_str(), "pending" | "waiting_for_files")
            || self.close_request.is_some()
            || self.native_id.is_some()
            || self.harness_pid.is_some()
        {
            return None;
        }
        self.receipts
            .values()
            .find(|r| r.command == "start" && r.state == "accepted")
            .map(|r| r.request_id.clone())
    }

    fn interrupt_pending(&self) -> bool {
        self.receipts.values().any(|receipt| {
            matches!(receipt.command.as_str(), "interrupt" | "stop")
                && matches!(receipt.state.as_str(), "accepted" | "delivered" | "unknown")
        })
    }
}

#[derive(Debug)]
pub enum Error {
    NotFound,
    Conflict(&'static str),
    Storage,
}
impl From<io::Error> for Error {
    fn from(_: io::Error) -> Self {
        Self::Storage
    }
}
type Result<T> = std::result::Result<T, Error>;

struct Local {
    journal: Journal,
    sessions: BTreeMap<String, Session>,
    // Status snapshots must not clone the potentially large replay index.
    sequences: BTreeMap<String, Vec<u64>>,
    changed: watch::Sender<u64>,
    database_available: bool,
}

pub struct Manager {
    config: Config,
    local: Mutex<Local>,
    history: history::History,
    pub(crate) observability: Observability,
    pub(crate) storage: Guard,
    pub(crate) workspaces: crate::workspace::Workspaces,
    pub(crate) sync: crate::sync::Sync,
    codex_models: tokio::sync::Mutex<Option<(Instant, Vec<runtime::Model>)>>,
    stopping: AtomicBool,
    startups: Semaphore,
}

impl Local {
    fn finish_receipt(&mut self, id: &str, request: &str, state: &str) -> Result<()> {
        let mut receipt = self
            .sessions
            .get(id)
            .and_then(|s| s.receipts.get(request))
            .cloned()
            .ok_or(Error::NotFound)?;
        if matches!(
            receipt.state.as_str(),
            "completed" | "interrupted" | "failed" | "unknown_after_restart"
        ) || receipt.state == state
        {
            return Ok(());
        }
        receipt.state = state.into();
        self.append(
            id,
            "receipt",
            serde_json::to_value(receipt).map_err(io::Error::other)?,
            None,
        )?;
        Ok(())
    }

    fn fail_pending(&mut self, id: &str) -> Result<()> {
        for request in self.sessions[id].queue.clone() {
            self.finish_receipt(id, &request, "failed")?;
        }
        Ok(())
    }

    fn apply(&mut self, record: &Record) -> io::Result<()> {
        let session = self
            .sessions
            .entry(record.session_id.clone())
            .or_insert_with(|| Session {
                session_id: record.session_id.clone(),
                state: "starting".into(),
                ..Session::default()
            });
        session.last_sequence = record.sequence;
        session.last_activity = record.timestamp_ms.or(session.last_activity);
        match record.kind.as_str() {
            "receipt" => {
                let receipt = serde_json::from_value::<Receipt>(record.data.clone())?;
                if receipt.command == "start" {
                    session.parent_session =
                        receipt.input["parent_session"].as_str().map(str::to_owned);
                    if session.parent_session.is_some()
                        && receipt.state == "accepted"
                        && !session.receipts.contains_key(&receipt.request_id)
                    {
                        let task = Receipt {
                            request_id: receipt.request_id.replacen("child_", "task_", 1),
                            command: "prompt".into(),
                            input: json!({"text":receipt.input["prompt"],"reasoning":receipt.input["reasoning"]}),
                            state: "accepted".into(),
                            model: None,
                            provider: None,
                            workspace: None,
                            error: None,
                        };
                        prompt::apply(&mut session.prompts, &task);
                        session.queue.push(task.request_id.clone());
                        session.receipts.insert(task.request_id.clone(), task);
                        session.state = "pending".into();
                    }
                    session.workspace = receipt.workspace.clone().or(session.workspace.clone());
                    if receipt.state == "accepted"
                        && !session.receipts.contains_key(&receipt.request_id)
                    {
                        session.state = "pending".into();
                    }
                    // Completing this receipt must not erase a default captured after it was accepted.
                    if let Some(level) = receipt.input["reasoning"].as_str() {
                        session.reasoning = Some(level.to_owned());
                    }
                    if receipt.model.is_some() {
                        session.model = receipt.model.clone();
                    }
                    if receipt.provider.is_some() {
                        session.provider = receipt.provider.clone();
                    }
                    session.harness = serde_json::from_value(
                        receipt
                            .input
                            .get("harness")
                            .cloned()
                            .unwrap_or(json!("codex")),
                    )?;
                }
                if receipt.state == "accepted"
                    && !session.receipts.contains_key(&receipt.request_id)
                {
                    if receipt.command == "stop" {
                        session.queue_paused = true;
                    }
                    if receipt.command == "resume" {
                        session.queue_paused = false;
                    }
                }
                if receipt.command == "close" {
                    session.close_request = Some(receipt.request_id.clone());
                }
                if receipt.command == "prompt" {
                    // Acceptance is also queue insertion: one fsynced record, including
                    // legacy receipts whose separate enqueue write never finished.
                    if receipt.state == "accepted"
                        && !session.receipts.contains_key(&receipt.request_id)
                    {
                        session.queue.push(receipt.request_id.clone());
                        session.recovery_attempted = false; // A new user request permits one recovery.
                    }
                    if !matches!(receipt.state.as_str(), "accepted" | "running" | "delivered") {
                        session.queue.retain(|id| id != &receipt.request_id);
                        session.last_prompt_state = Some(receipt.state.clone());
                    }
                    if matches!(receipt.state.as_str(), "completed" | "interrupted") {
                        session.recovery_attempted = false; // Healthy progress permits a later recovery.
                    }
                }
                if receipt.command == "cancel"
                    && matches!(receipt.state.as_str(), "accepted" | "completed")
                    && let Some(target) = receipt.input["target_request_id"].as_str()
                {
                    session.queue.retain(|id| id != target);
                }
                if receipt.command == "compact" {
                    let terminal = matches!(
                        receipt.state.as_str(),
                        "completed"
                            | "failed"
                            | "interrupted"
                            | "unknown"
                            | "unknown_after_restart"
                    );
                    session.compacting = !terminal
                        && matches!(
                            receipt.state.as_str(),
                            "accepted" | "delivered" | "running" | "unknown"
                        );
                    session.compact_request = (!terminal).then(|| receipt.request_id.clone());
                }
                if receipt.command == "rewind" && receipt.state == "accepted" {
                    session.rewind_request = Some(receipt.request_id.clone());
                    session.state = "rewinding".into();
                }
                prompt::apply(&mut session.prompts, &receipt);
                session.receipts.insert(receipt.request_id.clone(), receipt);
            }
            "native_identity" => {
                let id = record.data["id"]
                    .as_str()
                    .ok_or_else(|| io::Error::other("missing native identity"))?;
                if session.native_id.as_deref().is_some_and(|old| old != id)
                    && record.data["replaced"] != true
                {
                    return Err(io::Error::other("native identity changed"));
                }
                session.native_id = Some(id.into());
                if let Some(capabilities) = record.data.get("capabilities") {
                    session.capabilities = capabilities.clone();
                }
                if let Some(model) = record.data["model"].as_str() {
                    session.model = Some(model.into());
                }
                if let Some(provider) = record.data["provider"].as_str() {
                    session.provider = Some(provider.into());
                }
                if let Some(path) = record.data["path"].as_str() {
                    session.native_path = Some(path.into());
                }
            }
            "native_record" => {
                session.native_cursor = runtime::checkpoint(
                    session.harness,
                    &session.native_cursor,
                    &record.data,
                    record.native.as_deref(),
                )?;
                session.native_offset = session.native_cursor["offset"].as_u64().unwrap_or(0);
            }
            "state" => {
                session.state = record.data["state"].as_str().unwrap_or("unknown").into();
                if session.state == "resuming" {
                    session.recovery_attempted = true;
                }
                if let Some(error) = record.data["startup_error"].as_str() {
                    session.startup_error = Some(error.into());
                } else if record.data["reason"] == "native resume or process cleanup failed" {
                    session.startup_error = Some("resume_failed".into());
                } else if session.state == "idle" {
                    session.startup_error = None;
                }
                session.current_request = record.data["request_id"].as_str().map(str::to_owned);
                session.current_turn = record.data["turn_id"].as_str().map(str::to_owned);
                // Dispatching a queued prompt removes it from the pending queue.
                if session.state == "starting_turn"
                    && let Some(request) = record.data["request_id"].as_str()
                {
                    session.has_dispatched = true;
                    session.queue.retain(|id| id != request);
                }
            }
            "enqueue" => {} // Legacy queue membership is already reconstructed from its receipt.
            "harness" => {
                session.harness_pid = record.data["pid"].as_u64().map(|pid| pid as u32);
            }
            "launch_reasoning" => {
                if session.reasoning.is_none() {
                    session.reasoning = record.data["reasoning"].as_str().map(str::to_owned);
                }
            }
            "workspace" => session.workspace = Some(serde_json::from_value(record.data.clone())?),
            "checkpoint" => session.checkpoint = Some(record.data.clone()),
            "rewind" => {
                if let Some(id) = record.data["id"].as_str() {
                    session.native_id = Some(id.into());
                }
                if let Some(path) = record.data["path"].as_str() {
                    session.native_path = Some(path.into());
                }
                session.native_cursor = record.data["cursor"].clone();
                session.native_offset = session.native_cursor["offset"].as_u64().unwrap_or(0);
                session.checkpoint = None;
                session.rewind_request = None;
                session.state = "idle".into();
                if let Some(request) = record.data["request_id"].as_str()
                    && let Some(receipt) = session.receipts.get_mut(request)
                {
                    receipt.state = "completed".into();
                }
                if let Some(replacement) = record.data.get("replacement").filter(|v| !v.is_null()) {
                    let receipt: Receipt = serde_json::from_value(replacement.clone())?;
                    if !session.receipts.contains_key(&receipt.request_id) {
                        prompt::apply(&mut session.prompts, &receipt);
                        session.queue.push(receipt.request_id.clone());
                        session.receipts.insert(receipt.request_id.clone(), receipt);
                    }
                }
            }
            "rewind_failed" => {
                session.rewind_request = None;
                session.state = "idle".into();
            }
            "storage_warning" => session.storage_warned = true,
            "storage_pause" => session.storage_paused = record.data["paused"] == true,
            "storage_recovered" => {
                session.storage_warned = false;
                session.storage_paused = false;
            }
            _ => {}
        }
        self.sequences
            .entry(record.session_id.clone())
            .or_default()
            .push(record.sequence);
        Ok(())
    }

    fn append(
        &mut self,
        session: &str,
        kind: &str,
        data: Value,
        native: Option<String>,
    ) -> io::Result<Record> {
        if kind == "native_record" {
            let current = self
                .sessions
                .get(session)
                .ok_or_else(|| io::Error::other("native record has no session"))?;
            runtime::checkpoint(
                current.harness,
                &current.native_cursor,
                &data,
                native.as_deref(),
            )?;
        }
        let record = Record {
            sequence: self.journal.last() + 1,
            session_id: session.into(),
            kind: kind.into(),
            timestamp_ms: Some(now_ms()),
            data,
            native,
        };
        self.journal.append(&serde_json::to_vec(&record)?)?;
        self.apply(&record)?;
        self.changed.send_replace(record.sequence);
        Ok(record)
    }
}

impl Manager {
    pub fn open(config: Config) -> io::Result<Arc<Self>> {
        let journal = Journal::open(&config.state_dir)?;
        let (changed, _) = watch::channel(journal.last());
        let mut local = Local {
            journal,
            sessions: BTreeMap::new(),
            sequences: BTreeMap::new(),
            changed,
            database_available: false,
        };
        for sequence in 1..=local.journal.last() {
            let record: Record = serde_json::from_slice(&local.journal.read(sequence)?)?;
            if record.sequence != sequence {
                return Err(io::Error::other("journal sequence mismatch"));
            }
            local.apply(&record)?;
        }
        // Resume conversations, not uncertain actions. An interrupted resume attempt
        // is left failed instead of creating a service-restart recovery loop.
        for session in local.sessions.values().cloned().collect::<Vec<_>>() {
            if session.workspace.is_none() && !session.receipts.is_empty() {
                local.append(
                    &session.session_id,
                    "workspace",
                    json!({"id":"legacy","path":config.repository}),
                    None,
                )?;
            }
            if session.waiting_start().is_some() {
                continue;
            }
            let eligible =
                session.can_resume() || session.startup_error.as_deref() == Some("startup_timeout");
            for mut receipt in session.receipts.values().cloned() {
                let terminal = matches!(
                    receipt.state.as_str(),
                    "completed" | "interrupted" | "failed" | "unknown" | "unknown_after_restart"
                );
                if terminal {
                    continue;
                }
                let queued =
                    receipt.command == "prompt" && session.queue.contains(&receipt.request_id);
                if !eligible || !queued {
                    receipt.state = if queued {
                        "failed"
                    } else {
                        "unknown_after_restart"
                    }
                    .into();
                    local.append(
                        &session.session_id,
                        "receipt",
                        serde_json::to_value(receipt)?,
                        None,
                    )?;
                }
            }
            if !matches!(session.state.as_str(), "closed" | "process_lost" | "failed") {
                local.append(
                    &session.session_id,
                    "state",
                    json!({"state":if eligible { "suspended" } else { "process_lost" }}),
                    None,
                )?;
            }
        }
        for session in local.sessions.values().cloned().collect::<Vec<_>>() {
            if let Some(saved) = session.resume()
                && let Some(profile) = config.harnesses.get(&session.harness)
                && runtime::recover_records(
                    profile,
                    session.harness,
                    &saved,
                    config.storage.as_ref(),
                    |event| {
                        if let runtime::Event::Record { kind, data, native } = event {
                            local.append(&session.session_id, kind, data, native)?;
                        }
                        Ok(())
                    },
                )
                .is_err()
            {
                local.append(
                    &session.session_id,
                    "native_history_unavailable",
                    json!({"reason":"native history missing, changed or unreadable"}),
                    None,
                )?;
                if session.can_resume() {
                    local.append(
                        &session.session_id,
                        "state",
                        json!({"state":"process_lost"}),
                        None,
                    )?;
                    local
                        .fail_pending(&session.session_id)
                        .map_err(|_| io::Error::other("cannot record native recovery failure"))?;
                }
            }
        }
        let storage = Guard::new(&config)?;
        let workspaces = crate::workspace::Workspaces::open(&config)?;
        let sync = crate::sync::Sync::open(&config)?;
        let history = history::History::new(&config)?;
        let observability = Observability::start(&config, history.pool.clone());
        Ok(Arc::new(Self {
            config,
            local: Mutex::new(local),
            history,
            observability,
            storage,
            workspaces,
            sync,
            codex_models: tokio::sync::Mutex::new(None),
            stopping: AtomicBool::new(false),
            // Bound cold-start CPU contention, not the number of running sessions.
            startups: Semaphore::new(
                std::thread::available_parallelism().map_or(1, |n| (n.get() / 2).max(1)),
            ),
        }))
    }

    pub fn is_stopping(&self) -> bool {
        self.stopping.load(Ordering::Relaxed)
    }

    pub fn subscribe(&self) -> watch::Receiver<u64> {
        self.local.lock().unwrap().changed.subscribe()
    }

    pub(crate) fn recording_available(&self) -> bool {
        self.local.lock().unwrap().journal.writable()
    }

    pub async fn ready(&self) -> bool {
        !self.is_stopping()
            && !self.storage.blocks()
            && self.recording_available()
            && self.history.ready().await
    }

    pub fn dashboard(&self) -> Value {
        let (sampled_at, resources) = self.observability.resources();
        let local = self.local.lock().unwrap();
        let mut sessions: Vec<_> = local.sessions.values().collect();
        sessions.sort_unstable_by_key(|s| std::cmp::Reverse(s.last_sequence));
        // Only a safe summary leaves this endpoint, never receipts, prompts or native records.
        let summaries: Vec<_> = sessions
            .iter()
            .take(1000)
            .map(|s| {
                let state = if s.storage_paused {
                    "waiting"
                } else {
                    match s.state.as_str() {
                        "starting" | "resuming" => "queued",
                        "starting_turn" | "running" | "interrupting" | "closing" => "working",
                        "idle" => match s.last_prompt_state.as_deref() {
                            Some("failed") => "failed",
                            Some("unknown" | "unknown_after_restart") => "unknown",
                            _ => "waiting",
                        },
                        "closed" => "stopped",
                        "failed" | "process_lost" => "failed",
                        _ => "unknown",
                    }
                };
                json!({"id":s.session_id,"title":format!("{:?} session",s.harness),
                "repository":s.workspace.as_ref().map(|w| &w.path).unwrap_or(&self.config.repository).file_name().map(|n| n.to_string_lossy()),
                "harness":s.harness,"model":s.model,"state":state,
                "activity":if state == "waiting" { "waiting" } else { "unknown" },
                "lastActivity":s.last_activity})
            })
            .collect();
        json!({"version":1,"sampledAt":sampled_at,
            "runtime":{"ready":!self.is_stopping() && !self.storage.blocks() && local.journal.writable(),"version":env!("CARGO_PKG_VERSION"),
                "configured":!self.config.harnesses.is_empty()},
            "appConnectivity":"unknown","resources":resources,
            "onboarding":{"localConnected":null,"offlineTaskVerified":null},
            "capabilities":{"settings":true,"updates":false},
            "sessionCount":sessions.len(),"sessions":summaries})
    }

    async fn codex_models(&self) -> Result<Vec<runtime::Model>> {
        let mut cached = self.codex_models.lock().await;
        if let Some((checked, models)) = &*cached
            && checked.elapsed() < Duration::from_secs(60)
        {
            return Ok(models.clone());
        }
        let (result, exit) = runtime::codex_models(&self.config).await;
        if let Some(runtime::Event::Exited {
            reason,
            expected,
            details,
            ..
        }) = exit
        {
            self.observability
                .agent_exit(None, runtime::Kind::Codex, reason, expected, details);
        }
        let models = result.map_err(|_| Error::Conflict("model catalog unavailable"))?;
        *cached = Some((Instant::now(), models.clone()));
        Ok(models)
    }

    pub async fn capabilities(&self) -> Value {
        let mut harnesses = Vec::new();
        for (kind, profile) in &self.config.harnesses {
            let mut harness = json!({"id":kind,"model":profile.model,"provider":profile.provider});
            match kind {
                runtime::Kind::Codex => {
                    harness["models"] = match self.codex_models().await {
                        Ok(models) => json!(models),
                        Err(_) => Value::Null,
                    };
                    harness["steer"] = json!(true);
                    harness["compact"] = json!(true);
                    harness["service_tier"] = json!(true);
                    harness["rewind"] = json!(true);
                    harness["attachments"] = json!({"images":true,"files":true});
                    harness["subagents"] = json!(true);
                    harness["usage"] = json!(true);
                }
                runtime::Kind::Pi => {
                    harness["reasoning_levels"] = json!(runtime::PI_REASONING_LEVELS);
                    harness["provider_selection"] = json!(true);
                    harness["steer"] = json!(true);
                    harness["compact"] = json!(true);
                    harness["service_tier"] = json!(false);
                    harness["rewind"] = json!(true);
                    harness["attachments"] = json!({"images":true,"files":true});
                    harness["subagents"] = json!(true);
                    harness["usage"] = json!(true);
                }
            }
            harnesses.push(harness);
        }
        json!({"version":1,"repository":self.config.repository,"harnesses":harnesses,
            "stop":true,"resume":true,"launch_settings":true,"prompt_reasoning":true,"workspaces":true,"sync":true,"direct_workspaces":true,
            "structured_prompt":true,"queue_edit":true,"queue_cancel":true,"steer":true,"rewind":true,
            "attachments":true,"compact":true,"usage":true,"subagents":true})
    }

    pub fn saving(&self) -> Value {
        let local = self.local.lock().unwrap();
        json!({"locally_recorded_through":local.journal.last(),"externally_saved_through":local.journal.saved(),
            "pending_records":local.journal.last()-local.journal.saved(),"last_upload_succeeded":local.database_available})
    }

    pub async fn records(&self, session: &str, after: u64) -> Result<Vec<Record>> {
        {
            let local = self.local.lock().unwrap();
            if let Some(sequences) = local.sequences.get(session) {
                return sequences[sequences.partition_point(|id| *id <= after)..]
                    .iter()
                    .take(256)
                    .map(|sequence| {
                        let record: Record =
                            serde_json::from_slice(&local.journal.read(*sequence)?)
                                .map_err(io::Error::other)?;
                        record.for_client()
                    })
                    .collect();
            }
        }
        self.history
            .read(session, after)
            .await
            .map_err(|_| Error::Storage)?
            .into_iter()
            .map(Record::for_client)
            .collect()
    }

    pub async fn session(&self, id: &str) -> Result<Session> {
        if let Some(session) = self.local.lock().unwrap().sessions.get(id).cloned() {
            return Ok(session);
        }
        self.history
            .summary(id, None)
            .await
            .map_err(|_| Error::Storage)?
            .0
            .ok_or(Error::NotFound)
    }

    pub fn recovery_check(&self, session: &Session) -> Value {
        let (status, message) = if session.close_request.is_some()
            || session.state == "closed"
            || session.state == "saved_history_only"
        {
            (
                "unavailable",
                "This session is closed or has saved history only",
            )
        } else if let Some(saved) = session.resume()
            && let Some(profile) = self.config.harnesses.get(&session.harness)
        {
            match runtime::check_resume_history(profile, &saved, self.config.storage.as_ref()) {
                Ok(()) => (
                    "ready",
                    "Saved conversation is available; the native resume handshake is still required",
                ),
                Err(error)
                    if error.kind() == io::ErrorKind::NotFound && !session.has_dispatched =>
                {
                    (
                        "empty",
                        "No turn was dispatched and no saved conversation exists; there is nothing to resume",
                    )
                }
                Err(error) if error.kind() == io::ErrorKind::NotFound => (
                    "missing",
                    "Saved conversation is missing; do not create a replacement or replay work",
                ),
                Err(_) => ("unavailable", "Saved conversation could not be read safely"),
            }
        } else {
            (
                "unavailable",
                "No saved native conversation identity is available",
            )
        };
        json!({"status":status,"message":message})
    }

    fn retry(
        session: &Session,
        request: &str,
        command: &str,
        input: &Value,
    ) -> Result<Option<Receipt>> {
        match session.receipts.get(request) {
            Some(receipt) if receipt.command != command || receipt.input != *input => {
                Err(Error::Conflict("request_id already has different content"))
            }
            other => Ok(other.cloned()),
        }
    }

    pub async fn start(
        self: &Arc<Self>,
        request: String,
        harness: Option<runtime::Kind>,
        model: Option<String>,
        reasoning: Option<String>,
        (workspace, workspace_name): (Option<String>, Option<String>),
        provider: Option<String>,
    ) -> Result<(String, Receipt)> {
        let id = format!("cr_{request}");
        let kind = harness
            .or_else(|| {
                self.local
                    .lock()
                    .unwrap()
                    .sessions
                    .get(&id)
                    .map(|s| s.harness)
            })
            .unwrap_or(self.config.default_harness);
        let mut input = if kind == runtime::Kind::Codex {
            json!({})
        } else {
            json!({"harness":kind})
        };
        if let Some(model) = &model {
            input["model"] = json!(model);
        }
        if let Some(provider) = &provider {
            if kind != runtime::Kind::Pi {
                return Err(Error::Conflict("provider selection requires Pi"));
            }
            input["provider"] = json!(provider);
        }
        if let Some(reasoning) = &reasoning {
            input["reasoning"] = json!(reasoning);
        }
        if let Some(workspace) = &workspace {
            crate::workspace::valid_id(workspace)
                .map_err(|_| Error::Conflict("invalid workspace"))?;
            input["workspace"] = json!(workspace);
        }
        if let Some(name) = &workspace_name {
            if workspace.is_none() || crate::workspace::valid_name(name).is_err() {
                return Err(Error::Conflict("invalid workspace"));
            }
            input["workspace_name"] = json!(name);
        }
        if let Some(session) = self.local.lock().unwrap().sessions.get(&id) {
            return Self::retry(session, &request, "start", &input)?
                .map(|r| (id.clone(), r))
                .ok_or(Error::Conflict("session already exists"));
        }
        if !self.recording_available() {
            return Err(Error::Storage);
        }
        if self.storage.blocks() {
            return Err(Error::Conflict("storage unsafe; new execution is blocked"));
        }
        if self.config.harnesses.is_empty() {
            return Err(Error::Conflict("agent setup is incomplete"));
        }
        // Fresh-state retries must not recreate an externally saved session.
        let (external, receipt) = self
            .history
            .summary(&id, Some(&request))
            .await
            .map_err(|_| Error::Storage)?;
        if let Some(external) = external {
            if harness.is_some_and(|kind| kind != external.harness) {
                return Err(Error::Conflict("request_id already has different content"));
            }
            let receipt =
                receipt.ok_or(Error::Conflict("saved session has no matching receipt"))?;
            if receipt.input != input {
                return Err(Error::Conflict("request_id already has different content"));
            }
            return Ok((id, receipt));
        }
        if !self.config.harnesses.contains_key(&kind) {
            return Err(Error::Conflict("harness is not configured"));
        }
        if let Some(reasoning) = &reasoning {
            let supported = match kind {
                runtime::Kind::Codex => {
                    let models = self.codex_models().await?;
                    let selected = model
                        .as_ref()
                        .unwrap_or(&self.config.harnesses[&kind].model);
                    let entry = models
                        .iter()
                        .find(|entry| &entry.model == selected)
                        .ok_or(Error::Conflict("invalid model"))?;
                    entry.reasoning_levels.contains(reasoning)
                }
                runtime::Kind::Pi => runtime::PI_REASONING_LEVELS.contains(&reasoning.as_str()),
            };
            if !supported {
                return Err(Error::Conflict("invalid reasoning effort"));
            }
        }
        let workspace = match workspace {
            Some(id) => self
                .workspaces
                .resolve(&id, workspace_name.as_deref())
                .map_err(|_| Error::Conflict("workspace mapping unavailable"))?,
            None => crate::workspace::Workspace {
                id: "legacy".into(),
                path: self.config.repository.clone(),
                parent: None,
            },
        };
        let receipt = Receipt {
            request_id: request.clone(),
            command: "start".into(),
            input,
            state: "accepted".into(),
            model: Some(model.unwrap_or_else(|| self.config.harnesses[&kind].model.clone())),
            provider: provider.or_else(|| self.config.harnesses[&kind].provider.clone()),
            workspace: Some(workspace),
            error: None,
        };
        {
            let mut local = self.local.lock().unwrap();
            if let Some(session) = local.sessions.get(&id) {
                return Self::retry(session, &request, "start", &receipt.input)?
                    .map(|r| (id.clone(), r))
                    .ok_or(Error::Conflict("session already exists"));
            }
            if self.is_stopping() {
                return Err(Error::Conflict("service is stopping"));
            }
            if self.storage.blocks() {
                return Err(Error::Conflict("storage unsafe; new execution is blocked"));
            }
            local.append(
                &id,
                "receipt",
                serde_json::to_value(&receipt).map_err(io::Error::other)?,
                None,
            )?;
        }
        let manager = self.clone();
        let session = id.clone();
        tokio::spawn(async move {
            manager.launch(session, request).await;
        });
        Ok((id, receipt))
    }

    async fn launch(self: Arc<Self>, id: String, request: String) {
        let started = Instant::now();
        let Ok(_startup) = self.startups.acquire().await else {
            return;
        };
        let workspace = loop {
            let workspace = {
                let local = self.local.lock().unwrap();
                let session = &local.sessions[&id];
                if self.is_stopping() || session.close_request.is_some() {
                    return;
                }
                session.workspace.clone()
            };
            if !self.storage.blocks() {
                break workspace;
            }
            tokio::time::sleep(Duration::from_millis(500)).await;
        };
        if self.is_stopping() {
            return;
        }
        let mut failure_reason = "Cloud folder could not be opened or created; check its permissions, symlinks, and available disk space";
        let result = async {
            let workspace = workspace.ok_or_else(|| io::Error::other("session workspace missing"))?;
            self.workspaces.ensure_directory(&workspace).await?;
            failure_reason = "Harness initialization failed; inspect native history and protected local harness diagnostics for this session";
            let (handle, events) = {
                let mut local = self.local.lock().unwrap();
                if self.is_stopping() {
                    return Err(io::Error::other("service is stopping"));
                }
                if local.sessions[&id].close_request.is_some() { return Ok(()); }
                let session = &local.sessions[&id];
                let mut config = self.config.clone();
                failure_reason = "The selected harness is no longer configured";
                let profile = config.harnesses.get_mut(&session.harness)
                    .ok_or_else(|| io::Error::other(failure_reason))?;
                if let Some(model) = &session.model { profile.model = model.clone(); }
                if let Some(provider) = &session.provider { profile.provider = Some(provider.clone()); }
                failure_reason = "Harness initialization failed; inspect native history and protected local harness diagnostics for this session";
                config.repository = session.workspace.as_ref().ok_or_else(|| io::Error::other("session workspace missing"))?.path.canonicalize()?;
                let (kind, reasoning) = (session.harness, session.reasoning.clone());
                // Claim before spawning: a crash after this point must not repeat uncertain execution.
                local.append(&id, "state", json!({"state":"starting"}), None)?;
                let (handle, events) = runtime::Handle::spawn(&config, kind, None)?;
                let handle = handle.with_reasoning(reasoning);
                let pid = handle.pid();
                local.sessions.get_mut(&id).unwrap().handle = Some(handle.clone());
                local.append(&id, "harness", json!({"pid":pid}), None)?;
                (handle, events)
            };
            self.watch_events(id.clone(), handle.clone(), events);
            let native = handle.start_session().await?;
            let mut local = self.local.lock().unwrap();
            self.remember_launch_reasoning(&mut local, &id, &handle)?;
            local.append(
                &id,
                "native_identity",
                json!({"id":native,"model":handle.model(),"provider":handle.provider(),"capabilities":handle.capabilities()}),
                None,
            )?;
            if local.sessions[&id].handle.is_none() {
                return Err(io::Error::other("harness exited during initialization"));
            }
            local.sessions.get_mut(&id).unwrap().ready = true;
            local.append(&id, "state", json!({"state":"idle"}), None)?;
            local
                .finish_receipt(&id, &request, "completed")
                .map_err(|_| io::Error::other("cannot record startup"))?;
            // A stop accepted before the first harness existed only pauses the queue.
            let stops: Vec<_> = local.sessions[&id].receipts.values()
                .filter(|r| r.command == "stop" && r.state == "accepted")
                .map(|r| r.request_id.clone()).collect();
            for stop in stops {
                local.finish_receipt(&id, &stop, "completed")
                    .map_err(|_| io::Error::other("cannot record startup stop"))?;
            }
            self.advance(&mut local, &id, true);
            Ok::<_, io::Error>(())
        }
        .await;
        self.observability.record(Signal::AgentStart {
            session_id: id.clone(),
            success: result.is_ok(),
            duration_ms: elapsed_ms(started),
        });
        if result.is_err() {
            let mut local = self.local.lock().unwrap();
            let _ = local.finish_receipt(&id, &request, "failed");
            let _ = local.append(
                &id,
                "state",
                json!({"state":"failed","reason":failure_reason}),
                None,
            );
            let _ = local.fail_pending(&id);
            if let Some(handle) = local.sessions[&id].handle.as_ref() {
                handle.request_shutdown();
            }
        }
    }

    pub fn known_request(&self, id: &str, request: &str) -> bool {
        self.local
            .lock()
            .unwrap()
            .sessions
            .get(id)
            .is_some_and(|session| session.receipts.contains_key(request))
    }

    /// Follow-up reasoning is checked against the launch model. Omitted reasoning stays on the launch default.
    pub async fn check_prompt_reasoning(&self, id: &str, reasoning: Option<&str>) -> Result<()> {
        let Some(reasoning) = reasoning else {
            return Ok(());
        };
        let (kind, model) = {
            let local = self.local.lock().unwrap();
            let session = local.sessions.get(id).ok_or(Error::NotFound)?;
            let model = session.model.clone().or_else(|| {
                self.config
                    .harnesses
                    .get(&session.harness)
                    .map(|profile| profile.model.clone())
            });
            (session.harness, model)
        };
        let supported = match kind {
            runtime::Kind::Codex => {
                let models = self.codex_models().await?;
                let selected = model.ok_or(Error::Conflict("invalid model"))?;
                models
                    .iter()
                    .find(|entry| entry.model == selected)
                    .ok_or(Error::Conflict("invalid model"))?
                    .reasoning_levels
                    .iter()
                    .any(|level| level == reasoning)
            }
            runtime::Kind::Pi => runtime::PI_REASONING_LEVELS.contains(&reasoning),
        };
        if !supported {
            return Err(Error::Conflict("invalid reasoning effort"));
        }
        Ok(())
    }

    pub fn check_prompt_service_tier(&self, id: &str, tier: Option<&str>) -> Result<()> {
        let Some(tier) = tier else {
            return Ok(());
        };
        if tier != "default" && tier != "fast" {
            return Err(Error::Conflict("invalid service tier"));
        }
        let kind = self
            .local
            .lock()
            .unwrap()
            .sessions
            .get(id)
            .ok_or(Error::NotFound)?
            .harness;
        if kind != runtime::Kind::Codex && tier == "fast" {
            return Err(Error::Conflict("invalid service tier"));
        }
        Ok(())
    }

    pub fn receipt(&self, id: &str, request: &str) -> Option<Receipt> {
        self.local
            .lock()
            .unwrap()
            .sessions
            .get(id)
            .and_then(|session| session.receipts.get(request))
            .cloned()
    }

    pub fn command(
        self: &Arc<Self>,
        id: &str,
        request: String,
        command: &str,
        input: Value,
    ) -> Result<Receipt> {
        enum Next {
            None,
            Deliver(runtime::Handle, String),
            Interrupt(runtime::Handle, String),
            Steer(runtime::Handle, String, String),
            Compact(runtime::Handle, String),
            Rewind(runtime::Handle, Value, String),
            Close(Option<runtime::Handle>),
            Resume,
        }
        let (receipt, next) = {
            let mut local = self.local.lock().unwrap();
            let session = local.sessions.get(id).ok_or(Error::NotFound)?;
            if let Some(receipt) = Self::retry(session, &request, command, &input)? {
                return Ok(receipt);
            }
            if !local.journal.writable() {
                return Err(Error::Storage);
            }
            if self.is_stopping() {
                return Err(Error::Conflict("service is stopping"));
            }
            if session.rewind_request.is_some() {
                return Err(Error::Conflict("wait for the pending rewind"));
            }
            if session.close_request.is_some()
                || (command != "close" && session.interrupt_pending())
            {
                return Err(Error::Conflict("wait for the pending close or interrupt"));
            }
            if command == "close" && session.state == "starting" {
                return Err(Error::Conflict("session is still starting"));
            }
            if !matches!(
                command,
                "prompt"
                    | "interrupt"
                    | "close"
                    | "stop"
                    | "resume"
                    | "edit"
                    | "cancel"
                    | "steer"
                    | "compact"
                    | "rewind"
                    | "attach"
            ) {
                return Err(Error::Conflict("unsupported command"));
            }
            let handle = session.handle.clone();
            let retry_startup = command == "resume"
                && session.state == "process_lost"
                && session.startup_error.is_some()
                && session.current_request.is_none()
                && handle.is_none()
                && session.harness_pid.is_none();
            if retry_startup && self.recovery_check(session)["status"] != "ready" {
                return Err(Error::Conflict(
                    "saved conversation is unavailable; inspect session recovery before retrying",
                ));
            }
            if matches!(session.state.as_str(), "closed" | "process_lost" | "failed")
                && !retry_startup
            {
                return Err(Error::Conflict(
                    "process unavailable; history does not restore execution",
                ));
            }
            // A prompt starts its native turn immediately only when the harness is
            // idle with nothing queued; otherwise it is accepted and queued in order.
            if command == "prompt" && self.storage.blocks() {
                return Err(Error::Conflict("storage unsafe; new execution is blocked"));
            }
            if command == "interrupt" && session.storage_paused {
                return Err(Error::Conflict(
                    "workload is paused for storage; close it or wait for recovery",
                ));
            }
            if matches!(command, "edit" | "cancel") {
                let target = input["target_request_id"].as_str().unwrap_or_default();
                let cancelled = session
                    .prompts
                    .get(target)
                    .is_some_and(|prompt| prompt.cancelled);
                if target.is_empty() {
                    return Err(Error::Conflict("queued message is no longer pending"));
                }
                if command == "edit" || !cancelled {
                    if !session.queue.contains(&target.to_owned()) {
                        return Err(Error::Conflict("queued message is no longer pending"));
                    }
                    if session.current_request.as_deref() == Some(target) {
                        return Err(Error::Conflict("queued message is already being sent"));
                    }
                }
                if command == "edit" && cancelled {
                    return Err(Error::Conflict("queued message is no longer pending"));
                }
                if command == "edit" {
                    let expected = input["expected_revision"].as_u64().unwrap_or(0);
                    let current = session
                        .prompts
                        .get(target)
                        .map(|prompt| prompt.revision)
                        .unwrap_or(1);
                    if expected != current {
                        return Err(Error::Conflict(
                            "queued message changed since editing began",
                        ));
                    }
                }
            }
            if command == "steer"
                && (!session.ready
                    || handle.is_none()
                    || session.current_request.as_deref() != input["target_request_id"].as_str())
            {
                return Err(Error::Conflict(
                    "steer target is no longer the active request",
                ));
            }
            if command == "compact" {
                if session.compacting {
                    return Err(Error::Conflict("compaction is already running"));
                }
                if session.current_request.is_some()
                    || !matches!(session.state.as_str(), "idle" | "failed")
                {
                    return Err(Error::Conflict(
                        "context can only be compacted while the thread is idle",
                    ));
                }
                if !session.ready || handle.is_none() {
                    return Err(Error::Conflict("harness is not ready for compaction"));
                }
            }
            if command == "rewind" {
                if local.sessions.values().any(|child| {
                    child.parent_session.as_deref() == Some(id)
                        && (child.current_request.is_some()
                            || !child.queue.is_empty()
                            || matches!(
                                child.state.as_str(),
                                "pending" | "starting" | "resuming" | "waiting_for_files"
                            ))
                }) {
                    return Err(Error::Conflict(
                        "wait for child agents before editing a message",
                    ));
                }
                if input["before"]
                    .as_str()
                    .filter(|id| !id.is_empty())
                    .is_none()
                    && input["last_turn_id"]
                        .as_str()
                        .filter(|id| !id.is_empty())
                        .is_none()
                {
                    return Err(Error::Conflict("a native rewind checkpoint is required"));
                }
                if let Some(replacement) = input.get("replacement") {
                    let key = replacement["request_id"]
                        .as_str()
                        .ok_or(Error::Conflict("replacement request ID required"))?;
                    if key == request || session.receipts.contains_key(key) {
                        return Err(Error::Conflict("replacement request ID already exists"));
                    }
                }
                if session.compacting {
                    return Err(Error::Conflict("wait for compaction"));
                }
                if !session.queue.is_empty() {
                    return Err(Error::Conflict(
                        "send or remove queued messages before editing a message",
                    ));
                }
                if session.current_request.is_some()
                    || !matches!(session.state.as_str(), "idle" | "failed")
                {
                    return Err(Error::Conflict(
                        "wait for the active turn to finish before editing a sent message",
                    ));
                }
                if !session.ready || handle.is_none() {
                    return Err(Error::Conflict("harness is not ready to rewind"));
                }
            }
            let deliver_now = command == "prompt"
                && session.ready
                && session.state == "idle"
                && !session.queue_paused
                && !session.compacting
                && session.queue.is_empty();
            if command == "interrupt"
                && (!session.ready
                    || handle.is_none()
                    || session.current_request.as_deref() != input["target_request_id"].as_str())
            {
                return Err(Error::Conflict(
                    "interrupt target is no longer the active request",
                ));
            }
            let target = session.current_request.clone();
            let receipt = Receipt {
                request_id: request.clone(),
                command: command.into(),
                input: input.clone(),
                state: "accepted".into(),
                model: None,
                provider: None,
                workspace: None,
                error: None,
            };
            local.append(
                id,
                "receipt",
                serde_json::to_value(&receipt).map_err(io::Error::other)?,
                None,
            )?;
            let next = match command {
                "prompt" if deliver_now => {
                    self.begin_turn(&mut local, id, &request)?;
                    Next::Deliver(handle.expect("ready harness"), request.clone())
                }
                "prompt" => Next::None,
                "edit" | "cancel" | "attach" => {
                    local.finish_receipt(id, &request, "completed")?;
                    Next::None
                }
                "resume" if retry_startup => {
                    self.schedule_resume(&mut local, id)?;
                    Next::None
                }
                "resume" => {
                    local.finish_receipt(id, &request, "completed")?;
                    Next::Resume
                }
                "stop" if target.is_none() || handle.is_none() => {
                    local.finish_receipt(id, &request, "completed")?;
                    Next::None
                }
                "steer" => Next::Steer(
                    handle.expect("validated live steer target"),
                    input["target_request_id"]
                        .as_str()
                        .expect("validated steer target")
                        .to_owned(),
                    request.clone(),
                ),
                "compact" => {
                    local.append(id, "state", json!({"state":"running"}), None)?;
                    Next::Compact(handle.expect("validated compact harness"), request.clone())
                }
                "rewind" => Next::Rewind(
                    handle.expect("validated rewind harness"),
                    input,
                    request.clone(),
                ),
                "close" => {
                    let session = &local.sessions[id];
                    let data = json!({"state":"closing","request_id":session.current_request,"turn_id":session.current_turn});
                    local.append(id, "state", data, None)?;
                    local.fail_pending(id)?;
                    if handle.is_none() {
                        local.append(id, "state", json!({"state":"closed"}), None)?;
                        local.finish_receipt(id, &request, "completed")?;
                    }
                    Next::Close(handle)
                }
                _ => Next::Interrupt(
                    handle.expect("validated live interrupt target"),
                    target.expect("validated target"),
                ),
            };
            (receipt, next)
        };
        match next {
            Next::None => {}
            Next::Resume => self.advance(&mut self.local.lock().unwrap(), id, false),
            Next::Close(handle) => {
                if let Some(handle) = handle {
                    handle.request_shutdown();
                }
            }
            Next::Deliver(handle, request) => self.clone().deliver(id.to_owned(), request, handle),
            Next::Steer(handle, target, request) => {
                self.clone().steer(id.to_owned(), request, target, handle)
            }
            Next::Compact(handle, request) => self.clone().compact(id.to_owned(), request, handle),
            Next::Rewind(handle, input, request) => {
                self.clone().rewind(id.to_owned(), request, input, handle)
            }
            Next::Interrupt(handle, target) => {
                let manager = self.clone();
                let id = id.to_owned();
                tokio::spawn(async move {
                    let result = handle.interrupt(&target).await;
                    let state = match &result {
                        Ok(_) => "completed",
                        Err(e) if e.kind() == io::ErrorKind::InvalidInput => "failed",
                        Err(_) => "unknown",
                    };
                    let mut local = manager.local.lock().unwrap();
                    if !local.sessions[&id]
                        .handle
                        .as_ref()
                        .is_some_and(|current| current.same_process(&handle))
                    {
                        return;
                    }
                    let _ = local.finish_receipt(&id, &request, state);
                    // A settled interrupt returns the session to idle or the next turn.
                    if matches!(state, "completed" | "failed") {
                        manager.advance(&mut local, &id, false);
                    }
                });
            }
        }
        Ok(receipt)
    }

    /// Pi's process keeps the last thinking level. Remember the level it started with
    /// so a later message that omits reasoning can put that default back.
    fn remember_launch_reasoning(
        &self,
        local: &mut Local,
        id: &str,
        handle: &runtime::Handle,
    ) -> io::Result<()> {
        if local.sessions[id].reasoning.is_some() {
            return Ok(());
        }
        let Some(level) = handle.launch_baseline() else {
            return Ok(());
        };
        local.append(id, "launch_reasoning", json!({"reasoning": level}), None)?;
        Ok(())
    }

    /// Record the start of a queued or immediate turn; dequeues the request.
    fn begin_turn(&self, local: &mut Local, id: &str, request: &str) -> io::Result<()> {
        local.append(
            id,
            "state",
            json!({"state":"starting_turn","request_id":request}),
            None,
        )?;
        Ok(())
    }

    /// Deliver one prompt to the harness and record the outcome. On rejection the
    /// turn never began, so advance to the next queued prompt.
    fn deliver(self: Arc<Self>, id: String, request: String, handle: runtime::Handle) {
        tokio::spawn(async move {
            let (input, reasoning) = {
                let local = self.local.lock().unwrap();
                let session = &local.sessions[&id];
                let input = prompt::latest(&session.prompts, &session.receipts, &request)
                    .cloned()
                    .unwrap_or_else(|| json!({"text":""}));
                let reasoning = prompt::reasoning(&input).or_else(|| session.reasoning.clone());
                (input, reasoning)
            };
            // The stored handle keeps the launch level. This copy is only for the turn being started.
            let handle = handle.with_reasoning(reasoning);
            let result = async {
                handle.prepare_turn().await?;
                handle.send_prompt(&request, &input).await
            }
            .await;
            let state = match &result {
                Ok(_) => "delivered",
                Err(e) if e.kind() == io::ErrorKind::InvalidInput => "failed",
                Err(_) => "unknown",
            };
            let mut local = self.local.lock().unwrap();
            if !local.sessions[&id]
                .handle
                .as_ref()
                .is_some_and(|current| current.same_process(&handle))
            {
                return;
            }
            if let Err(error) = &result
                && error.kind() == io::ErrorKind::InvalidInput
                && let Some(receipt) = local
                    .sessions
                    .get_mut(&id)
                    .and_then(|session| session.receipts.get_mut(&request))
            {
                receipt.error = Some(error.to_string());
            }
            let _ = local.finish_receipt(&id, &request, state);
            if result.is_err_and(|e| e.kind() == io::ErrorKind::InvalidInput)
                && local.sessions.get(&id).is_some_and(|s| {
                    s.current_request.as_deref() == Some(&request) && s.close_request.is_none()
                })
            {
                self.advance(&mut local, &id, true);
            }
        });
    }

    fn steer(
        self: Arc<Self>,
        id: String,
        request: String,
        target: String,
        handle: runtime::Handle,
    ) {
        tokio::spawn(async move {
            let text = {
                let local = self.local.lock().unwrap();
                local.sessions[&id]
                    .receipts
                    .get(&request)
                    .map(|receipt| {
                        receipt.input["text"]
                            .as_str()
                            .unwrap_or_default()
                            .to_owned()
                    })
                    .unwrap_or_default()
            };
            let result = handle.steer(&target, &text).await;
            let state = match &result {
                Ok(_) => "completed",
                Err(e) if e.kind() == io::ErrorKind::InvalidInput => "failed",
                Err(_) => "unknown",
            };
            let mut local = self.local.lock().unwrap();
            if !local.sessions[&id]
                .handle
                .as_ref()
                .is_some_and(|current| current.same_process(&handle))
            {
                return;
            }
            if let Err(error) = &result
                && error.kind() == io::ErrorKind::InvalidInput
                && let Some(receipt) = local
                    .sessions
                    .get_mut(&id)
                    .and_then(|session| session.receipts.get_mut(&request))
            {
                receipt.error = Some(error.to_string());
            }
            let _ = local.finish_receipt(&id, &request, state);
        });
    }

    fn compact(self: Arc<Self>, id: String, request: String, handle: runtime::Handle) {
        tokio::spawn(async move {
            let result = handle.compact().await;
            let state = match &result {
                Ok(_) => "delivered",
                Err(e) if e.kind() == io::ErrorKind::InvalidInput => "failed",
                Err(_) => "unknown",
            };
            let mut local = self.local.lock().unwrap();
            if !local.sessions[&id]
                .handle
                .as_ref()
                .is_some_and(|current| current.same_process(&handle))
            {
                return;
            }
            if let Err(error) = &result
                && error.kind() == io::ErrorKind::InvalidInput
                && let Some(receipt) = local
                    .sessions
                    .get_mut(&id)
                    .and_then(|session| session.receipts.get_mut(&request))
                && !matches!(
                    receipt.state.as_str(),
                    "completed" | "failed" | "interrupted"
                )
            {
                receipt.error = Some(error.to_string());
            }
            let _ = local.finish_receipt(&id, &request, state);
            if state == "failed" {
                self.advance(&mut local, &id, false);
            }
        });
    }

    fn rewind(self: Arc<Self>, id: String, request: String, input: Value, handle: runtime::Handle) {
        tokio::spawn(async move {
            let result = handle.rewind(&input).await;
            let mut local = self.local.lock().unwrap();
            if !local.sessions[&id]
                .handle
                .as_ref()
                .is_some_and(|current| current.same_process(&handle))
            {
                return;
            }
            match result {
                Ok(_) => {} // The adapter's ordered rewind_ready event commits the identity before capture or dispatch.
                Err(error) if error.kind() == io::ErrorKind::InvalidInput => {
                    if let Some(receipt) = local
                        .sessions
                        .get_mut(&id)
                        .and_then(|session| session.receipts.get_mut(&request))
                    {
                        receipt.error = Some(error.to_string());
                    }
                    let _ = local.finish_receipt(&id, &request, "failed");
                    let _ = local.append(&id, "rewind_failed", json!({"request_id":request}), None);
                }
                Err(_) => {
                    let _ = local.finish_receipt(&id, &request, "unknown");
                }
            }
        });
    }

    /// Move a ready session to its next turn: dispatch the next queued prompt, wait
    /// out a pending interrupt, or return to idle. `turn_ended` bypasses the active
    /// turn guard when the just-finished turn is the reason for advancing.
    fn advance(self: &Arc<Self>, local: &mut Local, id: &str, turn_ended: bool) {
        let session = &local.sessions[id];
        if session.close_request.is_some()
            || session.rewind_request.is_some()
            || !session.ready
            || self.is_stopping()
        {
            return;
        }
        if !turn_ended && session.current_request.is_some() {
            return; // a turn is still active; wait for its completion
        }
        if session.interrupt_pending() {
            let _ = local.append(id, "state", json!({"state":"interrupting"}), None);
            return;
        }
        if self.storage.blocks()
            || session.storage_paused
            || session.queue_paused
            || session.compacting
        {
            let state = if session.compacting {
                "running"
            } else {
                "idle"
            };
            let _ = local.append(id, "state", json!({"state":state}), None);
            return;
        }
        if let Some(next) = session.queue.first().cloned()
            && let Some(handle) = session.handle.clone()
            && self.begin_turn(local, id, &next).is_ok()
        {
            self.clone().deliver(id.to_owned(), next, handle);
            return;
        }
        let _ = local.append(id, "state", json!({"state":"idle"}), None);
    }

    fn snapshot_usage(self: Arc<Self>, id: String, handle: runtime::Handle) {
        tokio::spawn(async move {
            let Ok(Some(usage)) = handle.usage().await else {
                return;
            };
            let mut local = self.local.lock().unwrap();
            if !local.sessions.get(&id).is_some_and(|session| {
                session
                    .handle
                    .as_ref()
                    .is_some_and(|current| current.same_process(&handle))
            }) {
                return;
            }
            if usage["sessionId"]
                .as_str()
                .is_some_and(|native| local.sessions[&id].native_id.as_deref() != Some(native))
            {
                return;
            }
            let _ = local.append(&id, "usage", usage, None);
        });
    }

    /// Forward native events into the record path, shutting the harness if recording fails.
    fn watch_events(
        self: &Arc<Self>,
        id: String,
        handle: runtime::Handle,
        mut events: mpsc::Receiver<runtime::Event>,
    ) {
        let manager = self.clone();
        tokio::spawn(async move {
            while let Some(event) = events.recv().await {
                if manager.record_native(&id, &handle, event).is_err() {
                    if let Some(handle) = manager
                        .local
                        .lock()
                        .unwrap()
                        .sessions
                        .get(&id)
                        .and_then(|s| s.handle.clone())
                    {
                        handle.request_shutdown();
                    }
                    break;
                }
            }
        });
    }

    /// Reconcile the previous service's protected workloads before admitting starts.
    /// Native handshakes then run in the background, keeping reconnects responsive.
    pub async fn restore_all(self: &Arc<Self>) -> io::Result<()> {
        runtime::reconcile(&self.config).await?;
        let mut local = self.local.lock().unwrap();
        let ids: Vec<_> = local
            .sessions
            .values()
            .filter(|s| s.can_resume() && s.handle.is_none())
            .map(|s| s.session_id.clone())
            .collect();
        for id in ids {
            self.schedule_resume(&mut local, &id)
                .map_err(|_| io::Error::other("cannot save recovery state"))?;
        }
        for session in local.sessions.values() {
            if let Some(request) = session.waiting_start() {
                let (manager, id) = (self.clone(), session.session_id.clone());
                tokio::spawn(async move {
                    manager.launch(id, request).await;
                });
            }
        }
        Ok(())
    }

    fn schedule_resume(self: &Arc<Self>, local: &mut Local, id: &str) -> Result<()> {
        // Claim the attempt durably before spawning; a failed or interrupted attempt
        // cannot trigger an endless restart loop. Completed turns rearm recovery.
        local.append(id, "state", json!({"state":"resuming"}), None)?;
        let (manager, id) = (self.clone(), id.to_owned());
        tokio::spawn(async move {
            manager.relaunch(id).await;
        });
        Ok(())
    }

    async fn relaunch(self: Arc<Self>, id: String) {
        let Ok(_startup) = self.startups.acquire().await else {
            return;
        };
        let result = async {
            loop {
                {
                    let mut local = self.local.lock().unwrap();
                    if local.sessions[&id].close_request.is_some() {
                        return Ok(());
                    }
                    if self.is_stopping() {
                        local.append(&id, "state", json!({"state":"suspended"}), None)?;
                        return Ok(());
                    }
                }
                if !self.storage.blocks() {
                    break;
                }
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            let previous = self.local.lock().unwrap().sessions[&id].harness_pid;
            if self.config.storage.is_none()
                && let Some(pid) = previous
            {
                runtime::wait_for_exit(pid).await?;
            }
            let (handle, events, native) = {
                let mut local = self.local.lock().unwrap();
                let session = &local.sessions[&id];
                if session.close_request.is_some() || self.is_stopping() {
                    return Ok(());
                }
                let native = session
                    .native_id
                    .clone()
                    .ok_or_else(|| io::Error::other("missing native identity"))?;
                let kind = session.harness;
                let mut saved = session
                    .resume()
                    .ok_or_else(|| io::Error::other("missing native resume state"))?;
                let profile = self
                    .config
                    .harnesses
                    .get(&kind)
                    .ok_or_else(|| io::Error::other("harness not configured"))?;
                // Capture any final orphan writes after ownership reconciliation.
                runtime::recover_records(profile, kind, &saved, self.config.storage.as_ref(), |event| {
                    if let runtime::Event::Record { kind, data, native } = event {
                        local.append(&id, kind, data, native)?;
                    }
                    Ok(())
                })?;
                saved.cursor = local.sessions[&id].native_cursor.clone();
                let mut config = self.config.clone();
                config.repository = local.sessions[&id].workspace.as_ref().ok_or_else(|| io::Error::other("session workspace missing"))?.path.canonicalize()?;
                let (handle, events) = runtime::Handle::spawn(&config, kind, Some(saved))?;
                local.sessions.get_mut(&id).unwrap().handle = Some(handle.clone());
                local.append(&id, "harness", json!({"pid":handle.pid()}), None)?;
                (handle, events, native)
            };
            self.watch_events(id.clone(), handle.clone(), events);
            handle.start_session().await?;
            let mut local = self.local.lock().unwrap();
            self.remember_launch_reasoning(&mut local, &id, &handle)?;
            if local.sessions[&id].close_request.is_some() || self.is_stopping() {
                return Ok(());
            }
            if local.sessions[&id].handle.is_none() {
                return Err(io::Error::other("resumed harness exited"));
            }
            local.sessions.get_mut(&id).unwrap().ready = true;
            local.append(
                &id,
                "native_identity",
                json!({"id":native,"model":handle.model(),"provider":handle.provider(),"capabilities":handle.capabilities()}),
                None,
            )?;
            local.append(&id, "state", json!({"state":"idle"}), None)?;
            let retries: Vec<_> = local.sessions[&id].receipts.values()
                .filter(|r| r.command == "resume" && r.state == "accepted")
                .map(|r| r.request_id.clone()).collect();
            for request in retries {
                local.finish_receipt(&id, &request, "completed").map_err(|_| io::Error::other("cannot record resume outcome"))?;
            }
            if local.sessions[&id].storage_paused {
                local.append(
                    &id,
                    "storage_recovered",
                    json!({"text":"Cloudroom: workload resumed after restart."}),
                    None,
                )?;
            }
            self.advance(&mut local, &id, true);
            Ok::<_, io::Error>(())
        }
        .await;
        if let Err(error) = result {
            if self.is_stopping() {
                return;
            }
            let mut local = self.local.lock().unwrap();
            if local.sessions[&id].close_request.is_none() {
                let check = self.recovery_check(&local.sessions[&id]);
                let (code, reason) = if error.kind() == io::ErrorKind::TimedOut {
                    (
                        "startup_timeout",
                        "Harness startup timed out; saved history and queued prompts are preserved. Retry resume when load settles.",
                    )
                } else if check["status"] == "empty" {
                    (
                        "empty_session",
                        "No saved native conversation exists and no turn was dispatched; nothing can be resumed.",
                    )
                } else if check["status"] == "missing" {
                    (
                        "missing_history",
                        "Saved native conversation is missing; refusing to replace it or replay work.",
                    )
                } else {
                    (
                        "resume_failed",
                        "Native resume failed; inspect native history and protected local harness diagnostics for this session, then retry explicitly after correcting the cause.",
                    )
                };
                let _ = local.append(
                    &id,
                    "state",
                    json!({"state":"process_lost","reason":reason,"startup_error":code}),
                    None,
                );
                if code != "startup_timeout" {
                    let _ = local.fail_pending(&id);
                }
                let retries: Vec<_> = local.sessions[&id]
                    .receipts
                    .values()
                    .filter(|r| r.command == "resume" && r.state == "accepted")
                    .map(|r| r.request_id.clone())
                    .collect();
                for request in retries {
                    let _ = local.finish_receipt(&id, &request, "failed");
                }
            }
            if let Some(handle) = local.sessions[&id].handle.as_ref() {
                handle.request_shutdown();
            }
        }
    }

    fn record_native(
        self: &Arc<Self>,
        id: &str,
        handle: &runtime::Handle,
        event: runtime::Event,
    ) -> Result<()> {
        let mut local = self.local.lock().unwrap();
        if !local.sessions[id]
            .handle
            .as_ref()
            .is_some_and(|current| current.same_process(handle))
        {
            return Ok(());
        }
        match event {
            runtime::Event::ChildRequest(child) => {
                self.start_child(&mut local, id, handle, child)?;
            }
            runtime::Event::Record {
                kind: "rewind_ready",
                mut data,
                ..
            } => {
                let request = local.sessions[id]
                    .rewind_request
                    .clone()
                    .ok_or(Error::Conflict("unexpected native replacement"))?;
                let input = local.sessions[id].receipts[&request].input.clone();
                data["request_id"] = json!(request);
                data["before"] = input["before"].clone();
                if let Some(replacement) = input.get("replacement") {
                    data["replacement"] = json!(Receipt {
                        request_id: replacement["request_id"]
                            .as_str()
                            .ok_or(Error::Conflict("invalid replacement"))?
                            .into(),
                        command: "prompt".into(),
                        input: replacement["input"].clone(),
                        state: "accepted".into(),
                        model: None,
                        provider: None,
                        workspace: None,
                        error: None,
                    });
                }
                local.append(id, "rewind", data, None)?;
                self.advance(&mut local, id, true);
            }
            runtime::Event::Record { kind, data, native } => {
                let usage = data.get("usage").cloned().filter(|value| !value.is_null());
                local.append(id, kind, data, native)?;
                if let Some(usage) = usage {
                    let _ = local.append(id, "usage", usage, None);
                }
            }
            runtime::Event::Compacted { status } => {
                if let Some(request) = local.sessions[id].compact_request.clone() {
                    let status = match status.as_str() {
                        "completed" => "completed",
                        "interrupted" => "interrupted",
                        _ => "failed",
                    };
                    if status == "failed" {
                        local
                            .sessions
                            .get_mut(id)
                            .unwrap()
                            .receipts
                            .get_mut(&request)
                            .unwrap()
                            .error = Some("Context compaction failed; see native history".into());
                    }
                    local.finish_receipt(id, &request, status)?;
                    self.clone().snapshot_usage(id.to_owned(), handle.clone());
                    self.advance(&mut local, id, false);
                }
            }
            runtime::Event::Started {
                request,
                native_turn,
            } => {
                let session = &local.sessions[id];
                if !session.ready || session.current_request.as_deref() != Some(&request) {
                    return Ok(());
                }
                let state = if session.close_request.is_some() {
                    "closing"
                } else {
                    "running"
                };
                let data = json!({"state":state,"request_id":request,"turn_id":native_turn});
                local.append(id, "state", data, None)?;
            }
            runtime::Event::Finished { request, status } => {
                if local.sessions[id].current_request.as_deref() == Some(&request) {
                    let session = &local.sessions[id];
                    let checkpoint = session
                        .current_turn
                        .clone()
                        .map(|turn| json!({"kind":"turn","id":turn,"request_id":request}));
                    if let Some(checkpoint) = checkpoint {
                        let _ = local.append(id, "checkpoint", checkpoint, None);
                    }
                    self.clone().snapshot_usage(id.to_owned(), handle.clone());
                    if let Some(request) = local.sessions[id].current_request.clone() {
                        local.finish_receipt(id, &request, &status)?;
                    }
                    self.advance(&mut local, id, true);
                }
            }
            runtime::Event::Exited {
                reason,
                expected,
                cleaned_up,
                details,
            } => {
                let diagnostic_id = self.observability.agent_exit(
                    Some(id),
                    local.sessions[id].harness,
                    reason,
                    expected,
                    details,
                );
                let session = local.sessions.get_mut(id).ok_or(Error::NotFound)?;
                let restoring = session.state == "resuming"
                    && session.native_id.is_some()
                    && session.native_path.is_some();
                let resumable = cleaned_up
                    && ((session.ready && session.can_resume())
                        || (self.is_stopping() && restoring));
                let recover = resumable && !self.is_stopping() && !session.recovery_attempted;
                session.handle = None;
                session.ready = false;
                let request = session.current_request.clone();
                let close = session.close_request.clone();
                let preserve_queue = session.startup_error.as_deref() == Some("startup_timeout");
                let interrupts: Vec<_> = session
                    .receipts
                    .values()
                    .filter(|r| {
                        matches!(r.command.as_str(), "interrupt" | "stop")
                            && matches!(r.state.as_str(), "accepted" | "delivered" | "unknown")
                    })
                    .map(|r| r.request_id.clone())
                    .collect();
                if let Some(request) = request {
                    local.finish_receipt(
                        id,
                        &request,
                        if self.is_stopping() || close.is_some() {
                            "unknown"
                        } else {
                            "unknown_after_restart"
                        },
                    )?;
                }
                for request in interrupts {
                    local.finish_receipt(id, &request, "unknown_after_restart")?;
                }
                if cleaned_up {
                    local.append(id, "harness", json!({"pid":null}), None)?;
                }
                let state = if expected && close.is_some() {
                    "closed"
                } else if resumable && (self.is_stopping() || recover) {
                    "suspended"
                } else {
                    "process_lost"
                };
                local.append(
                    id,
                    "state",
                    json!({"state":state,"reason":reason,"diagnostic_id":diagnostic_id}),
                    None,
                )?;
                if let Some(request) = close {
                    local.finish_receipt(
                        id,
                        &request,
                        if expected { "completed" } else { "unknown" },
                    )?;
                }
                if state != "suspended" && !preserve_queue {
                    local.fail_pending(id)?;
                }
                if recover {
                    self.schedule_resume(&mut local, id)?;
                }
            }
        }
        Ok(())
    }

    pub async fn check_storage(&self) -> Snapshot {
        self.storage.refresh(&self.config, &self.workspaces).await
    }

    fn has_storage_warning(&self) -> bool {
        self.local
            .lock()
            .unwrap()
            .sessions
            .values()
            .any(|s| s.storage_warned && s.handle.is_some())
    }

    pub fn start_storage_guard(self: &Arc<Self>) {
        if self.config.storage.is_none() {
            return;
        }
        let manager = self.clone();
        tokio::spawn(async move {
            let mut last_cleanup = Instant::now() - Duration::from_secs(60);
            let mut was_blocked = manager.storage.blocks();
            while !manager.is_stopping() {
                let snapshot = manager.check_storage().await;
                if snapshot.level != Level::Normal {
                    let warnings = {
                        let mut local = manager.local.lock().unwrap();
                        let ids: Vec<_> = local
                            .sessions
                            .values()
                            .filter(|s| {
                                s.handle.is_some() && !s.storage_warned && s.native_id.is_some()
                            })
                            .map(|s| s.session_id.clone())
                            .collect();
                        let mut warnings = Vec::new();
                        for id in ids {
                            let text = if snapshot.level == Level::Blocked {
                                "Cloudroom: disk space is critically low or could not be measured. Work is paused until storage recovers. Your files and history are preserved."
                            } else {
                                "Cloudroom: disk space is running low. Work and sync continue. Remove disposable files or add storage; large writes may trigger an emergency pause."
                            };
                            // Persist before delivery. A timed-out native notification is not blindly repeated.
                            if local
                                .append(
                                    &id,
                                    "storage_warning",
                                    json!({"text":text,"reason":snapshot.reason}),
                                    None,
                                )
                                .is_ok()
                            {
                                let s = &local.sessions[&id];
                                warnings.push((id, s.handle.clone().unwrap(), text));
                            }
                        }
                        warnings
                    };
                    let mut tasks = tokio::task::JoinSet::new();
                    for (id, handle, text) in warnings {
                        tasks.spawn(async move {
                            (
                                id,
                                tokio::time::timeout(
                                    Duration::from_secs(2),
                                    handle.system_message(text),
                                )
                                .await
                                .is_ok_and(|r| r.is_ok()),
                            )
                        });
                    }
                    while let Some(Ok((id, delivered))) = tasks.join_next().await {
                        let _ = manager.local.lock().unwrap().append(
                            &id,
                            "storage_warning_delivery",
                            json!({"confirmed":delivered,"meaning":"accepted_by_harness_not_model_consumption"}),
                            None,
                        );
                    }
                }
                if snapshot.level == Level::Blocked {
                    let handles: Vec<_> = manager
                        .local
                        .lock()
                        .unwrap()
                        .sessions
                        .values()
                        .filter_map(|s| s.handle.clone().map(|h| (s.session_id.clone(), h)))
                        .collect();
                    let mut all_paused = manager.storage.pause_writers(true).await.is_ok();
                    for (id, handle) in handles {
                        if handle.pause(true).await.is_ok() {
                            let mut local = manager.local.lock().unwrap();
                            if !local.sessions[&id].storage_paused {
                                let _ = local.append(
                                        &id,
                                        "storage_pause",
                                        json!({"paused":true,"text":"Cloudroom: work paused because disk space is critically low or could not be measured. Your files and history are preserved."}),
                                        None,
                                    );
                            }
                        } else {
                            all_paused = false;
                        }
                    }
                    if all_paused && last_cleanup.elapsed() >= Duration::from_secs(30) {
                        let _ = manager.storage.clean().await;
                        last_cleanup = Instant::now();
                    }
                    was_blocked = true;
                } else if was_blocked
                    || (snapshot.level == Level::Normal && manager.has_storage_warning())
                {
                    let writers_resumed = manager.storage.pause_writers(false).await.is_ok();
                    let handles: Vec<_> = manager
                        .local
                        .lock()
                        .unwrap()
                        .sessions
                        .values()
                        .filter_map(|s| s.handle.clone().map(|h| (s.session_id.clone(), h)))
                        .collect();
                    for (id, handle) in handles {
                        if handle.pause(false).await.is_ok() {
                            let mut local = manager.local.lock().unwrap();
                            let result = if snapshot.level == Level::Normal {
                                local.append(&id, "storage_recovered", json!({"text":"Cloudroom: disk space has recovered. Work can continue."}), None)
                            } else {
                                local.append(&id, "storage_pause", json!({"paused":false,"text":"Cloudroom: work resumed. Disk space is still low."}), None)
                            };
                            if result.is_ok() {
                                manager.advance(&mut local, &id, false);
                            }
                        }
                    }
                    // Resume only workloads this guard actually paused. Process-loss
                    // recovery remains the separate lifecycle owner's responsibility.
                    was_blocked = !writers_resumed;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    pub fn start_uploader(self: &Arc<Self>) {
        let manager = self.clone();
        tokio::spawn(async move {
            loop {
                let batch: io::Result<Vec<Record>> = {
                    let local = manager.local.lock().unwrap();
                    (local.journal.saved() + 1..=local.journal.last())
                        .take(128)
                        .map(|id| {
                            serde_json::from_slice(&local.journal.read(id)?)
                                .map_err(io::Error::other)
                        })
                        .collect()
                };
                match batch {
                    Ok(batch) if !batch.is_empty() => {
                        let started = Instant::now();
                        let saved = manager.history.upload(&batch).await.is_ok();
                        let mut local = manager.local.lock().unwrap();
                        local.database_available = saved;
                        let ack_failed = saved
                            && local
                                .journal
                                .acknowledge(batch.last().unwrap().sequence)
                                .is_err();
                        manager.observability.record(Signal::HistoryUpload {
                            records: batch.len(),
                            pending_records: local.journal.last() - local.journal.saved(),
                            success: saved,
                            duration_ms: elapsed_ms(started),
                        });
                        if ack_failed {
                            manager.observability.record(Signal::HistoryFault {
                                operation: "acknowledge",
                            });
                            break;
                        }
                    }
                    Err(_) => {
                        manager.observability.record(Signal::HistoryFault {
                            operation: "read_pending",
                        });
                        break;
                    }
                    _ => {}
                }
                tokio::time::sleep(std::time::Duration::from_millis(200)).await;
            }
        });
    }

    pub async fn shutdown(&self) {
        let mut changed = self.subscribe();
        {
            let mut local = self.local.lock().unwrap();
            self.stopping.store(true, Ordering::Relaxed);
            let waiting: Vec<_> = local
                .sessions
                .values()
                .filter(|s| s.state == "resuming" && s.handle.is_none())
                .map(|s| s.session_id.clone())
                .collect();
            for id in waiting {
                let _ = local.append(&id, "state", json!({"state":"suspended"}), None);
            }
            local.changed.send_replace(0);
            // Signal all harnesses together; their recorded Exited events confirm cleanup.
            for handle in local.sessions.values().filter_map(|s| s.handle.as_ref()) {
                handle.request_shutdown();
            }
        }
        let cleanup = async {
            while self
                .local
                .lock()
                .unwrap()
                .sessions
                .values()
                .any(|s| s.handle.is_some())
            {
                if changed.changed().await.is_err() {
                    break;
                }
            }
            self.observability.shutdown().await;
        };
        if tokio::time::timeout(runtime::SHUTDOWN_GRACE + Duration::from_secs(1), cleanup)
            .await
            .is_err()
        {
            eprintln!("Cloudroom shutdown cleanup deadline reached");
        }
    }
}
