use crate::config::{Config, HarnessConfig};
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use std::{
    collections::HashSet,
    io,
    path::{Path, PathBuf},
    process::Stdio,
    time::Duration,
};
use tokio::{process::Command, sync::mpsc};
mod codex;
pub(crate) mod linux;
mod pi;
mod process;

pub(crate) const SHUTDOWN_GRACE: Duration = Duration::from_secs(4);

#[derive(Clone, Copy, Debug, Default, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Kind {
    #[default]
    Codex,
    Pi,
}

#[derive(Clone, Debug)]
pub struct Resume {
    pub id: String,
    pub path: PathBuf,
    pub cursor: Value,
    pub model: Option<String>,
    pub provider: Option<String>,
    pub reasoning: Option<String>,
}

pub enum Event {
    Record {
        kind: &'static str,
        data: Value,
        native: Option<String>,
    },
    Started {
        request: String,
        native_turn: Option<String>,
    },
    Finished {
        request: String,
        status: String,
    },
    Exited {
        reason: &'static str,
        expected: bool,
        cleaned_up: bool,
    },
}

#[derive(Clone, Default)]
struct Progress {
    native: Option<String>,
    request: Option<String>,
    native_turn: Option<String>,
    started: bool,
    finished: bool,
    status: String,
    processes: HashSet<String>,
}
impl Progress {
    fn started(&mut self) -> Option<Event> {
        let request = self.request.clone()?;
        if self.started || self.finished {
            return None;
        }
        self.started = true;
        Some(Event::Started {
            request,
            native_turn: self.native_turn.clone(),
        })
    }
    fn finished(&mut self, status: &str) -> Option<Event> {
        let request = self.request.clone()?;
        if self.finished {
            return None;
        }
        self.finished = true;
        Some(Event::Finished {
            request,
            status: status.into(),
        })
    }
}

// Protocol differences stay here, not in Session Management or the process driver.
trait Adapter: Send {
    fn encode(&mut self, id: u64, method: &str, params: Value, request: Option<&str>) -> Value;
    fn receive(
        &mut self,
        value: &Value,
        raw: String,
        progress: &mut Progress,
    ) -> io::Result<Vec<Event>>;
    fn response(&self, value: &Value) -> Option<(u64, io::Result<Value>)>;
    fn respond(&self, _value: &Value) -> Vec<Value> {
        Vec::new()
    }
    fn capture(&mut self) -> io::Result<Vec<Event>>;
    fn capture_pending(&self) -> bool {
        false
    }
    fn close(&mut self) -> Vec<Value> {
        Vec::new()
    }
    fn closed(&mut self, _value: &Value) -> bool {
        false
    }
}

#[derive(Clone)]
pub struct Handle {
    process: process::Process,
    kind: Kind,
    profile: HarnessConfig,
    repository: PathBuf,
    resume: Option<Resume>,
    session_file: Option<PathBuf>,
    context_file: Option<PathBuf>,
    reasoning: Option<String>,
}
impl Handle {
    pub fn spawn(
        config: &Config,
        kind: Kind,
        resume: Option<Resume>,
    ) -> io::Result<(Self, mpsc::Receiver<Event>)> {
        let mut profile = config
            .harnesses
            .get(&kind)
            .cloned()
            .ok_or_else(|| io::Error::other("harness not configured"))?;
        if let Some(saved) = &resume {
            if let Some(model) = &saved.model {
                profile.model = model.clone();
            }
            if saved.provider.is_some() {
                profile.provider = saved.provider.clone();
            }
        }
        let (command, adapter, session_file, context_file): (_, Box<dyn Adapter>, _, _) = match kind
        {
            Kind::Codex => (
                codex::command(config, &profile),
                Box::new(codex::Protocol::new(&profile, resume.as_ref())),
                None,
                None,
            ),
            Kind::Pi => {
                let (command, path, helper) = pi::command(config, &profile, resume.as_ref())?;
                (
                    command,
                    Box::new(pi::Protocol::new(&profile, &path, resume.as_ref())?),
                    Some(path),
                    Some(helper),
                )
            }
        };
        let (process, events) = process::Process::spawn(config, command, adapter)?;
        Ok((
            Self {
                process,
                kind,
                profile,
                repository: config.repository.clone(),
                reasoning: resume.as_ref().and_then(|saved| saved.reasoning.clone()),
                resume,
                session_file,
                context_file,
            },
            events,
        ))
    }
    pub fn with_reasoning(mut self, reasoning: Option<String>) -> Self {
        self.reasoning = reasoning;
        self
    }

    pub fn pid(&self) -> u32 {
        self.process.pid()
    }
    pub fn same_process(&self, other: &Self) -> bool {
        self.process.progress.same_channel(&other.process.progress)
    }
    pub fn model(&self) -> &str {
        &self.profile.model
    }
    pub fn provider(&self) -> Option<&str> {
        self.profile.provider.as_deref()
    }
    pub fn capabilities(&self) -> Value {
        json!({"resume":true,"interrupt":true,"system_notice":true,"interactive_dialogs":false})
    }
    pub async fn start_session(&self) -> io::Result<String> {
        match self.kind {
            Kind::Codex => codex::start(self).await,
            Kind::Pi => pi::start(self).await,
        }
    }
    pub async fn send(&self, request: &str, text: &str) -> io::Result<()> {
        match self.kind {
            Kind::Codex => codex::send(self, request, text).await,
            Kind::Pi => pi::send(self, request, text).await,
        }
    }
    pub async fn interrupt(&self, request: &str) -> io::Result<()> {
        let state = self.process.progress.borrow().clone();
        if state.request.as_deref() != Some(request) || state.finished {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "interrupt target is no longer active",
            ));
        }
        match self.kind {
            Kind::Codex => codex::interrupt(self, state).await,
            Kind::Pi => pi::interrupt(self, request).await,
        }
    }
    pub async fn system_message(&self, text: &str) -> io::Result<()> {
        match self.kind {
            Kind::Codex => codex::notice(self, text).await,
            Kind::Pi => pi::notice(self, text).await,
        }
    }
    pub async fn pause(&self, paused: bool) -> io::Result<()> {
        self.process.pause(paused).await
    }
    pub fn request_shutdown(&self) {
        self.process.request_shutdown();
    }
    async fn call(&self, method: &str, params: Value) -> io::Result<Value> {
        self.process.call(method, params, None).await
    }
    fn native(&self) -> io::Result<String> {
        self.process
            .progress
            .borrow()
            .native
            .clone()
            .ok_or_else(|| io::Error::other("session not initialized"))
    }
}

pub fn recover_records(
    profile: &HarnessConfig,
    kind: Kind,
    saved: &Resume,
    emit: impl FnMut(Event) -> io::Result<()>,
) -> io::Result<()> {
    match kind {
        Kind::Codex => codex::recover(profile, saved, emit),
        Kind::Pi => pi::recover(profile, saved, emit),
    }
}

// Legacy records remain byte-for-byte unchanged on disk. Only their read projection changes.
pub fn for_client(data: &mut Value, native: Option<&str>) -> io::Result<()> {
    if data.get("method").is_some() && data.get("value").is_none() && data.get("harness").is_none()
    {
        codex::for_client(data, native)?;
    }
    Ok(())
}
pub fn checkpoint(
    kind: Kind,
    previous: &Value,
    data: &Value,
    native: Option<&str>,
) -> io::Result<Value> {
    match kind {
        Kind::Codex => codex::checkpoint(previous, data, native),
        Kind::Pi => pi::checkpoint(data, native),
    }
}

pub(super) fn command(binary: &Path, config: &Config) -> Command {
    let mut command = Command::new(binary);
    command
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .env("HOME", &config.account_home)
        .env("LANG", "C.UTF-8")
        .current_dir(&config.repository)
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped())
        .kill_on_drop(true);
    command
}

pub async fn reconcile(config: &Config) -> io::Result<()> {
    if let Some(policy) = &config.storage {
        linux::Workload::clear(&policy.cgroup_root).await?;
    }
    Ok(())
}

/// Unprotected tests never signal a saved PID: ownership cannot be proved after restart.
pub async fn wait_for_exit(pid: u32) -> io::Result<()> {
    tokio::time::timeout(SHUTDOWN_GRACE, async {
        loop {
            let output = Command::new("/bin/ps")
                .args(["-p", &pid.to_string(), "-o", "stat="])
                .kill_on_drop(true)
                .output()
                .await?;
            if output.status.code() == Some(1)
                && output.stderr.is_empty()
                && output.stdout.is_empty()
                || output.status.success()
                    && String::from_utf8_lossy(&output.stdout)
                        .trim_start()
                        .starts_with('Z')
            {
                return Ok(());
            }
            if !output.status.success() {
                return Err(io::Error::other("previous process ownership is unknown"));
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
    })
    .await
    .map_err(|_| io::Error::other("previous process has not exited; refusing replacement"))?
}
