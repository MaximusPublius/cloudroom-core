use super::{
    Adapter, Event, Handle, Progress, Resume, command as child_command, process::MAX_LINE,
};
use crate::config::{Config, HarnessConfig};
use serde_json::{Value, json};
use std::{
    fs::File,
    io::{self, BufRead, BufReader, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
};
use tokio::process::Command;

const CONTEXT: &str = include_str!("pi-context.ts");
fn unique() -> String {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    format!(
        "{}-{}-{}",
        std::process::id(),
        std::time::SystemTime::now()
            .duration_since(std::time::UNIX_EPOCH)
            .unwrap_or_default()
            .as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )
}

pub(super) fn command(
    config: &Config,
    profile: &HarnessConfig,
    saved: Option<&Resume>,
) -> io::Result<(Command, PathBuf, PathBuf)> {
    let directory = profile.home.join("sessions/cloudroom");
    let path = saved
        .map(|s| s.path.clone())
        .unwrap_or_else(|| directory.join(format!("{}.jsonl", unique())));
    if let Some(saved) = saved {
        validate(profile, &path, Some(&saved.id))?;
    }
    // The wrapper runs after Runtime drops identity. Pi itself initializes the empty
    // exclusively-created file, making even a pre-first-prompt session resumable.
    let mut command = child_command(Path::new("/bin/sh"), config);
    let helper = directory.join(format!("{}.context.ts", unique()));
    let script = if saved.is_some() {
        "set -eu; [ \"$(\"$5\" --version)\" = 0.85.1 ] || { echo 'unsupported Pi version: require 0.85.1' >&2; exit 1; }; umask 077; mkdir -p \"$1\"; set -C; printf '%s' \"$4\" > \"$3\"; shift 4; exec \"$@\""
    } else {
        "set -eu; [ \"$(\"$5\" --version)\" = 0.85.1 ] || { echo 'unsupported Pi version: require 0.85.1' >&2; exit 1; }; umask 077; mkdir -p \"$1\"; set -C; : > \"$2\"; printf '%s' \"$4\" > \"$3\"; shift 4; exec \"$@\""
    };
    command
        .env("PI_CODING_AGENT_DIR", &profile.home)
        .env("PI_OFFLINE", "1")
        .env("PI_TELEMETRY", "0")
        .args(["-c", script, "cloudroom-pi"])
        .arg(&directory)
        .arg(&path)
        .arg(&helper)
        .arg(CONTEXT)
        .arg(&profile.binary)
        .args(["--mode", "rpc", "--session"])
        .arg(&path)
        .arg("--session-dir")
        .arg(&directory)
        .arg("--provider")
        .arg(
            profile
                .provider
                .as_deref()
                .ok_or_else(|| io::Error::other("Pi provider required"))?,
        )
        .arg("--model")
        .arg(&profile.model)
        .arg("-e")
        .arg(&helper);
    Ok((command, path, helper))
}

pub(super) async fn start(handle: &Handle) -> io::Result<String> {
    let thinking = handle
        .reasoning
        .as_deref()
        .map(|level| if level == "none" { "off" } else { level });
    if let Some(level) = thinking {
        let supported = handle
            .call("get_available_thinking_levels", json!({}))
            .await?;
        if !supported["levels"]
            .as_array()
            .is_some_and(|levels| levels.iter().any(|value| value == level))
        {
            return Err(io::Error::other(
                "Pi model does not support the selected thinking level",
            ));
        }
        handle
            .call("set_thinking_level", json!({"level":level}))
            .await?;
    }
    let state = handle.call("get_state", json!({})).await?;
    if thinking.is_some_and(|level| state["thinkingLevel"].as_str() != Some(level)) {
        return Err(io::Error::other(
            "Pi did not apply the selected thinking level",
        ));
    }
    let id = state["sessionId"]
        .as_str()
        .ok_or_else(|| io::Error::other("missing Pi session ID"))?;
    let path = handle.session_file.as_ref().unwrap();
    if state["sessionFile"].as_str() != path.to_str()
        || state["model"]["id"].as_str() != Some(&handle.profile.model)
        || state["model"]["provider"].as_str() != handle.profile.provider.as_deref()
        || state["isStreaming"] != false
        || state["isCompacting"] != false
    {
        return Err(io::Error::other(
            "Pi startup state does not match configured session/model",
        ));
    }
    if handle.resume.as_ref().is_some_and(|s| s.id != id) {
        return Err(io::Error::other("Pi resumed a different session"));
    }
    let header = validate(&handle.profile, path, Some(id))?;
    if header["cwd"]
        .as_str()
        .map(Path::new)
        .and_then(|p| p.canonicalize().ok())
        != Some(handle.repository.canonicalize()?)
    {
        return Err(io::Error::other(
            "Pi session belongs to a different workspace",
        ));
    }
    File::open(path)?.sync_all()?;
    File::open(path.parent().unwrap())?.sync_all()?;
    let commands = handle.call("get_commands", json!({})).await?;
    let found = commands["commands"].as_array().is_some_and(|commands| {
        commands.iter().any(|c| {
            c["name"] == "cloudroom_context"
                && c["description"] == "Cloudroom context-only notice (v1)"
                && c["source"] == "extension"
                && c["sourceInfo"]["path"].as_str()
                    == handle.context_file.as_ref().and_then(|p| p.to_str())
        })
    });
    if !found {
        return Err(io::Error::other(
            "Pi context extension unavailable or conflicting",
        ));
    }
    Ok(id.into())
}
pub(super) async fn send(handle: &Handle, request: &str, text: &str) -> io::Result<()> {
    handle
        .process
        .call("prompt", json!({"message":text}), Some(request))
        .await?;
    Ok(())
}
pub(super) async fn interrupt(handle: &Handle, request: &str) -> io::Result<()> {
    // Pi abort alone can continue its memory-only queue. Cloudroom's durable
    // future requests stay in Session Management and are not cleared here.
    handle
        .process
        .control("clear_queue", json!({}), request)
        .await?;
    handle.process.control("abort", json!({}), request).await?;
    Ok(())
}

pub(super) async fn notice(handle: &Handle, text: &str) -> io::Result<()> {
    handle
        .call(
            "prompt",
            json!({"message":format!("/cloudroom_context {}",json!({"text":text}))}),
        )
        .await?;
    Ok(())
}

pub(super) fn checkpoint(data: &Value, native: Option<&str>) -> io::Result<Value> {
    let entry = data["cursor"]["entry"]
        .as_str()
        .filter(|id| !id.is_empty())
        .ok_or_else(|| io::Error::other("missing Pi entry cursor"))?;
    Ok(
        json!({"entry":entry,"line":native.ok_or_else(||io::Error::other("missing native record"))?}),
    )
}

pub(super) struct Protocol {
    capture: Capture,
    expected: Option<String>,
    prompt: Option<u64>,
    accepted: bool,
    interrupting: bool,
    close_mask: u8,
}
impl Protocol {
    pub fn new(profile: &HarnessConfig, path: &Path, saved: Option<&Resume>) -> io::Result<Self> {
        Ok(Self {
            capture: Capture::new(
                profile,
                path,
                saved.map(|s| s.cursor.clone()).unwrap_or(Value::Null),
            )?,
            expected: saved.map(|s| s.id.clone()),
            prompt: None,
            accepted: false,
            interrupting: false,
            close_mask: 0,
        })
    }
    fn probe(&self) -> Value {
        json!({"id":format!("settled-{}",self.prompt.unwrap_or(0)),"type":"get_state"})
    }
}
impl Adapter for Protocol {
    fn encode(&mut self, id: u64, method: &str, mut params: Value, request: Option<&str>) -> Value {
        if request.is_some() {
            self.prompt = Some(id);
            self.accepted = false;
            self.interrupting = false;
        }
        if method == "abort" {
            self.interrupting = true;
        }
        params["id"] = json!(id.to_string());
        params["type"] = json!(method);
        params
    }
    fn receive(&mut self, v: &Value, raw: String, s: &mut Progress) -> io::Result<Vec<Event>> {
        let t = v["type"].as_str().unwrap_or("");
        let update = &v["assistantMessageEvent"];
        let kind = match t {
            "message_update" => match update["type"].as_str() {
                Some("text_delta") => "text_delta",
                Some("thinking_delta") => "thinking_delta",
                _ => "native_event",
            },
            "tool_execution_start" => "item_started",
            "tool_execution_update" => "tool_snapshot",
            "tool_execution_end" => "item_completed",
            _ => "native_event",
        };
        let mut events = vec![Event::Record {
            kind,
            data: json!({"harness":"pi","type":t,"request_id":s.request,"item_id":v["toolCallId"],"tool_name":v["toolName"],"delta":update["delta"],"output":v["partialResult"],"result":v["result"],"message":v["message"],"usage":v["usage"]}),
            native: Some(raw),
        }];
        if t == "response" && v["command"] == "get_state" && v["success"] == true {
            let data = &v["data"];
            let id = data["sessionId"]
                .as_str()
                .ok_or_else(|| io::Error::other("missing Pi identity"))?;
            if self.expected.as_deref().is_some_and(|old| old != id)
                || data["sessionFile"].as_str() != self.capture.path.to_str()
            {
                return Err(io::Error::other("Pi identity changed"));
            }
            self.expected = Some(id.into());
            if s.native.is_none() {
                s.native = Some(id.into());
                events.push(Event::Record {
                    kind: "native_identity",
                    data: json!({"id":id,"path":self.capture.path}),
                    native: None,
                });
            }
            if v["id"] == self.probe()["id"]
                && self.accepted
                && data["isStreaming"] == false
                && data["isCompacting"] == false
                && data["pendingMessageCount"] == 0
            {
                let status = if self.interrupting {
                    "interrupted"
                } else if !s.status.is_empty() {
                    &s.status
                } else if !s.started {
                    "completed"
                } else {
                    "unknown"
                }
                .to_owned();
                events.extend(s.finished(&status));
            }
        }
        if t == "response"
            && v["id"].as_str().and_then(|id| id.parse::<u64>().ok()) == self.prompt
            && self.prompt.is_some()
        {
            self.accepted = v["success"] == true;
            if !self.accepted {
                s.finished = true;
            }
        }
        match t {
            "agent_start" => events.extend(s.started()),
            "extension_error" if !s.started && v["event"] == "command" => {
                s.status = "failed".into()
            }
            "message_end" if v["message"]["role"] == "assistant" => {
                s.status = match v["message"]["stopReason"].as_str() {
                    Some("aborted") => "interrupted",
                    Some("error" | "length") => "failed",
                    Some("stop" | "toolUse") => "completed",
                    _ => "unknown",
                }
                .into();
            }
            "extension_ui_request"
                if matches!(
                    v["method"].as_str(),
                    Some("select" | "confirm" | "input" | "editor")
                ) =>
            {
                events.push(Event::Record {
                    kind: "interaction_cancelled",
                    data: json!({"harness":"pi","id":v["id"],"reason":"interactive dialogs are unsupported; not approved"}),
                    native: None,
                });
            }
            _ => {}
        }
        Ok(events)
    }
    fn response(&self, v: &Value) -> Option<(u64, io::Result<Value>)> {
        if v["type"] != "response" {
            return None;
        }
        let id = v["id"].as_str()?.parse().ok()?;
        Some((
            id,
            if v["success"] == true {
                Ok(v.get("data").cloned().unwrap_or(Value::Null))
            } else {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "Pi rejected command; see native history",
                ))
            },
        ))
    }
    fn respond(&self, v: &Value) -> Vec<Value> {
        if v["type"] == "extension_ui_request"
            && matches!(
                v["method"].as_str(),
                Some("select" | "confirm" | "input" | "editor")
            )
        {
            return vec![json!({"type":"extension_ui_response","id":v["id"],"cancelled":true})];
        }
        if self.prompt.is_some()
            && (v["type"] == "agent_settled"
                || v["type"] == "response"
                    && v["command"] == "prompt"
                    && v["id"].as_str().and_then(|id| id.parse::<u64>().ok()) == self.prompt
                    && v["success"] == true)
        {
            return vec![self.probe()];
        }
        vec![]
    }
    fn capture(&mut self) -> io::Result<Vec<Event>> {
        self.capture.read()
    }
    fn close(&mut self) -> Vec<Value> {
        self.interrupting = true;
        vec![
            json!({"type":"clear_queue","id":"close-queue"}),
            json!({"type":"abort_bash","id":"close-bash"}),
            json!({"type":"abort","id":"close-agent"}),
        ]
    }
    fn closed(&mut self, v: &Value) -> bool {
        if v["type"] == "response" && v["success"] == true {
            self.close_mask |= match v["id"].as_str() {
                Some("close-queue") => 1,
                Some("close-bash") => 2,
                Some("close-agent") => 4,
                _ => 0,
            };
        }
        self.close_mask == 7
    }
}

fn validate(profile: &HarnessConfig, path: &Path, id: Option<&str>) -> io::Result<Value> {
    let canonical = path.canonicalize()?;
    if !canonical.starts_with(profile.home.canonicalize()?.join("sessions")) {
        return Err(io::Error::other("Pi session outside account sessions"));
    }
    let mut line = String::new();
    BufReader::new(File::open(canonical)?)
        .take(MAX_LINE as u64)
        .read_line(&mut line)?;
    let header: Value = serde_json::from_str(&line)?;
    if header["type"] != "session"
        || header["version"] != 3
        || header["id"].as_str().is_none()
        || id.is_some_and(|id| header["id"] != id)
    {
        return Err(io::Error::other(
            "invalid Pi session header; refusing implicit migration",
        ));
    }
    Ok(header)
}

struct Capture {
    profile: HarnessConfig,
    path: PathBuf,
    cursor: Value,
    offset: u64,
    anchor: String,
    initialized: bool,
    identity: Option<String>,
}
impl Capture {
    fn new(profile: &HarnessConfig, path: &Path, cursor: Value) -> io::Result<Self> {
        Ok(Self {
            profile: profile.clone(),
            path: path.into(),
            cursor,
            offset: 0,
            anchor: String::new(),
            initialized: false,
            identity: None,
        })
    }
    fn read(&mut self) -> io::Result<Vec<Event>> {
        if !self.path.exists() || self.path.metadata()?.len() == 0 {
            if !self.cursor.is_null() {
                return Err(io::Error::other("Pi history disappeared"));
            }
            return Ok(vec![]);
        }
        let header = validate(&self.profile, &self.path, self.identity.as_deref())?;
        self.identity = header["id"].as_str().map(str::to_owned);
        let mut file = File::open(&self.path)?;
        // Verify the captured boundary on each read. Rewrites are reconciled by native
        // entry ID and exact content, never by blindly reusing a Codex byte offset.
        if self.initialized && self.offset >= self.anchor.len() as u64 && !self.anchor.is_empty() {
            let mut anchor = vec![0; self.anchor.len()];
            file.seek(SeekFrom::Start(self.offset - self.anchor.len() as u64))?;
            if file.read_exact(&mut anchor).is_err() || anchor != self.anchor.as_bytes() {
                self.initialized = false;
                self.offset = 0;
            }
        }
        file.seek(SeekFrom::Start(self.offset))?;
        let mut reader = BufReader::new(file);
        let mut found = self.initialized || self.cursor.is_null();
        let mut events = Vec::new();
        loop {
            let mut raw = String::new();
            let n = (&mut reader)
                .take((MAX_LINE + 1) as u64)
                .read_line(&mut raw)?;
            if n == 0 {
                break;
            }
            if n > MAX_LINE {
                return Err(io::Error::other("Pi history record too large"));
            }
            if !raw.ends_with('\n') {
                break;
            }
            self.offset += n as u64;
            let entry: Value = serde_json::from_str(&raw)?;
            let key = if entry["type"] == "session" {
                format!("session:{}", header["id"].as_str().unwrap())
            } else {
                entry["id"]
                    .as_str()
                    .ok_or_else(|| io::Error::other("Pi entry missing ID"))?
                    .to_owned()
            };
            if !found {
                if self.cursor["entry"] == key {
                    if self.cursor["line"] != raw {
                        return Err(io::Error::other("Pi captured entry changed"));
                    }
                    found = true;
                    self.anchor = raw;
                }
                continue;
            }
            self.anchor = raw.clone();
            self.cursor = json!({"entry":key,"line":raw});
            events.push(Event::Record {
                kind: "native_record",
                data: json!({"harness":"pi","cursor":{"entry":self.cursor["entry"]}}),
                native: Some(self.anchor.clone()),
            });
            if events.len() == 128 {
                break;
            }
        }
        if !found {
            return Err(io::Error::other("Pi history cursor no longer exists"));
        }
        self.initialized = true;
        Ok(events)
    }
}
pub(super) fn recover(
    profile: &HarnessConfig,
    saved: &Resume,
    mut emit: impl FnMut(Event) -> io::Result<()>,
) -> io::Result<()> {
    validate(profile, &saved.path, Some(&saved.id))?;
    let mut capture = Capture::new(profile, &saved.path, saved.cursor.clone())?;
    loop {
        let events = capture.read()?;
        if events.is_empty() {
            return Ok(());
        }
        for event in events {
            emit(event)?;
        }
    }
}
