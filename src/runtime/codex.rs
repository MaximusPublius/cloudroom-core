use super::{
    Adapter, Event, Handle, Progress, Resume, command as child_command, process::MAX_LINE,
};
use crate::config::{Config, HarnessConfig};
use serde_json::{Value, json};
use std::{
    fs::File,
    io::{self, Read, Seek, SeekFrom},
    path::{Path, PathBuf},
    time::Duration,
};
use tokio::process::Command;

pub(super) fn command(config: &Config, profile: &HarnessConfig) -> Command {
    let mut command = child_command(&profile.binary, config);
    command
        .env("CODEX_HOME", &profile.home)
        .args(["app-server", "--listen", "stdio://"]);
    command
}

pub(super) async fn start(handle: &Handle) -> io::Result<String> {
    handle.call("initialize",json!({"clientInfo":{"name":"cloudroom","version":env!("CARGO_PKG_VERSION")},"capabilities":{"experimentalApi":true}})).await?;
    handle.process.notify("initialized").await?;
    let mut params = json!({"cwd":handle.repository,"model":handle.profile.model,"approvalPolicy":"never","sandbox":"danger-full-access","ephemeral":false});
    if let Some(reasoning) = &handle.reasoning {
        params["config"] = json!({"model_reasoning_effort":reasoning});
    }
    let method = if let Some(saved) = &handle.resume {
        params["threadId"] = json!(saved.id);
        params["excludeTurns"] = json!(true);
        "thread/resume"
    } else {
        "thread/start"
    };
    let result = handle.call(method, params).await?;
    if result["model"].as_str() != Some(&handle.profile.model) {
        return Err(io::Error::other("harness selected a different model"));
    }
    let id = result
        .pointer("/thread/id")
        .and_then(Value::as_str)
        .ok_or_else(|| io::Error::other("missing native identity"))?;
    if handle.resume.as_ref().is_some_and(|s| s.id != id) {
        return Err(io::Error::other("harness resumed a different session"));
    }
    Ok(id.into())
}
pub(super) async fn send(handle: &Handle, request: &str, text: &str) -> io::Result<()> {
    let mut params = json!({"threadId":handle.native()?,"clientUserMessageId":request,"input":[{"type":"text","text":text,"text_elements":[]}]});
    if let Some(reasoning) = &handle.reasoning {
        params["effort"] = json!(reasoning);
    }
    let result = handle
        .process
        .call("turn/start", params, Some(request))
        .await?;
    result
        .pointer("/turn/id")
        .and_then(Value::as_str)
        .map(|_| ())
        .ok_or_else(|| io::Error::other("missing native turn identity; outcome uncertain"))
}
pub(super) async fn notice(handle: &Handle, text: &str) -> io::Result<()> {
    handle.call("thread/inject_items", json!({"threadId":handle.native()?,"items":[{"type":"message","role":"developer","content":[{"type":"input_text","text":text}]}]})).await?;
    Ok(())
}

pub(super) fn checkpoint(
    previous: &Value,
    data: &Value,
    native: Option<&str>,
) -> io::Result<Value> {
    let offset = data["offset"]
        .as_u64()
        .ok_or_else(|| io::Error::other("missing native offset"))?;
    if offset != previous["offset"].as_u64().unwrap_or(0) {
        return Err(io::Error::other("native record offset mismatch"));
    }
    let length = native
        .ok_or_else(|| io::Error::other("missing native record"))?
        .len() as u64;
    Ok(
        json!({"offset":offset.checked_add(length).ok_or_else(||io::Error::other("native offset overflow"))?}),
    )
}

pub(super) async fn interrupt(handle: &Handle, state: Progress) -> io::Result<()> {
    let turn = state
        .native_turn
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "turn has not started"))?;
    let native = handle.native()?;
    handle
        .process
        .control(
            "turn/interrupt",
            json!({"threadId":native,"turnId":turn}),
            state.request.as_deref().unwrap(),
        )
        .await?;
    let mut progress = handle.process.progress.clone();
    let processes = tokio::time::timeout(
        Duration::from_secs(30),
        progress.wait_for(|s| s.request == state.request && s.finished),
    )
    .await
    .map_err(|_| io::Error::other("turn cancellation timed out; outcome uncertain"))?
    .map_err(|_| io::Error::other("harness ended during cancellation"))?
    .processes
    .clone();
    for process in processes {
        let result = handle
            .call(
                "thread/backgroundTerminals/terminate",
                json!({"threadId":native,"processId":process}),
            )
            .await?;
        if result["terminated"].as_bool().is_none() {
            return Err(io::Error::other("invalid tool termination response"));
        }
    }
    Ok(())
}

pub(super) struct Protocol {
    home: PathBuf,
    tail: Tail,
    root: Option<String>,
    prompt_id: Option<u64>,
}
impl Protocol {
    pub fn new(profile: &HarnessConfig, resume: Option<&Resume>) -> Self {
        Self {
            home: profile.home.clone(),
            tail: resume
                .map(|s| Tail::new(s.path.clone(), s.cursor["offset"].as_u64().unwrap_or(0)))
                .unwrap_or_default(),
            root: resume.map(|s| s.id.clone()),
            prompt_id: None,
        }
    }
}
impl Adapter for Protocol {
    fn encode(&mut self, id: u64, method: &str, params: Value, request: Option<&str>) -> Value {
        if request.is_some() {
            self.prompt_id = Some(id);
        }
        if method == "initialized" {
            json!({"method":method})
        } else {
            json!({"id":id,"method":method,"params":params})
        }
    }
    fn receive(
        &mut self,
        value: &Value,
        raw: String,
        state: &mut Progress,
    ) -> io::Result<Vec<Event>> {
        if let Some(path) = value
            .pointer("/params/thread/path")
            .or_else(|| value.pointer("/result/thread/path"))
            .and_then(Value::as_str)
            && self.tail.path.is_none()
        {
            self.tail.path = Some(path.into());
        }
        let identity = value
            .pointer("/params/thread")
            .or_else(|| value.pointer("/result/thread"));
        if self.root.is_none() {
            self.root = identity.and_then(|v| v["id"].as_str()).map(str::to_owned);
        }
        let method = value["method"].as_str().unwrap_or("response");
        let params = &value["params"];
        let thread = params["threadId"]
            .as_str()
            .or_else(|| params.pointer("/thread/id").and_then(Value::as_str));
        let root = thread.is_none() || thread == self.root.as_deref();
        let kind = match method {
            "item/agentMessage/delta" => "text_delta",
            "item/commandExecution/outputDelta" => "tool_delta",
            "item/started" => "item_started",
            "item/completed" => "item_completed",
            _ => "native_event",
        };
        let mut events = vec![Event::Record {
            kind: if root { kind } else { "native_event" },
            data: json!({"method":method,"item_id":params["itemId"].as_str().or_else(||params.pointer("/item/id").and_then(Value::as_str)),"request_id":params["item"]["clientId"],"delta":params["delta"],"text":params["item"]["text"],"tool_name":params["item"]["command"],"output":params["item"]["aggregatedOutput"],"status":params["item"]["status"]}),
            native: Some(raw),
        }];
        if let Some(identity) = identity
            && identity["id"].as_str() == self.root.as_deref()
            && let Some(id) = &self.root
        {
            state.native = Some(id.clone());
            events.push(Event::Record {
                kind: "native_identity",
                data: json!({"id":id,"path":identity["path"].as_str()}),
                native: None,
            });
        }
        if value["id"].as_u64() == self.prompt_id && value.get("error").is_some() {
            state.finished = true;
        }
        match if root { method } else { "" } {
            "turn/started" => {
                if let Some(id) = params.pointer("/turn/id").and_then(Value::as_str) {
                    state.native_turn = Some(id.into());
                    events.extend(state.started());
                }
            }
            "item/started" => {
                if params["item"]["type"] == "commandExecution"
                    && params["turnId"].as_str() == state.native_turn.as_deref()
                    && let Some(pid) = params["item"]["processId"].as_str()
                {
                    state.processes.insert(pid.into());
                }
            }
            "turn/completed"
                if params.pointer("/turn/id").and_then(Value::as_str)
                    == state.native_turn.as_deref()
                    && state.native_turn.is_some() =>
            {
                events
                    .extend(state.finished(params["turn"]["status"].as_str().unwrap_or("unknown")));
            }
            _ => {}
        }
        Ok(events)
    }
    fn response(&self, value: &Value) -> Option<(u64, io::Result<Value>)> {
        if value.get("method").is_some() {
            return None;
        }
        let id = value["id"].as_u64()?;
        Some((
            id,
            if value.get("error").is_some() {
                Err(io::Error::new(
                    io::ErrorKind::InvalidInput,
                    "harness rejected command; see native history",
                ))
            } else {
                Ok(value.get("result").cloned().unwrap_or(Value::Null))
            },
        ))
    }
    fn respond(&self, value: &Value) -> Vec<Value> {
        if value.get("method").is_some() && value.get("id").is_some() {
            vec![
                json!({"id":value["id"],"error":{"code":-32601,"message":"interaction unsupported by this core slice"}}),
            ]
        } else {
            vec![]
        }
    }
    fn capture(&mut self) -> io::Result<Vec<Event>> {
        self.tail.capture(&self.home)
    }
    fn capture_pending(&self) -> bool {
        self.tail.more
    }
}

pub(super) fn for_client(data: &mut Value, native: Option<&str>) -> io::Result<()> {
    let frame: Value = serde_json::from_str(native.unwrap_or("null"))?;
    data["value"] = frame.get("params").cloned().unwrap_or(Value::Null);
    Ok(())
}

#[derive(Default)]
struct Tail {
    path: Option<PathBuf>,
    read_offset: u64,
    committed_offset: u64,
    partial: Vec<u8>,
    more: bool,
}
impl Tail {
    fn new(path: PathBuf, offset: u64) -> Self {
        Self {
            path: Some(path),
            read_offset: offset,
            committed_offset: offset,
            partial: Vec::new(),
            more: false,
        }
    }
    fn capture(&mut self, home: &Path) -> io::Result<Vec<Event>> {
        self.more = false;
        let Some(path) = &self.path else {
            return Ok(vec![]);
        };
        let path = match path.canonicalize() {
            Ok(p) => p,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(vec![]),
            Err(e) => return Err(e),
        };
        if !path.starts_with(home.canonicalize()?.join("sessions")) {
            return Err(io::Error::other(
                "native history is outside the account sessions directory",
            ));
        }
        let mut file = File::open(path)?;
        if file.metadata()?.len() < self.read_offset {
            return Err(io::Error::other("native history was truncated"));
        }
        file.seek(SeekFrom::Start(self.read_offset))?;
        let mut events = Vec::new();
        // A bounded batch lets shutdown and RPC replies make progress during large rollouts.
        let mut bytes = vec![0; 65536];
        let n = file.read(&mut bytes)?;
        self.more = n > 0;
        self.read_offset += n as u64;
        self.partial.extend_from_slice(&bytes[..n]);
        while let Some(end) = self.partial.iter().position(|b| *b == b'\n') {
            let raw = String::from_utf8(self.partial.drain(..=end).collect())
                .map_err(io::Error::other)?;
            events.push(Event::Record {
                kind: "native_record",
                data: json!({"offset":self.committed_offset}),
                native: Some(raw.clone()),
            });
            self.committed_offset += raw.len() as u64;
        }
        if self.partial.len() > MAX_LINE {
            return Err(io::Error::other("native history record too large"));
        }
        Ok(events)
    }
}
pub(super) fn recover(
    profile: &HarnessConfig,
    saved: &Resume,
    mut emit: impl FnMut(Event) -> io::Result<()>,
) -> io::Result<()> {
    let mut tail = Tail::new(
        saved.path.clone(),
        saved.cursor["offset"].as_u64().unwrap_or(0),
    );
    loop {
        let before = tail.read_offset;
        for event in tail.capture(&profile.home)? {
            emit(event)?;
        }
        if before == tail.read_offset {
            return Ok(());
        }
    }
}
