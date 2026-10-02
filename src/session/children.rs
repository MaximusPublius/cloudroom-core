use super::*;

impl Manager {
    pub(super) fn start_child(
        self: &Arc<Self>,
        local: &mut Local,
        parent: &str,
        handle: &runtime::Handle,
        child: runtime::ChildRequest,
    ) -> Result<()> {
        let runtime::ChildRequest {
            request,
            id: key,
            tool_call_id,
            prompt: text,
        } = child;
        let session = &local.sessions[parent];
        if !matches!(session.harness, runtime::Kind::Pi | runtime::Kind::Claude)
            || session.current_request.as_deref() != Some(&request)
        {
            return Err(Error::Conflict(
                "child requires the current managed parent turn",
            ));
        }
        if self.storage.blocks() || self.is_stopping() {
            let handle = handle.clone();
            tokio::spawn(async move {
                let _ = handle.child_result(&request, json!({"id":key,"error":"Cloudroom cannot start a child while storage or shutdown blocks execution"})).await;
            });
            return Ok(());
        }
        let id = format!("cr_child_{key}");
        let start_id = format!("child_{key}");
        let prompt_id = format!("task_{key}");
        let reasoning = prompt::latest(&session.prompts, &session.receipts, &request)
            .and_then(prompt::reasoning)
            .or_else(|| session.reasoning.clone());
        let mut input = json!({"harness":session.harness,"parent_session":parent,"parent_request":request,"tool_call_id":tool_call_id,"prompt":text,"reasoning":reasoning});
        if !session.command_guard_enabled() {
            input["command_guard_enabled"] = json!(false);
        }
        if let Some(system_prompt) = session.system_prompt() {
            input["system_prompt"] = json!(system_prompt);
        }
        if let Some(existing) = local.sessions.get(&id) {
            Self::retry(existing, &start_id, "start", &input)?
                .ok_or(Error::Conflict("child identity conflict"))?;
        } else {
            let start = Receipt {
                request_id: start_id.clone(),
                command: "start".into(),
                input,
                state: "accepted".into(),
                model: session.model.clone(),
                provider: session.provider.clone(),
                workspace: session.workspace.clone(),
                error: None,
            };
            // The child task is part of its durable start so a crash cannot strand an empty child.
            local.append(&id, "receipt", json!(start), None)?;
            local.append(
                parent,
                "child",
                json!({"id":id,"request_id":request,"tool_call_id":tool_call_id,"state":"started"}),
                None,
            )?;
            let manager = self.clone();
            let child = id.clone();
            tokio::spawn(async move {
                manager.launch(child, start_id).await;
            });
        }
        let manager = self.clone();
        let handle = handle.clone();
        let parent = parent.to_owned();
        let mut changed = local.changed.subscribe();
        tokio::spawn(async move {
            loop {
                let result = {
                    let local = manager.local.lock().unwrap();
                    let child = &local.sessions[&id];
                    let state = child
                        .receipts
                        .get(&prompt_id)
                        .map(|receipt| receipt.state.as_str());
                    match state {
                        Some("completed") => Some(Ok(child.handle.clone())),
                        Some("failed" | "interrupted" | "unknown" | "unknown_after_restart") => {
                            Some(Err("Child task failed or its outcome is uncertain"))
                        }
                        _ if matches!(
                            child.state.as_str(),
                            "failed" | "closed" | "process_lost"
                        ) =>
                        {
                            Some(Err("Child runtime is unavailable"))
                        }
                        _ => None,
                    }
                };
                if let Some(outcome) = result {
                    let mut reply = json!({"id":key,"session_id":id});
                    match outcome {
                        Ok(Some(child)) => match child.last_text().await {
                            Ok(text) => {
                                reply["result"] =
                                    json!(text.chars().take(32768).collect::<String>())
                            }
                            Err(_) => {
                                reply["error"] = json!(
                                    "Child completed; result is available in its saved history"
                                )
                            }
                        },
                        Ok(None) => {
                            reply["error"] = json!("Child completed; runtime is no longer attached")
                        }
                        Err(error) => reply["error"] = json!(error),
                    }
                    {
                        let mut local = manager.local.lock().unwrap();
                        if local.append(&parent, "child", json!({"id":id,"request_id":request,"tool_call_id":tool_call_id,"state":if reply.get("error").is_some() {"failed"} else {"completed"},"result":reply}), None).is_err() { return; }
                    }
                    let _ = handle.child_result(&request, reply).await;
                    return;
                }
                if manager.is_stopping() || changed.changed().await.is_err() {
                    return;
                }
            }
        });
        Ok(())
    }
}

/// Harnesses a cloud agent may start as a child thread; the app has no Cloud thread type for the others.
const CHILD_HARNESSES: [runtime::Kind; 3] = [
    runtime::Kind::Codex,
    runtime::Kind::Claude,
    runtime::Kind::Pi,
];
const FINISHED: [&str; 5] = [
    "completed",
    "failed",
    "interrupted",
    "unknown",
    "unknown_after_restart",
];
/// How much of a child's reply its parent's notice quotes, as for Local threads.
const NOTICE_REPLY_CHARS: usize = 4000;

/// A child thread the parent does not wait for: `cloudroom thread spawn`, or the app's API.
pub struct Spawn {
    pub harness: Option<runtime::Kind>,
    pub model: Option<String>,
    pub reasoning: Option<String>,
    pub title: Option<String>,
    pub prompt: String,
}

fn notice_id(child: &str, request: &str) -> String {
    format!("notice_{child}_{request}")
}

fn title(session: &Session) -> Option<String> {
    session
        .receipts
        .values()
        .find(|r| r.command == "start")
        .and_then(|r| r.input["title"].as_str())
        .map(str::to_owned)
}

fn notice(child: &str, title: Option<String>, state: &str, reply: Option<String>) -> String {
    let name = title.map_or(child.to_owned(), |t| format!("\"{t}\" ({child})"));
    match state {
        "completed" => {
            let reply = reply.filter(|r| !r.trim().is_empty()).map_or(
                format!(
                    "Its reply is in its thread; read it with `cloudroom thread output {child}`."
                ),
                |r| {
                    let mut text: String = r.trim().chars().take(NOTICE_REPLY_CHARS).collect();
                    if r.trim().chars().count() > NOTICE_REPLY_CHARS {
                        text.push_str("\n\n[... reply truncated ...]");
                    }
                    text
                },
            );
            format!("[Cloudroom] Child thread {name} completed:\n\n{reply}")
        }
        "interrupted" => format!(
            "[Cloudroom] Child thread {name} was interrupted. If the user stopped it, do not restart or replace its work unless they ask."
        ),
        _ => format!(
            "[Cloudroom] Child thread {name} failed or its outcome is uncertain. Check it with `cloudroom thread output {child}` before deciding next steps."
        ),
    }
}

impl Manager {
    /// Starts a child thread in the parent's folder with any supported harness and model. The parent keeps
    /// working; Core queues it a notice whenever one of the child's turns ends, even while the app is offline.
    pub async fn spawn_child(
        self: &Arc<Self>,
        parent: &str,
        key: &str,
        spawn: Spawn,
    ) -> Result<(String, Receipt)> {
        let id = format!("cr_child_{key}");
        let start_id = format!("child_{key}");
        let (kind, input, model, provider, workspace) = {
            let local = self.local.lock().unwrap();
            let p = local.sessions.get(parent).ok_or(Error::NotFound)?;
            if p.close_request.is_some() || p.state == "closed" {
                return Err(Error::Conflict("the parent thread is closed"));
            }
            let kind = spawn.harness.unwrap_or(p.harness);
            if !CHILD_HARNESSES.contains(&kind) {
                return Err(Error::Conflict(
                    "child threads support codex, claude-code and pi",
                ));
            }
            let same = kind == p.harness;
            let model = spawn
                .model
                .clone()
                .or(same.then(|| p.model.clone()).flatten());
            let reasoning = spawn
                .reasoning
                .clone()
                .or(same.then(|| p.reasoning.clone()).flatten())
                .unwrap_or_else(|| "medium".into());
            let mut input = json!({"harness":kind,"parent_session":parent,"prompt":spawn.prompt,"reasoning":reasoning,"notify":true});
            if let Some(model) = &spawn.model {
                input["model"] = json!(model);
            }
            if let Some(title) = &spawn.title {
                input["title"] = json!(title);
            }
            if !p.command_guard_enabled() {
                input["command_guard_enabled"] = json!(false);
            }
            if let Some(system_prompt) = p.system_prompt() {
                input["system_prompt"] = json!(system_prompt);
            }
            if let Some(existing) = local.sessions.get(&id) {
                return Self::retry(existing, &start_id, "start", &input)?
                    .map(|r| (id.clone(), r))
                    .ok_or(Error::Conflict("child identity conflict"));
            }
            let provider = if same { p.provider.clone() } else { None };
            (kind, input, model, provider, p.workspace.clone())
        };
        if !self.recording_available() {
            return Err(Error::Storage(JOURNAL_UNWRITABLE.into()));
        }
        if !self.config.harnesses.contains_key(&kind) {
            return Err(Error::Conflict("harness is not configured"));
        }
        self.harness_ready(kind).await?;
        let reasoning = input["reasoning"].as_str().unwrap_or("medium");
        self.execution_supported(kind, model.as_ref(), reasoning)
            .await?;
        let profile = &self.config.harnesses[&kind];
        let receipt = Receipt {
            request_id: start_id.clone(),
            command: "start".into(),
            input,
            state: "accepted".into(),
            model: Some(model.unwrap_or_else(|| profile.model.clone())),
            provider: provider.or_else(|| profile.provider.clone()),
            workspace,
            error: None,
        };
        {
            let mut local = self.local.lock().unwrap();
            if let Some(existing) = local.sessions.get(&id) {
                return Self::retry(existing, &start_id, "start", &receipt.input)?
                    .map(|r| (id.clone(), r))
                    .ok_or(Error::Conflict("child identity conflict"));
            }
            if self.is_stopping() || local.draining || self.storage.blocks() {
                return Err(Error::Conflict(
                    "new execution is paused; try again shortly",
                ));
            }
            local.append(
                &id,
                "receipt",
                serde_json::to_value(&receipt).map_err(io::Error::other)?,
                None,
            )?;
            // Its own record kind: apps that predate child threads ignore it instead of failing on it.
            local.append(
                parent,
                "child_thread",
                json!({"id":id,"harness":kind,"title":receipt.input["title"]}),
                None,
            )?;
            self.watch_child(&local, id.clone());
        }
        let (manager, child) = (self.clone(), id.clone());
        tokio::spawn(async move {
            manager.launch(child, start_id).await;
        });
        Ok((id, receipt))
    }

    /// Resumes parent notices for spawned children after a restart.
    pub(super) fn watch_children(self: &Arc<Self>, local: &Local) {
        for child in local.sessions.values() {
            if child
                .receipts
                .values()
                .any(|r| r.command == "start" && r.input["notify"] == true)
            {
                self.watch_child(local, child.session_id.clone());
            }
        }
    }

    /// Queues a notice to the parent for each finished child turn, until either thread closes.
    fn watch_child(self: &Arc<Self>, local: &Local, child: String) {
        let manager = self.clone();
        let mut changed = local.changed.subscribe();
        tokio::spawn(async move {
            loop {
                let due = {
                    let local = manager.local.lock().unwrap();
                    let Some(c) = local.sessions.get(&child) else {
                        return;
                    };
                    let Some(p) = c
                        .parent_session
                        .as_ref()
                        .and_then(|p| local.sessions.get(p))
                    else {
                        return;
                    };
                    if p.close_request.is_some() || p.state == "closed" {
                        return;
                    }
                    let finished = c.receipts.values().find(|r| {
                        r.command == "prompt"
                            && FINISHED.contains(&r.state.as_str())
                            && !p.receipts.contains_key(&notice_id(&child, &r.request_id))
                    });
                    match finished {
                        Some(r) => Some((
                            p.session_id.clone(),
                            r.request_id.clone(),
                            r.state.clone(),
                            // A newer turn may already have replaced the harness's last reply.
                            c.handle.clone().filter(|_| c.current_request.is_none()),
                            title(c),
                        )),
                        None if c.close_request.is_some() || c.state == "closed" => return,
                        None => None,
                    }
                };
                if let Some((parent, request, state, handle, title)) = due {
                    let reply = match handle {
                        Some(handle) if state == "completed" => handle.last_text().await.ok(),
                        _ => None,
                    };
                    let text = notice(&child, title, &state, reply);
                    let id = notice_id(&child, &request);
                    // Storage or a drain can refuse it; retry after a pause, or after the next restart.
                    if manager
                        .command(&parent, id, "prompt", json!({"text":text}))
                        .is_ok()
                    {
                        continue;
                    }
                    tokio::time::sleep(Duration::from_secs(5)).await;
                }
                if manager.is_stopping() || changed.changed().await.is_err() {
                    return;
                }
            }
        });
    }

    /// The caller's own child, for `cloudroom thread output|tell`.
    pub(crate) fn own_child(&self, parent: &str, child: &str) -> Result<()> {
        let local = self.local.lock().unwrap();
        match local.sessions.get(child) {
            Some(c) if c.parent_session.as_deref() == Some(parent) => Ok(()),
            _ => Err(Error::Conflict("not a child thread of this thread")),
        }
    }

    /// The caller's children, or one child with its latest reply when its harness is attached.
    pub(crate) async fn children(&self, parent: &str, only: Option<&str>) -> Value {
        let (mut items, handle) = {
            let local = self.local.lock().unwrap();
            let items: Vec<_> = local
                .sessions
                .values()
                .filter(|c| c.parent_session.as_deref() == Some(parent))
                .filter(|c| only.is_none_or(|id| c.session_id == id))
                .map(|c| {
                    let busy = c.current_request.is_some() || c.has_work();
                    json!({"id":c.session_id,"title":title(c),"harness":c.harness,"model":c.model,"state":c.state,"busy":busy})
                })
                .collect();
            let handle = only
                .and_then(|id| local.sessions.get(id))
                .filter(|c| c.current_request.is_none())
                .and_then(|c| c.handle.clone());
            (items, handle)
        };
        if let (Some(item), Some(handle)) = (items.first_mut(), handle) {
            item["reply"] = json!(handle.last_text().await.ok());
        }
        json!({"children":items})
    }
}
