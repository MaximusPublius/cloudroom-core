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
