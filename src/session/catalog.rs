//! Read-only views: the session list, dashboard, harness capabilities, and model checks.
use super::*;

impl Manager {
    pub fn dashboard(&self) -> Value {
        let (sampled_at, resources) = self.observability.resources();
        let storage = self.storage.snapshot();
        let local = self.local.lock().unwrap();
        let mut sessions: Vec<_> = local.sessions.values().collect();
        sessions.sort_unstable_by_key(|s| std::cmp::Reverse(s.last_sequence));
        // Only a safe summary leaves this endpoint, never receipts, prompts or native records.
        let summaries: Vec<_> = sessions
            .iter()
            .take(1000)
            .map(|s| {
                let state = s.dashboard_state();
                json!({"id":s.session_id,"title":format!("{:?} session",s.harness),
                "repository":s.workspace.as_ref().map(|w| &w.path).unwrap_or(&self.config.repository).file_name().map(|n| n.to_string_lossy()),
                "harness":s.harness,"model":s.model,"state":state,
                "activity":if state == "waiting" { "waiting" } else { "unknown" },
                "lastActivity":s.last_activity})
            })
            .collect();
        json!({"version":1,"sampledAt":sampled_at,
            "runtime":{"ready":!self.is_stopping() && storage.level != Level::Blocked && local.journal.writable(),"version":crate::VERSION,"commit":crate::COMMIT,
                "configured":!self.config.harnesses.is_empty()},
            "appConnectivity":"unknown",
            "resources":{"cpu":resources["cpu"],"memory":resources["memory"],"disk":resources["disk"]},
            "resourceBytes":{"memoryUsedBytes":resources["memoryUsedBytes"],"memoryTotalBytes":resources["memoryTotalBytes"],
                "diskAvailableBytes":resources["diskAvailableBytes"],"diskTotalBytes":resources["diskTotalBytes"]},
            "storage":storage,
            "diskThresholds":self.config.storage.as_ref().map(|policy| json!({"warningBytes":policy.warning_bytes,"pauseBytes":policy.pause_bytes})),
            "onboarding":{"localConnected":null,"offlineTaskVerified":null},
            "capabilities":{"settings":true,"updates":false},
            "sessionCount":sessions.len(),"agents":local.agent_counts(),"sessions":summaries})
    }

    /// The latest 1,000 sessions, newest activity first. Prompts, receipts and records stay
    /// behind the per-session endpoints.
    pub fn list_sessions(&self) -> Value {
        let local = self.local.lock().unwrap();
        let mut sessions: Vec<_> = local.sessions.values().collect();
        sessions.sort_unstable_by_key(|s| std::cmp::Reverse(s.last_sequence));
        let summaries: Vec<_> = sessions
            .iter()
            .take(1000)
            .map(|s| {
                json!({"session_id":s.session_id,"harness":s.harness,"model":s.model,"provider":s.provider,
                "state":s.state,"workspace":s.workspace,"parent_session":s.parent_session,
                "current_request":s.current_request,"queued":s.queue.len(),
                "last_sequence":s.last_sequence,"last_activity_ms":s.last_activity})
            })
            .collect();
        json!({"total":sessions.len(),"sessions":summaries})
    }

    pub(super) async fn codex_models(&self) -> Result<Vec<runtime::Model>> {
        self.model_catalog(runtime::Kind::Codex).await
    }

    pub(super) async fn model_catalog(&self, kind: runtime::Kind) -> Result<Vec<runtime::Model>> {
        let cache = match kind {
            runtime::Kind::Codex => &self.codex_models,
            runtime::Kind::Claude => &self.claude_models,
            runtime::Kind::Cursor => &self.cursor_models,
            _ => return Err(Error::Conflict("model catalog unavailable")),
        };
        let mut cached = cache.lock().await;
        if let Some((checked, models)) = &*cached
            && checked.elapsed() < Duration::from_secs(60)
        {
            return Ok(models.clone());
        }
        let result = if kind == runtime::Kind::Cursor {
            runtime::cursor_models::list(&self.config, &self.storage).await
        } else {
            let (result, exit) = runtime::models(&self.config, kind).await;
            if let Some(runtime::Event::Exited {
                reason,
                expected,
                details,
                ..
            }) = exit
            {
                self.observability
                    .agent_exit(None, kind, reason, expected, details);
            }
            result
        };
        let models = result.map_err(|_| Error::Conflict("model catalog unavailable"))?;
        *cached = Some((Instant::now(), models.clone()));
        Ok(models)
    }

    pub async fn capabilities(&self) -> Value {
        let mut harnesses = Vec::new();
        for (kind, profile) in &self.config.harnesses {
            let mut harness = json!({"id":kind,"model":profile.model,"provider":profile.provider});
            match kind {
                runtime::Kind::Codex | runtime::Kind::Claude => {
                    harness["models"] = if self.storage.blocks() {
                        Value::Null
                    } else {
                        match self.model_catalog(*kind).await {
                            Ok(models) => json!(models),
                            Err(_) => Value::Null,
                        }
                    };
                    harness["steer"] = json!(*kind == runtime::Kind::Codex);
                    harness["compact"] = json!(true);
                    harness["service_tier"] = json!(*kind == runtime::Kind::Codex);
                    harness["skill_mentions"] = json!(*kind == runtime::Kind::Claude);
                    harness["rewind"] = json!(true);
                    harness["attachments"] = json!({"images":true,"files":true});
                    harness["subagents"] = json!(true);
                    harness["usage"] = json!(true);
                }
                runtime::Kind::Cursor | runtime::Kind::Fx => {
                    let capabilities = runtime::cursor::capabilities();
                    for (key, value) in capabilities.as_object().unwrap() {
                        harness[key] = value.clone();
                    }
                    harness["reasoning_levels"] = json!(runtime::PI_REASONING_LEVELS);
                    harness["attachments"] = json!({"images":false,"files":true});
                    harness["provider_selection"] = json!(false);
                    if *kind == runtime::Kind::Cursor {
                        harness["models"] = if self.storage.blocks() {
                            Value::Null
                        } else {
                            match self.model_catalog(*kind).await {
                                Ok(models) => json!(models),
                                Err(_) => Value::Null,
                            }
                        };
                    }
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
        let mut capabilities = json!({"version":1,"repository":self.config.repository,"harnesses":harnesses,
            "previews":self.previews.enabled(),"mac_access":self.previews.enabled()});
        for feature in crate::FEATURES {
            capabilities[*feature] = json!(true);
        }
        capabilities
    }

    /// Follow-up reasoning is checked against the launch model. Omitted reasoning stays on the launch default.
    pub async fn check_prompt_reasoning(&self, id: &str, reasoning: Option<&str>) -> Result<()> {
        let Some(reasoning) = reasoning else {
            return Ok(());
        };
        let (kind, model) = {
            let local = self.local.lock().unwrap();
            let session = local.sessions.get(id).ok_or(Error::NotFound)?;
            if matches!(session.harness, runtime::Kind::Cursor | runtime::Kind::Fx)
                && session.reasoning.as_deref() != Some(reasoning)
            {
                return Err(Error::Conflict("invalid reasoning effort"));
            }
            let model = session.model.clone().or_else(|| {
                self.config
                    .harnesses
                    .get(&session.harness)
                    .map(|profile| profile.model.clone())
            });
            (session.harness, model)
        };
        let supported = match kind {
            runtime::Kind::Codex | runtime::Kind::Claude | runtime::Kind::Cursor => {
                let models = self.model_catalog(kind).await?;
                let selected = model.ok_or(Error::Conflict("invalid model"))?;
                models
                    .iter()
                    .find(|entry| entry.model == selected)
                    .ok_or(Error::Conflict("invalid model"))?
                    .reasoning_levels
                    .iter()
                    .any(|level| level == reasoning)
            }
            runtime::Kind::Pi | runtime::Kind::Fx => {
                runtime::PI_REASONING_LEVELS.contains(&reasoning)
            }
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
}
