//! Background loops: the disk safety guard and the history uploader.
use super::*;

impl Manager {
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
                            let text = if snapshot.reason == "measurement_unavailable" {
                                "Cloudroom: disk space could not be measured. Work is paused until storage can be verified. Your files and history are preserved."
                            } else if snapshot.level == Level::Blocked {
                                "Cloudroom: disk space is critically low. Work is paused until storage recovers. Your files and history are preserved."
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
                                        json!({"paused":true,"reason":snapshot.reason,"text":if snapshot.reason == "measurement_unavailable" {
                                            "Cloudroom: disk space could not be measured. Work is paused until storage can be verified."
                                        } else {
                                            "Cloudroom: work paused because disk space is critically low. Work resumes automatically when space recovers."
                                        }}),
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
}
