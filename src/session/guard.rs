//! Background loops: the disk safety guard and the history uploader.
use super::*;
use crate::workspace::storage::FRESH;

/// How long work stays frozen before Cloudroom stops the command filling the disk.
const RESCUE_AFTER: Duration = Duration::from_secs(30);

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
            let mut blocked_since: Option<Instant> = None;
            let mut measured = Instant::now();
            while !manager.is_stopping() {
                let snapshot = manager.check_storage().await;
                if measured.elapsed() > FRESH {
                    let gap_ms = elapsed_ms(measured);
                    manager
                        .observability
                        .record(Signal::StorageStale { gap_ms });
                }
                measured = Instant::now();
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
                                "Cloudroom: the disk is almost full, so work is paused. If space does not recover within 30 seconds, Cloudroom stops the command filling it. Your files and history are preserved."
                            } else {
                                "Cloudroom: disk space is running low. Work and sync continue. Delete files you no longer need; if the disk fills, Cloudroom pauses work and stops the command filling it."
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
                                            "Cloudroom: work paused because the disk is almost full. If space does not recover within 30 seconds, Cloudroom stops the command filling it."
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
                    let since = *blocked_since.get_or_insert_with(Instant::now);
                    if all_paused
                        && snapshot.reason == "disk_capacity"
                        && since.elapsed() >= RESCUE_AFTER
                    {
                        manager.rescue().await;
                        blocked_since = None;
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
                    blocked_since = None;
                }
                tokio::time::sleep(Duration::from_secs(1)).await;
            }
        });
    }

    /// Frozen work never frees space, so a pause could last forever. Stops the command filling the disk in
    /// each session and tells its agent to clean up. The lowered threshold (`Guard::rescue`) then lets the
    /// guard loop thaw everything.
    async fn rescue(&self) {
        let handles: Vec<_> = self
            .local
            .lock()
            .unwrap()
            .sessions
            .values()
            .filter_map(|s| s.handle.clone().map(|h| (s.session_id.clone(), h)))
            .collect();
        self.storage.rescue();
        for (id, handle) in handles {
            let text = match handle.kill_top_writer() {
                Ok(Some(command)) => {
                    let command: String = command.replace('`', "'").chars().take(160).collect();
                    format!(
                        "Cloudroom: the disk is full, so Cloudroom stopped `{command}`, which was filling it. Free space before continuing: delete files you no longer need, like build output, caches, or node_modules, and check with `df -h`. Do not rerun that command as is."
                    )
                }
                _ => "Cloudroom: the disk is full. Work resumes so you can free space: delete files you no longer need, like build output, caches, or node_modules, and check with `df -h`.".into(),
            };
            // Recorded now for the user; the agent reads it once the guard loop thaws it.
            if self
                .local
                .lock()
                .unwrap()
                .append(
                    &id,
                    "storage_warning",
                    json!({"text":text,"reason":"rescue"}),
                    None,
                )
                .is_ok()
            {
                tokio::spawn(async move {
                    let _ =
                        tokio::time::timeout(Duration::from_secs(10), handle.system_message(&text))
                            .await;
                });
            }
        }
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
