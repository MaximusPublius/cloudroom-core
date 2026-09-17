use cloudroom::{config::Config, runtime};
use std::{
    fs,
    path::PathBuf,
    process::Command,
    sync::atomic::{AtomicU64, Ordering},
    time::{Duration, SystemTime, UNIX_EPOCH},
};

struct Directory(PathBuf);
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

fn fixture() -> (Directory, Config) {
    static NEXT: AtomicU64 = AtomicU64::new(0);
    let dir = Directory(std::env::temp_dir().join(format!(
        "cloudroom-runtime-{}-{}-{}", std::process::id(),
        SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_nanos(),
        NEXT.fetch_add(1, Ordering::Relaxed)
    )));
    fs::create_dir_all(dir.0.join(".codex")).unwrap();
    let config = Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        token: String::new(),
        state_dir: dir.0.join("state"),
        repository: dir.0.clone(),
        database_url: "postgres://127.0.0.1:1/fixture".into(),
        store: "fixture".into(),
        allow_insecure_database: true,
        account_home: dir.0.clone(),
        default_harness: runtime::Kind::Codex,
        harnesses: [(
            runtime::Kind::Codex,
            cloudroom::config::HarnessConfig {
                binary: PathBuf::from(env!("CARGO_MANIFEST_DIR")).join("tests/core_fixture.py"),
                home: dir.0.join(".codex"),
                model: "fixture".into(),
                provider: None,
            },
        )]
        .into(),
        max_harnesses: 1,
        storage: None,
    };
    (dir, config)
}

#[tokio::test]
async fn workload_cleanup_requires_confirmed_empty_group() {
    let (dir, mut config) = fixture();
    let root = dir.0.join("agents");
    fs::create_dir(&root).unwrap();
    // Controlled kernel-file responses; real cgroup containment is a Linux E2E check.
    fs::write(root.join("cgroup.events"), "populated 1\n").unwrap();
    config.storage = Some(cloudroom::workspace::storage::Policy {
        agent_uid: 1,
        agent_gid: 1,
        quota_mount: "/".into(),
        quota_limit_bytes: 1024,
        cache_dir: dir.0.join("cache"),
        cgroup_root: root.clone(),
        reserve_bytes: 1,
        warning_bytes: 2,
        pause_bytes: 1,
        resume_bytes: 3,
    });
    let caller = config.clone();
    let cleanup = tokio::spawn(async move { runtime::reconcile(&caller).await });
    tokio::time::sleep(Duration::from_millis(100)).await;
    assert_eq!(fs::read_to_string(root.join("cgroup.kill")).unwrap(), "1");
    assert!(
        !cleanup.is_finished(),
        "kill submission is not proof of exit"
    );
    fs::write(root.join("cgroup.events"), "populated 0\n").unwrap();
    cleanup.await.unwrap().unwrap();
    fs::write(root.join("cgroup.events"), "populated 1\n").unwrap();
    assert!(
        runtime::reconcile(&config).await.is_err(),
        "live group must time out, never permit replacement"
    );
    fs::remove_file(root.join("cgroup.kill")).unwrap();
    fs::create_dir(root.join("cgroup.kill")).unwrap();
    assert!(
        runtime::reconcile(&config).await.is_err(),
        "failed kill must not permit replacement"
    );
}

#[tokio::test]
async fn unresponsive_harness_is_not_reported_as_cleanly_closed() {
    let (_dir, config) = fixture();
    let (handle, mut events) = runtime::Handle::spawn(&config, runtime::Kind::Codex, None).unwrap();
    let exited = tokio::spawn(async move {
        while let Some(event) = events.recv().await {
            if let runtime::Event::Exited {
                expected, reason, ..
            } = event
            {
                return (expected, reason);
            }
        }
        panic!("missing process exit record");
    });
    handle.start_session().await.unwrap();
    handle.send("hang", "hang").await.unwrap();
    handle.request_shutdown();
    let result = tokio::time::timeout(Duration::from_secs(6), exited)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(result, (false, "harness shutdown timed out"));
}

#[tokio::test]
async fn turn_notifications_survive_both_rpc_reply_orders() {
    let (_dir, config) = fixture();
    let (handle, mut events) = runtime::Handle::spawn(&config, runtime::Kind::Codex, None).unwrap();
    handle.start_session().await.unwrap();
    for text in ["reply-first", "finish-first", "events-first"] {
        handle.send(text, text).await.unwrap();
        tokio::time::timeout(Duration::from_secs(3), async {
            let mut started = None;
            while let Some(event) = events.recv().await {
                match event {
                    runtime::Event::Started { request: id, .. } => {
                        assert!(started.is_none());
                        started = Some(id);
                    }
                    runtime::Event::Finished {
                        request: id,
                        status,
                    } => {
                        assert_eq!(started.as_deref(), Some(id.as_str()));
                        assert_eq!(status, "completed");
                        return;
                    }
                    _ => {}
                }
            }
            panic!("missing turn notifications");
        })
        .await
        .unwrap();
    }
    handle.request_shutdown();
    tokio::time::timeout(Duration::from_secs(6), async {
        while events.recv().await.is_some() {}
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn resume_reattaches_and_continues_the_native_rollout() {
    let (_dir, config) = fixture();
    // First run: start, take a turn, capture the native rollout bytes, then stop.
    let (handle, mut events) = runtime::Handle::spawn(&config, runtime::Kind::Codex, None).unwrap();
    let native = handle.start_session().await.unwrap();
    handle.send("hello", "hello").await.unwrap();
    handle.request_shutdown();
    let mut offset = 0u64;
    let mut path = None;
    tokio::time::timeout(Duration::from_secs(6), async {
        while let Some(event) = events.recv().await {
            match event {
                runtime::Event::Record {
                    kind: "native_record",
                    data,
                    native: Some(raw),
                } => {
                    assert_eq!(data["offset"].as_u64(), Some(offset));
                    offset += raw.len() as u64;
                }
                runtime::Event::Record {
                    kind: "native_identity",
                    data,
                    ..
                } => path = data["path"].as_str().map(PathBuf::from),
                runtime::Event::Exited { .. } => break,
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    let path = path.expect("native path");
    let first_offset = offset;
    assert!(first_offset > 0);

    // Resume seeded at the captured offset: only new appends, contiguously numbered.
    let (handle, mut events) = runtime::Handle::spawn(
        &config,
        runtime::Kind::Codex,
        Some(runtime::Resume {
            id: native.clone(),
            path,
            cursor: serde_json::json!({"offset":first_offset}),
            model: None,
            provider: None,
            reasoning: None,
        }),
    )
    .unwrap();
    assert_eq!(handle.start_session().await.unwrap(), native);
    handle.send("again", "again").await.unwrap();
    handle.request_shutdown();
    let mut resumed = Vec::new();
    tokio::time::timeout(Duration::from_secs(6), async {
        while let Some(event) = events.recv().await {
            match event {
                runtime::Event::Record {
                    kind: "native_record",
                    data,
                    native: Some(raw),
                } => resumed.push((data["offset"].as_u64().unwrap(), raw)),
                runtime::Event::Exited { .. } => break,
                _ => {}
            }
        }
    })
    .await
    .unwrap();
    assert!(!resumed.is_empty(), "resume captured no new rollout");
    let mut expect = first_offset;
    for (recorded, raw) in &resumed {
        assert_eq!(
            *recorded, expect,
            "resumed rollout offset is not contiguous"
        );
        expect += raw.len() as u64;
    }
}

#[tokio::test]
async fn dropping_the_last_handle_stops_its_tools() {
    check_shutdown("drop").await;
}

#[tokio::test]
async fn shutdown_cancels_pending_commands_as_uncertain() {
    let (dir, config) = fixture();
    let (handle, mut events) = runtime::Handle::spawn(&config, runtime::Kind::Codex, None).unwrap();
    let drained = tokio::spawn(async move { while events.recv().await.is_some() {} });
    handle.start_session().await.unwrap();
    let caller = handle.clone();
    let pending = tokio::spawn(async move { caller.send("pending", "no-reply").await });
    tokio::time::timeout(Duration::from_secs(3), async {
        while !dir.0.join("no-reply").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .unwrap();
    handle.request_shutdown();
    let error = tokio::time::timeout(Duration::from_secs(3), pending)
        .await
        .unwrap()
        .unwrap()
        .unwrap_err();
    assert_eq!(error.kind(), std::io::ErrorKind::Other);
    assert!(error.to_string().contains("uncertain"));
    tokio::time::timeout(Duration::from_secs(3), drained)
        .await
        .unwrap()
        .unwrap();
}

#[tokio::test]
async fn shutdown_stops_tools_and_records_the_final_native_tail() {
    check_shutdown("fixture").await;
}

#[tokio::test]
async fn stdout_eof_does_not_cut_short_tool_cleanup() {
    check_shutdown("slow-exit").await;
}

async fn check_shutdown(model: &str) {
    let (dir, mut config) = fixture();
    config
        .harnesses
        .get_mut(&runtime::Kind::Codex)
        .unwrap()
        .model = model.into();
    let (handle, mut incoming) =
        runtime::Handle::spawn(&config, runtime::Kind::Codex, None).unwrap();
    let records = tokio::spawn(async move {
        let mut native = String::new();
        let mut expected = None;
        let mut identities = Vec::new();
        while let Some(event) = incoming.recv().await {
            match event {
                runtime::Event::Record {
                    kind: "native_identity",
                    data,
                    native,
                } => {
                    assert!(native.is_none());
                    identities.push(data);
                }
                runtime::Event::Record {
                    kind,
                    data,
                    native: Some(raw),
                } => {
                    if kind == "native_record" {
                        native.push_str(&raw);
                    } else {
                        assert!(
                            data.get("value").is_none(),
                            "duplicated native params in storage payload"
                        );
                    }
                }
                runtime::Event::Exited {
                    expected: status, ..
                } => expected = Some(status),
                _ => {}
            }
        }
        (native, expected, identities)
    });
    let native_id = handle.start_session().await.unwrap();
    handle.send("hold", "hold").await.unwrap();
    let ticks = dir.0.join(format!("{native_id}.ticks"));
    tokio::time::timeout(Duration::from_secs(5), async {
        while !ticks.exists() {
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    if model == "drop" {
        drop(handle);
    } else {
        handle.request_shutdown();
    }
    let (captured, expected, identities) = tokio::time::timeout(Duration::from_secs(6), records)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(expected, Some(true));
    assert!(identities.contains(&serde_json::json!({"id":native_id,"path":config.harnesses[&runtime::Kind::Codex].home.join("sessions").join(format!("{native_id}.jsonl"))})));
    let pid = fs::read_to_string(dir.0.join(format!("{native_id}.pid"))).unwrap();
    assert!(
        !Command::new("/bin/kill")
            .args(["-0", pid.trim()])
            .output()
            .unwrap()
            .status
            .success(),
        "tool process survived shutdown"
    );
    let before = fs::read_to_string(&ticks).unwrap();
    tokio::time::sleep(Duration::from_millis(300)).await;
    assert_eq!(
        before,
        fs::read_to_string(&ticks).unwrap(),
        "tool kept writing after shutdown completed"
    );
    assert!(
        captured.contains("shutdown"),
        "missing final native rollout record"
    );
    assert_eq!(
        captured,
        fs::read_to_string(
            config.harnesses[&runtime::Kind::Codex]
                .home
                .join("sessions")
                .join(format!("{native_id}.jsonl"))
        )
        .unwrap()
    );
}
