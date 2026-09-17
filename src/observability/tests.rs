use super::*;
use std::os::unix::fs::PermissionsExt;

struct Directory(std::path::PathBuf);
impl Directory {
    fn new() -> Self {
        let path = std::env::temp_dir().join(format!(
            "cloudroom-diagnostics-{:x}",
            RandomState::new().hash_one(SystemTime::now())
        ));
        fs::create_dir(&path).unwrap();
        Self(path)
    }
}
impl Drop for Directory {
    fn drop(&mut self) {
        let _ = fs::remove_dir_all(&self.0);
    }
}

#[test]
fn local_rotation_preserves_the_previous_file_with_private_permissions() {
    let directory = Directory::new();
    write_local(&directory.0, b"first\n", 12).unwrap();
    write_local(&directory.0, b"second\n", 12).unwrap();
    assert_eq!(
        fs::read(directory.0.join("diagnostics.previous.jsonl")).unwrap(),
        b"first\n"
    );
    assert_eq!(
        fs::read(directory.0.join("diagnostics.jsonl")).unwrap(),
        b"second\n"
    );
    write_local(&directory.0, b"third\n", 12).unwrap();
    assert_eq!(
        fs::read(directory.0.join("diagnostics.previous.jsonl")).unwrap(),
        b"second\n"
    );
    for file in fs::read_dir(&directory.0).unwrap() {
        assert_eq!(
            file.unwrap().metadata().unwrap().permissions().mode() & 0o777,
            0o600
        );
    }
}

#[tokio::test]
async fn database_outage_keeps_local_diagnostics_and_overload_is_visible() {
    let directory = Directory::new();
    let config = Config {
        listen: "127.0.0.1:0".parse().unwrap(),
        token: String::new(),
        state_dir: directory.0.clone(),
        repository: directory.0.clone(),
        database_url: "postgres://127.0.0.1:1/test".into(),
        store: "test".into(),
        allow_insecure_database: true,
        account_home: directory.0.clone(),
        default_harness: crate::runtime::Kind::Codex,
        harnesses: [(
            crate::runtime::Kind::Codex,
            crate::config::HarnessConfig {
                binary: "/bin/false".into(),
                home: directory.0.clone(),
                model: "fixture".into(),
                provider: None,
            },
        )]
        .into(),
        max_harnesses: 1,
        storage: None,
    };
    let pool = sqlx::postgres::PgPoolOptions::new()
        .connect_lazy(&config.database_url)
        .unwrap();
    let diagnostics = Observability::start(&config, pool);
    // No await: the producer must neither block nor allocate an unbounded queue.
    for _ in 0..CAPACITY * 2 {
        diagnostics.record(Signal::HistoryFault { operation: "test" });
    }
    let path = directory.0.join("diagnostics.jsonl");
    let records = tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if let Ok(text) = fs::read_to_string(&path) {
                let records: Result<Vec<serde_json::Value>, _> =
                    text.lines().map(serde_json::from_str).collect();
                if let Ok(records) = records
                    && records.iter().any(|r| r["kind"] == "diagnostics")
                {
                    break records;
                }
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        records
            .iter()
            .filter(|r| r["kind"] == "history_fault")
            .count(),
        CAPACITY
    );
    assert!(
        records.iter().any(
            |r| r["kind"] == "diagnostics" && r["dropped"].as_u64().unwrap() >= CAPACITY as u64
        )
    );
    diagnostics.shutdown().await;
}
