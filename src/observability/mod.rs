mod system;
#[cfg(test)]
mod tests;

use crate::config::Config;
use serde::Serialize;
use serde_json::{Value, json};
use sqlx::PgPool;
use std::{
    collections::{VecDeque, hash_map::RandomState},
    fs::{self, OpenOptions},
    hash::BuildHasher,
    io::{self, Write},
    os::unix::fs::OpenOptionsExt,
    path::Path,
    sync::{
        Arc,
        atomic::{AtomicU64, Ordering},
    },
    time::{Duration, Instant, SystemTime, UNIX_EPOCH},
};
use tokio::sync::{mpsc, watch};

const CAPACITY: usize = 1024;
const LOCAL_BYTES: u64 = 8 * 1024 * 1024;

/// Only diagnostic fields belong here: never prompts, native payloads, paths or credentials.
#[derive(Serialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub(crate) enum Signal {
    Api {
        method: &'static str,
        route: String,
        status: u16,
        duration_ms: u64,
    },
    AgentStart {
        session_id: String,
        success: bool,
        duration_ms: u64,
    },
    AgentExit {
        session_id: String,
        expected: bool,
        reason: &'static str,
    },
    HistoryUpload {
        records: usize,
        pending_records: u64,
        success: bool,
        duration_ms: u64,
    },
    HistoryFault {
        operation: &'static str,
    },
    Resources(system::Resources),
    Diagnostics {
        dropped: u64,
        local_write_failures: u64,
        upload_failures: u64,
    },
}

#[derive(Serialize)]
struct Record {
    run_id: String,
    sequence: u64,
    timestamp_ms: u64,
    #[serde(flatten)]
    signal: Signal,
}

#[derive(Clone)]
pub(crate) struct Observability {
    sender: mpsc::Sender<Record>,
    run_id: Arc<String>,
    sequence: Arc<AtomicU64>,
    dropped: Arc<AtomicU64>,
    stop: watch::Sender<bool>,
    finished: watch::Receiver<bool>,
    resources: watch::Sender<(Option<u64>, Value)>,
}

pub(crate) fn now_ms() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_millis() as u64
}

pub(crate) fn elapsed_ms(start: Instant) -> u64 {
    start.elapsed().as_millis().min(u64::MAX as u128) as u64
}

impl Observability {
    pub fn start(config: &Config, pool: PgPool) -> Self {
        let (sender, receiver) = mpsc::channel(CAPACITY);
        let (stop, stopping) = watch::channel(false);
        let (done, finished) = watch::channel(false);
        let this = Self {
            sender,
            run_id: Arc::new(format!(
                "{:016x}",
                RandomState::new().hash_one(SystemTime::now())
            )),
            sequence: Arc::new(AtomicU64::new(0)),
            dropped: Arc::new(AtomicU64::new(0)),
            stop,
            finished,
            resources: watch::channel((None, json!({"cpu":null,"memory":null,"disk":null}))).0,
        };
        let config = config.clone();
        let worker = this.clone();
        tokio::spawn(async move {
            worker.run(config, pool, receiver, stopping).await;
            done.send_replace(true);
        });
        this
    }

    fn make_record(&self, signal: Signal) -> Record {
        Record {
            run_id: self.run_id.as_ref().clone(),
            sequence: self.sequence.fetch_add(1, Ordering::Relaxed) + 1,
            timestamp_ms: now_ms(),
            signal,
        }
    }

    /// No disk or database waits on the caller's path. Overload is counted, not backpressured.
    pub fn record(&self, signal: Signal) -> String {
        let record = self.make_record(signal);
        let id = format!("{}-{}", record.run_id, record.sequence);
        if self.sender.try_send(record).is_err() {
            self.dropped.fetch_add(1, Ordering::Relaxed);
        }
        id
    }

    pub fn resources(&self) -> (Option<u64>, Value) {
        self.resources.borrow().clone()
    }

    pub async fn shutdown(&self) {
        self.stop.send_replace(true);
        let mut finished = self.finished.clone();
        let _ = tokio::time::timeout(Duration::from_secs(5), finished.wait_for(|done| *done)).await;
    }

    async fn run(
        &self,
        config: Config,
        pool: PgPool,
        mut receiver: mpsc::Receiver<Record>,
        mut stop: watch::Receiver<bool>,
    ) {
        let mut tick = tokio::time::interval(Duration::from_secs(1));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        let mut pending = VecDeque::new();
        let mut sampler = system::Sampler::default();
        let mut sample_at = Instant::now();
        let mut prune_at = Instant::now();
        let (mut local_write_failures, mut upload_failures) = (0, 0);
        loop {
            tokio::select! { _ = tick.tick() => {}, _ = stop.changed() => {} }
            let stopping = *stop.borrow();
            if stopping {
                receiver.close();
            }
            let mut batch = Vec::new();
            for _ in 0..CAPACITY {
                match receiver.try_recv() {
                    Ok(record) => batch.push(record),
                    Err(_) => break,
                }
            }
            if !stopping && Instant::now() >= sample_at {
                let resources = sampler.sample(&config).await;
                self.resources
                    .send_replace((Some(now_ms()), resources.dashboard()));
                batch.push(self.make_record(Signal::Resources(resources)));
                batch.push(self.make_record(Signal::Diagnostics {
                    dropped: self.dropped.load(Ordering::Relaxed),
                    local_write_failures,
                    upload_failures,
                }));
                sample_at = Instant::now() + Duration::from_secs(10);
            }
            if !batch.is_empty() {
                let mut bytes = Vec::new();
                for record in &batch {
                    serde_json::to_writer(&mut bytes, record).expect("diagnostic serialization");
                    bytes.push(b'\n');
                }
                let directory = config.state_dir.clone();
                if !matches!(
                    tokio::task::spawn_blocking(move || write_local(
                        &directory,
                        &bytes,
                        LOCAL_BYTES
                    ))
                    .await,
                    Ok(Ok(()))
                ) {
                    local_write_failures += 1;
                    eprintln!("Cloudroom diagnostic file write failed");
                }
                pending.extend(batch);
                while pending.len() > CAPACITY {
                    pending.pop_front();
                    self.dropped.fetch_add(1, Ordering::Relaxed);
                }
            }
            if !pending.is_empty() {
                let payload = serde_json::to_string(&pending).expect("diagnostic serialization");
                let upload = sqlx::query(
                    "INSERT INTO cloudroom_diagnostics (store, run_id, sequence, timestamp_ms, record) \
                     SELECT $1, value->>'run_id', (value->>'sequence')::bigint, (value->>'timestamp_ms')::bigint, value \
                     FROM jsonb_array_elements($2::jsonb) ON CONFLICT (store, run_id, sequence) DO NOTHING")
                    .bind(&config.store).bind(payload).execute(&pool);
                if matches!(
                    tokio::time::timeout(Duration::from_secs(2), upload).await,
                    Ok(Ok(_))
                ) {
                    pending.clear();
                } else {
                    upload_failures += 1;
                }
            }
            if stopping {
                return;
            }
            if *stop.borrow() {
                continue;
            }
            if Instant::now() >= prune_at {
                // Scope retention to this store; never touch session history or another owner.
                let prune = sqlx::query("DELETE FROM cloudroom_diagnostics WHERE store=$1 AND timestamp_ms < (extract(epoch FROM now() - interval '7 days') * 1000)::bigint")
                    .bind(&config.store).execute(&pool);
                if !matches!(
                    tokio::time::timeout(Duration::from_secs(2), prune).await,
                    Ok(Ok(_))
                ) {
                    upload_failures += 1;
                }
                prune_at = Instant::now() + Duration::from_secs(3600);
            }
        }
    }
}

fn write_local(directory: &Path, bytes: &[u8], limit: u64) -> io::Result<()> {
    let path = directory.join("diagnostics.jsonl");
    if fs::metadata(&path).is_ok_and(|m| m.len() + bytes.len() as u64 > limit) {
        fs::rename(&path, directory.join("diagnostics.previous.jsonl"))?;
    }
    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .mode(0o600)
        .open(path)?;
    file.write_all(bytes)
}
