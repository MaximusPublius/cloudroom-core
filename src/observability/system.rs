use crate::config::Config;
use crate::workspace::storage::{Disk, disk};
use serde::Serialize;
use serde_json::{Value, json};
use std::fs;

#[derive(Serialize)]
pub(crate) struct Resources {
    cpu_used_percent: Option<f64>,
    memory_total_bytes: Option<u64>,
    memory_used_bytes: Option<u64>,
    workspace_disk: Option<Disk>,
    state_disk: Option<Disk>,
    pub(super) agents: Option<super::metrics::AgentCounts>,
}

impl Resources {
    pub(super) fn dashboard(&self) -> Value {
        let percent = |used: Option<u64>, total: Option<u64>| {
            used.zip(total)
                .filter(|(used, total)| *total > 0 && used <= total)
                .map(|(used, total)| used as f64 * 100.0 / total as f64)
        };
        json!({"cpu": self.cpu_used_percent,
            "memory": percent(self.memory_used_bytes, self.memory_total_bytes),
            "disk": self.workspace_disk.as_ref().and_then(|d| percent(Some(d.used_bytes), d.used_bytes.checked_add(d.available_bytes))),
            "memoryUsedBytes": self.memory_used_bytes, "memoryTotalBytes": self.memory_total_bytes,
            "diskAvailableBytes": self.workspace_disk.as_ref().map(|d| d.available_bytes),
            "diskTotalBytes": self.workspace_disk.as_ref().map(|d| d.total_bytes)})
    }
}

#[derive(Default)]
pub(super) struct Sampler {
    cpu: Option<(u64, u64)>,
}

impl Sampler {
    pub async fn sample(&mut self, config: &Config) -> Resources {
        let current = fs::read_to_string("/proc/stat").ok().and_then(|s| cpu(&s));
        let cpu_used_percent =
            current
                .zip(self.cpu)
                .and_then(|((total, idle), (old_total, old_idle))| {
                    let total = total.checked_sub(old_total)?;
                    let idle = idle.checked_sub(old_idle)?;
                    (total > 0 && idle <= total)
                        .then(|| (total - idle) as f64 * 100.0 / total as f64)
                });
        self.cpu = current;
        let memory = fs::read_to_string("/proc/meminfo").unwrap_or_default();
        let memory_total_bytes = memory_field(&memory, "MemTotal:");
        let memory_used_bytes = memory_total_bytes
            .zip(memory_field(&memory, "MemAvailable:"))
            .and_then(|(total, available)| total.checked_sub(available));
        let (workspace_disk, state_disk) =
            tokio::join!(disk(&config.repository), disk(&config.state_dir));
        Resources {
            cpu_used_percent,
            memory_total_bytes,
            memory_used_bytes,
            workspace_disk: workspace_disk.ok(),
            state_disk: state_disk.ok(),
            agents: None,
        }
    }
}

#[test]
fn disk_percentage_excludes_reserved_blocks() {
    let mut resources = Resources {
        cpu_used_percent: None,
        memory_total_bytes: None,
        memory_used_bytes: None,
        workspace_disk: Some(Disk {
            total_bytes: 80,
            used_bytes: 77,
            available_bytes: 0,
        }),
        state_disk: None,
        agents: None,
    };
    assert_eq!(resources.dashboard()["disk"], 100.0);
    resources.workspace_disk = Some(Disk {
        total_bytes: 80,
        used_bytes: 38,
        available_bytes: 38,
    });
    assert_eq!(resources.dashboard()["disk"], 50.0);
    resources.workspace_disk = None;
    assert!(resources.dashboard()["disk"].is_null());
}

fn cpu(text: &str) -> Option<(u64, u64)> {
    let mut line = text.lines().next()?.split_whitespace();
    if line.next()? != "cpu" {
        return None;
    }
    // Guest time is already included in user/nice; do not count the last two fields twice.
    let counters: Vec<u64> = line
        .take(8)
        .map(str::parse)
        .collect::<Result<_, _>>()
        .ok()?;
    if counters.len() < 8 {
        return None;
    }
    Some((counters.iter().sum(), counters[3] + counters[4]))
}

fn memory_field(text: &str, name: &str) -> Option<u64> {
    text.lines().find_map(|line| {
        let mut fields = line.split_whitespace();
        if fields.next()? != name {
            return None;
        }
        fields.next()?.parse::<u64>().ok()?.checked_mul(1024)
    })
}
