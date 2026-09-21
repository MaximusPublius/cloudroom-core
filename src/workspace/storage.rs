//! Disk policy and explicitly disposable npm download-cache cleanup, not history retention.
use crate::{config::Config, runtime::linux};
use serde::{Deserialize, Serialize};
use std::{
    fs, io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::{Arc, Mutex, Weak},
    time::{Duration, Instant},
};
use tokio::process::Command;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub agent_uid: u32,
    pub agent_gid: u32,
    pub cache_dir: PathBuf,
    pub cgroup_root: PathBuf,
    #[serde(default = "warning")]
    pub warning_bytes: u64,
    #[serde(default = "pause")]
    pub pause_bytes: u64,
    #[serde(default = "resume")]
    pub resume_bytes: u64,
}
fn warning() -> u64 {
    5_000_000_000
}
fn pause() -> u64 {
    2_000_000_000
}
fn resume() -> u64 {
    2_500_000_000
}

impl Policy {
    pub fn load(path: &Path) -> io::Result<Self> {
        let policy: Self = serde_json::from_slice(&fs::read(path)?)?;
        if policy.agent_uid == 0
            || policy.agent_gid == 0
            || !(0 < policy.pause_bytes
                && policy.pause_bytes < policy.resume_bytes
                && policy.resume_bytes < policy.warning_bytes)
        {
            return Err(io::Error::other("invalid disk protection policy"));
        }
        // Neither the policy nor its ancestors may be replaced by agent writes.
        for part in path.ancestors() {
            let m = fs::symlink_metadata(part)?;
            if m.file_type().is_symlink() || m.uid() == policy.agent_uid || m.mode() & 0o022 != 0 {
                return Err(io::Error::other(
                    "disk policy must be protected from agent writes",
                ));
            }
        }
        Ok(policy)
    }

    fn check_filesystems(&self, config: &Config) -> io::Result<()> {
        let mount = fs::metadata(&config.repository)?;
        for path in [
            &config.repository,
            &config.state_dir,
            &config.account_home,
            &self.cache_dir,
            Path::new("/tmp"),
            Path::new("/var/tmp"),
            Path::new("/code"),
        ]
        .into_iter()
        .chain(
            config
                .harnesses
                .values()
                .map(|profile| profile.home.as_path()),
        ) {
            if fs::metadata(path)?.dev() != mount.dev() {
                return Err(io::Error::other(
                    "agent paths must share the monitored filesystem",
                ));
            }
        }
        Ok(())
    }

    pub fn prepare(&self, config: &Config) -> io::Result<()> {
        if !cfg!(target_os = "linux") {
            return Err(io::Error::other("disk protection requires Linux"));
        }
        self.check_filesystems(config)?;
        if fs::metadata(&config.state_dir)?.uid() == self.agent_uid {
            return Err(io::Error::other(
                "history must belong to the protected service account",
            ));
        }
        let parent = self
            .cache_dir
            .parent()
            .ok_or_else(|| io::Error::other("invalid cache root"))?;
        for part in parent.ancestors() {
            let m = fs::symlink_metadata(part)?;
            if m.file_type().is_symlink() || m.uid() == self.agent_uid || m.mode() & 0o022 != 0 {
                return Err(io::Error::other("cache root requires protected ancestors"));
            }
        }
        let current = fs::read_to_string("/proc/self/cgroup")?;
        let current = current
            .lines()
            .find_map(|l| l.strip_prefix("0::"))
            .ok_or_else(|| io::Error::other("cgroup v2 is required"))?;
        let owner = Path::new("/sys/fs/cgroup").join(current.trim_start_matches('/'));
        if self.cgroup_root != owner.join("agents") {
            return Err(io::Error::other(
                "agent cgroups must be inside the core's delegated service",
            ));
        }
        if !self.cgroup_root.exists() {
            fs::create_dir(&self.cgroup_root)?;
        }
        if !self.cgroup_root.join("cgroup.freeze").is_file()
            || !self.cgroup_root.join("cgroup.kill").is_file()
        {
            return Err(io::Error::other(
                "workload freeze and kill controls are required",
            ));
        }
        Ok(())
    }

    fn level(&self, available: u64, previous: Level) -> Level {
        if available < self.pause_bytes
            || (previous == Level::Blocked && available <= self.resume_bytes)
        {
            Level::Blocked
        } else if available <= self.warning_bytes {
            Level::LowSpace
        } else {
            Level::Normal
        }
    }
}

#[derive(Clone, Copy, Debug, PartialEq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum Level {
    Normal,
    LowSpace,
    Blocked,
}
#[derive(Clone, Serialize)]
pub struct Snapshot {
    pub enabled: bool,
    pub level: Level,
    pub workspace_available_bytes: Option<u64>,
    pub history_available_bytes: Option<u64>,
    pub reason: &'static str,
}
impl Snapshot {
    fn unavailable(enabled: bool) -> Self {
        Self {
            enabled,
            level: if enabled {
                Level::Blocked
            } else {
                Level::Normal
            },
            workspace_available_bytes: None,
            history_available_bytes: None,
            reason: if enabled {
                "measurement_unavailable"
            } else {
                "unprotected_test_mode"
            },
        }
    }
}

pub struct Guard {
    policy: Option<Policy>,
    value: Mutex<(Snapshot, Instant)>,
    writers: Mutex<Vec<Weak<linux::Workload>>>,
}
impl Guard {
    pub fn new(config: &Config) -> io::Result<Self> {
        if let Some(p) = &config.storage {
            p.prepare(config)?;
        }
        Ok(Self {
            policy: config.storage.clone(),
            writers: Mutex::new(Vec::new()),
            value: Mutex::new((
                Snapshot::unavailable(config.storage.is_some()),
                Instant::now(),
            )),
        })
    }
    pub fn snapshot(&self) -> Snapshot {
        let (value, sampled) = &*self.value.lock().unwrap();
        if value.enabled && sampled.elapsed() > Duration::from_secs(5) {
            Snapshot::unavailable(true)
        } else {
            value.clone()
        }
    }
    pub fn blocks(&self) -> bool {
        self.snapshot().level == Level::Blocked
    }
    pub(crate) fn spawn_writer(
        &self,
        command: &mut Command,
    ) -> io::Result<(tokio::process::Child, Option<Arc<linux::Workload>>)> {
        // Serialize admission with pause snapshots so a just-started writer is never missed.
        let mut writers = self.writers.lock().unwrap();
        if self.blocks() {
            return Err(io::Error::new(
                io::ErrorKind::WouldBlock,
                "storage unsafe; file writes are blocked",
            ));
        }
        let workload = self
            .policy
            .as_ref()
            .map(|policy| {
                let group = Arc::new(linux::Workload::create(&policy.cgroup_root)?);
                group.attach(command, policy.agent_uid, policy.agent_gid)?;
                Ok::<_, io::Error>(group)
            })
            .transpose()?;
        let child = command.spawn()?;
        writers.retain(|writer| writer.strong_count() > 0);
        if let Some(group) = &workload {
            writers.push(Arc::downgrade(group));
        }
        Ok((child, workload))
    }

    pub(crate) async fn pause_writers(&self, paused: bool) -> io::Result<()> {
        let writers: Vec<_> = self
            .writers
            .lock()
            .unwrap()
            .iter()
            .filter_map(Weak::upgrade)
            .collect();
        let mut result = Ok(());
        for writer in writers {
            if let Err(error) = writer.freeze(paused).await {
                result = Err(error);
            }
        }
        result
    }

    pub async fn refresh(&self, config: &Config, workspaces: &super::Workspaces) -> Snapshot {
        let Some(p) = &self.policy else {
            return self.snapshot();
        };
        let previous = self.value.lock().unwrap().0.level;
        let measured = async {
            p.check_filesystems(config)?;
            workspaces.check_filesystems()?;
            let (workspace, history) =
                tokio::join!(disk(workspaces.root()), disk(&config.state_dir));
            let (workspace, history) = (workspace?, history?);
            let level = p.level(
                workspace.available_bytes.min(history.available_bytes),
                previous,
            );
            Ok::<_, io::Error>(Snapshot {
                enabled: true,
                level,
                workspace_available_bytes: Some(workspace.available_bytes),
                history_available_bytes: Some(history.available_bytes),
                reason: "disk_capacity",
            })
        }
        .await
        .unwrap_or_else(|_| Snapshot::unavailable(true));
        *self.value.lock().unwrap() = (measured.clone(), Instant::now());
        measured
    }
    pub async fn clean(&self) -> io::Result<()> {
        let Some(p) = &self.policy else {
            return Ok(());
        };
        let mut command = Command::new(std::env::current_exe()?);
        command
            .env_clear()
            .arg("--clean-npm-cache")
            .arg(&p.cache_dir)
            .arg(&p.cgroup_root)
            .kill_on_drop(true);
        linux::as_agent(&mut command, p.agent_uid, p.agent_gid, None)?;
        let result = tokio::time::timeout(Duration::from_secs(5), command.output())
            .await
            .map_err(|_| io::Error::other("cache cleanup timed out"))??;
        if !result.status.success() {
            return Err(io::Error::other("cache cleanup refused"));
        }
        Ok(())
    }
}

#[derive(Clone, Serialize)]
pub(crate) struct Disk {
    pub total_bytes: u64,
    pub used_bytes: u64,
    pub available_bytes: u64,
}
pub(crate) async fn disk(path: &Path) -> io::Result<Disk> {
    let output = tokio::time::timeout(
        Duration::from_secs(1),
        Command::new("/bin/df")
            .env_clear()
            .env("LC_ALL", "C")
            .arg("-Pk")
            .arg(path)
            .kill_on_drop(true)
            .output(),
    )
    .await
    .map_err(|_| io::Error::other("filesystem measurement timed out"))??;
    if !output.status.success() {
        return Err(io::Error::other("filesystem measurement failed"));
    }
    let text = String::from_utf8(output.stdout).map_err(io::Error::other)?;
    let fields: Vec<&str> = text
        .lines()
        .nth(1)
        .unwrap_or_default()
        .split_whitespace()
        .collect();
    let n = |i: usize| {
        fields
            .get(i)
            .and_then(|s| s.parse::<u64>().ok())
            .and_then(|n| n.checked_mul(1024))
            .ok_or_else(|| io::Error::other("invalid filesystem measurement"))
    };
    Ok(Disk {
        total_bytes: n(1)?,
        used_bytes: n(2)?,
        available_bytes: n(3)?,
    })
}

/// Runs only as the agent account, while workloads are frozen or absent. Never traverses
/// arbitrary projects, npm's executable _npx cache, histories, or symlinked directories.
#[cfg(target_os = "linux")]
pub fn clean_npm_cache(root: &Path, groups: &Path) -> io::Result<usize> {
    use std::{
        collections::HashSet,
        os::{fd::AsRawFd, unix::fs::OpenOptionsExt},
    };
    let open = |path: &Path| {
        fs::OpenOptions::new()
            .read(true)
            .custom_flags(0x20000 | 0x10000)
            .open(path)
    }; // O_NOFOLLOW | O_DIRECTORY
    let mut dir = open(root)?;
    for name in ["npm", "_cacache", "content-v2", "sha512"] {
        let next = PathBuf::from(format!("/proc/self/fd/{}/{}", dir.as_raw_fd(), name));
        match open(&next) {
            Ok(d) => dir = d,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(0),
            Err(e) => return Err(e),
        }
    }
    let uid = fs::metadata(root)?.uid();
    let mut active = HashSet::new();
    for proc in fs::read_dir("/proc")? {
        let proc = proc?;
        if !proc
            .file_name()
            .to_string_lossy()
            .bytes()
            .all(|b| b.is_ascii_digit())
        {
            continue;
        }
        let Ok(meta) = proc.metadata() else {
            continue;
        };
        if meta.uid() != uid || proc.file_name().to_string_lossy() == std::process::id().to_string()
        {
            continue;
        }
        let membership = fs::read_to_string(proc.path().join("cgroup"))?;
        let relative = membership
            .lines()
            .find_map(|l| l.strip_prefix("0::"))
            .ok_or_else(|| io::Error::other("unknown agent containment"))?;
        let group = Path::new("/sys/fs/cgroup").join(relative.trim_start_matches('/'));
        if !group.starts_with(groups)
            || !fs::read_to_string(group.join("cgroup.events"))?
                .lines()
                .any(|l| l == "frozen 1")
        {
            return Err(io::Error::other(
                "cleanup requires all agent processes to be quiescent",
            ));
        }
        // Mapped files can remain active after their file descriptor is closed.
        // Refuse this cleanup pass if any process maps content from the cache.
        let maps = fs::read_to_string(proc.path().join("maps"))?;
        if maps.contains(root.to_string_lossy().as_ref()) {
            return Err(io::Error::other("cache has active memory mappings"));
        }
        match fs::read_dir(proc.path().join("fd")) {
            Ok(fds) => {
                for fd in fds.flatten() {
                    if let Ok(m) = fs::metadata(fd.path()) {
                        active.insert((m.dev(), m.ino()));
                    }
                }
            }
            Err(e) if e.kind() == io::ErrorKind::NotFound => {}
            Err(e) => return Err(e), // Unverifiable active users mean no cleanup.
        }
    }
    let hex = |s: &str, len: usize| {
        s.len() == len
            && s.bytes()
                .all(|b| b.is_ascii_digit() || (b'a'..=b'f').contains(&b))
    };
    let mut removed = 0;
    for a in fs::read_dir(format!("/proc/self/fd/{}", dir.as_raw_fd()))? {
        let a = a?;
        if !hex(&a.file_name().to_string_lossy(), 2) {
            continue;
        }
        let Ok(a) = open(&a.path()) else {
            continue;
        };
        for b in fs::read_dir(format!("/proc/self/fd/{}", a.as_raw_fd()))? {
            let b = b?;
            if !hex(&b.file_name().to_string_lossy(), 2) {
                continue;
            }
            let Ok(b) = open(&b.path()) else {
                continue;
            };
            for leaf in fs::read_dir(format!("/proc/self/fd/{}", b.as_raw_fd()))? {
                let leaf = leaf?;
                if !hex(&leaf.file_name().to_string_lossy(), 124) {
                    continue;
                }
                let m = fs::symlink_metadata(leaf.path())?;
                if m.is_file()
                    && m.uid() == uid
                    && m.nlink() == 1
                    && !active.contains(&(m.dev(), m.ino()))
                {
                    // A cache-shaped filename alone never makes customer data disposable.
                    let file = fs::OpenOptions::new()
                        .read(true)
                        .custom_flags(0x20000)
                        .open(leaf.path())?;
                    let opened = file.metadata()?;
                    if (opened.dev(), opened.ino()) != (m.dev(), m.ino()) {
                        continue;
                    }
                    let digest = std::process::Command::new("/usr/bin/sha512sum")
                        .env_clear()
                        .stdin(file)
                        .output()?;
                    let digest = String::from_utf8_lossy(&digest.stdout);
                    let expected = format!(
                        "{}{}{}",
                        a_name(&a)?,
                        a_name(&b)?,
                        leaf.file_name().to_string_lossy()
                    );
                    if !digest
                        .split_whitespace()
                        .next()
                        .is_some_and(|d| d == expected)
                    {
                        continue;
                    }
                    fs::remove_file(leaf.path())?;
                    removed += 1;
                    if removed >= 512 {
                        return Ok(removed);
                    }
                }
            }
        }
    }
    Ok(removed)
}
#[cfg(target_os = "linux")]
fn a_name(file: &fs::File) -> io::Result<String> {
    use std::os::fd::AsRawFd;
    let path = fs::read_link(format!("/proc/self/fd/{}", file.as_raw_fd()))?;
    path.file_name()
        .and_then(|s| s.to_str())
        .map(str::to_owned)
        .ok_or_else(|| io::Error::other("invalid cache directory"))
}
#[cfg(not(target_os = "linux"))]
pub fn clean_npm_cache(_: &Path, _: &Path) -> io::Result<usize> {
    Err(io::Error::other("cache cleanup requires Linux"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn actual_free_space_warns_without_blocking_and_resumes_below_warning() {
        let policy: Policy = serde_json::from_str(
            r#"{"agent_uid":1001,"agent_gid":1001,"cache_dir":"/cache","cgroup_root":"/agents"}"#,
        )
        .unwrap();
        for (available, before, expected) in [
            (20_000_000_000, Level::Normal, Level::Normal),
            (5_000_000_001, Level::LowSpace, Level::Normal),
            (5_000_000_000, Level::Normal, Level::LowSpace),
            (4_999_999_999, Level::Normal, Level::LowSpace),
            (3_000_000_000, Level::LowSpace, Level::LowSpace),
            (2_500_000_001, Level::Normal, Level::LowSpace),
            (2_500_000_001, Level::Blocked, Level::LowSpace),
            (2_500_000_000, Level::Blocked, Level::Blocked),
            (2_499_999_999, Level::Blocked, Level::Blocked),
            (2_000_000_001, Level::Normal, Level::LowSpace),
            (2_000_000_000, Level::Normal, Level::LowSpace),
            (1_999_999_999, Level::LowSpace, Level::Blocked),
            (512_000_000, Level::Normal, Level::Blocked),
            (0, Level::Normal, Level::Blocked),
            (5_000_000_000, Level::Blocked, Level::LowSpace),
            (5_000_000_001, Level::Blocked, Level::Normal),
        ] {
            let level = policy.level(available, before);
            assert_eq!(level, expected, "{available} bytes, previously {before:?}");
            let guard = Guard {
                policy: None,
                writers: Mutex::new(Vec::new()),
                value: Mutex::new((
                    Snapshot {
                        enabled: true,
                        level,
                        workspace_available_bytes: Some(available),
                        history_available_bytes: Some(available),
                        reason: "disk_capacity",
                    },
                    Instant::now(),
                )),
            };
            assert_eq!(guard.blocks(), expected == Level::Blocked);
        }
    }

    #[test]
    fn missing_or_stale_measurements_do_not_authorize_writes() {
        let guard = Guard {
            policy: None,
            writers: Mutex::new(Vec::new()),
            value: Mutex::new((Snapshot::unavailable(true), Instant::now())),
        };
        assert!(guard.blocks());
        let mut snapshot = Snapshot::unavailable(true);
        snapshot.level = Level::Normal;
        *guard.value.lock().unwrap() = (snapshot, Instant::now() - Duration::from_secs(6));
        assert!(guard.blocks());
    }
}
