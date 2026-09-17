//! Disk policy and explicitly disposable npm download-cache cleanup, not history retention.
use crate::{config::Config, runtime::linux};
use serde::{Deserialize, Serialize};
use std::{
    fs, io,
    os::unix::fs::MetadataExt,
    path::{Path, PathBuf},
    sync::Mutex,
    time::{Duration, Instant},
};
use tokio::process::Command;

#[derive(Clone, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Policy {
    pub agent_uid: u32,
    pub agent_gid: u32,
    pub quota_mount: PathBuf,
    pub quota_limit_bytes: u64,
    pub cache_dir: PathBuf,
    pub cgroup_root: PathBuf,
    #[serde(default = "reserve")]
    pub reserve_bytes: u64,
    #[serde(default = "warning")]
    pub warning_bytes: u64,
    #[serde(default = "pause")]
    pub pause_bytes: u64,
    #[serde(default = "resume")]
    pub resume_bytes: u64,
}
fn reserve() -> u64 {
    10_000_000_000
}
fn warning() -> u64 {
    2_000_000_000
}
fn pause() -> u64 {
    512_000_000
}
fn resume() -> u64 {
    3_000_000_000
}

impl Policy {
    pub fn load(path: &Path) -> io::Result<Self> {
        let policy: Self = serde_json::from_slice(&fs::read(path)?)?;
        if policy.agent_uid == 0
            || policy.agent_gid == 0
            || policy.quota_mount != Path::new("/")
            || policy.quota_limit_bytes == 0
            || !policy.quota_limit_bytes.is_multiple_of(1024)
            || !(policy.pause_bytes < policy.warning_bytes
                && policy.warning_bytes < policy.resume_bytes)
            || policy.reserve_bytes == 0
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

    pub fn prepare(&self, config: &Config) -> io::Result<()> {
        if !cfg!(target_os = "linux") {
            return Err(io::Error::other("disk protection requires Linux"));
        }
        let mount = fs::metadata(&self.quota_mount)?;
        for path in [
            &config.repository,
            &config.state_dir,
            &config.account_home,
            &self.cache_dir,
            Path::new("/tmp"),
            Path::new("/var/tmp"),
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
                    "agent paths require the quota-protected root filesystem",
                ));
            }
        }
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

    async fn quota(&self) -> io::Result<(u64, u64)> {
        let mut command = Command::new("/usr/bin/quota");
        command
            .env_clear()
            .env("LC_ALL", "C")
            .args([
                "--user",
                &self.agent_uid.to_string(),
                "--no-wrap",
                "--verbose",
                "--raw-grace",
                "--show-mntpoint",
                "--hide-device",
                "--filesystem=/",
            ])
            .kill_on_drop(true);
        linux::as_agent(&mut command, self.agent_uid, self.agent_gid, None)?;
        let output = tokio::time::timeout(Duration::from_secs(2), command.output())
            .await
            .map_err(|_| io::Error::other("quota measurement timed out"))??;
        // quota exits nonzero when a limit is exceeded. The complete row remains authoritative.
        let text = String::from_utf8(output.stdout).map_err(io::Error::other)?;
        let fields: Vec<&str> = text
            .lines()
            .find(|l| l.split_whitespace().next() == Some("/"))
            .ok_or_else(|| io::Error::other("quota is disabled or unavailable"))?
            .split_whitespace()
            .collect();
        quota_row(&fields, self.quota_limit_bytes)
    }
}

fn quota_row(fields: &[&str], expected: u64) -> io::Result<(u64, u64)> {
    let n = |i: usize| {
        fields
            .get(i)
            .and_then(|v| v.trim_end_matches('*').parse::<u64>().ok())
            .ok_or_else(|| io::Error::other("invalid quota measurement"))
    };
    if fields.len() != 9 || n(2)? != 0 || n(3)?.checked_mul(1024) != Some(expected) || n(7)? == 0 {
        return Err(io::Error::other("configured hard quota is not enforced"));
    }
    Ok((
        expected.saturating_sub(n(1)?.saturating_mul(1024)),
        n(7)?.saturating_sub(n(5)?),
    ))
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
    pub agent_remaining_bytes: Option<u64>,
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
            agent_remaining_bytes: None,
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
}
impl Guard {
    pub fn new(config: &Config) -> io::Result<Self> {
        if let Some(p) = &config.storage {
            p.prepare(config)?;
        }
        Ok(Self {
            policy: config.storage.clone(),
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
        self.snapshot().level != Level::Normal
    }
    pub async fn refresh(&self, config: &Config) -> Snapshot {
        let Some(p) = &self.policy else {
            return self.snapshot();
        };
        let previous = self.value.lock().unwrap().0.level;
        let measured = async {
            let (workspace, history, quota) =
                tokio::join!(disk(&config.repository), disk(&config.state_dir), p.quota());
            let (workspace, history, (remaining, files)) = (workspace?, history?, quota?);
            let headroom = remaining
                .min(workspace.available_bytes.saturating_sub(p.reserve_bytes))
                .min(history.available_bytes.saturating_sub(p.reserve_bytes));
            let level = if files < 128
                || headroom <= p.pause_bytes
                || (previous == Level::Blocked && headroom < p.resume_bytes)
            {
                Level::Blocked
            } else if headroom <= p.warning_bytes {
                Level::LowSpace
            } else {
                Level::Normal
            };
            Ok::<_, io::Error>(Snapshot {
                enabled: true,
                level,
                workspace_available_bytes: Some(workspace.available_bytes),
                history_available_bytes: Some(history.available_bytes),
                agent_remaining_bytes: Some(remaining),
                reason: if files < 128 {
                    "inode_limit"
                } else {
                    "disk_capacity"
                },
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
