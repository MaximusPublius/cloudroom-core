//! Linux workload containment. The installer delegates only this service's cgroup.
use std::{
    fs, io,
    path::{Path, PathBuf},
    sync::atomic::{AtomicU64, Ordering},
    time::Duration,
};
use tokio::{process::Command, sync::watch};

static NEXT: AtomicU64 = AtomicU64::new(0);

pub struct Workload {
    directory: PathBuf,
    paused: watch::Sender<bool>,
}

impl Workload {
    pub fn create(root: &Path) -> io::Result<Self> {
        let directory = root.join(format!(
            "agent-{}-{}",
            std::process::id(),
            NEXT.fetch_add(1, Ordering::Relaxed)
        ));
        fs::create_dir(&directory)?;
        let (paused, _) = watch::channel(false);
        Ok(Self { directory, paused })
    }
    pub fn attach(&self, command: &mut Command, uid: u32, gid: u32) -> io::Result<()> {
        as_agent(command, uid, gid, Some(&self.directory))
    }
    pub fn paused(&self) -> watch::Receiver<bool> {
        self.paused.subscribe()
    }
    pub async fn freeze(&self, frozen: bool) -> io::Result<()> {
        // Never thaw a group this owner did not request to freeze.
        if !frozen && !*self.paused.borrow() {
            return Ok(());
        }
        fs::write(
            self.directory.join("cgroup.freeze"),
            if frozen { "1" } else { "0" },
        )?;
        if frozen {
            self.paused.send_replace(true);
        }
        tokio::time::timeout(Duration::from_secs(2), async {
            loop {
                let events = fs::read_to_string(self.directory.join("cgroup.events"))?;
                if events
                    .lines()
                    .any(|l| l == if frozen { "frozen 1" } else { "frozen 0" })
                {
                    break Ok::<_, io::Error>(());
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .map_err(|_| io::Error::other("workload freeze state unconfirmed"))??;
        if !frozen {
            self.paused.send_replace(false);
        }
        Ok(())
    }
    pub fn terminate(&self) -> io::Result<()> {
        // The directory is created exclusively by this Runtime, never supplied by a client.
        fs::write(self.directory.join("cgroup.kill"), "1")
    }
    pub async fn stop(&self) -> io::Result<()> {
        Self::clear(&self.directory).await
    }
    /// Only Runtime-created groups, or the validated service root at startup.
    pub async fn clear(directory: &Path) -> io::Result<()> {
        fs::write(directory.join("cgroup.kill"), "1")?;
        tokio::time::timeout(super::SHUTDOWN_GRACE, async {
            loop {
                let events = fs::read_to_string(directory.join("cgroup.events"))?;
                if events.lines().any(|line| line == "populated 0") {
                    return Ok(());
                }
                tokio::time::sleep(Duration::from_millis(20)).await;
            }
        })
        .await
        .map_err(|_| io::Error::other("workload exit unconfirmed; refusing replacement"))?
    }
}
impl Drop for Workload {
    fn drop(&mut self) {
        let _ = self.terminate();
        let _ = fs::remove_dir(&self.directory); // Busy kernel groups are left intact, never recursively deleted.
    }
}

#[cfg(target_os = "linux")]
pub(crate) fn as_agent(
    command: &mut Command,
    uid: u32,
    gid: u32,
    group: Option<&Path>,
) -> io::Result<()> {
    use std::{io::Write, os::unix::process::CommandExt};
    unsafe extern "C" {
        fn setgroups(size: usize, list: *const u32) -> i32;
        fn setgid(gid: u32) -> i32;
        fn setuid(uid: u32) -> i32;
        fn prctl(option: i32, ...) -> i32;
        fn capset(header: *const [u32; 2], data: *const [u32; 6]) -> i32;
    }
    if uid == 0 || gid == 0 {
        return Err(io::Error::other("agent identity must be unprivileged"));
    }
    let mut membership = group
        .map(|g| {
            fs::OpenOptions::new()
                .write(true)
                .open(g.join("cgroup.procs"))
        })
        .transpose()?;
    // Only pre-opened fd writes and Linux syscalls after fork: no allocation or locks.
    // Attach before dropping identity; descendants inherit containment even after setsid().
    unsafe {
        command.as_std_mut().pre_exec(move || {
            if let Some(file) = membership.as_mut() {
                file.write_all(b"0")?;
            }
            if prctl(38, 1_u64, 0_u64, 0_u64, 0_u64) != 0 // PR_SET_NO_NEW_PRIVS
            || setgroups(0, std::ptr::null()) != 0 || setgid(gid) != 0 || setuid(uid) != 0
            || prctl(47, 4_u64, 0_u64, 0_u64, 0_u64) != 0 // clear ambient capabilities
            || capset(&[0x20080522, 0], &[0; 6]) != 0
            {
                return Err(io::Error::last_os_error());
            }
            Ok(())
        });
    }
    Ok(())
}
#[cfg(not(target_os = "linux"))]
pub(crate) fn as_agent(_: &mut Command, _: u32, _: u32, _: Option<&Path>) -> io::Result<()> {
    Err(io::Error::other("disk protection requires Linux"))
}

/// Pausing work must not turn an in-flight native RPC into a timeout/replayed task.
pub(super) async fn reply<T>(
    mut result: tokio::sync::oneshot::Receiver<T>,
    mut paused: watch::Receiver<bool>,
) -> io::Result<T> {
    let mut remaining = Duration::from_secs(30);
    loop {
        if *paused.borrow() {
            tokio::select! {
                value = &mut result => return value.map_err(|_| io::Error::other("harness response lost; outcome uncertain")),
                changed = paused.changed() => if changed.is_err() { return Err(io::Error::other("workload owner lost")); },
            }
        } else {
            let start = tokio::time::Instant::now();
            tokio::select! {
                value = &mut result => return value.map_err(|_| io::Error::other("harness response lost; outcome uncertain")),
                _ = tokio::time::sleep(remaining) => return Err(io::Error::other("harness response timed out; outcome uncertain")),
                changed = paused.changed() => if changed.is_err() { return Err(io::Error::other("workload owner lost")); },
            }
            remaining = remaining.saturating_sub(start.elapsed());
        }
    }
}
