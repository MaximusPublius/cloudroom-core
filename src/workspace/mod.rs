pub mod storage;

use axum::body::Body;
use serde::{Deserialize, Serialize};
use std::{
    collections::BTreeMap,
    fs, io,
    os::unix::fs::MetadataExt,
    path::PathBuf,
    process::Stdio,
    sync::{Arc, Mutex},
};
use tokio::io::AsyncWriteExt;
use tokio::process::Command;
use tokio_stream::StreamExt;

pub const MAX_IMPORT_BYTES: u64 = 4 * 1024 * 1024 * 1024;

#[derive(Clone, Debug, Deserialize, Serialize)]
pub struct Workspace {
    pub id: String,
    pub path: PathBuf,
}

pub struct Workspaces {
    root: PathBuf,
    registry: PathBuf,
    policy: Option<storage::Policy>,
    imports: Mutex<BTreeMap<String, Arc<tokio::sync::Mutex<()>>>>,
    allocation: Mutex<()>,
}

impl Workspaces {
    pub fn open(config: &crate::config::Config) -> io::Result<Self> {
        let mut root = if config.storage.is_some() {
            PathBuf::from("/code")
        } else {
            config.state_dir.join("code")
        };
        let registry = config.state_dir.join("workspaces");
        fs::create_dir_all(&registry)?;
        if config.storage.is_none() {
            fs::create_dir_all(&root)?;
            root = root.canonicalize()?;
        }
        Ok(Self {
            root,
            registry,
            policy: config.storage.clone(),
            imports: Mutex::new(BTreeMap::new()),
            allocation: Mutex::new(()),
        })
    }

    pub fn get(&self, id: &str) -> io::Result<Option<Workspace>> {
        valid_id(id)?;
        let _guard = self.allocation.lock().unwrap();
        let path = self.registry.join(format!("{id}.json"));
        let pending = self.registry.join(format!("{id}.pending"));
        if !path.exists() && pending.exists() {
            let workspace: Workspace = serde_json::from_slice(&fs::read(&pending)?)?;
            if workspace.path.parent() == Some(self.root.as_path())
                && fs::read_to_string(workspace.path.join(".cloudroom-imported"))
                    .is_ok_and(|marker| marker == id)
            {
                fs::rename(&pending, &path)?;
                fs::File::open(&self.registry)?.sync_all()?;
            }
        }
        let data = match fs::read(path) {
            Ok(data) => data,
            Err(e) if e.kind() == io::ErrorKind::NotFound => return Ok(None),
            Err(e) => return Err(e),
        };
        let workspace: Workspace = serde_json::from_slice(&data)?;
        if workspace.id != id
            || workspace.path.parent() != Some(self.root.as_path())
            || fs::symlink_metadata(&workspace.path)?
                .file_type()
                .is_symlink()
            || !workspace.path.is_dir()
        {
            return Err(io::Error::other("Cloud workspace is missing or has moved"));
        }
        let _ = fs::remove_file(workspace.path.join(".cloudroom-imported"));
        Ok(Some(workspace))
    }

    fn reserve(&self, id: &str, name: &str) -> io::Result<Workspace> {
        let _guard = self.allocation.lock().unwrap();
        let pending = self.registry.join(format!("{id}.pending"));
        if pending.exists() {
            return Ok(serde_json::from_slice(&fs::read(pending)?)?);
        }
        let occupied = fs::read_dir(&self.registry)?
            .filter(|entry| {
                entry
                    .as_ref()
                    .map(|entry| {
                        matches!(
                            entry.path().extension().and_then(|s| s.to_str()),
                            Some("json" | "pending")
                        )
                    })
                    .unwrap_or(true)
            })
            .map(|entry| {
                let workspace: Workspace = serde_json::from_slice(&fs::read(entry?.path())?)?;
                Ok(workspace.path)
            })
            .collect::<io::Result<Vec<_>>>()?;
        let mut path = self.root.join(name);
        if occupied.contains(&path) || fs::symlink_metadata(&path).is_ok() {
            path = self.root.join(format!("{name}-{id}"));
        }
        if occupied.contains(&path) || fs::symlink_metadata(&path).is_ok() {
            return Err(io::Error::other("Workspace destination is occupied"));
        }
        let workspace = Workspace {
            id: id.to_owned(),
            path,
        };
        let temporary = self.registry.join(format!("{id}.tmp"));
        fs::write(&temporary, serde_json::to_vec(&workspace)?)?;
        fs::File::open(&temporary)?.sync_all()?;
        fs::rename(&temporary, &pending)?;
        fs::File::open(&self.registry)?.sync_all()?;
        Ok(workspace)
    }

    pub async fn import(&self, id: &str, name: &str, body: Body) -> io::Result<Workspace> {
        valid_id(id)?;
        if name.is_empty()
            || name.len() > 80
            || name.starts_with('.')
            || !name
                .bytes()
                .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
        {
            return Err(io::Error::other("Invalid workspace name"));
        }
        let lock = self
            .imports
            .lock()
            .unwrap()
            .entry(id.to_owned())
            .or_default()
            .clone();
        let _guard = lock.lock().await;
        if let Some(workspace) = self.get(id)? {
            return Ok(workspace);
        }
        let root = self.root.canonicalize()?;
        if root != self.root || fs::symlink_metadata(&root)?.file_type().is_symlink() {
            return Err(io::Error::other("Workspace root must be a real directory"));
        }
        if let Some(policy) = &self.policy {
            let metadata = fs::metadata(&root)?;
            if metadata.uid() != policy.agent_uid
                || metadata.dev() != fs::metadata(&policy.quota_mount)?.dev()
            {
                return Err(io::Error::other(
                    "/code must belong to the agent on its quota-protected filesystem",
                ));
            }
        }
        let workspace = self.reserve(id, name)?;
        let mut command = Command::new("python3");
        command
            .env_clear()
            .env("PATH", "/usr/local/bin:/usr/bin:/bin")
            .args(["-c", include_str!("transfer.py"), "unpack"])
            .arg(&self.root)
            .arg(&workspace.path)
            .arg(id)
            .stdin(Stdio::piped())
            .stdout(Stdio::null())
            .stderr(Stdio::piped())
            .kill_on_drop(true);
        let workload = self
            .policy
            .as_ref()
            .map(|policy| crate::runtime::linux::Workload::create(&policy.cgroup_root))
            .transpose()?;
        if let (Some(policy), Some(workload)) = (&self.policy, &workload) {
            workload.attach(&mut command, policy.agent_uid, policy.agent_gid)?;
        }
        let mut child = command.spawn()?;
        let mut input = child.stdin.take().unwrap();
        let transfer = async {
            let mut bytes = 0u64;
            let mut stream = body.into_data_stream();
            while let Some(chunk) = stream.next().await {
                let chunk = chunk.map_err(io::Error::other)?;
                bytes += chunk.len() as u64;
                if bytes > MAX_IMPORT_BYTES {
                    return Err(io::Error::other("Project upload exceeds 4 GiB"));
                }
                input.write_all(&chunk).await?;
            }
            Ok::<_, io::Error>(())
        }
        .await;
        drop(input);
        let output = child.wait_with_output().await?;
        transfer?;
        if !output.status.success() {
            return Err(io::Error::other(
                "Could not import project; check Git state, symlinks, size, and disk space",
            ));
        }
        self.get(id)?
            .ok_or_else(|| io::Error::other("Workspace import did not finish"))
    }
}

pub fn valid_id(id: &str) -> io::Result<()> {
    if id.is_empty()
        || id.len() > 64
        || !id
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"_-".contains(&b))
    {
        return Err(io::Error::other("Invalid workspace ID"));
    }
    Ok(())
}
