//! Pi provider logins on the VM. The Mac helper copies providers the VM lacks; users can also set one key.
use super::Kind;
use crate::{config::Config, workspace::storage::Guard};
use serde_json::{Map, Value, json};
use std::{io, process::Stdio, time::Duration};
use tokio::{io::AsyncWriteExt, process::Command};

const MAX_ENTRY_BYTES: usize = 16 * 1024;

fn name(value: &str, extra: &[u8]) -> bool {
    !value.is_empty()
        && value.len() <= 64
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || extra.contains(&b))
}
fn secret(value: &Value) -> bool {
    value
        .as_str()
        .is_some_and(|s| !s.is_empty() && s.len() <= 8192 && !s.chars().any(char::is_control))
}

/// A well-formed Pi credential, or None. `!command` keys run Mac-only secret managers, so they stay behind.
fn entry(value: &Value) -> Option<Value> {
    if serde_json::to_vec(value).ok()?.len() > MAX_ENTRY_BYTES {
        return None;
    }
    match value["type"].as_str()? {
        "api_key" if secret(&value["key"]) && !value["key"].as_str()?.starts_with('!') => {
            let mut entry = json!({"type":"api_key","key":value["key"]});
            if let Some(env) = value.get("env") {
                let env = env.as_object()?;
                if !env.iter().all(|(k, v)| name(k, &[]) && secret(v)) {
                    return None;
                }
                entry["env"] = json!(env);
            }
            Some(entry)
        }
        // OAuth fields vary by provider, so keep the entry as Pi wrote it.
        "oauth"
            if secret(&value["access"])
                && secret(&value["refresh"])
                && value["expires"].is_number() =>
        {
            Some(value.clone())
        }
        _ => None,
    }
}

async fn update(
    config: &Config,
    storage: &Guard,
    credentials: Map<String, Value>,
    replace: bool,
) -> io::Result<Value> {
    let home = &config
        .harnesses
        .get(&Kind::Pi)
        .ok_or_else(|| io::Error::other("Pi is not configured"))?
        .home;
    let mut command = Command::new("python3");
    command
        .env_clear()
        .env("PATH", "/usr/local/bin:/usr/bin:/bin")
        .args(["-I", "-c", include_str!("../sync/files.py")])
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true);
    let (mut child, _workload) = storage.spawn_writer(&mut command)?;
    let mut stdin = child.stdin.take().unwrap();
    // Keys travel only over private stdin, never process arguments or diagnostic records.
    let request = json!({"op":"pi_auth","tree":{"root":home,"kind":"auth","filename":"auth.json"},"credentials":credentials,"replace":replace});
    stdin.write_all(&serde_json::to_vec(&request)?).await?;
    stdin.write_all(b"\n").await?;
    drop(stdin);
    let output = tokio::time::timeout(Duration::from_secs(10), child.wait_with_output()).await??;
    let result: Value = serde_json::from_slice(&output.stdout).unwrap_or_default();
    if !output.status.success() || result["ok"] != true {
        return Err(io::Error::other(format!(
            "Pi login file update failed ({})",
            result["error"].as_str().unwrap_or("no helper result")
        )));
    }
    Ok(json!({"providers":result["providers"],"added":result["added"]}))
}

/// Provider names with a saved login. Never returns secrets.
pub(crate) async fn providers(config: &Config, storage: &Guard) -> io::Result<Value> {
    let result = update(config, storage, Map::new(), false).await?;
    Ok(json!({"providers":result["providers"]}))
}

/// Adds valid providers the VM lacks. An existing VM login always wins.
pub(crate) async fn import(config: &Config, storage: &Guard, value: Value) -> io::Result<Value> {
    let credentials = value
        .as_object()
        .ok_or_else(|| io::Error::new(io::ErrorKind::InvalidInput, "invalid Pi logins"))?
        .iter()
        .filter(|(provider, _)| name(provider, b"-."))
        .filter_map(|(provider, value)| Some((provider.clone(), entry(value)?)))
        .collect();
    update(config, storage, credentials, false).await
}

/// Saves or replaces one provider's API key, as the user asked.
pub(crate) async fn set_key(
    config: &Config,
    storage: &Guard,
    provider: String,
    key: String,
) -> io::Result<Value> {
    let value = json!({"type":"api_key","key":key});
    if !name(&provider, b"-.") || entry(&value).is_none() {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "invalid Pi provider key",
        ));
    }
    update(config, storage, Map::from_iter([(provider, value)]), true).await
}
