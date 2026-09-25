//! Vercel fx over ACP. Reuses the Cursor ACP protocol; fx keeps its own session files.
use super::{Handle, Resume, command as child_command, cursor};
use crate::config::{Config, HarnessConfig};
use serde_json::{Value, json};
use std::io;
use tokio::process::Command;

pub(super) const FX: cursor::Flavor = cursor::Flavor {
    harness: "fx",
    name: "fx",
    sessions: "sessions",
    meta: "session.json",
    valid_id,
    capture: false,
};

pub(super) fn command(config: &Config, profile: &HarnessConfig) -> io::Result<Command> {
    if profile.home.canonicalize()? != config.account_home.join(".fx").canonicalize()? {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            "fx home must belong to the configured agent account",
        ));
    }
    let mut command = child_command(&profile.binary, config);
    // Cloud agents run in full access (ADR 0045); the VM pins the fx version.
    command
        .env("FX_PERMISSION_MODE", "full-access")
        .env("FX_AUTO_UPGRADE", "0")
        .arg("acp");
    Ok(command)
}

pub(super) async fn start(handle: &Handle) -> io::Result<String> {
    let (id, session) = cursor::open(handle, "fx").await?;
    let requested = handle.profile.model.as_str();
    if requested == "default" {
        return Ok(id);
    }
    let offered = session["configOptions"]
        .as_array()
        .and_then(|options| options.iter().find(|option| option["id"] == "model"))
        .and_then(|option| option["options"].as_array())
        .is_some_and(|options| options.iter().any(|option| option["value"] == requested));
    if !offered {
        return Err(io::Error::new(
            io::ErrorKind::InvalidInput,
            format!("fx model {requested} is unavailable for this account"),
        ));
    }
    let result = handle
        .call(
            "session/set_config_option",
            json!({"sessionId":id,"configId":"model","value":requested}),
        )
        .await?;
    if cursor::option_value(&result, "model") != Some(requested) {
        return Err(io::Error::other(format!(
            "fx did not confirm model {requested}"
        )));
    }
    Ok(id)
}

pub(super) fn validate(
    profile: &HarnessConfig,
    saved: &Resume,
    identity: super::files::Identity,
) -> io::Result<()> {
    let root = profile.home.join(FX.sessions);
    if !valid_id(&saved.id) || saved.path != root.join(&saved.id).join(FX.meta) {
        return Err(io::Error::other(
            "fx native path does not match its session",
        ));
    }
    let file = super::files::open(&root, &saved.path, identity)?;
    let value: Value =
        serde_json::from_reader(io::Read::take(file, super::process::MAX_LINE as u64))?;
    if value["id"] != saved.id.as_str() {
        return Err(io::Error::other("Unsupported fx native metadata"));
    }
    Ok(())
}

/// fx session IDs look like `<ms>-<ns>-<16 hex>`.
fn valid_id(id: &str) -> bool {
    let parts: Vec<&str> = id.split('-').collect();
    parts.len() == 3
        && parts[..2].iter().all(|part| {
            !part.is_empty() && part.len() <= 20 && part.bytes().all(|b| b.is_ascii_digit())
        })
        && parts[2].len() == 16
        && parts[2]
            .bytes()
            .all(|b| b.is_ascii_hexdigit() && !b.is_ascii_uppercase())
}
