//! OpenCode over ACP (ADR 0172). Reuses the Cursor ACP protocol; OpenCode keeps its sessions in its own SQLite file.
use super::{Handle, Resume, command as child_command, cursor};
use crate::config::{Config, HarnessConfig};
use serde_json::{Value, json};
use std::{io, path::Path};
use tokio::process::Command;

pub(super) const OPENCODE: cursor::Flavor = cursor::Flavor {
    harness: "opencode",
    name: "OpenCode",
    native_path: |home, _| home.join(DATABASE),
    valid_id,
    capture: false,
    notices: &[],
};
const DATABASE: &str = "opencode.db";
/// Cloudroom levels for OpenCode; each model offers its own subset of efforts.
pub const REASONING_LEVELS: &[&str] = &["none", "low", "medium", "high", "xhigh", "max"];

/// OpenCode's efforts for a Cloudroom level, in the order Local threads try them (provider-bridge-acp).
fn efforts(level: &str) -> Vec<&str> {
    match level {
        "low" => vec!["low", "minimal"],
        "max" => vec!["max", "xhigh"],
        _ => vec![level],
    }
}

fn invalid(message: String) -> io::Error {
    io::Error::new(io::ErrorKind::InvalidInput, message)
}

pub(super) fn command(config: &Config, profile: &HarnessConfig) -> io::Result<Command> {
    let home = config.account_home.join(".local/share/opencode");
    if profile.home.canonicalize()? != home.canonicalize()? {
        return Err(invalid(
            "OpenCode home must belong to the configured agent account".into(),
        ));
    }
    let mut command = child_command(&profile.binary, config);
    // The image pins the OpenCode version.
    command.env("OPENCODE_DISABLE_AUTOUPDATE", "1").arg("acp");
    Ok(command)
}

pub(super) async fn start(handle: &Handle) -> io::Result<String> {
    let (id, mut session) = cursor::open(handle, "OpenCode").await?;
    let requested = handle.profile.model.as_str();
    if requested != "default" && cursor::option_value(&session, "model") != Some(requested) {
        if !choices(&session, "model").contains(&requested) {
            return Err(invalid(format!(
                "OpenCode model {requested} is unavailable in Cloud. Sign in to its provider with `opencode auth login` on your Mac, and Cloudroom copies that login to Cloud. Provider keys set only as shell variables are not copied; add them in Settings → Cloud environment."
            )));
        }
        session = set(handle, &id, "model", requested).await?;
    }
    // Like Local threads, a model without this effort keeps OpenCode's default (ADR 0133).
    let offered = choices(&session, "effort");
    if let Some(effort) = handle
        .reasoning
        .as_deref()
        .and_then(|reasoning| efforts(reasoning).into_iter().find(|e| offered.contains(e)))
        && cursor::option_value(&session, "effort") != Some(effort)
    {
        set(handle, &id, "effort", effort).await?;
    }
    Ok(id)
}

async fn set(handle: &Handle, id: &str, option: &str, value: &str) -> io::Result<Value> {
    let result = handle
        .call(
            "session/set_config_option",
            json!({"sessionId":id,"configId":option,"value":value}),
        )
        .await?;
    if cursor::option_value(&result, option) != Some(value) {
        return Err(io::Error::other(format!(
            "OpenCode did not confirm {option} {value}"
        )));
    }
    Ok(result)
}

fn session_option<'a>(session: &'a Value, name: &str) -> Option<&'a Value> {
    session["configOptions"]
        .as_array()?
        .iter()
        .find(|option| option["id"] == name)
}

fn choices<'a>(session: &'a Value, name: &str) -> Vec<&'a str> {
    session_option(session, name)
        .and_then(|option| option["options"].as_array())
        .map(|options| options.iter().filter_map(|o| o["value"].as_str()).collect())
        .unwrap_or_default()
}

pub(super) fn validate(
    profile: &HarnessConfig,
    saved: &Resume,
    identity: super::files::Identity,
) -> io::Result<()> {
    if !valid_id(&saved.id) || saved.path != profile.home.join(DATABASE) {
        return Err(io::Error::other(
            "OpenCode native path does not match its session",
        ));
    }
    let file = super::files::open(&profile.home, Path::new(&saved.path), identity)?;
    if file.metadata()?.len() == 0 {
        return Err(io::Error::new(
            io::ErrorKind::NotFound,
            "OpenCode session store is empty",
        ));
    }
    Ok(())
}

/// OpenCode session IDs look like `ses_` and 26 letters or digits.
fn valid_id(id: &str) -> bool {
    id.strip_prefix("ses_")
        .is_some_and(|rest| rest.len() == 26 && rest.bytes().all(|b| b.is_ascii_alphanumeric()))
}
