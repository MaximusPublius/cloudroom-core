//! Cloud agents rename or archive their own thread. Each request is a session record, so the app
//! applies it whenever it next reads the thread, including after it was offline.
use super::Manager;
use crate::preview::Peer;
use axum::{
    Json, Router,
    extract::{ConnectInfo, State},
    http::StatusCode,
    routing::post,
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{io, sync::Arc};

const USAGE: &str = "cloudroom thread update --self --title TITLE
cloudroom thread archive --self
cloudroom thread stop --self";
type Failure = (StatusCode, Json<Value>);

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Rename {
    title: String,
}

fn valid(title: &str) -> bool {
    !title.is_empty() && title.chars().count() <= 200 && !title.chars().any(char::is_control)
}
fn fail(status: StatusCode, error: &str) -> Failure {
    (status, Json(json!({"error":error})))
}

/// Served on the agent-only socket that `cloudroom mac` also uses.
pub(crate) fn agent_routes() -> Router<Arc<Manager>> {
    Router::new()
        .route("/title", post(rename))
        .route("/archive", post(archive))
}

/// The session whose harness process sent this request.
fn caller(m: &Manager, peer: Peer) -> Result<String, Failure> {
    if peer.0.is_none() || m.previews.agent().map(|(_, uid)| uid) != peer.0 {
        return Err(fail(
            StatusCode::FORBIDDEN,
            "Only the VM agent account may change threads",
        ));
    }
    peer.1
        .and_then(|pid| m.harness_session(&crate::secrets::ancestors(pid)))
        .ok_or(fail(
            StatusCode::CONFLICT,
            "Run cloudroom thread from a Cloudroom cloud thread",
        ))
}

fn record(m: &Manager, session: &str, kind: &str, data: Value) -> Result<Json<Value>, Failure> {
    m.note(session, kind, data.clone()).map_err(|error| {
        fail(
            StatusCode::SERVICE_UNAVAILABLE,
            &format!("Could not record the {kind}: {error}"),
        )
    })?;
    Ok(Json(data))
}

async fn rename(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
    Json(input): Json<Rename>,
) -> Result<Json<Value>, Failure> {
    let session = caller(&m, peer)?;
    let title = input.title.trim();
    if !valid(title) {
        return Err(fail(
            StatusCode::CONFLICT,
            "Use a title of 1-200 characters",
        ));
    }
    record(&m, &session, "title", json!({"title":title}))
}

/// The app archives the thread and its children, which also stops this session.
async fn archive(
    State(m): State<Arc<Manager>>,
    ConnectInfo(peer): ConnectInfo<Peer>,
) -> Result<Json<Value>, Failure> {
    let session = caller(&m, peer)?;
    record(&m, &session, "archive", json!({"archived":true}))
}

/// The same `cloudroom thread ... --self` forms local threads use. `--json` is accepted and
/// ignored because the answer is always JSON.
pub async fn cli(args: &[String]) -> io::Result<i32> {
    let words: Vec<&str> = args
        .iter()
        .map(String::as_str)
        .filter(|word| *word != "--json")
        .collect();
    let (path, body) = match words.as_slice() {
        ["update", "--self", "--title", title] | ["update", "--title", title, "--self"] => {
            ("/title", Some(json!({"title":title})))
        }
        ["archive", "--self"] => ("/archive", None),
        ["stop", "--self"] => {
            println!(
                "{}",
                json!({"stopped":true,"note":"Archiving a cloud thread stops it; nothing else to do."})
            );
            return Ok(0);
        }
        _ => {
            println!(
                "{USAGE}\nRenames or archives this cloud thread. The app applies it when it next connects."
            );
            return Ok(if matches!(words.as_slice(), [] | ["--help"]) {
                0
            } else {
                2
            });
        }
    };
    let answer = crate::mac::request(crate::mac::SOCKET, "POST", path, body).await?;
    println!("{answer}");
    Ok(0)
}
