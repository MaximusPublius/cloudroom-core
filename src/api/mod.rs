use crate::{
    observability::{Observability, Signal, elapsed_ms},
    session::{self, Manager},
};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, MatchedPath, Path, Query, Request, State},
    http::{HeaderMap, StatusCode, header},
    middleware::{self, Next},
    response::{
        IntoResponse, Response,
        sse::{Event, KeepAlive, Sse},
    },
    routing::{get, post},
};
use serde::Deserialize;
use serde_json::{Value, json};
use std::{convert::Infallible, sync::Arc, time::Instant};
use tokio_stream::wrappers::ReceiverStream;

pub fn router(manager: Arc<Manager>, token: String) -> Router {
    Router::new()
        .route("/v1/health", get(health))
        .route("/v1/ready", get(ready))
        .route("/v1/capabilities", get(capabilities))
        .route(
            "/v1/workspaces/{id}",
            get(workspace)
                .post(import_workspace)
                .layer(DefaultBodyLimit::disable()),
        )
        .route("/v1/dashboard", get(dashboard))
        .route("/v1/sessions", post(start))
        .route("/v1/sessions/{id}", get(status))
        .route("/v1/sessions/{id}/prompts", post(prompt))
        .route("/v1/sessions/{id}/interrupt", post(interrupt))
        .route("/v1/sessions/{id}/stop", post(stop))
        .route("/v1/sessions/{id}/resume", post(resume))
        .route("/v1/sessions/{id}/close", post(close))
        .route("/v1/sessions/{id}/events", get(events))
        .route("/v1/sessions/{id}/stream", get(stream))
        .layer(DefaultBodyLimit::max(64 * 1024))
        .layer(middleware::from_fn_with_state(
            Arc::new(token),
            authenticate,
        ))
        .layer(middleware::from_fn_with_state(
            manager.observability.clone(),
            observe,
        ))
        .with_state(manager)
}

async fn observe(
    State(diagnostics): State<Observability>,
    request: Request,
    next: Next,
) -> Response {
    let started = Instant::now();
    let method = match request.method().as_str() {
        "GET" => "GET",
        "POST" => "POST",
        _ => "other",
    };
    // Route templates only: arbitrary URLs, query strings and headers may contain secrets.
    let route = request
        .extensions()
        .get::<MatchedPath>()
        .map(|p| p.as_str())
        .unwrap_or("unmatched")
        .to_owned();
    let mut response = next.run(request).await;
    let id = diagnostics.record(Signal::Api {
        method,
        route,
        status: response.status().as_u16(),
        duration_ms: elapsed_ms(started),
    });
    response.headers_mut().insert(
        "x-cloudroom-diagnostic-id",
        id.parse().expect("generated diagnostic ID"),
    );
    response
}

async fn authenticate(State(token): State<Arc<String>>, request: Request, next: Next) -> Response {
    let authorized = request
        .headers()
        .get(header::AUTHORIZATION)
        .and_then(|h| h.to_str().ok())
        .and_then(|h| h.strip_prefix("Bearer "))
        .is_some_and(|supplied| supplied == token.as_str());
    let mut response = if authorized {
        next.run(request).await
    } else {
        (
            StatusCode::UNAUTHORIZED,
            [(header::WWW_AUTHENTICATE, "Bearer")],
            Json(json!({"error":"unauthorized"})),
        )
            .into_response()
    };
    response
        .headers_mut()
        .insert(header::CACHE_CONTROL, "no-store".parse().unwrap());
    response
}

impl IntoResponse for session::Error {
    fn into_response(self) -> Response {
        let (status, error) = match self {
            Self::NotFound => (StatusCode::NOT_FOUND, "session not found"),
            Self::Conflict(message) => (StatusCode::CONFLICT, message),
            Self::Storage => (
                StatusCode::SERVICE_UNAVAILABLE,
                "storage unavailable; retry with the same request_id",
            ),
        };
        (status, Json(json!({"error":error}))).into_response()
    }
}

type Result<T> = std::result::Result<T, session::Error>;

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RequestId {
    request_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Start {
    request_id: String,
    harness: Option<crate::runtime::Kind>,
    model: Option<String>,
    reasoning: Option<String>,
    workspace: Option<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct WorkspaceName {
    name: String,
}

async fn workspace(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match manager.workspaces.get(&id) {
        Ok(Some(workspace)) => (StatusCode::OK, Json(json!(workspace))),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"workspace not prepared"})),
        ),
        Err(_) => (
            StatusCode::CONFLICT,
            Json(json!({"error":"workspace unavailable"})),
        ),
    }
}

async fn import_workspace(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Query(query): Query<WorkspaceName>,
    body: Body,
) -> impl IntoResponse {
    if manager.storage.blocks() || manager.is_stopping() {
        return (
            StatusCode::SERVICE_UNAVAILABLE,
            Json(json!({"error":"workspace storage unavailable"})),
        );
    }
    match tokio::time::timeout(
        std::time::Duration::from_secs(15 * 60),
        manager.workspaces.import(&id, &query.name, body),
    )
    .await
    {
        Ok(Ok(workspace)) => (StatusCode::CREATED, Json(json!(workspace))),
        _ => (
            StatusCode::CONFLICT,
            Json(
                json!({"error":"project import failed; check size, symlinks, Git state, and disk space"}),
            ),
        ),
    }
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Prompt {
    request_id: String,
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Interrupt {
    request_id: String,
    target_request_id: String,
}
#[derive(Default, Deserialize)]
#[serde(deny_unknown_fields)]
struct Cursor {
    #[serde(default)]
    after: u64,
}

fn key(value: &str) -> Result<()> {
    if value.is_empty()
        || value.len() > 64
        || !value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b == b'_' || b == b'-')
    {
        return Err(session::Error::Conflict(
            "request_id must be 1-64 ASCII letters, digits, underscores or hyphens",
        ));
    }
    Ok(())
}
fn cursor(after: u64) -> Result<()> {
    if after > i64::MAX as u64 {
        return Err(session::Error::Conflict("invalid replay cursor"));
    }
    Ok(())
}

async fn dashboard(State(manager): State<Arc<Manager>>) -> Json<Value> {
    Json(manager.dashboard())
}

async fn ready(State(manager): State<Arc<Manager>>) -> impl IntoResponse {
    let ready = manager.ready().await;
    (
        if ready {
            StatusCode::OK
        } else {
            StatusCode::SERVICE_UNAVAILABLE
        },
        Json(json!({"ready":ready})),
    )
}

async fn capabilities(State(manager): State<Arc<Manager>>) -> Json<Value> {
    Json(manager.capabilities())
}

async fn stop(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<RequestId>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    let receipt = manager.command(&id, body.request_id, "stop", json!({}))?;
    Ok(accepted(&manager, &id, receipt))
}

async fn resume(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<RequestId>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    let receipt = manager.command(&id, body.request_id, "resume", json!({}))?;
    Ok(accepted(&manager, &id, receipt))
}

async fn health(State(manager): State<Arc<Manager>>) -> Json<Value> {
    Json(json!({"status":"ready","saving":manager.saving(),"storage":manager.storage.snapshot()}))
}
async fn start(
    State(manager): State<Arc<Manager>>,
    Json(body): Json<Start>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    if body
        .model
        .as_ref()
        .is_some_and(|m| m.is_empty() || m.len() > 256 || m.chars().any(char::is_control))
    {
        return Err(session::Error::Conflict("invalid model"));
    }
    if body
        .reasoning
        .as_deref()
        .is_some_and(|r| !matches!(r, "none" | "minimal" | "low" | "medium" | "high" | "xhigh"))
    {
        return Err(session::Error::Conflict("invalid reasoning effort"));
    }
    let (id, receipt) = manager
        .start(
            body.request_id,
            body.harness,
            body.model,
            body.reasoning,
            body.workspace,
        )
        .await?;
    Ok(accepted(&manager, &id, receipt))
}

fn accepted(manager: &Manager, id: &str, receipt: session::Receipt) -> (StatusCode, Json<Value>) {
    (
        StatusCode::ACCEPTED,
        Json(json!({"session_id":id,"receipt":receipt,"saving":manager.saving()})),
    )
}

async fn close(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<RequestId>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    let receipt = manager.command(&id, body.request_id, "close", json!({}))?;
    Ok(accepted(&manager, &id, receipt))
}

async fn status(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
) -> Result<Json<Value>> {
    Ok(Json(
        json!({"session":manager.session(&id).await?,"saving":manager.saving()}),
    ))
}
async fn prompt(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Prompt>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    if body.text.trim().is_empty() || body.text.len() > 32768 {
        return Err(session::Error::Conflict(
            "prompt must contain 1-32768 bytes of text",
        ));
    }
    let receipt = manager.command(&id, body.request_id, "prompt", json!({"text":body.text}))?;
    Ok(accepted(&manager, &id, receipt))
}
async fn interrupt(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Interrupt>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    key(&body.target_request_id)?;
    let receipt = manager.command(
        &id,
        body.request_id,
        "interrupt",
        json!({"target_request_id":body.target_request_id}),
    )?;
    Ok(accepted(&manager, &id, receipt))
}
async fn events(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Query(query): Query<Cursor>,
) -> Result<Json<Value>> {
    cursor(query.after)?;
    manager.session(&id).await?;
    Ok(Json(
        json!({"events":manager.records(&id,query.after).await?,"saving":manager.saving()}),
    ))
}
async fn stream(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Query(query): Query<Cursor>,
    headers: HeaderMap,
) -> Result<impl IntoResponse> {
    let mut after = match headers.get("last-event-id") {
        Some(header) => header
            .to_str()
            .ok()
            .and_then(|h| h.parse().ok())
            .ok_or(session::Error::Conflict("invalid Last-Event-ID"))?,
        None => query.after,
    };
    cursor(after)?;
    let mut changed = manager.subscribe();
    let session = manager.session(&id).await?;
    if after > session.last_sequence {
        return Err(session::Error::Conflict("cursor exceeds session history"));
    }
    let (sender, receiver) = tokio::sync::mpsc::channel(32);
    tokio::spawn(async move {
        loop {
            if manager.is_stopping() {
                return;
            }
            match manager.records(&id, after).await {
                Ok(records) if !records.is_empty() => {
                    for record in records {
                        let sequence = record.sequence;
                        let event = Event::default()
                            .id(sequence.to_string())
                            .event("record")
                            .data(serde_json::to_string(&record).expect("record serialization"));
                        tokio::select! {
                            result = sender.send(Ok::<_, Infallible>(event)) => if result.is_err() { return; },
                            _ = changed.wait_for(|_| manager.is_stopping()) => return,
                        }
                        after = sequence;
                    }
                }
                Ok(_) => tokio::select! {
                    _=sender.closed()=>return,
                    result=changed.changed()=>if result.is_err(){return;},
                },
                Err(_) => return,
            }
        }
    });
    Ok(Sse::new(ReceiverStream::new(receiver)).keep_alive(KeepAlive::default()))
}
