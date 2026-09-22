use crate::{
    observability::{Observability, Signal, elapsed_ms},
    session::{self, Manager},
};
use axum::{
    Json, Router,
    body::Body,
    extract::{DefaultBodyLimit, MatchedPath, Path, Query, Request, State},
    http::{HeaderMap, StatusCode, Version, header},
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
        .route("/v1/workspaces/{id}", get(workspace))
        .route("/v1/settings", get(crate::sync::settings))
        .route("/v1/sync", post(crate::sync::check_in))
        .route("/v1/sync/{id}", get(crate::sync::scan))
        .route(
            "/v1/sync/{id}/file",
            get(crate::sync::read)
                .put(crate::sync::apply)
                .layer(DefaultBodyLimit::disable()),
        )
        .route("/v1/dashboard", get(dashboard))
        .route("/v1/sessions", post(start))
        .route("/v1/sessions/{id}", get(status))
        .route("/v1/sessions/{id}/workspace", get(session_workspace))
        .route("/v1/sessions/{id}/recovery", get(recovery))
        .route("/v1/sessions/{id}/prompts", post(prompt))
        .route("/v1/sessions/{id}/edit", post(edit))
        .route("/v1/sessions/{id}/cancel", post(cancel))
        .route("/v1/sessions/{id}/steer", post(steer))
        .route("/v1/sessions/{id}/compact", post(compact))
        .route("/v1/sessions/{id}/rewind", post(rewind))
        .route(
            "/v1/sessions/{id}/attachments",
            post(attach).layer(DefaultBodyLimit::max(26 * 1024 * 1024)),
        )
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
    let version = request.version();
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
    // The hosting gateway retains idle upstream sockets. Keep SSE streams, not completed requests.
    if matches!(version, Version::HTTP_10 | Version::HTTP_11)
        && !response
            .headers()
            .get(header::CONTENT_TYPE)
            .is_some_and(|value| value.as_bytes().starts_with(b"text/event-stream"))
    {
        response
            .headers_mut()
            .insert(header::CONNECTION, "close".parse().unwrap());
    }
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
        let code = match error {
            "invalid model" => "invalid_model",
            "invalid provider" | "provider selection requires Pi" => "invalid_provider",
            "invalid reasoning effort" => "invalid_reasoning_effort",
            "invalid service tier" => "invalid_service_tier",
            "Cloud folder permission denied" => "attachment_permission_denied",
            "attachment upload failed" => "invalid_attachment",
            "image exceeds the 10 MiB limit" | "file exceeds the 25 MiB limit" => {
                "attachment_too_large"
            }
            "request_id already has different content"
            | "session already exists"
            | "saved session has no matching receipt" => "request_conflict",
            "agent setup is incomplete" | "harness is not configured" => "harness_not_configured",
            "invalid workspace" | "workspace mapping unavailable" => "invalid_workspace",
            "storage unsafe; new execution is blocked" => "storage_blocked",
            "service is stopping" => "service_stopping",
            "model catalog unavailable" => "model_catalog_unavailable",
            _ => "request_rejected",
        };
        (status, Json(json!({"error":error,"code":code}))).into_response()
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
    provider: Option<String>,
    workspace_name: Option<String>,
}

async fn workspace(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
) -> impl IntoResponse {
    match manager.workspaces.get(&id) {
        Ok(Some(workspace)) => (StatusCode::OK, Json(json!(workspace))),
        Ok(None) => (
            StatusCode::NOT_FOUND,
            Json(json!({"error":"workspace not found"})),
        ),
        Err(_) => (
            StatusCode::CONFLICT,
            Json(json!({"error":"workspace unavailable"})),
        ),
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Prompt {
    request_id: String,
    #[serde(default)]
    text: String,
    #[serde(default)]
    content: Option<Value>,
    #[serde(default)]
    attachments: Option<Value>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    service_tier: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Edit {
    request_id: String,
    target_request_id: String,
    expected_revision: u64,
    #[serde(default)]
    text: String,
    #[serde(default)]
    content: Option<Value>,
    #[serde(default)]
    attachments: Option<Value>,
    #[serde(default)]
    reasoning: Option<String>,
    #[serde(default)]
    service_tier: Option<String>,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Target {
    request_id: String,
    target_request_id: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Steer {
    request_id: String,
    target_request_id: String,
    text: String,
}
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Rewind {
    request_id: String,
    replacement: Option<Prompt>,
    #[serde(default)]
    before: Option<String>,
    #[serde(default)]
    last_turn_id: Option<String>,
}
#[derive(Deserialize)]
struct AttachQuery {
    request_id: String,
    name: String,
    kind: String,
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
    Json(manager.capabilities().await)
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
    if body.provider.as_ref().is_some_and(|p| {
        p.is_empty()
            || p.chars()
                .any(|c| c.is_control() || c.is_whitespace() || c == '/')
    }) {
        return Err(session::Error::Conflict("invalid provider"));
    }
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
        .is_some_and(|r| r.is_empty() || r.len() > 64 || !r.bytes().all(|b| b.is_ascii_lowercase()))
    {
        return Err(session::Error::Conflict("invalid reasoning effort"));
    }
    let (id, receipt) = manager
        .start(
            body.request_id,
            body.harness,
            body.model,
            body.reasoning,
            (body.workspace, body.workspace_name),
            body.provider,
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
async fn recovery(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
) -> Result<Json<Value>> {
    let session = manager.session(&id).await?;
    Ok(Json(manager.recovery_check(&session)))
}
async fn session_workspace(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
) -> Result<Json<Value>> {
    let session = manager.session(&id).await?;
    let workspace = session.workspace.ok_or(session::Error::NotFound)?;
    Ok(Json(manager.workspaces.checkout(&workspace).await?))
}

fn prompt_input(
    text: String,
    content: Option<Value>,
    attachments: Option<Value>,
    reasoning: Option<String>,
    service_tier: Option<String>,
) -> Result<Value> {
    let has_attachments = attachments
        .as_ref()
        .and_then(Value::as_array)
        .is_some_and(|items| !items.is_empty());
    if (text.trim().is_empty() && !has_attachments) || text.len() > 32768 {
        return Err(session::Error::Conflict(
            "prompt must contain 1-32768 bytes of text",
        ));
    }
    let mut input = json!({"text": text});
    if let Some(content) = content {
        input["content"] = content;
    }
    if let Some(attachments) = attachments {
        input["attachments"] = attachments;
    }
    if let Some(reasoning) = reasoning {
        input["reasoning"] = json!(reasoning);
    }
    if let Some(service_tier) = service_tier {
        input["service_tier"] = json!(service_tier);
    }
    Ok(input)
}

async fn prompt(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Prompt>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    let input = prompt_input(
        body.text,
        body.content,
        body.attachments,
        body.reasoning.clone(),
        body.service_tier.clone(),
    )?;
    // An accepted retry must not depend on the model catalog still being available.
    if !manager.known_request(&id, &body.request_id) {
        if body.reasoning.as_deref().is_some_and(|level| {
            level.is_empty() || level.len() > 64 || !level.bytes().all(|b| b.is_ascii_lowercase())
        }) {
            return Err(session::Error::Conflict("invalid reasoning effort"));
        }
        manager
            .check_prompt_reasoning(&id, body.reasoning.as_deref())
            .await?;
        manager.check_prompt_service_tier(&id, body.service_tier.as_deref())?;
    }
    let receipt = manager.command(&id, body.request_id, "prompt", input)?;
    Ok(accepted(&manager, &id, receipt))
}

async fn edit(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Edit>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    key(&body.target_request_id)?;
    let mut input = prompt_input(
        body.text,
        body.content,
        body.attachments,
        body.reasoning.clone(),
        body.service_tier.clone(),
    )?;
    input["target_request_id"] = json!(body.target_request_id);
    input["expected_revision"] = json!(body.expected_revision);
    if !manager.known_request(&id, &body.request_id) {
        manager
            .check_prompt_reasoning(&id, body.reasoning.as_deref())
            .await?;
        manager.check_prompt_service_tier(&id, body.service_tier.as_deref())?;
    }
    let receipt = manager.command(&id, body.request_id, "edit", input)?;
    Ok(accepted(&manager, &id, receipt))
}

async fn cancel(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Target>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    key(&body.target_request_id)?;
    let receipt = manager.command(
        &id,
        body.request_id,
        "cancel",
        json!({"target_request_id":body.target_request_id}),
    )?;
    Ok(accepted(&manager, &id, receipt))
}

async fn steer(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Steer>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    key(&body.target_request_id)?;
    if body.text.trim().is_empty() || body.text.len() > 32768 {
        return Err(session::Error::Conflict(
            "prompt must contain 1-32768 bytes of text",
        ));
    }
    let receipt = manager.command(
        &id,
        body.request_id,
        "steer",
        json!({"target_request_id":body.target_request_id,"text":body.text}),
    )?;
    Ok(accepted(&manager, &id, receipt))
}

async fn compact(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<RequestId>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    let receipt = manager.command(&id, body.request_id, "compact", json!({}))?;
    Ok(accepted(&manager, &id, receipt))
}

async fn rewind(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Json(body): Json<Rewind>,
) -> Result<impl IntoResponse> {
    key(&body.request_id)?;
    let mut input = json!({});
    if let Some(before) = body.before {
        input["before"] = json!(before);
    }
    if let Some(last_turn_id) = body.last_turn_id {
        input["last_turn_id"] = json!(last_turn_id);
    }
    if let Some(prompt) = body.replacement {
        key(&prompt.request_id)?;
        let payload = prompt_input(
            prompt.text,
            prompt.content,
            prompt.attachments,
            prompt.reasoning.clone(),
            prompt.service_tier.clone(),
        )?;
        if !manager.known_request(&id, &body.request_id) {
            manager
                .check_prompt_reasoning(&id, prompt.reasoning.as_deref())
                .await?;
            manager.check_prompt_service_tier(&id, prompt.service_tier.as_deref())?;
        }
        input["replacement"] = json!({"request_id":prompt.request_id,"input":payload});
    }
    let receipt = manager.command(&id, body.request_id, "rewind", input)?;
    Ok(accepted(&manager, &id, receipt))
}

async fn attach(
    State(manager): State<Arc<Manager>>,
    Path(id): Path<String>,
    Query(query): Query<AttachQuery>,
    body: Body,
) -> Result<impl IntoResponse> {
    key(&query.request_id)?;
    if let Some(receipt) = manager.receipt(&id, &query.request_id) {
        if receipt.command != "attach"
            || receipt.input["name"].as_str() != Some(query.name.as_str())
            || receipt.input["kind"].as_str() != Some(query.kind.as_str())
        {
            return Err(session::Error::Conflict(
                "request_id already has different content",
            ));
        }
        // Continue through storage so an exact retry verifies the submitted bytes
        // against the already-saved attachment before returning the old receipt.
    }
    let session = manager.session(&id).await?;
    let workspace = session
        .workspace
        .ok_or(session::Error::Conflict("invalid workspace"))?;
    if workspace.id != "legacy" && manager.workspaces.get(&workspace.id)?.is_none() {
        return Err(session::Error::Conflict("workspace mapping unavailable"));
    }
    if !manager.recording_available() {
        return Err(session::Error::Storage);
    }
    if manager.is_stopping() || manager.storage.blocks() {
        return Err(session::Error::Conflict(
            "storage unsafe; uploads are blocked",
        ));
    }
    let written = manager
        .workspaces
        .attach(
            &workspace,
            &id,
            &query.request_id,
            &query.name,
            &query.kind,
            body,
            &manager.storage,
        )
        .await
        .map_err(|error| {
            eprintln!(
                "attachment upload failed: session={id} request={} kind={:?} errno={:?}",
                query.request_id,
                error.kind(),
                error.raw_os_error()
            );
            session::Error::Conflict(match error.kind() {
                std::io::ErrorKind::WouldBlock => "storage unsafe; uploads are blocked",
                std::io::ErrorKind::AlreadyExists => "request_id already has different content",
                std::io::ErrorKind::PermissionDenied => "Cloud folder permission denied",
                std::io::ErrorKind::FileTooLarge if query.kind == "image" => {
                    "image exceeds the 10 MiB limit"
                }
                std::io::ErrorKind::FileTooLarge => "file exceeds the 25 MiB limit",
                _ => "attachment upload failed",
            })
        })?;
    let receipt = manager.command(&id, query.request_id, "attach", written)?;
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
