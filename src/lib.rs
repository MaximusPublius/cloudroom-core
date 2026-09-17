pub mod api;
pub mod config;
mod observability;
pub mod runtime;
pub mod session;
pub mod workspace;

pub async fn serve(mut config: config::Config) -> Result<(), Box<dyn std::error::Error>> {
    use std::{future::IntoFuture, time::Duration};
    config.account_home = config.account_home.canonicalize()?;
    config.repository = config.repository.canonicalize()?;
    for profile in config.harnesses.values_mut() {
        profile.home = profile.home.canonicalize()?;
        profile.binary = profile.binary.canonicalize()?;
    }
    let manager = session::Manager::open(config.clone())?;
    manager.check_storage().await;
    // Reconcile previous workloads before the API or storage guard can start work.
    manager.restore_all().await?;
    manager.start_storage_guard();
    manager.start_uploader();
    let listener = tokio::net::TcpListener::bind(config.listen).await?;
    eprintln!("Cloudroom listening on {}", listener.local_addr()?);
    let shutdown = manager.clone();
    let mut changed = manager.subscribe();
    let server = axum::serve(listener, api::router(manager.clone(), config.token))
        .with_graceful_shutdown(async move {
            let mut terminate =
                tokio::signal::unix::signal(tokio::signal::unix::SignalKind::terminate())
                    .expect("signal handler");
            tokio::select! { _=tokio::signal::ctrl_c()=>{}, _=terminate.recv()=>{} }
            shutdown.shutdown().await;
        })
        .into_future();
    tokio::pin!(server);
    tokio::select! {
        result = &mut server => result?,
        _ = async {
            let _ = changed.wait_for(|_| manager.is_stopping()).await;
            // A peer may stop reading bytes already buffered by HTTP, outside the SSE queue.
            tokio::time::sleep(runtime::SHUTDOWN_GRACE + Duration::from_secs(2)).await;
        } => eprintln!("Cloudroom stopped waiting for unresponsive HTTP clients"),
    }
    Ok(())
}
