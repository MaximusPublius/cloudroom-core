#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    if std::env::args().nth(1).as_deref() == Some("--clean-npm-cache") {
        let root = std::env::args_os().nth(2).ok_or("missing cache root")?;
        let groups = std::env::args_os()
            .nth(3)
            .ok_or("missing workload groups")?;
        cloudroom::workspace::storage::clean_npm_cache(
            std::path::Path::new(&root),
            std::path::Path::new(&groups),
        )?;
        return Ok(());
    }
    cloudroom::serve(cloudroom::config::Config::from_env()?).await
}
