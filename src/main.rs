#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("preview") {
        cloudroom::preview::cli(&args[1..]).await?;
        return Ok(());
    }
    if args.first().map(String::as_str) == Some("--preview-skill") {
        print!("{}", cloudroom::preview::SKILL);
        return Ok(());
    }
    if args.first().map(String::as_str) == Some("--preview-authorized-keys") {
        if args.len() != 4 || args[3] != "cloudroom-preview" {
            return Err("invalid preview key lookup".into());
        }
        cloudroom::preview::authorized_keys(std::path::Path::new(&args[1]), args[2].parse()?)?;
        return Ok(());
    }
    if args.first().map(String::as_str) == Some("--clean-npm-cache") {
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
