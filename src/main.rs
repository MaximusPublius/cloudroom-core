#[tokio::main(worker_threads = 2)]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let args: Vec<String> = std::env::args().skip(1).collect();
    if args.first().map(String::as_str) == Some("--version") {
        println!("cloudroom {} ({})", cloudroom::VERSION, cloudroom::COMMIT);
        return Ok(());
    }
    if args.first().map(String::as_str) == Some("--features") {
        println!("{}", cloudroom::FEATURES.join("\n"));
        return Ok(());
    }
    if args.first().map(String::as_str) == Some("preview") {
        cloudroom::preview::cli(&args[1..]).await?;
        return Ok(());
    }
    if args.first().map(String::as_str) == Some("mac") {
        match cloudroom::mac::cli(&args[1..]).await {
            Ok(code) => std::process::exit(code),
            Err(error) => {
                eprintln!("{error}");
                std::process::exit(1);
            }
        }
    }
    if args.first().map(String::as_str) == Some("secret") {
        match cloudroom::secrets::cli(&args[1..]).await {
            Ok(code) => std::process::exit(code),
            Err(error) => {
                eprintln!("{error}");
                std::process::exit(1);
            }
        }
    }
    if args.first().map(String::as_str) == Some("--secrets-skill") {
        print!("{}", cloudroom::secrets::SKILL);
        return Ok(());
    }
    if args.first().map(String::as_str) == Some("--mac-skill") {
        print!("{}", cloudroom::mac::SKILL);
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
    if args.first().map(String::as_str) == Some("--cursor-driver") {
        let binary = args.get(1).ok_or("missing Cursor binary")?;
        cloudroom::runtime::cursor_print::run(binary)?;
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
    // The service starts without arguments; anything else is a mistyped agent command.
    if let Some(command) = args.first() {
        eprintln!(
            "Unknown command {command}. Use cloudroom preview, cloudroom mac, or cloudroom secret."
        );
        std::process::exit(2);
    }
    cloudroom::serve(cloudroom::config::Config::from_env()?).await
}
