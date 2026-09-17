use crate::runtime::Kind;
use serde::{Deserialize, Serialize};
use std::{collections::BTreeMap, env, io, net::SocketAddr, path::PathBuf};

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HarnessConfig {
    pub binary: PathBuf,
    pub home: PathBuf,
    pub model: String,
    pub provider: Option<String>,
}

#[derive(Clone)]
pub struct Config {
    pub listen: SocketAddr,
    pub token: String,
    pub state_dir: PathBuf,
    pub repository: PathBuf,
    pub database_url: String,
    pub store: String,
    pub allow_insecure_database: bool,
    pub account_home: PathBuf,
    pub default_harness: Kind,
    pub harnesses: BTreeMap<Kind, HarnessConfig>,
    pub max_harnesses: usize,
    pub storage: Option<crate::workspace::storage::Policy>,
}

impl Config {
    pub fn from_env() -> io::Result<Self> {
        let token = required("CLOUDROOM_TOKEN")?;
        if token.len() < 32 || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(io::Error::other(
                "CLOUDROOM_TOKEN must contain at least 32 visible ASCII characters",
            ));
        }
        let listen: SocketAddr = env::var("CLOUDROOM_LISTEN")
            .unwrap_or_else(|_| "127.0.0.1:9840".into())
            .parse()
            .map_err(|_| io::Error::other("invalid CLOUDROOM_LISTEN"))?;
        let storage = if env::var("CLOUDROOM_UNPROTECTED_TEST_MODE").as_deref() == Ok("1") {
            None
        } else {
            Some(crate::workspace::storage::Policy::load(&PathBuf::from(
                required("CLOUDROOM_STORAGE_POLICY")?,
            ))?)
        };
        if !listen.ip().is_loopback() && storage.is_none() {
            return Err(io::Error::other(
                "public binding requires protected deployment behind HTTPS",
            ));
        }
        let default_harness = match env::var("CLOUDROOM_HARNESS").as_deref().unwrap_or("codex") {
            "codex" => Kind::Codex,
            "pi" => Kind::Pi,
            _ => return Err(io::Error::other("unsupported CLOUDROOM_HARNESS")),
        };
        let account_home: PathBuf = required("CLOUDROOM_ACCOUNT_HOME")?.into();
        let mut harnesses = BTreeMap::new();
        for (kind, prefix) in [(Kind::Codex, "CODEX"), (Kind::Pi, "PI")] {
            let binary = format!("CLOUDROOM_{prefix}_BINARY");
            if env::var_os(&binary).is_some() {
                let model = env::var(format!("CLOUDROOM_{prefix}_MODEL"))
                    .ok()
                    .filter(|m| !m.is_empty())
                    .map(Ok)
                    .unwrap_or_else(|| required("CLOUDROOM_MODEL"))?;
                let provider = if kind == Kind::Pi {
                    Some(required("CLOUDROOM_PI_PROVIDER")?)
                } else {
                    None
                };
                harnesses.insert(
                    kind,
                    HarnessConfig {
                        binary: required(&binary)?.into(),
                        home: required(&format!("CLOUDROOM_{prefix}_HOME"))?.into(),
                        model,
                        provider,
                    },
                );
            }
        }
        Ok(Self {
            storage,
            default_harness,
            harnesses,
            listen,
            token,
            state_dir: required("CLOUDROOM_STATE_DIR")?.into(),
            repository: env::var_os("CLOUDROOM_REPOSITORY")
                .map(PathBuf::from)
                .unwrap_or_else(|| account_home.clone()),
            database_url: required("CLOUDROOM_DATABASE_URL")?,
            store: required("CLOUDROOM_STORE")?,
            allow_insecure_database: env::var("CLOUDROOM_ALLOW_INSECURE_DATABASE").as_deref()
                == Ok("1"),
            account_home,
            max_harnesses: env::var("CLOUDROOM_MAX_HARNESSES")
                .unwrap_or_else(|_| "2".into())
                .parse::<usize>()
                .ok()
                .filter(|n| *n > 0)
                .ok_or_else(|| io::Error::other("CLOUDROOM_MAX_HARNESSES must be positive"))?,
        })
    }
}

fn required(name: &str) -> io::Result<String> {
    env::var(name)
        .ok()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| io::Error::other(format!("{name} is required")))
}
