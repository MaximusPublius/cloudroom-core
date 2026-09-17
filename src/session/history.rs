use super::{Receipt, Record, Session};
use crate::config::Config;
use sqlx::{
    PgPool, Row,
    postgres::{PgConnectOptions, PgPoolOptions, PgSslMode},
};
use std::{io, str::FromStr, time::Duration};
use tokio_stream::StreamExt;

pub struct History {
    pub(super) pool: PgPool,
    store: String,
}

impl History {
    pub fn new(config: &Config) -> io::Result<Self> {
        let mut options = PgConnectOptions::from_str(&config.database_url)
            .map_err(|_| io::Error::other("invalid CLOUDROOM_DATABASE_URL"))?;
        if config.allow_insecure_database {
            if !matches!(options.get_host(), "127.0.0.1" | "localhost" | "::1") {
                return Err(io::Error::other(
                    "insecure database access is restricted to loopback tests",
                ));
            }
            options = options.ssl_mode(PgSslMode::Disable);
        } else {
            options = options.ssl_mode(PgSslMode::VerifyFull);
        }
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .acquire_timeout(Duration::from_secs(2))
            .connect_lazy_with(options);
        Ok(Self {
            pool,
            store: config.store.clone(),
        })
    }

    pub async fn ready(&self) -> bool {
        // Check the real connection and both required tables without creating test records.
        sqlx::query("SELECT 1 FROM cloudroom_records WHERE store=$1 LIMIT 0")
            .bind(&self.store)
            .execute(&self.pool)
            .await
            .is_ok()
            && sqlx::query("SELECT 1 FROM cloudroom_diagnostics WHERE store=$1 LIMIT 0")
                .bind(&self.store)
                .execute(&self.pool)
                .await
                .is_ok()
    }

    pub async fn upload(&self, records: &[Record]) -> Result<(), sqlx::Error> {
        let mut tx = self.pool.begin().await?;
        for record in records {
            let text = serde_json::to_string(record).map_err(|e| sqlx::Error::Decode(e.into()))?;
            // A lost commit reply may cause retransmission. Only identical content is a retry.
            let matches: bool = sqlx::query_scalar(
                "INSERT INTO cloudroom_records (store, session_id, sequence, record) VALUES ($1,$2,$3,$4) \
                 ON CONFLICT (store, session_id, sequence) DO UPDATE SET record=cloudroom_records.record \
                 RETURNING record=$4")
                .bind(&self.store).bind(&record.session_id).bind(record.sequence as i64)
                .bind(text).fetch_one(&mut *tx).await?;
            if !matches {
                return Err(sqlx::Error::Protocol("conflicting history record".into()));
            }
        }
        tx.commit().await
    }

    pub async fn summary(
        &self,
        id: &str,
        request: Option<&str>,
    ) -> Result<(Option<Session>, Option<Receipt>), sqlx::Error> {
        let mut receipt = None; // Decode only the latest matching receipt, not superseded versions.
        let mut session = Session {
            session_id: id.into(),
            state: "saved_history_only".into(),
            ..Session::default()
        };
        // Stream metadata scans in Rust: native text can contain NUL, which PostgreSQL JSON
        // processing rejects. Replay's page limit must not truncate identity or status.
        let mut rows = sqlx::query("SELECT record FROM cloudroom_records WHERE store=$1 AND session_id=$2 ORDER BY sequence")
            .bind(&self.store).bind(id).fetch(&self.pool);
        while let Some(row) = rows.next().await {
            let record: Record = serde_json::from_str(row?.get("record"))
                .map_err(|e| sqlx::Error::Decode(e.into()))?;
            session.last_sequence = record.sequence;
            if session.native_id.is_none() && record.kind == "native_identity" {
                session.native_id = record.data["id"].as_str().map(str::to_owned);
            }
            if record.kind == "receipt" && record.data["command"] == "start" {
                session.workspace = record
                    .data
                    .get("workspace")
                    .filter(|w| !w.is_null())
                    .map(|w| serde_json::from_value(w.clone()))
                    .transpose()
                    .map_err(|e| sqlx::Error::Decode(e.into()))?;
                session.harness = serde_json::from_value(
                    record.data["input"]
                        .get("harness")
                        .cloned()
                        .unwrap_or(serde_json::json!("codex")),
                )
                .map_err(|e| sqlx::Error::Decode(e.into()))?;
            }
            if record.kind == "workspace" {
                session.workspace = Some(
                    serde_json::from_value(record.data.clone())
                        .map_err(|e| sqlx::Error::Decode(e.into()))?,
                );
            }
            if record.kind == "receipt" && request.is_some_and(|id| record.data["request_id"] == id)
            {
                receipt = Some(record.data);
            }
        }
        let receipt = receipt
            .map(serde_json::from_value)
            .transpose()
            .map_err(|e| sqlx::Error::Decode(e.into()))?;
        Ok(((session.last_sequence > 0).then_some(session), receipt))
    }

    pub async fn read(&self, session: &str, after: u64) -> Result<Vec<Record>, sqlx::Error> {
        let rows = sqlx::query("SELECT record FROM cloudroom_records WHERE store=$1 AND session_id=$2 AND sequence>$3 ORDER BY sequence LIMIT 256")
            .bind(&self.store).bind(session).bind(after as i64).fetch_all(&self.pool).await?;
        rows.into_iter()
            .map(|r| {
                serde_json::from_str(r.get::<&str, _>("record"))
                    .map_err(|e| sqlx::Error::Decode(e.into()))
            })
            .collect()
    }
}
