use super::now_ms;
use serde::{Deserialize, Serialize};
use serde_json::{Value, json};
use sqlx::PgPool;
use std::{
    collections::BTreeMap,
    time::{Duration, Instant},
};
use tokio::sync::Mutex;

#[derive(Clone, Copy, Default, Serialize)]
pub(crate) struct AgentCounts {
    pub working: usize,
    pub queued: usize,
    pub waiting: usize,
    pub sleeping: usize,
    pub failed: usize,
}

#[derive(Clone, Copy, Default, Deserialize, Serialize, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) enum Range {
    #[default]
    #[serde(rename = "1h")]
    Hour,
    #[serde(rename = "24h")]
    Day,
    #[serde(rename = "7d")]
    Week,
    #[serde(rename = "30d")]
    Month,
}
impl Range {
    fn window(self) -> (u64, u64) {
        match self {
            Self::Hour => (3_600_000, 30_000),
            Self::Day => (86_400_000, 300_000),
            Self::Week => (604_800_000, 1_800_000),
            Self::Month => (2_592_000_000, 10_800_000),
        }
    }
}

pub(crate) struct History {
    pool: PgPool,
    store: String,
    // One in-flight read across all ranges; polling must not crowd out history uploads.
    cache: Mutex<BTreeMap<Range, (Instant, Value)>>,
}
impl History {
    pub fn new(pool: PgPool, store: String) -> Self {
        Self {
            pool,
            store,
            cache: Mutex::new(BTreeMap::new()),
        }
    }

    pub async fn read(&self, range: Range) -> Result<Value, sqlx::Error> {
        let mut cache = self.cache.lock().await;
        if let Some((at, value)) = cache.get(&range)
            && at.elapsed() < Duration::from_secs(30)
        {
            return Ok(value.clone());
        }
        let (span, step) = range.window();
        let to = now_ms();
        let from = to.saturating_sub(span) / step * step;
        let query = async {
            let mut tx = self.pool.begin().await?;
            sqlx::query("SET LOCAL statement_timeout = '6s'")
                .execute(&mut *tx)
                .await?;
            // Materialize numeric readings so window sorts never carry full diagnostic JSON.
            let rows: Vec<String> = sqlx::query_scalar(
                "WITH readings AS MATERIALIZED (
                    SELECT timestamp_ms, timestamp_ms / $4 * $4 AS bucket,
                        (record->>'cpu_used_percent')::double precision AS cpu,
                        100.0 * (record->>'memory_used_bytes')::double precision /
                            nullif((record->>'memory_total_bytes')::double precision, 0) AS memory
                    FROM cloudroom_diagnostics
                    WHERE store=$1 AND timestamp_ms >= $2 AND timestamp_ms <= $3
                        AND record->>'kind'='resources'
                ), samples AS (
                    SELECT *, lag(timestamp_ms) OVER (ORDER BY timestamp_ms) AS previous FROM readings
                ), buckets AS (
                    SELECT bucket, max(timestamp_ms) AS sampled_at,
                        coalesce(bool_or(timestamp_ms - previous > 30000), false) AS gap,
                        CASE WHEN count(cpu)=count(*) THEN avg(cpu) END AS cpu,
                        max(cpu) AS cpu_peak,
                        CASE WHEN count(memory)=count(*) THEN avg(memory) END AS memory,
                        max(memory) AS memory_peak
                    FROM samples GROUP BY bucket
                ) SELECT json_build_object(
                    'at', bucket, 'sampledAt', sampled_at, 'gap', gap,
                    'cpu', cpu, 'cpuPeak', cpu_peak, 'memory', memory, 'memoryPeak', memory_peak,
                    'memoryUsedBytes', (latest.record->>'memory_used_bytes')::bigint,
                    'memoryTotalBytes', (latest.record->>'memory_total_bytes')::bigint,
                    'diskAvailableBytes', (latest.record#>>'{workspace_disk,available_bytes}')::bigint,
                    'diskTotalBytes', (latest.record#>>'{workspace_disk,total_bytes}')::bigint,
                    'agents', CASE WHEN jsonb_typeof(latest.record->'agents')='object' THEN
                        json_build_object(
                            'working', (latest.record#>>'{agents,working}')::bigint,
                            'queued', (latest.record#>>'{agents,queued}')::bigint,
                            'waiting', (latest.record#>>'{agents,waiting}')::bigint,
                            'sleeping', (latest.record#>>'{agents,sleeping}')::bigint,
                            'failed', (latest.record#>>'{agents,failed}')::bigint
                        ) ELSE NULL END
                )::text FROM buckets
                JOIN LATERAL (
                    SELECT record FROM cloudroom_diagnostics
                    WHERE store=$1 AND timestamp_ms=buckets.sampled_at AND record->>'kind'='resources'
                    ORDER BY run_id, sequence LIMIT 1
                ) AS latest ON true ORDER BY bucket",
            )
            .bind(&self.store)
            .bind(from as i64)
            .bind(to as i64)
            .bind(step as i64)
            .fetch_all(&mut *tx)
            .await?;
            tx.commit().await?;
            let points = rows
                .into_iter()
                .map(|row| {
                    serde_json::from_str::<Value>(&row)
                        .map_err(|error| sqlx::Error::Decode(Box::new(error)))
                })
                .collect::<Result<Vec<_>, _>>()?;
            Ok::<_, sqlx::Error>(
                json!({"version":1,"range":range,"from":from,"to":to,"bucketMs":step,"points":points}),
            )
        };
        let result = tokio::time::timeout(Duration::from_secs(8), query)
            .await
            .map_err(|_| sqlx::Error::PoolTimedOut)??;
        cache.insert(range, (Instant::now(), result.clone()));
        Ok(result)
    }
}
