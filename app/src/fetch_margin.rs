#![allow(dead_code)]

use std::env;

use anyhow::{anyhow, Result};
use chrono::{Duration as ChronoDuration, Utc};
use common::logger;
use connector::{BinanceCredentials, ConnectorConfig, MarginInterestRecord, RestClient};
use dotenvy::dotenv;
use tracing::{info, warn};
use utils::db::{connect_db, persist_margin_interest, MarginInterestPoint};
const HOURS_IN_LOOKBACK: i64 = 24 * 90;

fn rolling_window_range() -> (i64, i64) {
    let end = Utc::now().timestamp_millis();
    let start = (Utc::now() - ChronoDuration::hours(HOURS_IN_LOOKBACK)).timestamp_millis();
    (start, end)
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    logger::init_logging();
    let db = connect_db().await?;
    let api_key = env::var("BINANCE_API_KEY").map_err(|_| anyhow!("BINANCE_API_KEY not set"))?;
    let api_secret =
        env::var("BINANCE_API_SECRET").map_err(|_| anyhow!("BINANCE_API_SECRET not set"))?;
    let cfg = ConnectorConfig {
        credentials: Some(BinanceCredentials {
            api_key,
            api_secret,
        }),
        ..Default::default()
    };
    let client = RestClient::from_config(&cfg);
    let assets = client.fetch_margin_assets().await?;
    info!("assets: {:?}", assets);
    let mut total_records = 0usize;
    let (start, end) = rolling_window_range();
    for asset in assets.iter().filter(|asset| asset.is_borrowable) {
        match client
            .fetch_margin_interest_history(&asset.asset, start, end)
            .await
        {
            Ok(records) if records.is_empty() => continue,
            Ok(records) => {
                let points = records
                    .into_iter()
                    .filter_map(|record| margin_point_from_record(&record))
                    .collect::<Vec<_>>();
                if points.is_empty() {
                    continue;
                }
                total_records += points.len();
                persist_margin_interest(&db, points).await?;
            }
            Err(err) => {
                warn!(asset = %asset.asset, ?err, "failed to fetch margin interest");
            }
        }
    }

    info!(
        assets = assets.len(),
        records = total_records,
        "persisted margin interest history"
    );
    Ok(())
}

fn margin_point_from_record(record: &MarginInterestRecord) -> Option<MarginInterestPoint> {
    let interest_rate = record.interest_rate.parse::<f64>().ok()?;
    let interest = record.interest.parse::<f64>().ok()?;
    let principal = record.principal_amount.parse::<f64>().ok()?;
    Some(MarginInterestPoint {
        asset: record.asset.clone(),
        ts_ms: record.interest_accrued_time,
        interest_rate,
        interest,
        principal,
    })
}
