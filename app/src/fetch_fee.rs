#![allow(dead_code)]

use anyhow::Result;
use common::logger;
use connector::RestClient;
use dotenvy::dotenv;
use tokio::time::{sleep, Duration};
use tracing::info;
use utils::db::{connect_db, persist_funding_rates};
use utils::tokens::fetch_all_tokens;

#[allow(dead_code)]
const FUNDING_HISTORY_LIMIT: usize = 1000;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    logger::init_logging();
    let db = connect_db().await?;
    let tokens = fetch_all_tokens().await?;
    let symbols: Vec<String> = tokens
        .into_iter()
        .filter(|token| token.has_perp)
        .map(|token| token.symbol)
        .collect();

    let mut total_points = 0usize;
    let mut client = RestClient::new();
    for symbol in symbols {
        match client
            .fetch_funding_rates(&symbol, FUNDING_HISTORY_LIMIT)
            .await
        {
            data if data.is_empty() => continue,
            data => {
                total_points += data.len();
                persist_funding_rates(&db, &symbol, data).await?;
                sleep(Duration::from_millis(60)).await;
            }
        }
    }

    info!(entries = total_points, "persisted funding rate history");
    Ok(())
}
