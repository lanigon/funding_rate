#![allow(dead_code)]

use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use common::{logger, Venue};
use dotenvy::dotenv;
use sea_orm::DatabaseConnection;
use tokio::time::sleep;
use tracing::{info, warn};
use utils::db::{connect_db, persist_price_candles};
use utils::prices::{fetch_hourly_price_history, PriceCandle};
use utils::tokens::fetch_all_tokens;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    logger::init_logging();
    let db = connect_db().await?;
    let tokens = fetch_all_tokens().await?;

    let spot_symbols: Vec<String> = tokens
        .iter()
        .filter(|token| token.has_spot)
        .map(|token| token.symbol.clone())
        .collect();
    let perp_symbols: Vec<String> = tokens
        .iter()
        .filter(|token| token.has_perp)
        .map(|token| token.symbol.clone())
        .collect();

    let mut candle_count = 0usize;
    if !spot_symbols.is_empty() {
        candle_count += fetch_and_persist_symbols(&db, spot_symbols, Venue::Spot).await?;
    }
    if !perp_symbols.is_empty() {
        candle_count += fetch_and_persist_symbols(&db, perp_symbols, Venue::Perp).await?;
    }

    info!(candles = candle_count, "persisted hourly price history");
    Ok(())
}

async fn fetch_and_persist_symbols(
    db: &DatabaseConnection,
    symbols: Vec<String>,
    venue: Venue,
) -> Result<usize> {
    let mut total = 0usize;
    let throttle = Duration::from_millis(150);
    let total_symbols = symbols.len();
    for (idx, symbol) in symbols.into_iter().enumerate() {
        let payload = vec![symbol.clone()];
        match fetch_hourly_price_history(&payload, venue).await {
            Ok(map) => {
                let added = persist_price_map(db, map).await?;
                total += added;
                info!(
                    %symbol,
                    ?venue,
                    candles = added,
                    processed = idx + 1,
                    total_symbols,
                    "persisted hourly prices for symbol"
                );
            }
            Err(err) => {
                warn!(
                    %symbol,
                    ?venue,
                    ?err,
                    "failed to fetch hourly prices for symbol"
                );
            }
        }
        sleep(throttle).await;
    }
    Ok(total)
}

async fn persist_price_map(
    db: &DatabaseConnection,
    map: HashMap<String, Vec<PriceCandle>>,
) -> Result<usize> {
    let mut all = Vec::new();
    for (_, candles) in map {
        all.extend(candles);
    }
    let count = all.len();
    persist_price_candles(db, all).await?;
    Ok(count)
}
