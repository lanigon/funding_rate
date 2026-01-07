#![allow(dead_code)]

use anyhow::{anyhow, Result};
use chrono::Utc;
use common::logger;
use dotenvy::dotenv;
use sea_orm::{ColumnTrait, DatabaseConnection, EntityTrait, QueryFilter, QueryOrder, QuerySelect};
use store::entities::{funding_rates, margin_interest, price_candles, token_pairs};
use tracing::info;
use utils::db::connect_db;

const HOUR_MS: i64 = 60 * 60 * 1000;
const MIN_SAMPLES: usize = 4;
const MIN_FUNDING_DIFF_HOURS: f64 = 1.0;

struct SelectConfig {
    limit: usize,
    funding_points: u64,
    price_hours: u64,
    margin_hours: u64,
}

impl Default for SelectConfig {
    fn default() -> Self {
        Self {
            limit: 5,
            funding_points: 48,
            price_hours: 48,
            margin_hours: 240,
        }
    }
}

pub async fn run() -> Result<()> {
    let cfg = SelectConfig::default();
    let db = connect_db().await?;
    let symbols = load_symbols(&db).await?;
    if symbols.is_empty() {
        return Err(anyhow!("token_pairs table is empty; run fetch_token first"));
    }

    let mut scores = Vec::new();
    for token in symbols {
        if !token.has_spot {
            continue;
        }
        let Some(funding) = load_funding_stats(
            &db,
            &token.symbol,
            cfg.funding_points,
            token.funding_interval_secs,
        )
        .await?
        else {
            continue;
        };
        let asset = token.symbol.trim_end_matches("USDT").to_string();
        let margin = load_margin_stats(&db, &asset, cfg.margin_hours).await?;
        let Some(price_vol) = load_price_volatility(&db, &token.symbol, cfg.price_hours).await?
        else {
            continue;
        };
        let leverage = leverage_from_volatility(price_vol);
        let margin_rate = margin.unwrap_or(0.0);
        let net_per_hour = funding.per_hour - margin_rate;
        let net_per_day_bps = net_per_hour * 24.0 * 10_000.0;
        scores.push(SymbolScore {
            symbol: token.symbol.clone(),
            funding_bps_day: funding.per_hour * 24.0 * 10_000.0,
            margin_bps_day: margin_rate * 24.0 * 10_000.0,
            net_bps_day: net_per_day_bps,
            leverage,
            volatility: price_vol,
        });
    }

    if scores.is_empty() {
        info!("no symbols with sufficient data found");
        return Ok(());
    }

    scores.sort_by(|a, b| {
        b.net_bps_day
            .partial_cmp(&a.net_bps_day)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    println!(
        "{:<10} {:>12} {:>12} {:>12} {:>10} {:>9}",
        "Symbol", "Funding bps/day", "Margin bps/day", "Net bps/day", "Volatility", "Leverage"
    );
    for row in scores.into_iter().take(cfg.limit.max(1)) {
        println!(
            "{:<10} {:>12.2} {:>12.2} {:>12.2} {:>10.4} {:>9.2}",
            row.symbol,
            row.funding_bps_day,
            row.margin_bps_day,
            row.net_bps_day,
            row.volatility,
            row.leverage,
        );
    }

    Ok(())
}

async fn load_symbols(db: &DatabaseConnection) -> Result<Vec<token_pairs::Model>> {
    let rows = token_pairs::Entity::find().all(db).await?;
    Ok(rows)
}

struct FundingStats {
    per_hour: f64,
}

async fn load_funding_stats(
    db: &DatabaseConnection,
    symbol: &str,
    limit: u64,
    interval_secs: i64,
) -> Result<Option<FundingStats>> {
    let rows = funding_rates::Entity::find()
        .filter(funding_rates::Column::Symbol.eq(symbol))
        .order_by_desc(funding_rates::Column::TsMs)
        .limit(limit.max(MIN_SAMPLES as u64))
        .all(db)
        .await?;
    if rows.len() < MIN_SAMPLES {
        return Ok(None);
    }
    let mut rows = rows;
    rows.sort_by_key(|row| row.ts_ms);
    let mut total = 0.0;
    let mut samples = 0.0;
    let default_hours = (interval_secs as f64 / 3600.0).max(MIN_FUNDING_DIFF_HOURS);
    let mut prev_ts = None;
    for row in rows {
        let dt_hours = prev_ts
            .map(|ts| ((row.ts_ms - ts) as f64 / HOUR_MS as f64).abs())
            .filter(|hours| *hours > 0.0)
            .unwrap_or(default_hours)
            .max(MIN_FUNDING_DIFF_HOURS);
        total += row.funding_rate / dt_hours;
        samples += 1.0;
        prev_ts = Some(row.ts_ms);
    }
    if samples == 0.0 {
        return Ok(None);
    }
    Ok(Some(FundingStats {
        per_hour: total / samples,
    }))
}

async fn load_margin_stats(
    db: &DatabaseConnection,
    asset: &str,
    hours: u64,
) -> Result<Option<f64>> {
    let until = Utc::now().timestamp_millis();
    let from = until - (hours as i64) * HOUR_MS;
    let rows = margin_interest::Entity::find()
        .filter(margin_interest::Column::Asset.eq(asset))
        .filter(margin_interest::Column::TsMs.gte(from))
        .order_by_desc(margin_interest::Column::TsMs)
        .all(db)
        .await?;
    if rows.is_empty() {
        return Ok(None);
    }
    let mut rows = rows;
    rows.sort_by_key(|row| row.ts_ms);
    let mut total = 0.0;
    let mut count = 0.0;
    let mut prev_ts = None;
    for row in rows {
        let dt_hours = prev_ts
            .map(|ts| ((row.ts_ms - ts) as f64 / HOUR_MS as f64).abs())
            .filter(|hours| *hours > 0.0)
            .unwrap_or(1.0);
        total += row.interest_rate / dt_hours.max(1.0);
        count += 1.0;
        prev_ts = Some(row.ts_ms);
    }
    if count == 0.0 {
        return Ok(None);
    }
    Ok(Some(total / count))
}

async fn load_price_volatility(
    db: &DatabaseConnection,
    symbol: &str,
    hours: u64,
) -> Result<Option<f64>> {
    let rows = price_candles::Entity::find()
        .filter(price_candles::Column::Symbol.eq(symbol))
        .filter(price_candles::Column::Venue.eq("Spot"))
        .order_by_desc(price_candles::Column::OpenTime)
        .limit(hours.max(MIN_SAMPLES as u64))
        .all(db)
        .await?;
    if rows.len() < MIN_SAMPLES {
        return Ok(None);
    }
    let closes: Vec<f64> = rows.into_iter().map(|row| row.close).collect();
    Ok(compute_volatility(&closes))
}

fn compute_volatility(series: &[f64]) -> Option<f64> {
    if series.len() < MIN_SAMPLES {
        return None;
    }
    let mean = series.iter().copied().sum::<f64>() / series.len() as f64;
    if mean <= 0.0 {
        return None;
    }
    let variance = series
        .iter()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (series.len() as f64 - 1.0);
    Some((variance.sqrt() / mean).max(0.0))
}

fn leverage_from_volatility(vol: f64) -> f64 {
    if vol <= 0.01 {
        return 9.0;
    }
    if vol >= 0.06 {
        return 3.0;
    }
    let span = 0.06 - 0.01;
    let normalized = ((vol - 0.01) / span).clamp(0.0, 1.0);
    9.0 - normalized * 6.0
}

struct SymbolScore {
    symbol: String,
    funding_bps_day: f64,
    margin_bps_day: f64,
    net_bps_day: f64,
    leverage: f64,
    volatility: f64,
}

#[allow(dead_code)]
#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    logger::init_logging();
    run().await
}
