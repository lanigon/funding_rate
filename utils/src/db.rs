use std::env;

use anyhow::{anyhow, Result};
use common::{FundingSnapshot, PriceCandle, Venue};
use sea_orm::sea_query::OnConflict;
use sea_orm::{
    ActiveValue::Set, ColumnTrait, Database, DatabaseConnection, EntityTrait, QueryFilter,
    QueryOrder, QuerySelect,
};
use store::entities::{funding_rates, margin_interest, price_candles, token_pairs};
use store::migration;

use crate::tokens::TokenInfo;

const PRICE_BATCH_SIZE: usize = 500;
const FUNDING_BATCH_SIZE: usize = 500;
const MARGIN_BATCH_SIZE: usize = 500;

pub struct MarginInterestPoint {
    pub asset: String,
    pub ts_ms: i64,
    pub interest_rate: f64,
    pub interest: f64,
    pub principal: f64,
}

pub async fn connect_db() -> Result<DatabaseConnection> {
    let url = env::var("DATABASE_URL").map_err(|_| anyhow!("DATABASE_URL not set"))?;
    let db = Database::connect(&url).await?;
    migration::run_migrations(&db).await?;
    Ok(db)
}

pub async fn maybe_connect_db() -> Result<Option<DatabaseConnection>> {
    match env::var("DATABASE_URL") {
        Ok(url) => {
            let db = Database::connect(&url).await?;
            migration::run_migrations(&db).await?;
            Ok(Some(db))
        }
        Err(_) => Ok(None),
    }
}

pub async fn load_tradable_symbols() -> Result<Vec<String>> {
    let Some(db) = maybe_connect_db().await? else {
        return Ok(Vec::new());
    };
    let rows = token_pairs::Entity::find()
        .filter(
            token_pairs::Column::HasSpot
                .eq(true)
                .and(token_pairs::Column::PerpTakerFee.gt(0.0)),
        )
        .order_by_asc(token_pairs::Column::Symbol)
        .all(&db)
        .await?;
    Ok(rows.into_iter().map(|row| row.symbol).collect())
}

pub async fn persist_tokens(db: &DatabaseConnection, tokens: &[TokenInfo]) -> Result<()> {
    if tokens.is_empty() {
        return Ok(());
    }
    let models = tokens.iter().map(token_to_model).collect::<Vec<_>>();
    token_pairs::Entity::insert_many(models)
        .on_conflict(
            OnConflict::column(token_pairs::Column::Symbol)
                .update_columns([
                    token_pairs::Column::HasSpot,
                    token_pairs::Column::SpotTakerFee,
                    token_pairs::Column::SpotMakerFee,
                    token_pairs::Column::PerpTakerFee,
                    token_pairs::Column::PerpMakerFee,
                    token_pairs::Column::FundingIntervalSecs,
                    token_pairs::Column::MarginEnabled,
                ])
                .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}

fn token_to_model(token: &TokenInfo) -> token_pairs::ActiveModel {
    token_pairs::ActiveModel {
        symbol: Set(token.symbol.clone()),
        has_spot: Set(token.has_spot),
        spot_taker_fee: Set(token.spot_taker_fee.unwrap_or(0.0)),
        spot_maker_fee: Set(token.spot_maker_fee.unwrap_or(0.0)),
        perp_taker_fee: Set(token.perp_taker_fee.unwrap_or(0.0)),
        perp_maker_fee: Set(token.perp_maker_fee.unwrap_or(0.0)),
        funding_interval_secs: Set(token.funding_interval_secs.unwrap_or(0)),
        margin_enabled: Set(token.margin_enabled),
    }
}

pub async fn persist_price_candles(
    db: &DatabaseConnection,
    candles: Vec<PriceCandle>,
) -> Result<()> {
    if candles.is_empty() {
        return Ok(());
    }
    let mut batch = Vec::with_capacity(PRICE_BATCH_SIZE);
    for candle in candles {
        batch.push(price_candles::ActiveModel {
            venue: Set(format!("{:?}", candle.venue)),
            symbol: Set(candle.symbol.clone()),
            open_time: Set(candle.open_time),
            close_time: Set(candle.close_time),
            open: Set(candle.open),
            high: Set(candle.high),
            low: Set(candle.low),
            close: Set(candle.close),
            volume: Set(candle.volume),
        });
        if batch.len() >= PRICE_BATCH_SIZE {
            flush_price_batch(db, &mut batch).await?;
        }
    }
    if !batch.is_empty() {
        flush_price_batch(db, &mut batch).await?;
    }
    Ok(())
}

pub async fn load_price_candles(
    db: &DatabaseConnection,
    symbol: &str,
    venue: Venue,
    limit: usize,
) -> Result<Vec<PriceCandle>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let venue_str = format!("{:?}", venue);
    let rows = price_candles::Entity::find()
        .filter(price_candles::Column::Symbol.eq(symbol))
        .filter(price_candles::Column::Venue.eq(venue_str))
        .order_by_desc(price_candles::Column::OpenTime)
        .limit(limit as u64)
        .all(db)
        .await?;
    let mut candles: Vec<PriceCandle> = rows
        .into_iter()
        .map(|row| PriceCandle {
            venue,
            symbol: row.symbol,
            open_time: row.open_time,
            close_time: row.close_time,
            open: row.open,
            high: row.high,
            low: row.low,
            close: row.close,
            volume: row.volume,
        })
        .collect();
    candles.sort_by_key(|c| c.open_time);
    Ok(candles)
}

async fn flush_price_batch(
    db: &DatabaseConnection,
    batch: &mut Vec<price_candles::ActiveModel>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let models = std::mem::take(batch);
    price_candles::Entity::insert_many(models)
        .on_conflict(
            OnConflict::columns([
                price_candles::Column::Venue,
                price_candles::Column::Symbol,
                price_candles::Column::OpenTime,
            ])
            .update_columns([
                price_candles::Column::CloseTime,
                price_candles::Column::Open,
                price_candles::Column::High,
                price_candles::Column::Low,
                price_candles::Column::Close,
                price_candles::Column::Volume,
            ])
            .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}

pub async fn persist_funding_rates(
    db: &DatabaseConnection,
    symbol: &str,
    entries: Vec<FundingSnapshot>,
) -> Result<()> {
    if entries.is_empty() {
        return Ok(());
    }
    let mut batch = Vec::with_capacity(FUNDING_BATCH_SIZE);
    for entry in entries {
        batch.push(funding_rates::ActiveModel {
            symbol: Set(symbol.to_string()),
            ts_ms: Set(entry.ts_ms),
            funding_rate: Set(entry.funding_rate),
        });
        if batch.len() >= FUNDING_BATCH_SIZE {
            flush_funding_batch(db, &mut batch).await?;
        }
    }
    if !batch.is_empty() {
        flush_funding_batch(db, &mut batch).await?;
    }
    Ok(())
}

pub async fn load_recent_funding(
    db: &DatabaseConnection,
    symbol: &str,
    limit: usize,
) -> Result<Vec<FundingSnapshot>> {
    if limit == 0 {
        return Ok(Vec::new());
    }
    let rows = funding_rates::Entity::find()
        .filter(funding_rates::Column::Symbol.eq(symbol))
        .order_by_desc(funding_rates::Column::TsMs)
        .limit(limit as u64)
        .all(db)
        .await?;
    let mut snapshots = rows
        .into_iter()
        .map(|row| FundingSnapshot {
            ts_ms: row.ts_ms,
            symbol: row.symbol,
            funding_rate: row.funding_rate,
        })
        .collect::<Vec<_>>();
    snapshots.sort_by_key(|snapshot| snapshot.ts_ms);
    Ok(snapshots)
}

async fn flush_funding_batch(
    db: &DatabaseConnection,
    batch: &mut Vec<funding_rates::ActiveModel>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let models = std::mem::take(batch);
    funding_rates::Entity::insert_many(models)
        .on_conflict(
            OnConflict::columns([funding_rates::Column::Symbol, funding_rates::Column::TsMs])
                .update_column(funding_rates::Column::FundingRate)
                .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}

pub async fn persist_margin_interest(
    db: &DatabaseConnection,
    points: Vec<MarginInterestPoint>,
) -> Result<()> {
    if points.is_empty() {
        return Ok(());
    }
    let mut batch = Vec::with_capacity(MARGIN_BATCH_SIZE);
    for point in points {
        batch.push(margin_interest::ActiveModel {
            asset: Set(point.asset.clone()),
            ts_ms: Set(point.ts_ms),
            interest_rate: Set(point.interest_rate),
            interest: Set(point.interest),
            principal: Set(point.principal),
        });
        if batch.len() >= MARGIN_BATCH_SIZE {
            flush_margin_batch(db, &mut batch).await?;
        }
    }
    if !batch.is_empty() {
        flush_margin_batch(db, &mut batch).await?;
    }
    Ok(())
}

async fn flush_margin_batch(
    db: &DatabaseConnection,
    batch: &mut Vec<margin_interest::ActiveModel>,
) -> Result<()> {
    if batch.is_empty() {
        return Ok(());
    }
    let models = std::mem::take(batch);
    margin_interest::Entity::insert_many(models)
        .on_conflict(
            OnConflict::columns([
                margin_interest::Column::Asset,
                margin_interest::Column::TsMs,
            ])
            .update_columns([
                margin_interest::Column::InterestRate,
                margin_interest::Column::Interest,
                margin_interest::Column::Principal,
            ])
            .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}
