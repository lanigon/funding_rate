use std::collections::HashMap;
use std::env;

use anyhow::{anyhow, Result};
use clap::Parser;
use common::constant::DEFAULT_SYMBOLS;
use common::Venue;
use connector::{BinanceCredentials, ConnectorConfig, RestClient, TradeFill};
use dotenvy::dotenv;
use sea_orm::sea_query::OnConflict;
use sea_orm::ActiveValue::Set;
use sea_orm::EntityTrait;
use store::entities::{order_groups, orders};
use tracing::info;
use utils::db::{connect_db, load_tradable_symbols};
use uuid::Uuid;

use common::logger;

#[derive(Parser, Debug)]
#[command(name = "sync_trades", about = "Sync Binance trades into the local database")]
struct Args {
    /// Comma-separated symbols (e.g. BTCUSDT,ETHUSDT). Defaults to DB symbols or DEFAULT_SYMBOLS.
    #[arg(long)]
    symbols: Option<String>,
    /// Start timestamp (ms).
    #[arg(long)]
    start_ms: Option<i64>,
    /// End timestamp (ms).
    #[arg(long)]
    end_ms: Option<i64>,
    /// Venues to import: spot, perp, or both (comma-separated).
    #[arg(long, default_value = "spot,perp")]
    venues: String,
    /// Max rows per request (1-1000).
    #[arg(long, default_value_t = 1000)]
    limit: usize,
}

#[derive(Default)]
struct OrderAggregate {
    order_id: i64,
    symbol: String,
    venue: Venue,
    side: String,
    ts_ms: i64,
    qty_sum: f64,
    quote_sum: f64,
    price_qty_sum: f64,
    fee_sum: f64,
    fee_asset: String,
    fee_asset_mixed: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    logger::init_logging();
    let args = Args::parse();
    let db = connect_db().await?;
    let symbols = resolve_symbols(&args).await;
    let venues = parse_venues(&args.venues)?;

    let credentials = load_binance_credentials()?;
    let cfg = ConnectorConfig {
        credentials: Some(credentials),
        ..Default::default()
    };
    let client = RestClient::from_config(&cfg);

    let start_ms = args.start_ms;
    let end_ms = args.end_ms;
    if let (Some(start), Some(end)) = (start_ms, end_ms) {
        if start > end {
            return Err(anyhow!("start_ms must be <= end_ms"));
        }
    }

    let mut total_orders = 0usize;
    for symbol in symbols {
        for venue in &venues {
            let trades = fetch_all_trades(&client, &symbol, *venue, start_ms, end_ms, args.limit)
                .await?;
            let aggregates = aggregate_trades(trades, &symbol, *venue);
            let inserted = persist_orders(&db, &aggregates).await?;
            total_orders += inserted;
        }
    }
    info!(orders = total_orders, "trade sync completed");
    Ok(())
}

async fn resolve_symbols(args: &Args) -> Vec<String> {
    if let Some(list) = args.symbols.as_ref() {
        return list
            .split(',')
            .map(|s| s.trim().to_string())
            .filter(|s| !s.is_empty())
            .collect();
    }
    match load_tradable_symbols().await {
        Ok(list) if !list.is_empty() => list,
        _ => DEFAULT_SYMBOLS.iter().map(|s| s.to_string()).collect(),
    }
}

fn parse_venues(value: &str) -> Result<Vec<Venue>> {
    let mut venues = Vec::new();
    for entry in value.split(',') {
        match entry.trim().to_ascii_lowercase().as_str() {
            "spot" => venues.push(Venue::Spot),
            "perp" | "futures" => venues.push(Venue::Perp),
            "" => {}
            other => return Err(anyhow!("unknown venue: {other}")),
        }
    }
    if venues.is_empty() {
        return Err(anyhow!("no venues specified"));
    }
    Ok(venues)
}

async fn fetch_all_trades(
    client: &RestClient,
    symbol: &str,
    venue: Venue,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    limit: usize,
) -> Result<Vec<TradeFill>> {
    let mut out = Vec::new();
    let mut next_from_id: Option<i64> = None;
    let mut first = true;
    let limit = limit.max(1);
    loop {
        let start = if first { start_ms } else { None };
        let batch = match venue {
            Venue::Spot => {
                client
                    .fetch_spot_trades(symbol, start, end_ms, next_from_id, limit)
                    .await?
            }
            Venue::Perp => {
                client
                    .fetch_perp_trades(symbol, start, end_ms, next_from_id, limit)
                    .await?
            }
        };
        if batch.is_empty() {
            break;
        }
        let last_id = batch.last().map(|trade| trade.trade_id).unwrap_or(0);
        let batch_len = batch.len();
        out.extend(batch);
        let next = last_id.saturating_add(1);
        if next_from_id == Some(next) {
            break;
        }
        next_from_id = Some(next);
        first = false;
        if batch_len < limit {
            break;
        }
    }
    Ok(out)
}

fn aggregate_trades(
    trades: Vec<TradeFill>,
    symbol: &str,
    venue: Venue,
) -> Vec<OrderAggregate> {
    let mut map: HashMap<i64, OrderAggregate> = HashMap::new();
    for trade in trades {
        let entry = map.entry(trade.order_id).or_insert_with(|| OrderAggregate {
            order_id: trade.order_id,
            symbol: symbol.to_string(),
            venue,
            side: trade.side.clone(),
            ts_ms: trade.time,
            ..Default::default()
        });
        entry.ts_ms = entry.ts_ms.max(trade.time);
        let signed_qty = if entry.side == "buy" { trade.qty } else { -trade.qty };
        entry.qty_sum += signed_qty;
        entry.quote_sum += trade.quote_qty;
        entry.price_qty_sum += trade.price * trade.qty;
        entry.fee_sum += trade.commission;
        if entry.fee_asset.is_empty() {
            entry.fee_asset = trade.commission_asset;
        } else if entry.fee_asset != trade.commission_asset {
            entry.fee_asset_mixed = true;
        }
    }
    map.into_values().collect()
}

async fn persist_orders(db: &sea_orm::DatabaseConnection, orders_in: &[OrderAggregate]) -> Result<usize> {
    let mut inserted = 0usize;
    for agg in orders_in {
        let group_id = uuid_from_key(&format!(
            "binance-group:{:?}:{}:{}",
            agg.venue, agg.symbol, agg.order_id
        ));
        let order_id = uuid_from_key(&format!(
            "binance-order:{:?}:{}:{}",
            agg.venue, agg.symbol, agg.order_id
        ));
        let avg_price = if agg.qty_sum.abs() > 0.0 {
            agg.quote_sum.abs() / agg.qty_sum.abs()
        } else if agg.price_qty_sum.abs() > 0.0 {
            agg.price_qty_sum / agg.qty_sum.abs().max(1e-9)
        } else {
            0.0
        };
        let fee_asset = if agg.fee_asset_mixed {
            "MIXED".to_string()
        } else {
            agg.fee_asset.clone()
        };
        let group_model = order_groups::ActiveModel {
            group_id: Set(group_id),
            ts_ms: Set(agg.ts_ms),
            symbol: Set(agg.symbol.clone()),
            target_notional: Set(agg.quote_sum.abs()),
            funding_rate_at_signal: Set(0.0),
            basis_bps_at_signal: Set(0.0),
            status: Set("imported".to_string()),
            note: Set(format!("binance {:?} order_id={}", agg.venue, agg.order_id)),
        };
        order_groups::Entity::insert(group_model)
            .on_conflict(
                OnConflict::column(order_groups::Column::GroupId)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(db)
            .await?;
        let order_model = orders::ActiveModel {
            id: Set(order_id),
            group_id: Set(group_id),
            ts_ms: Set(agg.ts_ms),
            symbol: Set(agg.symbol.clone()),
            venue: Set(format!("{:?}", agg.venue)),
            side: Set(agg.side.clone()),
            order_type: Set("market".to_string()),
            price: Set(avg_price),
            qty: Set(agg.qty_sum),
            notional: Set(agg.quote_sum.abs()),
            quote_qty: Set(agg.quote_sum.abs()),
            qty_executed: Set(agg.qty_sum),
            funding_rate_at_order: Set(0.0),
            basis_bps_at_order: Set(0.0),
            fee_estimated: Set(0.0),
            fee_paid: Set(agg.fee_sum),
            fee_asset: Set(fee_asset),
            status: Set("filled".to_string()),
            note: Set(format!("binance order_id={}", agg.order_id)),
        };
        orders::Entity::insert(order_model)
            .on_conflict(
                OnConflict::column(orders::Column::Id)
                    .do_nothing()
                    .to_owned(),
            )
            .exec(db)
            .await?;
        inserted += 1;
    }
    Ok(inserted)
}

fn uuid_from_key(key: &str) -> Uuid {
    Uuid::new_v5(&Uuid::NAMESPACE_OID, key.as_bytes())
}

fn load_binance_credentials() -> Result<BinanceCredentials> {
    let api_key =
        env::var("BINANCE_API_KEY").map_err(|_| anyhow!("BINANCE_API_KEY not set"))?;
    let api_secret =
        env::var("BINANCE_API_SECRET").map_err(|_| anyhow!("BINANCE_API_SECRET not set"))?;
    Ok(BinanceCredentials {
        api_key,
        api_secret,
    })
}
