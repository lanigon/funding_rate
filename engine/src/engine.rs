use std::collections::{HashMap, HashSet};
use std::env;
use std::fs;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use anyhow::{anyhow, Result};
use chrono::{Duration as ChronoDuration, Timelike, Utc};
use common::{
    constant::{
        BINANCE_DEFAULT_FUNDING_INTERVAL_SECS, DEFAULT_STRATEGY_REBALANCE_THRESHOLD,
        DEFAULT_STRATEGY_TARGET_NOTIONAL_USD, DEFAULT_SYMBOLS,
    },
    logger, now_ms, Event, FundingSnapshot, Level as BookLevel, OrderBookSnapshot, OrderBookTopN,
    PriceCandle, PriceWindow, Venue,
};
use connector::{BinanceCredentials, BinanceFeed, ConnectorConfig, RestClient, TradeFill};
use datalayer::{spawn as spawn_datalayer, SharedState};
use executor::{self, BinanceTrader, ExecutionBackend};
use futures::stream::{self, StreamExt};
use metrics::spawn as spawn_metrics;
use sea_orm::DatabaseConnection;
use serde::Deserialize;
use store::StoreConfig;
use strategy::{spawn as spawn_strategy, StrategyConfig};
use tokio::{
    sync::mpsc,
    time::{interval, sleep, sleep_until, Instant},
};
use tracing::{debug, info, warn};
use utils::db::{
    load_price_candles, load_recent_funding, load_tradable_symbols, maybe_connect_db,
    persist_funding_rates, persist_price_candles,
};
use utils::prices::{align_to_hour, fetch_hourly_range};

use crate::research;

const FUNDING_SEED_LIMIT: usize = 64;
const FUNDING_SEED_CONCURRENCY: usize = 4;
const FUNDING_SEED_DELAY_MS: u64 = 300;
const SPOT_CAPITAL_USD: f64 = 80.0;
const PERP_MARGIN_USD: f64 = 20.0;
const PRICE_SEED_HOURS: i64 = 120;
const HOUR_MS: i64 = 60 * 60 * 1000;
const PRICE_SEED_CONCURRENCY: usize = 8;
const TRADE_SEED_LOOKBACK_MS: i64 = 3 * 24 * 60 * 60 * 1000;
const TRADE_SEED_LIMIT: usize = 1000;
const TRADE_SYNC_INTERVAL_SECS: u64 = 60;

/// User-configurable runtime switches picked up by the thin CLI.
#[derive(Clone, Debug, Default)]
pub struct EngineOptions {
    /// Run the offline research flow instead of the live engine.
    pub run_research: bool,
}

#[derive(Deserialize)]
struct UniverseEntry {
    symbol: String,
}

pub async fn run(opts: EngineOptions) -> Result<()> {
    logger::init_logging();

    if opts.run_research {
        research::run_research().await?;
        return Ok(());
    }

    let mut source = "database";
    let mut symbols = match load_tradable_symbols().await {
        Ok(list) => list,
        Err(err) => {
            warn!(?err, "failed to load symbols from database");
            Vec::new()
        }
    };
    info!("{:#?}", symbols);
    if symbols.is_empty() {
        let mut symbols_opt = load_universe();
        if symbols_opt.is_none() {
            research::run_research().await?;
            symbols_opt = load_universe();
        }
        symbols = symbols_opt.unwrap_or_else(|| {
            source = "default";
            DEFAULT_SYMBOLS
                .iter()
                .map(|symbol| symbol.to_string())
                .collect()
        });
        if source != "default" {
            source = "universe_file";
        }
    }
    info!(
        source,
        count = symbols.len(),
        ?symbols,
        "engine starting with universe"
    );

    let (datalayer_tx, datalayer_rx) = mpsc::channel::<Event>(8096);
    let (store_tx, store_rx) = mpsc::channel::<Event>(512);
    let store_handle = store::spawn(store_rx, StoreConfig::default());
    let (handles, datalayer_task) = spawn_datalayer(datalayer_rx);

    let strategy_cfg = StrategyConfig {
        symbols: symbols.clone(),
        target_notional_usd: DEFAULT_STRATEGY_TARGET_NOTIONAL_USD,
        rebalance_threshold: DEFAULT_STRATEGY_REBALANCE_THRESHOLD,
        spot_budget_usd: SPOT_CAPITAL_USD,
        perp_budget_usd: PERP_MARGIN_USD,
    };
    let credentials = load_binance_credentials()?;
    let connector_cfg = ConnectorConfig {
        symbols: symbols.clone(),
        credentials: Some(credentials),
        ..Default::default()
    };

    let seed_cfg = connector_cfg.clone();
    let seed_state = handles.state.clone();
    tokio::spawn(async move {
        if let Err(err) = run_trade_sync(seed_cfg, seed_state).await {
            warn!(?err, "trade sync task failed");
        }
    });

    let trader = BinanceTrader::from_config(&connector_cfg, store_tx.clone());
    let backend = ExecutionBackend::binance(trader);
    let executor_handle = executor::spawn(handles.executor_rx, datalayer_tx.clone(), backend);
    let strategy_handle = spawn_strategy(
        handles.strategy_rx,
        datalayer_tx.clone(),
        handles.state.clone(),
        strategy_cfg.clone(),
    );
    let metrics_handle = spawn_metrics(handles.state.clone());

    if let Err(err) = seed_initial_state(&connector_cfg, &datalayer_tx).await {
        warn!(?err, "failed to seed initial state");
    }
    if let Err(err) = seed_price_windows(&connector_cfg, &datalayer_tx).await {
        warn!(?err, "failed to seed price windows");
    }

    info!("spawning binance connector");
    let feed_cfg = connector_cfg.clone();
    let feed_tx = datalayer_tx.clone();
    let connector_handle = tokio::spawn(async move {
        let feed = BinanceFeed::new(feed_cfg, feed_tx);
        feed.run().await;
    });

    let refresh_handle = tokio::spawn(run_hourly_refresh(
        connector_cfg.clone(),
        datalayer_tx.clone(),
    ));

    let _ = tokio::join!(
        connector_handle,
        refresh_handle,
        datalayer_task,
        strategy_handle,
        executor_handle,
        store_handle,
        metrics_handle,
    );
    Ok(())
}

fn load_universe() -> Option<Vec<String>> {
    let path = Path::new("universe.json");
    let data = fs::read_to_string(path).ok()?;
    let entries: Vec<UniverseEntry> = serde_json::from_str(&data).ok()?;
    Some(entries.into_iter().map(|entry| entry.symbol).collect())
}

fn load_binance_credentials() -> Result<BinanceCredentials> {
    let api_key = env::var("BINANCE_API_KEY")
        .map_err(|_| anyhow!("BINANCE_API_KEY not set in environment/.env"))?;
    let api_secret = env::var("BINANCE_API_SECRET")
        .map_err(|_| anyhow!("BINANCE_API_SECRET not set in environment/.env"))?;
    Ok(BinanceCredentials {
        api_key,
        api_secret,
    })
}


async fn seed_initial_state(cfg: &ConnectorConfig, inbound_tx: &mpsc::Sender<Event>) -> Result<()> {
    let seed_start = Instant::now();
    let db = match maybe_connect_db().await {
        Ok(conn) => conn,
        Err(err) => {
            warn!(?err, "unable to connect database for boot seed");
            None
        }
    };
    let default_interval_ms = (cfg
        .funding_interval_secs
        .max(BINANCE_DEFAULT_FUNDING_INTERVAL_SECS) as i64)
        * 1000;
    let symbols = cfg.symbols.clone();
    let cfg = Arc::new(cfg.clone());
    let stream = stream::iter(symbols.into_iter().map(|symbol| {
        let cfg = Arc::clone(&cfg);
        let db = db.clone();
        let tx = inbound_tx.clone();
        async move { seed_symbol_state(symbol, cfg, tx, db, default_interval_ms).await }
    }))
    .buffer_unordered(FUNDING_SEED_CONCURRENCY);

    let mut _completed = 0usize;
    let mut _failures = 0usize;
    let mut errors = Vec::new();
    let results = stream.collect::<Vec<_>>().await;
    for result in results {
        match result {
            Ok(_) => _completed += 1,
            Err(err) => {
                _failures += 1;
                errors.push(err);
            }
        }
    }
    info!(
        total = cfg.symbols.len(),
        completed = _completed,
        failed = _failures,
        elapsed_ms = seed_start.elapsed().as_millis() as u64,
        "funding seed finished"
    );
    if let Some(err) = errors.into_iter().next() {
        return Err(err);
    }
    Ok(())
}

async fn seed_price_windows(cfg: &ConnectorConfig, inbound_tx: &mpsc::Sender<Event>) -> Result<()> {
    let seed_start = Instant::now();
    let db = match maybe_connect_db().await {
        Ok(conn) => conn,
        Err(err) => {
            warn!(?err, "unable to connect database for price seed");
            None
        }
    };
    let end = align_to_hour(Utc::now().timestamp_millis());
    let start = end - PRICE_SEED_HOURS * HOUR_MS;
    let target_len = PRICE_SEED_HOURS as usize;
    let venues = [Venue::Spot, Venue::Perp];
    let mut restored = 0usize;
    let mut needs_fetch: Vec<(String, Venue)> = Vec::new();

    if let Some(conn) = db.as_ref() {
        for symbol in &cfg.symbols {
            for &venue in &venues {
                match load_price_candles(conn, symbol, venue, target_len).await {
                    Ok(candles) if window_complete(&candles, start, end, target_len) => {
                        let window = PriceWindow {
                            symbol: symbol.clone(),
                            venue,
                            candles,
                        };
                        inbound_tx
                            .send(Event::PriceWindow(window))
                            .await
                            .map_err(|_| anyhow!("datalayer channel closed"))?;
                        restored += 1;
                        debug!(%symbol, ?venue, "price window restored from db");
                    }
                    Ok(_) => needs_fetch.push((symbol.clone(), venue)),
                    Err(err) => {
                        warn!(?err, %symbol, ?venue, "failed to load price candles from db");
                        needs_fetch.push((symbol.clone(), venue));
                    }
                }
            }
        }
    } else {
        needs_fetch = cfg
            .symbols
            .iter()
            .flat_map(|symbol| venues.into_iter().map(move |venue| (symbol.clone(), venue)))
            .collect();
    }

    let mut fetched = 0usize;
    let mut failures = 0usize;
    if !needs_fetch.is_empty() {
        let total = needs_fetch.len();
        let mut stream = stream::iter(needs_fetch.into_iter().map(|(symbol, venue)| {
            let db = db.clone();
            async move {
                match fetch_hourly_range(&symbol, venue, start, end).await {
                    Ok(candles) if candles.is_empty() => Err(PriceSeedFailure::new(
                        symbol,
                        venue,
                        anyhow!("price seed returned empty data"),
                    )),
                    Ok(candles) => {
                        if let Some(conn) = db.as_ref() {
                            if let Err(err) = persist_price_candles(conn, candles.clone()).await {
                                warn!(?err, %symbol, ?venue, "failed to persist price candles");
                            }
                        }
                        Ok((symbol, venue, candles))
                    }
                    Err(err) => Err(PriceSeedFailure::new(symbol, venue, err)),
                }
            }
        }))
        .buffer_unordered(PRICE_SEED_CONCURRENCY);

        while let Some(result) = stream.next().await {
            match result {
                Ok((symbol, venue, candles)) => {
                    let window = PriceWindow {
                        symbol: symbol.clone(),
                        venue,
                        candles,
                    };
                    inbound_tx
                        .send(Event::PriceWindow(window))
                        .await
                        .map_err(|_| anyhow!("datalayer channel closed"))?;
                    fetched += 1;
                    debug!(%symbol, ?venue, fetched, total, "seeded price window");
                }
                Err(failure) => {
                    failures += 1;
                    warn!(
                        symbol = %failure.symbol,
                        venue = ?failure.venue,
                        error = ?failure.error,
                        "failed to fetch hourly prices"
                    );
                }
            }
        }
    }

    info!(
        symbols = cfg.symbols.len(),
        restored,
        fetched,
        failures,
        elapsed_ms = seed_start.elapsed().as_millis() as u64,
        "price window seed finished"
    );
    if failures > 0 {
        Err(anyhow!("price seed encountered errors"))
    } else {
        Ok(())
    }
}

async fn run_trade_sync(cfg: ConnectorConfig, state: SharedState) -> Result<()> {
    if cfg.credentials.is_none() {
        warn!("trade sync skipped: missing api credentials");
        return Ok(());
    }
    let total_symbols = cfg.symbols.len();
    let total_venues = cfg.venues.len();
    info!(
        symbols = total_symbols,
        venues = total_venues,
        lookback_ms = TRADE_SEED_LOOKBACK_MS,
        "seeding counters from myTrades"
    );
    let mut client = RestClient::from_config(&cfg);
    let start_ms = now_ms().saturating_sub(TRADE_SEED_LOOKBACK_MS);
    let mut price_cache: HashMap<String, f64> = HashMap::new();
    let mut cursors: HashMap<(String, Venue), i64> = HashMap::new();
    let mut total_order_count = 0u64;
    let mut total_fee_usdt = 0.0;
    let mut total_trade_count = 0usize;
    let mut processed_symbols = 0usize;

    for symbol in &cfg.symbols {
        for venue in &cfg.venues {
            let trades = match fetch_all_trades(
                &client,
                symbol,
                *venue,
                Some(start_ms),
                None,
                None,
                TRADE_SEED_LIMIT,
            )
            .await
            {
                Ok(list) => list,
                Err(err) => {
                    warn!(?err, symbol, ?venue, "failed to fetch myTrades");
                    continue;
                }
            };
            if trades.is_empty() {
                continue;
            }
            if let Some(last_id) = trades.iter().map(|trade| trade.trade_id).max() {
                cursors.insert((symbol.to_string(), *venue), last_id);
            }
            let (order_count, fee_usdt) =
                summarize_trades(&client, &trades, &mut price_cache).await;
            total_order_count += order_count as u64;
            total_fee_usdt += fee_usdt;
            total_trade_count += trades.len();
            info!(
                symbol,
                ?venue,
                trades = trades.len(),
                orders = order_count,
                fee_usdt,
                "seeded myTrades"
            );
        }
        processed_symbols += 1;
        if processed_symbols % 25 == 0 || processed_symbols == total_symbols {
            info!(
                processed = processed_symbols,
                total = total_symbols,
                orders = total_order_count,
                fee_usdt = total_fee_usdt,
                "trade seed progress"
            );
        }
    }

    state.seed_counters(total_order_count, total_fee_usdt);
    info!(
        order_count_total = total_order_count,
        fee_cum = total_fee_usdt,
        trade_count = total_trade_count,
        "seeded counters from myTrades"
    );

    let mut ticker = interval(Duration::from_secs(TRADE_SYNC_INTERVAL_SECS));
    loop {
        ticker.tick().await;
        let positions = client.fetch_position_risk().await;
        let active_symbols: HashSet<String> = positions
            .into_iter()
            .filter(|pos| pos.position_amt.abs() > 1e-9)
            .map(|pos| pos.symbol)
            .collect();
        if active_symbols.is_empty() {
            continue;
        }
        for symbol in active_symbols {
            for venue in &cfg.venues {
                let from_id = cursors
                    .get(&(symbol.clone(), *venue))
                    .map(|id| id.saturating_add(1));
                let trades = match fetch_all_trades(
                    &client,
                    &symbol,
                    *venue,
                    None,
                    None,
                    from_id,
                    TRADE_SEED_LIMIT,
                )
                .await
                {
                    Ok(list) => list,
                    Err(err) => {
                        warn!(?err, symbol, ?venue, "failed to refresh myTrades");
                        continue;
                    }
                };
                if trades.is_empty() {
                    continue;
                }
                if let Some(last_id) = trades.iter().map(|trade| trade.trade_id).max() {
                    cursors.insert((symbol.clone(), *venue), last_id);
                }
                let (order_count, fee_usdt) =
                    summarize_trades(&client, &trades, &mut price_cache).await;
                state.apply_trade_delta(order_count as u64, fee_usdt);
                info!(
                    symbol,
                    ?venue,
                    trades = trades.len(),
                    orders = order_count,
                    fee_usdt,
                    "synced myTrades"
                );
            }
        }
    }
}

async fn fetch_all_trades(
    client: &RestClient,
    symbol: &str,
    venue: Venue,
    start_ms: Option<i64>,
    end_ms: Option<i64>,
    from_id: Option<i64>,
    limit: usize,
) -> Result<Vec<TradeFill>> {
    let mut out = Vec::new();
    let mut next_from_id: Option<i64> = from_id;
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

async fn summarize_trades(
    client: &RestClient,
    trades: &[TradeFill],
    price_cache: &mut HashMap<String, f64>,
) -> (usize, f64) {
    let mut order_ids: HashSet<i64> = HashSet::new();
    let mut fee_usdt = 0.0;
    for trade in trades {
        order_ids.insert(trade.order_id);
        if let Some(fee) = trade_fee_usdt(client, trade, price_cache).await {
            fee_usdt += fee;
        }
    }
    (order_ids.len(), fee_usdt)
}

async fn trade_fee_usdt(
    client: &RestClient,
    trade: &TradeFill,
    price_cache: &mut HashMap<String, f64>,
) -> Option<f64> {
    if trade.commission <= 0.0 {
        return None;
    }
    if is_stable_asset(&trade.commission_asset) {
        return Some(trade.commission);
    }
    let asset = trade.commission_asset.to_ascii_uppercase();
    if let Some(price) = price_cache.get(&asset).copied() {
        return Some(trade.commission * price);
    }
    let symbol = format!("{asset}USDT");
    match client.fetch_spot_price(&symbol).await {
        Some(price) if price > 0.0 => {
            price_cache.insert(asset, price);
            Some(trade.commission * price)
        }
        _ => {
            warn!(
                asset = trade.commission_asset,
                symbol,
                "unable to convert trade fee to usdt"
            );
            None
        }
    }
}

fn is_stable_asset(asset: &str) -> bool {
    matches!(
        asset.to_ascii_uppercase().as_str(),
        "USDT" | "BUSD" | "USDC" | "TUSD" | "FDUSD" | "USDP"
    )
}

struct PriceSeedFailure {
    symbol: String,
    venue: Venue,
    error: anyhow::Error,
}

impl PriceSeedFailure {
    fn new(symbol: String, venue: Venue, error: anyhow::Error) -> Self {
        Self {
            symbol,
            venue,
            error,
        }
    }
}

fn window_complete(candles: &[PriceCandle], start: i64, end: i64, target_len: usize) -> bool {
    if candles.len() < target_len {
        return false;
    }
    let first_open = candles.first().map(|c| c.open_time).unwrap_or(i64::MAX);
    let last_close = candles.last().map(|c| c.close_time).unwrap_or(0);
    first_open <= start && last_close >= end - HOUR_MS
}

fn infer_funding_interval_ms(entries: &[FundingSnapshot]) -> Option<i64> {
    if entries.len() < 2 {
        return None;
    }
    let last = entries.last().map(|entry| entry.ts_ms)?;
    let prev = entries
        .get(entries.len() - 2)
        .map(|entry| entry.ts_ms)
        .unwrap_or(last);
    let delta = (last - prev).abs();
    if delta > 0 {
        Some(delta)
    } else {
        None
    }
}

async fn seed_symbol_state(
    symbol: String,
    cfg: Arc<ConnectorConfig>,
    inbound_tx: mpsc::Sender<Event>,
    db: Option<DatabaseConnection>,
    default_interval_ms: i64,
) -> Result<()> {
    let start = Instant::now();
    let mut existing = 0usize;
    let mut latest_ts: Option<i64> = None;
    let mut cached_funding: Vec<FundingSnapshot> = Vec::new();
    if let Some(conn) = db.as_ref() {
        match load_recent_funding(conn, &symbol, FUNDING_SEED_LIMIT).await {
            Ok(entries) if !entries.is_empty() => {
                existing = entries.len();
                let mut _restored = 0usize;
                latest_ts = entries.last().map(|snapshot| snapshot.ts_ms);
                cached_funding = entries;
                for snapshot in cached_funding.iter().cloned() {
                    _restored += 1;
                    inbound_tx
                        .send(Event::FundingSnapshot(snapshot))
                        .await
                        .map_err(|_| anyhow!("datalayer channel closed"))?;
                }
            }
            Ok(_) => {}
            Err(err) => warn!(%symbol, ?err, "failed to load funding from database"),
        }
    }
    let mut interval_ms = default_interval_ms.max(60_000);
    if let Some(derived) = infer_funding_interval_ms(&cached_funding) {
        interval_ms = derived.max(interval_ms);
    }
    let needed = {
        let now = now_ms();
        match latest_ts {
            Some(ts) => {
                let gap = now.saturating_sub(ts);
                if gap > interval_ms {
                    ((gap / interval_ms) as usize)
                        .min(FUNDING_SEED_LIMIT)
                        .max(1)
                } else {
                    0
                }
            }
            None => FUNDING_SEED_LIMIT,
        }
    };
    if needed == 0 {
        debug!(
            %symbol,
            existing,
            fetched = 0,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "funding seed complete"
        );
        return Ok(());
    }
    let mut rest_client = RestClient::from_config(&cfg);
    sleep(Duration::from_millis(FUNDING_SEED_DELAY_MS)).await;
    let fetched = rest_client.fetch_funding_rates(&symbol, needed).await;
    if fetched.is_empty() {
        debug!(
            %symbol,
            needed,
            "funding seed: remote returned no data, skipping"
        );
        debug!(
            %symbol,
            existing,
            fetched = 0,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "funding seed complete"
        );
        return Ok(());
    }
    let mut new_points: Vec<_> = fetched
        .into_iter()
        .filter(|snapshot| latest_ts.map(|ts| snapshot.ts_ms > ts).unwrap_or(true))
        .collect();
    if new_points.is_empty() {
        debug!(
            %symbol,
            needed,
            "funding seed: fetched data were all stale, skipping"
        );
        debug!(
            %symbol,
            existing,
            fetched = 0,
            elapsed_ms = start.elapsed().as_millis() as u64,
            "funding seed complete"
        );
        return Ok(());
    }
    new_points.sort_by_key(|snapshot| snapshot.ts_ms);
    if let Some(conn) = db.as_ref() {
        if let Err(err) = persist_funding_rates(conn, &symbol, new_points.clone()).await {
            warn!(%symbol, ?err, "failed to persist seeded funding");
        }
    }
    for snapshot in new_points.iter().cloned() {
        inbound_tx
            .send(Event::FundingSnapshot(snapshot))
            .await
            .map_err(|_| anyhow!("datalayer channel closed"))?;
    }
    debug!(%symbol, existing, fetched = new_points.len(), elapsed_ms = start.elapsed().as_millis() as u64, "funding seed complete");
    Ok(())
}

async fn run_hourly_refresh(cfg: ConnectorConfig, inbound_tx: mpsc::Sender<Event>) {
    let db = maybe_connect_db().await.unwrap_or(None);
    let mut client = RestClient::from_config(&cfg);
    loop {
        let trigger = next_refresh_instant();
        sleep_until(trigger).await;
        if let Err(err) = refresh_once(&mut client, &cfg, &inbound_tx, db.as_ref()).await {
            warn!(?err, "hourly refresh iteration failed");
        }
    }
}

fn next_refresh_instant() -> Instant {
    let now = Utc::now();
    let mut trigger = now
        .with_minute(50)
        .and_then(|dt| dt.with_second(0))
        .and_then(|dt| dt.with_nanosecond(0))
        .expect("valid trigger minute");
    if trigger <= now {
        trigger = (now + ChronoDuration::hours(1))
            .with_minute(50)
            .and_then(|dt| dt.with_second(0))
            .and_then(|dt| dt.with_nanosecond(0))
            .expect("valid trigger minute");
    }
    let delay_ms = (trigger - now).num_milliseconds().max(0) as u64;
    Instant::now() + Duration::from_millis(delay_ms)
}

async fn refresh_once(
    client: &mut RestClient,
    cfg: &ConnectorConfig,
    inbound_tx: &mpsc::Sender<Event>,
    db: Option<&DatabaseConnection>,
) -> Result<()> {
    for symbol in &cfg.symbols {
        if let Some(snapshot) = client
            .fetch_funding_rates(symbol, 1)
            .await
            .into_iter()
            .next()
        {
            if let Some(conn) = db {
                if let Err(err) = persist_funding_rates(conn, symbol, vec![snapshot.clone()]).await
                {
                    warn!(%symbol, ?err, "failed to persist hourly funding refresh");
                }
            }
            inbound_tx
                .send(Event::FundingSnapshot(snapshot))
                .await
                .map_err(|_| anyhow!("datalayer channel closed"))?;
        }
        match client
            .fetch_orderbook_snapshot(symbol, cfg.depth, Venue::Spot)
            .await
        {
            Ok(snapshot) => {
                let top = snapshot_to_topn(snapshot, cfg.top_n);
                inbound_tx
                    .send(Event::OrderBookTopN(top))
                    .await
                    .map_err(|_| anyhow!("datalayer channel closed"))?;
            }
            Err(err) => {
                warn!(?err, ?symbol, "failed to refresh spot snapshot");
            }
        }
        match client
            .fetch_orderbook_snapshot(symbol, cfg.depth, Venue::Perp)
            .await
        {
            Ok(snapshot) => {
                let top = snapshot_to_topn(snapshot, cfg.top_n);
                inbound_tx
                    .send(Event::OrderBookTopN(top))
                    .await
                    .map_err(|_| anyhow!("datalayer channel closed"))?;
            }
            Err(err) => {
                warn!(?err, ?symbol, "failed to refresh perp snapshot");
            }
        }
    }
    Ok(())
}

fn snapshot_to_topn(snapshot: OrderBookSnapshot, take: usize) -> OrderBookTopN {
    let bids: Vec<BookLevel> = snapshot.bids.into_iter().take(take).collect();
    let asks: Vec<BookLevel> = snapshot.asks.into_iter().take(take).collect();
    let best_bid = bids.first().map(|lvl| lvl.price).unwrap_or(0.0);
    let best_ask = asks.first().map(|lvl| lvl.price).unwrap_or(0.0);
    let mid = if best_bid > 0.0 && best_ask > 0.0 {
        (best_bid + best_ask) / 2.0
    } else {
        0.0
    };
    let spread_bps = if mid > 0.0 {
        ((best_ask - best_bid) / mid) * 10_000.0
    } else {
        0.0
    };
    OrderBookTopN {
        ts_ms: snapshot.ts_ms,
        symbol: snapshot.symbol,
        venue: snapshot.venue,
        update_id: snapshot.last_update_id,
        bids,
        asks,
        best_bid,
        best_ask,
        mid,
        spread_bps,
    }
}
