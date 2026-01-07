use std::collections::HashMap;
use std::time::Duration;

use chrono::Utc;
use common::{
    constant::BINANCE_WS_DEPTH_INTERVAL_MS, AccountSnapshot, Event, Level, MarkPriceUpdate,
    OrderBookBuilder, OrderBookDelta, OrderBookSnapshot, PositionSnapshot, Symbol, Venue,
};
use futures_util::{future, SinkExt, StreamExt};
use rand::{rngs::StdRng, Rng, SeedableRng};
use serde::Deserialize;
use serde_json;
use tokio::sync::mpsc;
use tokio::time::{interval, sleep};
use tokio_tungstenite::{connect_async, tungstenite::Message};
use tracing::warn;

use crate::{rest::RestClient, ConnectorConfig};

const WS_RECONNECT_DELAY: Duration = Duration::from_secs(2);

#[derive(Clone)]
pub struct BinanceWebsocketClient {
    cfg: ConnectorConfig,
    inbound_tx: mpsc::Sender<Event>,
}

#[derive(Clone)]
pub struct MockWebsocketClient {
    cfg: ConnectorConfig,
    inbound_tx: mpsc::Sender<Event>,
}

#[derive(Debug, Deserialize)]
struct DepthStreamEnvelope {
    _stream: String,
    data: DepthStreamData,
}

#[derive(Debug, Deserialize)]
struct DepthStreamData {
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "U")]
    first_update_id: i64,
    #[serde(rename = "u")]
    final_update_id: i64,
    #[serde(rename = "pu")]
    _previous_final_update_id: Option<i64>,
    #[serde(rename = "b")]
    bids: Vec<[String; 2]>,
    #[serde(rename = "a")]
    asks: Vec<[String; 2]>,
}

#[derive(Debug, Deserialize)]
struct MarkPriceEnvelope {
    data: MarkPriceData,
}

#[derive(Debug, Deserialize)]
struct MarkPriceData {
    #[serde(rename = "E")]
    event_time: i64,
    #[serde(rename = "s")]
    symbol: String,
    #[serde(rename = "p")]
    mark_price: String,
}

impl BinanceWebsocketClient {
    pub fn new(cfg: ConnectorConfig, inbound_tx: mpsc::Sender<Event>) -> Self {
        Self { cfg, inbound_tx }
    }

    /// Subscribe to Binance depth streams for every configured venue.
    pub async fn subscribe_orderbooks(&self) {
        if self.cfg.symbols.is_empty() {
            return;
        }
        let tasks =
            self.cfg.venues.iter().copied().map(|venue| {
                Self::run_depth_stream(self.cfg.clone(), venue, self.inbound_tx.clone())
            });
        let _ = future::join_all(tasks).await;
    }

    /// Placeholder account subscription that emits synthetic snapshots unless replaced by
    /// a real user data stream integration.
    pub async fn subscribe_account_updates(&self) {
        if self.cfg.credentials.is_none() {
            warn!("binance account stream requires API credentials");
            return;
        }
        let mut ticker = interval(Duration::from_secs(30));
        let mut rng = StdRng::from_entropy();
        loop {
            ticker.tick().await;
            let spot_balance = rng.gen_range(500.0..5_000.0);
            let perp_balance = rng.gen_range(200.0..2_000.0);
            let equity_usd = spot_balance + perp_balance;
            let snapshot = AccountSnapshot {
                account_id: "binance".to_string(),
                equity_usd,
                spot_balance_usd: spot_balance,
                spot_non_usdt_usd: 0.0,
                spot_available_usd: spot_balance,
                perp_balance_usd: perp_balance,
                perp_available_usd: perp_balance,
                maintenance_margin_ratio: 0.05,
                positions: self
                    .cfg
                    .symbols
                    .iter()
                    .map(|symbol| PositionSnapshot {
                        symbol: symbol.clone(),
                        spot_qty: 0.0,
                        perp_qty: 0.0,
                        liquidation_price: None,
                    })
                    .collect(),
                updated_ms: Utc::now().timestamp_millis(),
            };
            if self
                .inbound_tx
                .send(Event::AccountSnapshot(snapshot))
                .await
                .is_err()
            {
                warn!("binance account stream receiver dropped");
                return;
            }
        }
    }

    /// Subscribe to Binance perpetual mark price stream for each configured symbol.
    pub async fn subscribe_mark_prices(&self) {
        if self.cfg.symbols.is_empty() {
            return;
        }
        let streams = self
            .cfg
            .symbols
            .iter()
            .map(|symbol| format!("{}@markPrice@1s", symbol.to_lowercase()))
            .collect::<Vec<_>>()
            .join("/");
        let url = format!(
            "{}?streams={}",
            self.cfg.futures_websocket_endpoint, streams
        );
        loop {
            match connect_async(&url).await {
                Ok((mut ws_stream, _)) => {
                    while let Some(msg) = ws_stream.next().await {
                        match msg {
                            Ok(Message::Text(payload)) => {
                                if let Ok(parsed) =
                                    serde_json::from_str::<MarkPriceEnvelope>(&payload)
                                {
                                    if let Ok(price) = parsed.data.mark_price.parse::<f64>() {
                                        let update = MarkPriceUpdate {
                                            ts_ms: parsed.data.event_time,
                                            symbol: parsed.data.symbol.to_uppercase(),
                                            mark_price: price,
                                        };
                                        if self
                                            .inbound_tx
                                            .send(Event::MarkPrice(update))
                                            .await
                                            .is_err()
                                        {
                                            return;
                                        }
                                    }
                                }
                            }
                            Ok(Message::Binary(bin)) => {
                                if let Ok(payload) = String::from_utf8(bin) {
                                    if let Ok(parsed) =
                                        serde_json::from_str::<MarkPriceEnvelope>(&payload)
                                    {
                                        if let Ok(price) = parsed.data.mark_price.parse::<f64>() {
                                            let update = MarkPriceUpdate {
                                                ts_ms: parsed.data.event_time,
                                                symbol: parsed.data.symbol.to_uppercase(),
                                                mark_price: price,
                                            };
                                            if self
                                                .inbound_tx
                                                .send(Event::MarkPrice(update))
                                                .await
                                                .is_err()
                                            {
                                                return;
                                            }
                                        }
                                    }
                                }
                            }
                            Ok(Message::Close(_)) => break,
                            Err(err) => {
                                warn!(?err, "binance mark price stream error");
                                break;
                            }
                            _ => {}
                        }
                    }
                }
                Err(err) => warn!(?err, "failed to connect binance mark price stream"),
            }
            sleep(WS_RECONNECT_DELAY).await;
        }
    }

    async fn run_depth_stream(cfg: ConnectorConfig, venue: Venue, inbound_tx: mpsc::Sender<Event>) {
        let ws_endpoint = match venue {
            Venue::Spot => cfg.websocket_endpoint.clone(),
            Venue::Perp => cfg.futures_websocket_endpoint.clone(),
        };
        let depth_stream = Self::depth_stream_size(cfg.depth);
        let mut rest_client = RestClient::from_config(&cfg);
        let mut builders = HashMap::new();
        for symbol in &cfg.symbols {
            let key = symbol.to_uppercase();
            builders.insert(
                key,
                OrderBookBuilder::new(symbol.clone(), venue, cfg.price_scale),
            );
        }

        loop {
            Self::bootstrap_books(&cfg, venue, &mut rest_client, &mut builders, &inbound_tx).await;
            let streams = cfg
                .symbols
                .iter()
                .map(|symbol| {
                    format!(
                        "{}@depth{}@{}ms",
                        symbol.to_lowercase(),
                        depth_stream,
                        BINANCE_WS_DEPTH_INTERVAL_MS
                    )
                })
                .collect::<Vec<_>>()
                .join("/");
            let url = format!("{}?streams={}", ws_endpoint, streams);
            match connect_async(&url).await {
                Ok((mut ws_stream, _)) => {
                    while let Some(msg) = ws_stream.next().await {
                        match msg {
                            Ok(Message::Text(payload)) => {
                                Self::handle_depth_payload(
                                    &payload,
                                    venue,
                                    &mut builders,
                                    &cfg,
                                    &inbound_tx,
                                    &mut rest_client,
                                )
                                .await;
                            }
                            Ok(Message::Binary(bin)) => {
                                if let Ok(payload) = String::from_utf8(bin) {
                                    Self::handle_depth_payload(
                                        &payload,
                                        venue,
                                        &mut builders,
                                        &cfg,
                                        &inbound_tx,
                                        &mut rest_client,
                                    )
                                    .await;
                                }
                            }
                            Ok(Message::Ping(frame)) => {
                                let _ = ws_stream.send(Message::Pong(frame)).await;
                            }
                            Ok(Message::Pong(_)) => {}
                            Ok(Message::Frame(_)) => {}
                            Ok(Message::Close(_)) => break,
                            Err(err) => {
                                warn!(?err, ?venue, "binance depth stream error");
                                break;
                            }
                        }
                    }
                }
                Err(err) => {
                    warn!(?err, ?venue, "failed to connect binance depth stream");
                }
            }
            sleep(WS_RECONNECT_DELAY).await;
        }
    }

    async fn bootstrap_books(
        cfg: &ConnectorConfig,
        venue: Venue,
        rest_client: &mut RestClient,
        builders: &mut HashMap<Symbol, OrderBookBuilder>,
        inbound_tx: &mpsc::Sender<Event>,
    ) {
        for symbol in &cfg.symbols {
            let key = symbol.to_uppercase();
            if let Some(builder) = builders.get_mut(&key) {
                match rest_client
                    .fetch_orderbook_snapshot(symbol, cfg.depth, venue)
                    .await
                {
                    Ok(snapshot) => {
                        builder.apply_snapshot(&snapshot);
                        let top = builder.top_n(cfg.top_n, snapshot.ts_ms);
                        if inbound_tx.send(Event::OrderBookTopN(top)).await.is_err() {
                            return;
                        }
                    }
                    Err(_) => {
                        builder.last_update_id = 0;
                    }
                }
            }
        }
    }

    async fn handle_depth_payload(
        payload: &str,
        venue: Venue,
        builders: &mut HashMap<Symbol, OrderBookBuilder>,
        cfg: &ConnectorConfig,
        inbound_tx: &mpsc::Sender<Event>,
        rest_client: &mut RestClient,
    ) {
        let parsed: DepthStreamEnvelope = match serde_json::from_str(payload) {
            Ok(data) => data,
            Err(err) => {
                warn!(?err, "failed to parse binance depth payload");
                return;
            }
        };
        let symbol = parsed.data.symbol.to_uppercase();
        let Some(builder) = builders.get_mut(&symbol) else {
            return;
        };

        if builder.last_update_id == 0 {
            Self::resync_symbol(builder, cfg, venue, rest_client, inbound_tx).await;
            return;
        }
        let expected = builder.last_update_id + 1;
        if parsed.data.final_update_id < expected {
            return;
        }
        if parsed.data.first_update_id > expected {
            warn!(
                symbol,
                ?venue,
                expected,
                "binance depth gap detected, resyncing"
            );
            Self::resync_symbol(builder, cfg, venue, rest_client, inbound_tx).await;
            return;
        }

        let delta = OrderBookDelta {
            ts_ms: parsed.data.event_time,
            symbol: builder.symbol().to_string(),
            venue: builder.venue(),
            first_update_id: parsed.data.first_update_id,
            final_update_id: parsed.data.final_update_id,
            bids: Self::depth_to_levels(parsed.data.bids),
            asks: Self::depth_to_levels(parsed.data.asks),
        };
        builder.apply_delta(&delta);
        let top = builder.top_n(cfg.top_n, delta.ts_ms);
        if inbound_tx.send(Event::OrderBookTopN(top)).await.is_err() {
            return;
        }
    }

    async fn resync_symbol(
        builder: &mut OrderBookBuilder,
        cfg: &ConnectorConfig,
        venue: Venue,
        rest_client: &mut RestClient,
        inbound_tx: &mpsc::Sender<Event>,
    ) {
        match rest_client
            .fetch_orderbook_snapshot(builder.symbol(), cfg.depth, venue)
            .await
        {
            Ok(snapshot) => {
                builder.apply_snapshot(&snapshot);
                let top = builder.top_n(cfg.top_n, snapshot.ts_ms);
                let _ = inbound_tx.send(Event::OrderBookTopN(top)).await;
            }
            Err(_) => {
                builder.last_update_id = 0;
            }
        }
    }

    fn depth_stream_size(depth: usize) -> usize {
        if depth >= 20 {
            20
        } else if depth >= 10 {
            10
        } else {
            5
        }
    }

    fn depth_to_levels(levels: Vec<[String; 2]>) -> Vec<Level> {
        levels
            .into_iter()
            .filter_map(|pair| {
                let [price, qty] = pair;
                let price = price.parse::<f64>().ok()?;
                let qty = qty.parse::<f64>().ok()?;
                Some(Level::new(price, qty))
            })
            .collect()
    }
}

impl MockWebsocketClient {
    pub fn new(cfg: ConnectorConfig, inbound_tx: mpsc::Sender<Event>) -> Self {
        Self { cfg, inbound_tx }
    }

    pub async fn stream_orderbooks(&self) {
        let cfg = self.cfg.clone();
        let inbound_tx = self.inbound_tx.clone();
        let mut builders: HashMap<(Symbol, Venue), OrderBookBuilder> = HashMap::new();
        for symbol in &cfg.symbols {
            for venue in &cfg.venues {
                builders.insert(
                    (symbol.clone(), *venue),
                    OrderBookBuilder::new(symbol.clone(), *venue, cfg.price_scale),
                );
            }
        }

        let mut rng = StdRng::from_entropy();
        for ((symbol, venue), builder) in builders.iter_mut() {
            let snapshot = generate_snapshot(
                symbol.as_str(),
                *venue,
                cfg.depth,
                builder.price_scale(),
                &mut rng,
            );
            builder.apply_snapshot(&snapshot);
            let top = builder.top_n(cfg.top_n, snapshot.ts_ms);
            if let Err(err) = inbound_tx.send(Event::OrderBookTopN(top)).await {
                warn!("websocket initial snapshot send failed: {err}");
            }
        }

        let mut ticker = interval(Duration::from_secs(1));
        loop {
            ticker.tick().await;
            let ts_ms = Utc::now().timestamp_millis();
            for builder in builders.values_mut() {
                let delta = generate_delta(
                    builder.symbol().as_str(),
                    builder.venue(),
                    builder.indicative_mid(),
                    cfg.price_scale,
                    cfg.depth,
                    &mut rng,
                    builder.last_update_id + 1,
                );
                builder.apply_delta(&delta);
                let top = builder.top_n(cfg.top_n, ts_ms);
                if let Err(err) = inbound_tx.send(Event::OrderBookTopN(top)).await {
                    warn!("websocket delta send failed: {err}");
                    return;
                }
            }
        }
    }

    pub async fn stream_account_updates(&self) {
        let mut ticker = interval(Duration::from_secs(10));
        loop {
            ticker.tick().await;
            let snapshot = AccountSnapshot {
                account_id: "mock".to_string(),
                equity_usd: 10_000.0,
                spot_balance_usd: 5_000.0,
                spot_non_usdt_usd: 0.0,
                spot_available_usd: 5_000.0,
                perp_balance_usd: 5_000.0,
                perp_available_usd: 5_000.0,
                maintenance_margin_ratio: 0.01,
                positions: self
                    .cfg
                    .symbols
                    .iter()
                    .map(|symbol| PositionSnapshot {
                        symbol: symbol.clone(),
                        spot_qty: 0.0,
                        perp_qty: 0.0,
                        liquidation_price: None,
                    })
                    .collect(),
                updated_ms: Utc::now().timestamp_millis(),
            };
            if self
                .inbound_tx
                .send(Event::AccountSnapshot(snapshot))
                .await
                .is_err()
            {
                warn!("mock account stream receiver dropped");
                return;
            }
        }
    }

    pub async fn stream_mark_prices(&self) {
        let mut ticker = interval(Duration::from_secs(1));
        let mut rng = StdRng::from_entropy();
        loop {
            ticker.tick().await;
            for symbol in &self.cfg.symbols {
                let base = base_price(symbol, Venue::Perp);
                let price = base * (1.0 + rng.gen_range(-0.003..0.003));
                let update = MarkPriceUpdate {
                    ts_ms: Utc::now().timestamp_millis(),
                    symbol: symbol.clone(),
                    mark_price: price,
                };
                if self
                    .inbound_tx
                    .send(Event::MarkPrice(update))
                    .await
                    .is_err()
                {
                    warn!("mock mark price stream receiver dropped");
                    return;
                }
            }
        }
    }
}

fn generate_snapshot(
    symbol: &str,
    venue: Venue,
    depth: usize,
    price_scale: f64,
    rng: &mut StdRng,
) -> OrderBookSnapshot {
    let mid = base_price(symbol, venue);
    let ts_ms = Utc::now().timestamp_millis();
    let step = (1.0 / price_scale.max(1.0)).max(0.0001);
    let bids = (0..depth)
        .map(|i| {
            let price = mid * (1.0 - (i as f64 + rng.gen_range(0.0..0.2)) * step);
            Level::new(price, rng.gen_range(0.5..5.0))
        })
        .collect::<Vec<_>>();
    let asks = (0..depth)
        .map(|i| {
            let price = mid * (1.0 + (i as f64 + rng.gen_range(0.0..0.2)) * step);
            Level::new(price, rng.gen_range(0.5..5.0))
        })
        .collect::<Vec<_>>();

    OrderBookSnapshot {
        ts_ms,
        symbol: symbol.to_string(),
        venue,
        last_update_id: rng.gen_range(1..1_000_000),
        bids,
        asks,
    }
}

fn generate_delta(
    symbol: &str,
    venue: Venue,
    mid: f64,
    price_scale: f64,
    depth: usize,
    rng: &mut StdRng,
    update_id: i64,
) -> OrderBookDelta {
    let ts_ms = Utc::now().timestamp_millis();
    let max_changes = depth.max(1).min(5);
    let changes = rng.gen_range(1..=max_changes);
    let step = (1.0 / price_scale.max(1.0)).max(0.0001) * 5.0;
    let mut bids = Vec::with_capacity(changes);
    let mut asks = Vec::with_capacity(changes);
    for _ in 0..changes {
        let price = mid * (1.0 - rng.gen_range(0.0..step));
        let size = if rng.gen_bool(0.2) {
            0.0
        } else {
            rng.gen_range(0.2..4.0)
        };
        bids.push(Level::new(price, size));
    }
    for _ in 0..changes {
        let price = mid * (1.0 + rng.gen_range(0.0..step));
        let size = if rng.gen_bool(0.2) {
            0.0
        } else {
            rng.gen_range(0.2..4.0)
        };
        asks.push(Level::new(price, size));
    }

    OrderBookDelta {
        ts_ms,
        symbol: symbol.to_string(),
        venue,
        first_update_id: update_id,
        final_update_id: update_id,
        bids,
        asks,
    }
}

fn base_price(symbol: &str, venue: Venue) -> f64 {
    let base = match symbol {
        "BTCUSDT" => 30_000.0,
        "ETHUSDT" => 2_000.0,
        _ => 10_000.0,
    };
    match venue {
        Venue::Spot => base,
        Venue::Perp => base * 1.001,
    }
}
