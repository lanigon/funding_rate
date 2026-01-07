#[allow(non_snake_case)]
mod binanceFeed;
mod rest;
mod types;
mod utils;
mod websocket;

use common::constant::{
    BINANCE_FUTURES_API_BASE, BINANCE_FUTURES_WS_BASE, BINANCE_SPOT_API_BASE, BINANCE_SPOT_WS_BASE,
    DEFAULT_CONNECTOR_DEPTH, DEFAULT_CONNECTOR_FUNDING_INTERVAL_SECS,
    DEFAULT_CONNECTOR_KLINE_INTERVAL, DEFAULT_CONNECTOR_PRICE_SCALE, DEFAULT_CONNECTOR_TOP_N,
    DEFAULT_SYMBOLS,
};

pub use binanceFeed::{BinanceFeed, BinanceFeedHandle};
pub use rest::{
    fetch_recent_klines, generate_mock_funding_history, generate_mock_liquidity_samples,
    MarginAssetStatus, MarginInterestRecord, PositionRisk, RestClient, RestError,
    SpotEquityBreakdown, TradeFill,
};
pub use types::{
    KlinePoint, LimitOrderParams, MarketOrderParams, OrderRequest, OrderResponse, SymbolFilters,
};
pub use websocket::{BinanceWebsocketClient, MockWebsocketClient};

#[derive(Clone, Debug)]
pub struct BinanceCredentials {
    pub api_key: String,
    pub api_secret: String,
}

#[derive(Clone)]
pub struct ConnectorConfig {
    pub symbols: Vec<String>,
    pub top_n: usize,
    pub depth: usize,
    pub price_scale: f64,
    pub venues: Vec<common::Venue>,
    pub funding_interval_secs: u64,
    pub kline_interval: String,
    pub spot_rest_endpoint: String,
    pub futures_rest_endpoint: String,
    pub websocket_endpoint: String,
    pub futures_websocket_endpoint: String,
    pub user_stream_rest_endpoint: String,
    pub credentials: Option<BinanceCredentials>,
}

impl Default for ConnectorConfig {
    fn default() -> Self {
        Self {
            symbols: DEFAULT_SYMBOLS.iter().map(|s| s.to_string()).collect(),
            top_n: DEFAULT_CONNECTOR_TOP_N,
            depth: DEFAULT_CONNECTOR_DEPTH,
            price_scale: DEFAULT_CONNECTOR_PRICE_SCALE,
            venues: vec![common::Venue::Spot, common::Venue::Perp],
            funding_interval_secs: DEFAULT_CONNECTOR_FUNDING_INTERVAL_SECS,
            kline_interval: DEFAULT_CONNECTOR_KLINE_INTERVAL.to_string(),
            spot_rest_endpoint: BINANCE_SPOT_API_BASE.to_string(),
            futures_rest_endpoint: BINANCE_FUTURES_API_BASE.to_string(),
            websocket_endpoint: BINANCE_SPOT_WS_BASE.to_string(),
            futures_websocket_endpoint: BINANCE_FUTURES_WS_BASE.to_string(),
            user_stream_rest_endpoint: BINANCE_SPOT_API_BASE.to_string(),
            credentials: None,
        }
    }
}

// Connector spawning is now orchestrated directly from the engine crate so it can
// decide which data loops to run. This module exposes run_* helpers via pub use.
