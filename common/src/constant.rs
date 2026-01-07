/// Base URL for Binance spot REST endpoints.
pub const BINANCE_SPOT_API_BASE: &str = "https://api.binance.com";
/// Base URL for Binance USDT-margined futures REST endpoints.
pub const BINANCE_FUTURES_API_BASE: &str = "https://fapi.binance.com";
/// Base URL for Binance spot websocket multiplexed streams.
pub const BINANCE_SPOT_WS_BASE: &str = "wss://stream.binance.com:9443/stream";
/// Base URL for Binance futures websocket multiplexed streams.
pub const BINANCE_FUTURES_WS_BASE: &str = "wss://fstream.binance.com/stream";

/// Default websocket depth stream interval in milliseconds.
pub const BINANCE_WS_DEPTH_INTERVAL_MS: u64 = 100;
/// Maximum number of kline points fetched per REST call.
pub const BINANCE_MAX_KLINE_LIMIT: usize = 1000;
/// Default funding interval on Binance futs (8 hours).
pub const BINANCE_DEFAULT_FUNDING_INTERVAL_SECS: u64 = 8 * 60 * 60;

/// Default symbol universe used whenever no research output is found.
pub const DEFAULT_SYMBOLS: [&str; 2] = ["BTCUSDT", "ETHUSDT"];
/// Default connector depth (levels per side) streamed into the strategy.
pub const DEFAULT_CONNECTOR_DEPTH: usize = 20;
/// Default number of top levels shared with the rest of the pipeline.
pub const DEFAULT_CONNECTOR_TOP_N: usize = 10;
/// Price scale used when generating mock order books.
pub const DEFAULT_CONNECTOR_PRICE_SCALE: f64 = 100.0;
/// Mock-mode funding interval (seconds) for the connector REST loop.
pub const DEFAULT_CONNECTOR_FUNDING_INTERVAL_SECS: u64 = 10;
/// Default Binance spot kline interval requested by the connector.
pub const DEFAULT_CONNECTOR_KLINE_INTERVAL: &str = "1m";

/// Default strategy target notional (USD) for each hedged pair.
pub const DEFAULT_STRATEGY_TARGET_NOTIONAL_USD: f64 = 5_000.0;
/// Strategy rebalance threshold before emitting new targets.
pub const DEFAULT_STRATEGY_REBALANCE_THRESHOLD: f64 = 0.05;

/// Default listen window for the interactive `order` helper binary.
pub const DEFAULT_ORDER_ACCOUNT_LISTEN_WINDOW_SECS: u64 = 100;
/// Default USD notional allocated by the interactive `order` binary.
pub const DEFAULT_ORDER_TOTAL_NOTIONAL_USD: f64 = 18.0;
/// Default perp leverage assumed by the interactive `order` binary.
pub const DEFAULT_ORDER_PERP_LEVERAGE: f64 = 9.0;
