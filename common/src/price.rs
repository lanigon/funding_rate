use serde::{Deserialize, Serialize};

use crate::{Symbol, Venue};

/// Hourly OHLCV candle snapshot shared through events/state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceCandle {
    pub symbol: Symbol,
    pub venue: Venue,
    pub open_time: i64,
    pub close_time: i64,
    pub open: f64,
    pub high: f64,
    pub low: f64,
    pub close: f64,
    pub volume: f64,
}
