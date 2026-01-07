use serde::{Deserialize, Serialize};

use crate::Symbol;

/// Simplified view of an open position across spot/perp legs.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PositionSnapshot {
    /// Trading symbol the position belongs to.
    pub symbol: Symbol,
    /// Current net spot quantity.
    pub spot_qty: f64,
    /// Current net perpetual quantity.
    pub perp_qty: f64,
    /// Current liquidation price for the perp leg (if available).
    #[serde(default)]
    pub liquidation_price: Option<f64>,
}

/// Aggregated account metrics fed into the datalayer for liquidation checks.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct AccountSnapshot {
    /// Unique identifier for the account/feed source.
    pub account_id: String,
    /// Estimated account equity expressed in USD.
    pub equity_usd: f64,
    /// Estimated spot wallet equity (USD notional).
    #[serde(default)]
    pub spot_balance_usd: f64,
    /// Estimated non-USDT spot equity (USD notional).
    #[serde(default)]
    pub spot_non_usdt_usd: f64,
    /// Estimated free USDT balance in the spot wallet (USD notional) for budgeting.
    #[serde(default)]
    pub spot_available_usd: f64,
    /// Estimated futures account equity (margin balance + unrealized PnL).
    #[serde(default)]
    pub perp_balance_usd: f64,
    /// Estimated free balance in the perp/futures wallet (USD notional) for budgeting.
    #[serde(default)]
    pub perp_available_usd: f64,
    /// Maintenance margin ratio threshold enforced by the venue.
    pub maintenance_margin_ratio: f64,
    /// All tracked positions for this account.
    pub positions: Vec<PositionSnapshot>,
    /// Millisecond timestamp of the snapshot.
    pub updated_ms: i64,
}
