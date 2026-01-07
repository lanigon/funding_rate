use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::{Symbol, Venue};

/// Target position signal produced by strategy after reading shared state.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct TargetPosition {
    pub ts_ms: i64,
    pub symbol: Symbol,
    pub target_notional_usd: f64,
    pub spot_qty: f64,
    pub perp_qty: f64,
    pub funding_rate_at_signal: f64,
    pub basis_bps_at_signal: f64,
    pub leverage: f64,
    pub repay_margin: bool,
}

/// Event emitted when executor spins up a new hedging group.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderGroupCreated {
    pub ts_ms: i64,
    pub group_id: Uuid,
    pub symbol: Symbol,
    pub target_notional_usd: f64,
    pub funding_rate_at_signal: f64,
    pub basis_bps_at_signal: f64,
    pub status: String,
}

/// Downstream order request metadata (used by store + metrics).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderRequested {
    pub ts_ms: i64,
    pub child_order_id: Uuid,
    pub group_id: Uuid,
    pub symbol: Symbol,
    pub venue: Venue,
    pub side: String,
    pub order_type: String,
    pub price: f64,
    pub qty: f64,
    pub notional: f64,
    pub funding_rate_at_order: f64,
    pub basis_bps_at_order: f64,
    pub fee_estimated: f64,
    pub fee_asset: String,
    pub status: String,
}

/// Final fill info tracked for reconciliation and storage.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderFilled {
    pub ts_ms: i64,
    pub child_order_id: Uuid,
    pub group_id: Uuid,
    pub symbol: Symbol,
    pub venue: Venue,
    pub filled_price: f64,
    pub filled_qty: f64,
    pub fee_paid: f64,
    pub status: String,
}

/// Snapshot of the last target submitted per symbol (shared state helper).
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StrategyTargetState {
    pub spot_qty: f64,
    pub perp_qty: f64,
    pub leverage: f64,
}
