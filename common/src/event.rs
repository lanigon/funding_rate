use std::collections::HashMap;

use serde::{Deserialize, Serialize};

use crate::{
    account::AccountSnapshot, order::StrategyTargetState, orderbook::BookState, price::PriceCandle,
    OrderBookTopN, OrderFilled, OrderGroupCreated, OrderRequested, Symbol, TargetPosition, Venue,
};

/// Funding point distributed by connector (mocked snapshot per venue).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FundingSnapshot {
    pub ts_ms: i64,
    pub symbol: Symbol,
    pub funding_rate: f64,
}

/// Mock historical funding sample used by research scoring.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct HistoricalFundingPoint {
    pub ts_ms: i64,
    pub funding_rate: f64,
}

/// Aggregate liquidity proxy used for research scoring + metrics.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct LiquiditySample {
    pub ts_ms: i64,
    pub symbol: Symbol,
    pub venue: Venue,
    pub total_depth: f64,
}

/// Mark price update streamed from venues.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarkPriceUpdate {
    pub ts_ms: i64,
    pub symbol: Symbol,
    pub mark_price: f64,
}

/// Latest borrow interest rate snapshot for a margin asset.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct MarginRateSnapshot {
    pub ts_ms: i64,
    pub asset: String,
    pub interest_rate: f64,
}

/// Funding income reported by the venue (positive means received).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct FundingIncome {
    pub ts_ms: i64,
    pub symbol: Symbol,
    pub amount: f64,
}

/// Hourly price window snapshot propagated from engine to strategy.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PriceWindow {
    pub symbol: Symbol,
    pub venue: Venue,
    pub candles: Vec<PriceCandle>,
}

/// Detailed funding state tracked per symbol.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct FundingStateSnapshot {
    pub latest_rate: f64,
    pub mark_price: Option<f64>,
    pub next_rebalance_ms: Option<i64>,
    pub next_settlement_ms: Option<i64>,
    pub fee_cum_usd: f64,
    pub history: Vec<FundingSnapshot>,
}

/// Misc counters surfaced through metrics/state snapshots.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct StateCounters {
    pub order_count_total: u64,
    pub fee_cum: f64,
    pub last_equity_estimate: f64,
    pub funding_income_total: f64,
}

/// Snapshot of the shared in-memory state stored by datalayer.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct SharedStateSnapshot {
    pub books: HashMap<(Symbol, Venue), BookState>,
    pub last_funding: HashMap<Symbol, f64>,
    pub funding_details: HashMap<Symbol, FundingStateSnapshot>,
    pub margin_rates: HashMap<String, f64>,
    pub last_target: HashMap<Symbol, StrategyTargetState>,
    pub accounts: HashMap<String, AccountSnapshot>,
    pub counters: StateCounters,
    pub funding_income: HashMap<Symbol, f64>,
}

/// Central event enum routed across modules.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub enum Event {
    OrderBookTopN(OrderBookTopN),
    FundingSnapshot(FundingSnapshot),
    MarkPrice(MarkPriceUpdate),
    MarginRate(MarginRateSnapshot),
    FundingIncome(FundingIncome),
    PriceWindow(PriceWindow),
    AccountSnapshot(AccountSnapshot),
    TargetPosition(TargetPosition),
    OrderGroupCreated(OrderGroupCreated),
    OrderRequested(OrderRequested),
    OrderFilled(OrderFilled),
}
