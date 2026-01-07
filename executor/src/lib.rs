mod binance_trader;

use std::time::Duration;

use anyhow::Result;
use common::{Event, OrderBookSnapshot, OrderFilled, OrderGroupCreated, OrderRequested, TargetPosition, Venue};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::sleep;
use tracing::warn;
use uuid::Uuid;

pub use binance_trader::BinanceTrader;

#[derive(Clone)]
pub struct ExecutionRecord {
    pub requested: OrderRequested,
    pub filled: OrderFilled,
}

#[derive(Clone)]
pub struct ExecutionBackend {
    trader: BinanceTrader,
}

impl ExecutionBackend {
    pub fn binance(trader: BinanceTrader) -> Self {
        ExecutionBackend { trader }
    }

    async fn record_group(&self, group: &OrderGroupCreated) -> Result<()> {
        self.trader.record_group(group).await
    }

    async fn execute_leg(
        &self,
        symbol: String,
        venue: Venue,
        qty: f64,
        price_hint: f64,
        target: &TargetPosition,
        group_id: Uuid,
    ) -> Result<ExecutionRecord> {
        self.trader
            .execute_leg(&symbol, venue, qty, price_hint, target, group_id)
            .await
    }

    async fn fetch_orderbook(&self, symbol: &str, venue: Venue, depth: usize) -> Option<OrderBookSnapshot> {
        self.trader.fetch_orderbook_snapshot(symbol, venue, depth).await
    }
}

pub fn spawn(
    mut executor_rx: mpsc::Receiver<Event>,
    datalayer_tx: mpsc::Sender<Event>,
    backend: ExecutionBackend,
) -> JoinHandle<Result<()>> {
    let worker_backend = backend.clone();
    tokio::spawn(async move {
        while let Some(event) = executor_rx.recv().await {
            if let Event::TargetPosition(target) = event {
                let exec_tx = datalayer_tx.clone();
                let backend = worker_backend.clone();
                tokio::spawn(async move {
                    if let Err(err) = handle_target(target, exec_tx, backend).await {
                        warn!("executor failed to process target: {err:?}");
                    }
                });
            }
        }
        Ok(())
    })
}

async fn handle_target(
    target: TargetPosition,
    datalayer_tx: mpsc::Sender<Event>,
    backend: ExecutionBackend,
) -> Result<()> {
    let group_id = Uuid::new_v4();
    let group_event = OrderGroupCreated {
        ts_ms: target.ts_ms,
        group_id,
        symbol: target.symbol.clone(),
        target_notional_usd: target.target_notional_usd,
        funding_rate_at_signal: target.funding_rate_at_signal,
        basis_bps_at_signal: target.basis_bps_at_signal,
        status: "created".to_string(),
    };
    datalayer_tx
        .send(Event::OrderGroupCreated(group_event.clone()))
        .await?;
    backend.record_group(&group_event).await?;

    let (spot_qty, spot_price) = quote_from_book(
        &backend,
        &target.symbol,
        Venue::Spot,
        target.spot_qty,
        target.target_notional_usd,
    )
    .await;
    let (perp_qty, perp_price) = quote_from_book(
        &backend,
        &target.symbol,
        Venue::Perp,
        target.perp_qty,
        target.target_notional_usd,
    )
    .await;

    for (venue, qty, price_hint) in [
        (Venue::Spot, spot_qty, spot_price),
        (Venue::Perp, perp_qty, perp_price),
    ] {
        if qty.abs() < 1e-9 {
            continue;
        }
        if price_hint <= 0.0 {
            warn!(?venue, qty, "skip order leg with invalid price hint");
            continue;
        }
        let record = backend
            .execute_leg(
                target.symbol.clone(),
                venue,
                qty,
                price_hint,
                &target,
                group_id,
            )
            .await?;
        datalayer_tx
            .send(Event::OrderRequested(record.requested.clone()))
            .await?;
        sleep(Duration::from_millis(50)).await;
        datalayer_tx
            .send(Event::OrderFilled(record.filled.clone()))
            .await?;
    }

    Ok(())
}

fn adjust_qty(raw_qty: f64, target_notional_usd: f64, price: f64) -> f64 {
    if raw_qty.abs() < 1e-9 {
        return 0.0;
    }
    if price <= 0.0 || target_notional_usd <= 0.0 {
        return raw_qty;
    }
    raw_qty.signum() * (target_notional_usd / price)
}

async fn quote_from_book(
    backend: &ExecutionBackend,
    symbol: &str,
    venue: Venue,
    raw_qty: f64,
    target_notional_usd: f64,
) -> (f64, f64) {
    if raw_qty.abs() < 1e-9 {
        return (0.0, 0.0);
    }
    if let Some(snapshot) = backend.fetch_orderbook(symbol, venue, 20).await {
        let is_buy = raw_qty > 0.0;
        if let Some((price, qty, exhausted)) =
            vwap_quote(&snapshot, is_buy, target_notional_usd)
        {
            if exhausted {
                warn!(
                    symbol,
                    ?venue,
                    target_notional_usd,
                    "orderbook depth exhausted when quoting"
                );
            }
            return (raw_qty.signum() * qty, price);
        }
    }
    let fallback_price = if target_notional_usd > 0.0 {
        target_notional_usd / raw_qty.abs()
    } else {
        0.0
    };
    (adjust_qty(raw_qty, target_notional_usd, fallback_price), fallback_price)
}

fn vwap_quote(
    snapshot: &OrderBookSnapshot,
    is_buy: bool,
    target_notional_usd: f64,
) -> Option<(f64, f64, bool)> {
    if target_notional_usd <= 0.0 {
        return None;
    }
    let levels = if is_buy {
        &snapshot.asks
    } else {
        &snapshot.bids
    };
    if levels.is_empty() {
        return None;
    }
    let mut remaining = target_notional_usd;
    let mut qty = 0.0;
    let mut notional_used = 0.0;
    for level in levels {
        if remaining <= 0.0 {
            break;
        }
        if level.price <= 0.0 || level.size <= 0.0 {
            continue;
        }
        let level_notional = level.price * level.size;
        let take_notional = remaining.min(level_notional);
        let take_qty = take_notional / level.price;
        qty += take_qty;
        notional_used += take_notional;
        remaining -= take_notional;
    }
    if qty <= 0.0 {
        return None;
    }
    let mut depth_exhausted = false;
    if remaining > 0.0 {
        let mut last_price = 0.0;
        for level in levels.iter().rev() {
            if level.price > 0.0 {
                last_price = level.price;
                break;
            }
        }
        if last_price > 0.0 {
            qty += remaining / last_price;
            notional_used += remaining;
            remaining = 0.0;
            depth_exhausted = true;
        }
    }
    if qty <= 0.0 {
        return None;
    }
    Some((notional_used / qty, qty, depth_exhausted))
}
