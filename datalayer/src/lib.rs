use anyhow::{anyhow, Result};
use common::Event;
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

mod state;

pub use state::{SharedState, State, SymbolMetrics};

const WORKER_COUNT: usize = 4;
const WORKER_CHANNEL_CAPACITY: usize = 512;

pub struct DataLayerHandles {
    pub state: SharedState,
    pub strategy_rx: mpsc::Receiver<Event>,
    pub executor_rx: mpsc::Receiver<Event>,
}

pub fn spawn(mut inbound_rx: mpsc::Receiver<Event>) -> (DataLayerHandles, JoinHandle<Result<()>>) {
    let state = State::shared();
    let (strategy_tx, strategy_rx) = mpsc::channel(2048);
    let (executor_tx, executor_rx) = mpsc::channel(128);
    let state_clone = state.clone();

    let worker_count = WORKER_COUNT.max(1);
    let mut worker_txs = Vec::with_capacity(worker_count);
    let mut worker_handles = Vec::with_capacity(worker_count);
    for worker_id in 0..worker_count {
        let (tx, rx) = mpsc::channel::<Event>(WORKER_CHANNEL_CAPACITY);
        worker_txs.push(tx);
        let worker_state = state.clone();
        let strategy_tx_clone = strategy_tx.clone();
        let executor_tx_clone = executor_tx.clone();
        let handle = tokio::spawn(run_worker(
            worker_id,
            rx,
            worker_state,
            strategy_tx_clone,
            executor_tx_clone,
        ));
        worker_handles.push(handle);
    }

    let handle = tokio::spawn(async move {
        if worker_txs.is_empty() {
            return Err(anyhow!("no datalayer workers configured"));
        }
        let mut idx = 0usize;
        while let Some(event) = inbound_rx.recv().await {
            let target = idx % worker_txs.len();
            if worker_txs[target].send(event).await.is_err() {
                warn!(target, "datalayer worker channel closed");
            }
            idx = idx.wrapping_add(1);
        }
        info!("datalayer inbound channel closed");
        drop(worker_txs);
        for handle in worker_handles {
            if let Err(err) = handle.await {
                warn!("datalayer worker join failed: {err}");
            }
        }
        Ok(())
    });

    (
        DataLayerHandles {
            state: state_clone,
            strategy_rx,
            executor_rx,
        },
        handle,
    )
}

async fn run_worker(
    worker_id: usize,
    mut rx: mpsc::Receiver<Event>,
    state: SharedState,
    strategy_tx: mpsc::Sender<Event>,
    executor_tx: mpsc::Sender<Event>,
) {
    while let Some(event) = rx.recv().await {
        if let Err(err) = handle_event(event, &state, &strategy_tx, &executor_tx).await {
            warn!(worker_id, ?err, "datalayer worker exiting");
            break;
        }
    }
    info!(worker_id, "datalayer worker finished");
}

async fn handle_event(
    event: Event,
    state: &SharedState,
    strategy_tx: &mpsc::Sender<Event>,
    executor_tx: &mpsc::Sender<Event>,
) -> Result<()> {
    match event {
        Event::OrderBookTopN(book) => {
            state.update_book(&book);
            let liquidation_targets = state.liquidation_orders();
            if let Err(err) = strategy_tx.try_send(Event::OrderBookTopN(book)) {
                warn!("drop strategy book event: {err}");
            }
            for target in liquidation_targets {
                executor_tx
                    .send(Event::TargetPosition(target))
                    .await
                    .map_err(|err| anyhow!("executor channel closed (book liquidation): {err}"))?;
            }
        }
        Event::FundingSnapshot(funding) => {
            state.update_funding(funding.clone());
            strategy_tx
                .send(Event::FundingSnapshot(funding))
                .await
                .map_err(|err| anyhow!("strategy channel closed for funding events: {err}"))?;
        }
        Event::AccountSnapshot(account) => {
            state.update_account(account);
            let liquidation_targets = state.liquidation_orders();
            for target in liquidation_targets {
                executor_tx
                    .send(Event::TargetPosition(target))
                    .await
                    .map_err(|err| {
                        anyhow!("executor channel closed (account liquidation): {err}")
                    })?;
            }
        }
        Event::TargetPosition(mut target) => {
            let (delta_spot, delta_perp) = state.update_target(&target);
            if delta_spot.abs() < 1e-9 && delta_perp.abs() < 1e-9 {
                return Ok(());
            }
            let mut notional = target.target_notional_usd.abs();
            if let Some((spot_book, perp_book, _, _, _, _)) = state.symbol_snapshot(&target.symbol)
            {
                if delta_spot.abs() > 1e-9 && spot_book.mid.abs() > 0.0 {
                    notional = delta_spot.abs() * spot_book.mid.abs();
                } else if delta_perp.abs() > 1e-9 && perp_book.mid.abs() > 0.0 {
                    notional = delta_perp.abs() * perp_book.mid.abs();
                }
            }
            target.spot_qty = delta_spot;
            target.perp_qty = delta_perp;
            target.target_notional_usd = notional;
            executor_tx
                .send(Event::TargetPosition(target))
                .await
                .map_err(|err| anyhow!("executor channel closed: {err}"))?;
        }
        Event::MarkPrice(update) => {
            state.update_mark_price(update.clone());
            if let Err(err) = strategy_tx.try_send(Event::MarkPrice(update)) {
                warn!("drop strategy mark price event: {err}");
            }
        }
        Event::FundingIncome(income) => {
            state.record_funding_income(&income);
        }
        Event::PriceWindow(window) => {
            state.update_price_window(window);
        }
        Event::MarginRate(snapshot) => {
            state.update_margin_rate(snapshot);
        }
        Event::OrderGroupCreated(_) => {
            // Order group lifecycle is persisted by the store pipeline.
        }
        Event::OrderRequested(request) => {
            state.record_order_requested(&request);
        }
        Event::OrderFilled(fill) => {
            state.record_order_filled(&fill);
        }
    }
    Ok(())
}
