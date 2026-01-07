use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use common::{
    now_ms, AccountSnapshot, Event, FundingSnapshot, Level, MarkPriceUpdate, OrderBookTopN,
    PositionSnapshot, Symbol, TargetPosition, Venue,
};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{interval, Duration};
use tracing::{info, warn};

use crate::{
    rest::SpotEquityBreakdown, ConnectorConfig, KlinePoint, PositionRisk, RestClient,
};

const FUNDING_WINDOW_CAPACITY: usize = 256;
const KLINE_WINDOW_CAPACITY: usize = 512;
const KLINE_REFRESH_SECS: u64 = 60;
const ACCOUNT_POLL_INTERVAL_SECS: u64 = 5;
const LIQUIDATION_THRESHOLD: f64 = 0.02;
const INCOME_POLL_INTERVAL_SECS: u64 = 5;
const INCOME_FETCH_LIMIT: usize = 100;
const INCOME_START_LOOKBACK_MS: i64 = 3 * 24 * 60 * 60 * 1000;
const PRICE_SPREAD_BPS: f64 = 1.0;

type FundingWindowMap = Arc<Mutex<HashMap<Symbol, DataWindow<FundingSnapshot>>>>;
type KlineWindowMap = Arc<Mutex<HashMap<Symbol, DataWindow<KlinePoint>>>>;
type SharedRestClient = Arc<Mutex<RestClient>>;

/// REST-only Binance feed that polls account state and maintains sliding funding/price windows.
pub struct BinanceFeed {
    cfg: ConnectorConfig,
    inbound_tx: mpsc::Sender<Event>,
    rest_client: SharedRestClient,
    funding_windows: FundingWindowMap,
    kline_windows: KlineWindowMap,
}

impl BinanceFeed {
    pub fn new(cfg: ConnectorConfig, inbound_tx: mpsc::Sender<Event>) -> Self {
        let rest_client = Arc::new(Mutex::new(RestClient::from_config(&cfg)));
        Self {
            cfg,
            inbound_tx,
            rest_client,
            funding_windows: Arc::new(Mutex::new(HashMap::new())),
            kline_windows: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub fn handle(&self) -> BinanceFeedHandle {
        BinanceFeedHandle {
            funding_windows: self.funding_windows.clone(),
            kline_windows: self.kline_windows.clone(),
        }
    }

    pub async fn run(self) {
        let rest_for_positions = self.rest_client.clone();
        let rest_for_income = self.rest_client.clone();
        let rest_for_funding = self.rest_client.clone();
        let rest_for_klines = self.rest_client.clone();
        let funding_windows = self.funding_windows.clone();
        let kline_windows = self.kline_windows.clone();
        let cfg_for_funding = self.cfg.clone();
        let cfg_for_klines = self.cfg.clone();
        let tx_for_positions = self.inbound_tx.clone();
        let tx_for_funding = self.inbound_tx.clone();
        let tx_for_income = self.inbound_tx.clone();

        tokio::join!(
            Self::poll_account_positions(rest_for_positions, tx_for_positions),
            Self::poll_funding_income(rest_for_income, tx_for_income),
            Self::run_funding_windows(
                cfg_for_funding,
                rest_for_funding,
                tx_for_funding,
                funding_windows,
            ),
            Self::run_kline_windows(cfg_for_klines, rest_for_klines, kline_windows),
        );
    }

    async fn poll_account_positions(
        rest_client: SharedRestClient,
        inbound_tx: mpsc::Sender<Event>,
    ) {
        let mut ticker = interval(Duration::from_secs(ACCOUNT_POLL_INTERVAL_SECS));
        loop {
            ticker.tick().await;
            let (positions, futures_balances, spot_balances, spot_equity_usdt, spot_non_usdt_usd) =
                {
                let mut client = rest_client.lock().await;
                let positions = client.fetch_position_risk().await;
                let futures_balances = client.fetch_futures_balances().await;
                let spot_balances = client.fetch_spot_balances().await;
                let breakdown = match spot_balances.as_ref() {
                    Some(snapshot) => {
                        client.spot_equity_breakdown_from_balances(snapshot).await
                    }
                    None => SpotEquityBreakdown::default(),
                };
                (
                    positions,
                    futures_balances,
                    spot_balances,
                    breakdown.total_usdt,
                    breakdown.non_usdt_usdt,
                )
            };
            let (futures_total, futures_available) = futures_balances
                .map(|balances| (balances.total_margin_balance, balances.available_balance))
                .unwrap_or((0.0, 0.0));
            let spot_usdt_free = spot_balances
                .as_ref()
                .map(|balances| balances.usdt_free)
                .unwrap_or(0.0);
            let has_balance = futures_balances.is_some() || spot_balances.is_some();
            if let Some(snapshot) =
                Self::build_account_snapshot(
                    &positions,
                    futures_total,
                    futures_available,
                    spot_equity_usdt,
                    spot_non_usdt_usd,
                    spot_usdt_free,
                    has_balance,
                )
            {
                if inbound_tx
                    .send(Event::AccountSnapshot(snapshot))
                    .await
                    .is_err()
                {
                    return;
                }
            }
            if !positions.is_empty() {
                if Self::emit_perp_prices(&positions, &inbound_tx).await {
                    return;
                }
                if Self::emit_liquidation_signals(&positions, &inbound_tx).await {
                    return;
                }
            }
        }
    }

    fn build_account_snapshot(
        positions: &[PositionRisk],
        futures_total: f64,
        futures_available: f64,
        spot_equity_usdt: f64,
        spot_non_usdt_usd: f64,
        spot_usdt_free: f64,
        has_balance: bool,
    ) -> Option<AccountSnapshot> {
        let total_equity = futures_total + spot_equity_usdt;
        let mut legs = Vec::new();
        for risk in positions {
            if risk.position_amt.abs() < 1e-9 {
                continue;
            }
            legs.push(PositionSnapshot {
                symbol: risk.symbol.clone(),
                spot_qty: 0.0,
                perp_qty: risk.position_amt,
                liquidation_price: if risk.liquidation_price > 0.0 {
                    Some(risk.liquidation_price)
                } else {
                    None
                },
            });
        }
        if legs.is_empty() && total_equity == 0.0 && !has_balance {
            return None;
        }
        Some(AccountSnapshot {
            account_id: "binance-rest".to_string(),
            equity_usd: total_equity,
            spot_balance_usd: spot_equity_usdt,
            spot_non_usdt_usd,
            spot_available_usd: spot_usdt_free,
            perp_balance_usd: futures_total,
            perp_available_usd: futures_available,
            maintenance_margin_ratio: 0.05,
            positions: legs,
            updated_ms: now_ms(),
        })
    }

    async fn emit_liquidation_signals(
        positions: &[PositionRisk],
        inbound_tx: &mpsc::Sender<Event>,
    ) -> bool {
        for risk in positions {
            if risk.position_amt.abs() < 1e-9 {
                continue;
            }
            if risk.mark_price <= 0.0 || risk.liquidation_price <= 0.0 {
                continue;
            }
            let diff_ratio =
                ((risk.mark_price - risk.liquidation_price).abs() / risk.mark_price).max(0.0);
            if diff_ratio <= LIQUIDATION_THRESHOLD {
                let target = TargetPosition {
                    ts_ms: now_ms(),
                    symbol: risk.symbol.clone(),
                    target_notional_usd: 0.0,
                    spot_qty: 0.0,
                    perp_qty: 0.0,
                    funding_rate_at_signal: 0.0,
                    basis_bps_at_signal: 0.0,
                    leverage: 0.0,
                    repay_margin: false,
                };
                if inbound_tx
                    .send(Event::TargetPosition(target))
                    .await
                    .is_err()
                {
                    return true;
                }
            }
        }
        false
    }

    async fn emit_perp_prices(
        positions: &[PositionRisk],
        inbound_tx: &mpsc::Sender<Event>,
    ) -> bool {
        let ts_ms = now_ms();
        for risk in positions {
            if risk.mark_price <= 0.0 {
                continue;
            }
            let mark = MarkPriceUpdate {
                ts_ms,
                symbol: risk.symbol.clone(),
                mark_price: risk.mark_price,
            };
            if inbound_tx.send(Event::MarkPrice(mark)).await.is_err() {
                return true;
            }
            let top = Self::build_top_of_book(&risk.symbol, Venue::Perp, risk.mark_price, ts_ms);
            if inbound_tx.send(Event::OrderBookTopN(top)).await.is_err() {
                return true;
            }
        }
        false
    }

    fn build_top_of_book(symbol: &str, venue: Venue, mid: f64, ts_ms: i64) -> OrderBookTopN {
        let spread = (mid * (PRICE_SPREAD_BPS / 10_000.0)).max(1e-6);
        let best_bid = (mid - spread / 2.0).max(0.0);
        let best_ask = mid + spread / 2.0;
        let bid_level = Level::new(best_bid, 1.0);
        let ask_level = Level::new(best_ask, 1.0);
        OrderBookTopN {
            ts_ms,
            symbol: symbol.to_string(),
            venue,
            update_id: ts_ms,
            bids: vec![bid_level.clone()],
            asks: vec![ask_level.clone()],
            best_bid,
            best_ask,
            mid,
            spread_bps: PRICE_SPREAD_BPS,
        }
    }

    async fn poll_funding_income(rest_client: SharedRestClient, inbound_tx: mpsc::Sender<Event>) {
        let mut ticker = interval(Duration::from_secs(INCOME_POLL_INTERVAL_SECS));
        let mut cursor = now_ms() - (INCOME_START_LOOKBACK_MS + 1);
        if cursor < 0 {
            cursor = 0;
        }
        loop {
            ticker.tick().await;
            let incomes = {
                let mut client = rest_client.lock().await;
                let start_time = if cursor > 0 { Some(cursor + 1) } else { None };
                client
                    .fetch_income_history(start_time, INCOME_FETCH_LIMIT, Some("FUNDING_FEE"))
                    .await
            };
            if incomes.is_empty() {
                continue;
            }
            let mut total_amount = 0.0;
            let mut min_ts: Option<i64> = None;
            let mut max_ts: Option<i64> = None;
            for income in &incomes {
                total_amount += income.amount;
                min_ts = Some(match min_ts {
                    Some(ts) => ts.min(income.ts_ms),
                    None => income.ts_ms,
                });
                max_ts = Some(match max_ts {
                    Some(ts) => ts.max(income.ts_ms),
                    None => income.ts_ms,
                });
            }
            info!(
                count = incomes.len(),
                total_amount,
                start_ts = min_ts.unwrap_or(0),
                end_ts = max_ts.unwrap_or(0),
                "fetched funding income"
            );
            for income in incomes {
                cursor = cursor.max(income.ts_ms);
                if inbound_tx.send(Event::FundingIncome(income)).await.is_err() {
                    return;
                }
            }
        }
    }

    async fn run_funding_windows(
        cfg: ConnectorConfig,
        rest_client: SharedRestClient,
        inbound_tx: mpsc::Sender<Event>,
        windows: FundingWindowMap,
    ) {
        if !Self::refresh_funding_windows(&cfg, rest_client.clone(), &inbound_tx, &windows).await {
            return;
        }
        let interval_secs = cfg.funding_interval_secs.max(1);
        let mut ticker = interval(Duration::from_secs(interval_secs));
        loop {
            ticker.tick().await;
            if !Self::refresh_funding_windows(&cfg, rest_client.clone(), &inbound_tx, &windows)
                .await
            {
                return;
            }
        }
    }

    async fn refresh_funding_windows(
        cfg: &ConnectorConfig,
        rest_client: SharedRestClient,
        inbound_tx: &mpsc::Sender<Event>,
        windows: &FundingWindowMap,
    ) -> bool {
        for symbol in &cfg.symbols {
            let snapshots = {
                let mut client = rest_client.lock().await;
                client
                    .fetch_funding_rates(symbol, FUNDING_WINDOW_CAPACITY)
                    .await
            };
            if snapshots.is_empty() {
                continue;
            }
            {
                let mut guard = windows.lock().await;
                let window = guard
                    .entry(symbol.clone())
                    .or_insert_with(|| DataWindow::new(FUNDING_WINDOW_CAPACITY));
                window.replace_with(snapshots.clone());
            }
            if let Some(latest) = snapshots.last().cloned() {
                if inbound_tx
                    .send(Event::FundingSnapshot(latest))
                    .await
                    .is_err()
                {
                    warn!("binance funding loop terminated");
                    return false;
                }
            }
        }
        true
    }

    async fn run_kline_windows(
        cfg: ConnectorConfig,
        rest_client: SharedRestClient,
        windows: KlineWindowMap,
    ) {
        Self::refresh_kline_windows(&cfg, rest_client.clone(), &windows).await;
        let mut ticker = interval(Duration::from_secs(KLINE_REFRESH_SECS.max(1)));
        loop {
            ticker.tick().await;
            Self::refresh_kline_windows(&cfg, rest_client.clone(), &windows).await;
        }
    }

    async fn refresh_kline_windows(
        cfg: &ConnectorConfig,
        rest_client: SharedRestClient,
        windows: &KlineWindowMap,
    ) {
        for symbol in &cfg.symbols {
            let klines = {
                let mut client = rest_client.lock().await;
                client
                    .fetch_recent_klines(symbol, KLINE_WINDOW_CAPACITY)
                    .await
            };
            if klines.is_empty() {
                continue;
            }
            let mut guard = windows.lock().await;
            let window = guard
                .entry(symbol.clone())
                .or_insert_with(|| DataWindow::new(KLINE_WINDOW_CAPACITY));
            window.replace_with(klines);
        }
    }
}

#[derive(Clone)]
pub struct BinanceFeedHandle {
    funding_windows: FundingWindowMap,
    kline_windows: KlineWindowMap,
}

impl BinanceFeedHandle {
    pub async fn funding_window(&self, symbol: &str) -> Vec<FundingSnapshot> {
        let guard = self.funding_windows.lock().await;
        guard
            .get(symbol)
            .map(|window| window.snapshot())
            .unwrap_or_default()
    }

    pub async fn kline_window(&self, symbol: &str) -> Vec<KlinePoint> {
        let guard = self.kline_windows.lock().await;
        guard
            .get(symbol)
            .map(|window| window.snapshot())
            .unwrap_or_default()
    }
}

struct DataWindow<T> {
    max_len: usize,
    entries: VecDeque<T>,
}

impl<T> DataWindow<T> {
    fn new(max_len: usize) -> Self {
        Self {
            max_len,
            entries: VecDeque::with_capacity(max_len),
        }
    }

    fn replace_with<I>(&mut self, values: I)
    where
        I: IntoIterator<Item = T>,
    {
        self.entries.clear();
        for value in values {
            self.push(value);
        }
    }

    fn push(&mut self, value: T) {
        if self.entries.len() == self.max_len {
            self.entries.pop_front();
        }
        self.entries.push_back(value);
    }
}

impl<T: Clone> DataWindow<T> {
    fn snapshot(&self) -> Vec<T> {
        self.entries.iter().cloned().collect()
    }
}
