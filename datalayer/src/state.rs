use std::collections::{HashMap, VecDeque};
use std::f64;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

use common::{
    now_ms, AccountSnapshot, BookState, FundingIncome, FundingSnapshot, FundingStateSnapshot,
    MarginRateSnapshot, MarkPriceUpdate, OrderBookTopN, OrderFilled, OrderRequested,
    PositionSnapshot, PriceCandle, PriceWindow, SharedStateSnapshot, StateCounters,
    StrategyTargetState, Symbol, TargetPosition, Venue,
};
use scc::hash_map::Entry;
use scc::HashMap as ConcurrentHashMap;

#[derive(Clone, Debug)]
struct AccountState {
    snapshot: AccountSnapshot,
    pending_liquidation: bool,
    last_liquidation_ms: Option<i64>,
}

#[derive(Clone, Debug, Default)]
struct FundingHistory {
    latest: Option<FundingSnapshot>,
    next_rebalance_ms: Option<i64>,
    next_settlement_ms: Option<i64>,
    fee_cum_usd: f64,
    history: VecDeque<FundingSnapshot>,
}

impl FundingHistory {
    fn record(&mut self, snapshot: FundingSnapshot) {
        let prev_ts = self.latest.as_ref().map(|last| last.ts_ms);
        let ts_ms = snapshot.ts_ms;
        self.latest = Some(snapshot.clone());
        self.history.push_back(snapshot);
        while self.history.len() > 256 {
            self.history.pop_front();
        }
        let interval = prev_ts
            .and_then(|prev| {
                let diff = (ts_ms - prev).abs();
                if diff > 0 {
                    Some(diff)
                } else {
                    None
                }
            })
            .unwrap_or(DEFAULT_SETTLEMENT_INTERVAL_MS);
        let next_settlement = ts_ms + interval;
        self.next_settlement_ms = Some(next_settlement);
        self.next_rebalance_ms = Some(next_settlement - 10 * 60 * 1000);
    }

    fn add_fee(&mut self, fee: f64) {
        self.fee_cum_usd += fee;
    }
}

#[derive(Default)]
struct AtomicCounters {
    order_count_total: AtomicU64,
    fee_cum_bits: AtomicU64,
    last_equity_bits: AtomicU64,
    funding_income_bits: AtomicU64,
}

impl AtomicCounters {
    fn add_orders(&self, delta: u64) -> u64 {
        if delta == 0 {
            return self.order_count_total.load(Ordering::Relaxed);
        }
        self.order_count_total.fetch_add(delta, Ordering::Relaxed) + delta
    }

    fn add_fee(&self, fee: f64) -> f64 {
        self.update_f64(&self.fee_cum_bits, |current| current + fee)
    }

    fn set_last_equity(&self, equity: f64) {
        self.last_equity_bits
            .store(equity.to_bits(), Ordering::Relaxed);
    }

    fn set_orders(&self, value: u64) {
        self.order_count_total.store(value, Ordering::Relaxed);
    }

    fn set_fee_cum(&self, fee: f64) {
        self.fee_cum_bits.store(fee.to_bits(), Ordering::Relaxed);
    }

    fn add_funding_income(&self, income: f64) {
        self.update_f64(&self.funding_income_bits, |current| current + income);
    }

    fn snapshot(&self) -> StateCounters {
        StateCounters {
            order_count_total: self.order_count_total.load(Ordering::Relaxed),
            fee_cum: f64::from_bits(self.fee_cum_bits.load(Ordering::Relaxed)),
            last_equity_estimate: f64::from_bits(self.last_equity_bits.load(Ordering::Relaxed)),
            funding_income_total: f64::from_bits(self.funding_income_bits.load(Ordering::Relaxed)),
        }
    }

    fn update_f64(&self, cell: &AtomicU64, compute: impl Fn(f64) -> f64) -> f64 {
        let mut current = cell.load(Ordering::Relaxed);
        loop {
            let next_value = compute(f64::from_bits(current));
            let next_bits = next_value.to_bits();
            match cell.compare_exchange(current, next_bits, Ordering::Relaxed, Ordering::Relaxed) {
                Ok(_) => return next_value,
                Err(prev) => current = prev,
            }
        }
    }
}

#[derive(Default)]
pub struct State {
    books: ConcurrentHashMap<(Symbol, Venue), BookState>,
    funding: ConcurrentHashMap<Symbol, FundingHistory>,
    margin_rates: ConcurrentHashMap<String, f64>,
    mark_prices: ConcurrentHashMap<Symbol, f64>,
    liquidation_prices: ConcurrentHashMap<Symbol, f64>,
    metrics: ConcurrentHashMap<Symbol, MetricWindow>,
    last_target: ConcurrentHashMap<Symbol, StrategyTargetState>,
    accounts: ConcurrentHashMap<String, AccountState>,
    funding_income: ConcurrentHashMap<Symbol, f64>,
    price_windows: ConcurrentHashMap<(Symbol, Venue), Vec<PriceCandle>>,
    counters: AtomicCounters,
    spot_cash_bits: AtomicU64,
    perp_cash_bits: AtomicU64,
}

pub type SharedState = Arc<State>;

impl State {
    pub fn shared() -> SharedState {
        Arc::new(Self::default())
    }

    pub fn update_book(&self, book: &OrderBookTopN) {
        let state = BookState {
            symbol: book.symbol.clone(),
            venue: book.venue,
            ts_ms: book.ts_ms,
            bids: book.bids.clone(),
            asks: book.asks.clone(),
            best_bid: book.best_bid,
            best_ask: book.best_ask,
            mid: book.mid,
            spread_bps: book.spread_bps,
        };
        let key = (book.symbol.clone(), book.venue);
        self.books.upsert(key, state);
        self.metrics
            .entry(book.symbol.clone())
            .and_modify(|window| window.record_price(book.ts_ms, book.mid))
            .or_insert_with(|| {
                let mut window = MetricWindow::default();
                window.record_price(book.ts_ms, book.mid);
                window
            });
    }

    pub fn update_funding(&self, funding: FundingSnapshot) {
        self.funding
            .entry(funding.symbol.clone())
            .and_modify(|hist| hist.record(funding.clone()))
            .or_insert_with(|| {
                let mut hist = FundingHistory::default();
                hist.record(funding.clone());
                hist
            });
        self.metrics
            .entry(funding.symbol.clone())
            .and_modify(|window| window.record_funding(funding.ts_ms, funding.funding_rate))
            .or_insert_with(|| {
                let mut window = MetricWindow::default();
                window.record_funding(funding.ts_ms, funding.funding_rate);
                window
            });
    }

    pub fn update_mark_price(&self, update: MarkPriceUpdate) {
        self.mark_prices
            .upsert(update.symbol.clone(), update.mark_price);
    }

    pub fn update_target(&self, target: &TargetPosition) -> (f64, f64) {
        let delta = self
            .last_target
            .read(&target.symbol, |_, last| {
                (
                    target.spot_qty - last.spot_qty,
                    target.perp_qty - last.perp_qty,
                )
            })
            .unwrap_or((target.spot_qty, target.perp_qty));
        self.last_target.upsert(
            target.symbol.clone(),
            StrategyTargetState {
                spot_qty: target.spot_qty,
                perp_qty: target.perp_qty,
                leverage: target.leverage,
            },
        );
        delta
    }

    pub fn update_price_window(&self, window: PriceWindow) {
        self.price_windows
            .upsert((window.symbol, window.venue), window.candles);
    }

    pub fn price_window(&self, symbol: &str, venue: Venue) -> Option<Vec<PriceCandle>> {
        let key = (symbol.to_string(), venue);
        self.price_windows.read(&key, |_, candles| candles.clone())
    }

    pub fn update_account(&self, snapshot: AccountSnapshot) {
        let account_id = snapshot.account_id.clone();
        let equity = snapshot.equity_usd;
        for position in &snapshot.positions {
            if let Some(liq) = position.liquidation_price {
                if liq > 0.0 {
                    self.liquidation_prices.upsert(position.symbol.clone(), liq);
                }
            }
        }
        tracing::info!(
            account_id = account_id,
            equity_usd = snapshot.equity_usd,
            spot_balance_usd = snapshot.spot_balance_usd,
            spot_available_usd = snapshot.spot_available_usd,
            perp_balance_usd = snapshot.perp_balance_usd,
            perp_available_usd = snapshot.perp_available_usd,
            positions = snapshot.positions.len(),
            "state received account snapshot"
        );
        self.spot_cash_bits
            .store(snapshot.spot_available_usd.to_bits(), Ordering::Relaxed);
        self.perp_cash_bits
            .store(snapshot.perp_available_usd.to_bits(), Ordering::Relaxed);
        match self.accounts.entry(account_id) {
            Entry::Occupied(mut entry) => {
                let account = entry.get_mut();
                account.snapshot = snapshot;
            }
            Entry::Vacant(entry) => {
                entry.insert_entry(AccountState {
                    snapshot,
                    pending_liquidation: false,
                    last_liquidation_ms: None,
                });
            }
        }
        self.counters.set_last_equity(equity);
    }

    pub fn available_budgets(&self) -> (f64, f64) {
        let spot = f64::from_bits(self.spot_cash_bits.load(Ordering::Relaxed));
        let perp = f64::from_bits(self.perp_cash_bits.load(Ordering::Relaxed));
        (spot, perp)
    }

    pub fn non_usdt_equity_ratio(&self) -> Option<(f64, f64, f64)> {
        let mut total_equity = 0.0;
        let mut non_usdt_equity = 0.0;
        self.accounts.scan(|_, account| {
            total_equity += account.snapshot.equity_usd;
            non_usdt_equity += account.snapshot.spot_non_usdt_usd;
        });
        if total_equity > 0.0 {
            Some((non_usdt_equity / total_equity, non_usdt_equity, total_equity))
        } else {
            None
        }
    }

    pub fn liquidation_orders(&self) -> Vec<TargetPosition> {
        let mut targets = Vec::new();
        let mut account_ids = Vec::new();
        self.accounts
            .scan(|account_id, _| account_ids.push(account_id.clone()));
        for account_id in account_ids {
            if let Entry::Occupied(mut entry) = self.accounts.entry(account_id) {
                let account = entry.get_mut();
            if let Some(mut account_targets) =
                evaluate_account(account, &self.mark_prices, &self.funding)
            {
                targets.append(&mut account_targets);
            }
        }
        }
        targets
    }

    pub fn record_order_requested(&self, _event: &OrderRequested) {}

    pub fn record_order_filled(&self, event: &OrderFilled) {
        tracing::info!(
            fee_paid = event.fee_paid,
            symbol = event.symbol,
            venue = ?event.venue,
            "order fee observed"
        );
        self.funding
            .entry(event.symbol.clone())
            .and_modify(|hist| hist.add_fee(event.fee_paid))
            .or_insert_with(|| {
                let mut hist = FundingHistory::default();
                hist.add_fee(event.fee_paid);
                hist
            });
    }

    pub fn record_funding_income(&self, income: &FundingIncome) {
        self.funding_income
            .entry(income.symbol.clone())
            .and_modify(|value| *value += income.amount)
            .or_insert(income.amount);
        self.counters.add_funding_income(income.amount);
    }

    pub fn update_margin_rate(&self, snapshot: MarginRateSnapshot) {
        self.margin_rates
            .upsert(snapshot.asset.clone(), snapshot.interest_rate);
    }

    pub fn snapshot(&self) -> SharedStateSnapshot {
        let mut books = HashMap::new();
        self.books.scan(|key, value| {
            books.insert(key.clone(), value.clone());
        });
        let mut last_funding = HashMap::new();
        let mut funding_details = HashMap::new();
        self.funding.scan(|symbol, history| {
            let latest_rate = history
                .latest
                .as_ref()
                .map(|f| f.funding_rate)
                .unwrap_or_default();
            last_funding.insert(symbol.clone(), latest_rate);
            let mark_price = self.mark_prices.read(symbol, |_, price| *price);
            funding_details.insert(
                symbol.clone(),
                FundingStateSnapshot {
                    latest_rate,
                    mark_price,
                    next_rebalance_ms: history.next_rebalance_ms,
                    next_settlement_ms: history.next_settlement_ms,
                    fee_cum_usd: history.fee_cum_usd,
                    history: history.history.iter().cloned().collect(),
                },
            );
        });
        let mut last_target = HashMap::new();
        self.last_target.scan(|symbol, target| {
            last_target.insert(symbol.clone(), target.clone());
        });
        let mut accounts = HashMap::new();
        self.accounts.scan(|account_id, account| {
            accounts.insert(account_id.clone(), account.snapshot.clone());
        });
        let mut margin_rates = HashMap::new();
        self.margin_rates.scan(|asset, rate| {
            margin_rates.insert(asset.clone(), *rate);
        });
        let mut funding_income = HashMap::new();
        self.funding_income.scan(|symbol, value| {
            funding_income.insert(symbol.clone(), *value);
        });
        SharedStateSnapshot {
            books,
            last_funding,
            funding_details,
            margin_rates,
            last_target,
            accounts,
            counters: self.counters.snapshot(),
            funding_income,
        }
    }

    pub fn seed_counters(&self, order_count_total: u64, fee_cum: f64) {
        self.counters.set_orders(order_count_total);
        self.counters.set_fee_cum(fee_cum);
    }

    pub fn apply_trade_delta(&self, order_delta: u64, fee_delta: f64) {
        if order_delta == 0 && fee_delta.abs() <= f64::EPSILON {
            return;
        }
        let order_count_total = self.counters.add_orders(order_delta);
        let fee_cum = self.counters.add_fee(fee_delta);
        tracing::info!(
            order_delta,
            fee_delta,
            order_count_total,
            fee_cum,
            "trade counters updated"
        );
    }

    pub fn symbol_snapshot(
        &self,
        symbol: &str,
    ) -> Option<(
        BookState,
        BookState,
        f64,
        Option<StrategyTargetState>,
        Option<f64>,
        Option<f64>,
    )> {
        let symbol_owned = symbol.to_string();
        let spot_key = (symbol_owned.clone(), Venue::Spot);
        let perp_key = (symbol_owned.clone(), Venue::Perp);
        let spot_book = self.books.read(&spot_key, |_, v| v.clone())?;
        let perp_book = self.books.read(&perp_key, |_, v| v.clone())?;
        let funding_rate = self
            .funding
            .read(&symbol_owned, |_, hist| {
                hist.latest.as_ref().map(|f| f.funding_rate)
            })
            .flatten()
            .unwrap_or_default();
        let last_target = self
            .last_target
            .read(&symbol_owned, |_, target| target.clone());
        let mark_price = self.mark_prices.read(&symbol_owned, |_, price| *price);
        let liquidation_price = self
            .liquidation_prices
            .read(&symbol_owned, |_, price| *price);
        Some((
            spot_book,
            perp_book,
            funding_rate,
            last_target,
            mark_price,
            liquidation_price,
        ))
    }

    pub fn symbol_metrics(&self, symbol: &str) -> Option<SymbolMetrics> {
        self.metrics
            .read(symbol, |_, window| window.snapshot())
            .flatten()
    }

    pub fn funding_history(&self, symbol: &str) -> Vec<FundingSnapshot> {
        self.funding
            .read(symbol, |_, hist| hist.history.iter().cloned().collect())
            .unwrap_or_default()
    }

    pub fn margin_rate(&self, asset: &str) -> Option<f64> {
        self.margin_rates.read(asset, |_, rate| *rate)
    }

    pub fn next_rebalance_ts(&self, symbol: &str) -> Option<i64> {
        self.funding
            .read(symbol, |_, hist| hist.next_rebalance_ms)
            .flatten()
    }

    pub fn next_settlement_ts(&self, symbol: &str) -> Option<i64> {
        self.funding
            .read(symbol, |_, hist| hist.next_settlement_ms)
            .flatten()
    }

    pub fn has_account_data(&self) -> bool {
        let mut found = false;
        self.accounts.scan(|_, _| {
            found = true;
        });
        found
    }

    pub fn has_open_positions_above(&self, min_notional: f64) -> bool {
        let mut found = false;
        self.accounts.scan(|_, account| {
            if found {
                return;
            }
            for position in &account.snapshot.positions {
                if self.position_notional_usd(position) > min_notional {
                    found = true;
                    break;
                }
            }
        });
        found
    }

    fn position_notional_usd(&self, position: &PositionSnapshot) -> f64 {
        let spot_mid = self
            .books
            .read(&(position.symbol.clone(), Venue::Spot), |_, book| book.mid)
            .unwrap_or(0.0)
            .abs();
        let perp_mid = self
            .books
            .read(&(position.symbol.clone(), Venue::Perp), |_, book| book.mid)
            .or_else(|| self.mark_prices.read(&position.symbol, |_, price| *price))
            .unwrap_or(0.0)
            .abs();
        position.spot_qty.abs() * spot_mid + position.perp_qty.abs() * perp_mid
    }
}

fn evaluate_account(
    account: &mut AccountState,
    mark_prices: &ConcurrentHashMap<Symbol, f64>,
    funding: &ConcurrentHashMap<Symbol, FundingHistory>,
) -> Option<Vec<TargetPosition>> {
    let mut total_exposure = 0.0;
    let mut legs = Vec::new();
    for position in &account.snapshot.positions {
        let mark_price = mark_prices
            .read(&position.symbol, |_, price| *price)
            .unwrap_or(0.0)
            .abs();
        if mark_price <= 0.0 {
            continue;
        }
        let spot_exposure = position.spot_qty.abs() * mark_price;
        let perp_exposure = position.perp_qty.abs() * mark_price;
        total_exposure += spot_exposure + perp_exposure;
        legs.push((position.clone(), mark_price));
    }

    if total_exposure <= 0.0 {
        account.pending_liquidation = false;
        account.last_liquidation_ms = None;
        return None;
    }

    let requirement = total_exposure * account.snapshot.maintenance_margin_ratio;
    if account.snapshot.equity_usd > requirement {
        account.pending_liquidation = false;
        account.last_liquidation_ms = None;
        return None;
    }

    let now = now_ms();
    if account.pending_liquidation {
        if let Some(last) = account.last_liquidation_ms {
            if now.saturating_sub(last) < LIQUIDATION_COOLDOWN_MS {
                return None;
            }
        }
    }

    let mut targets = Vec::new();
    for (position, mark_price) in legs {
        let spot_qty = -position.spot_qty;
        let perp_qty = -position.perp_qty;
        if spot_qty.abs() < 1e-9 && perp_qty.abs() < 1e-9 {
            continue;
        }
        let spot_notional = spot_qty.abs() * mark_price;
        let perp_notional = perp_qty.abs() * mark_price;
        let funding_rate = funding
            .read(&position.symbol, |_, hist| {
                hist.latest.as_ref().map(|f| f.funding_rate)
            })
            .flatten()
            .unwrap_or_default();
        targets.push(TargetPosition {
            ts_ms: now,
            symbol: position.symbol.clone(),
            target_notional_usd: spot_notional.max(perp_notional),
            spot_qty,
            perp_qty,
            funding_rate_at_signal: funding_rate,
            basis_bps_at_signal: 0.0,
            leverage: 0.0,
            repay_margin: false,
        });
    }
    if targets.is_empty() {
        account.pending_liquidation = false;
        account.last_liquidation_ms = None;
        None
    } else {
        account.pending_liquidation = true;
        account.last_liquidation_ms = Some(now);
        Some(targets)
    }
}

const LIQUIDATION_COOLDOWN_MS: i64 = 60_000;
const METRIC_WINDOW_MS: i64 = 60 * 60 * 1000;
const MAX_METRIC_POINTS: usize = 512;
const DEFAULT_SETTLEMENT_INTERVAL_MS: i64 = 8 * 60 * 60 * 1000;

#[derive(Clone, Debug)]
struct SeriesPoint {
    ts_ms: i64,
    value: f64,
}

#[derive(Clone, Debug, Default)]
struct MetricWindow {
    price_points: VecDeque<SeriesPoint>,
    funding_points: VecDeque<SeriesPoint>,
}

impl MetricWindow {
    fn record_price(&mut self, ts_ms: i64, price: f64) {
        if !price.is_finite() || price <= 0.0 {
            return;
        }
        self.price_points.push_back(SeriesPoint {
            ts_ms,
            value: price,
        });
        Self::truncate(&mut self.price_points, ts_ms);
    }

    fn record_funding(&mut self, ts_ms: i64, rate: f64) {
        if !rate.is_finite() {
            return;
        }
        self.funding_points
            .push_back(SeriesPoint { ts_ms, value: rate });
        Self::truncate(&mut self.funding_points, ts_ms);
    }

    fn truncate(series: &mut VecDeque<SeriesPoint>, ts_ms: i64) {
        let cutoff = ts_ms - METRIC_WINDOW_MS;
        while series.len() > MAX_METRIC_POINTS {
            series.pop_front();
        }
        while let Some(front) = series.front() {
            if front.ts_ms < cutoff && series.len() > 2 {
                series.pop_front();
            } else {
                break;
            }
        }
    }

    fn snapshot(&self) -> Option<SymbolMetrics> {
        if self.price_points.is_empty() {
            return None;
        }
        let avg_price = mean(&self.price_points);
        let price_volatility = normalized_std(&self.price_points, avg_price);
        let funding_volatility = std_dev(&self.funding_points);
        let latest_funding_abs = self
            .funding_points
            .back()
            .map(|pt| pt.value.abs())
            .unwrap_or(0.0);
        let avg_funding_abs = mean_abs(&self.funding_points);
        Some(SymbolMetrics {
            avg_price,
            price_volatility,
            funding_volatility,
            latest_funding_abs,
            avg_funding_abs,
        })
    }
}

fn mean(series: &VecDeque<SeriesPoint>) -> f64 {
    if series.is_empty() {
        return 0.0;
    }
    let sum: f64 = series.iter().map(|pt| pt.value).sum();
    sum / series.len() as f64
}

fn normalized_std(series: &VecDeque<SeriesPoint>, mean: f64) -> f64 {
    if series.len() < 2 || mean <= 0.0 {
        return 0.0;
    }
    let variance: f64 = series
        .iter()
        .map(|pt| {
            let diff = pt.value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (series.len() as f64 - 1.0);
    (variance.sqrt() / mean).max(0.0)
}

fn std_dev(series: &VecDeque<SeriesPoint>) -> f64 {
    if series.len() < 2 {
        return 0.0;
    }
    let mean_val = mean(series);
    let variance: f64 = series
        .iter()
        .map(|pt| {
            let diff = pt.value - mean_val;
            diff * diff
        })
        .sum::<f64>()
        / (series.len() as f64 - 1.0);
    variance.sqrt()
}

fn mean_abs(series: &VecDeque<SeriesPoint>) -> f64 {
    if series.is_empty() {
        return 0.0;
    }
    let sum: f64 = series.iter().map(|pt| pt.value.abs()).sum();
    sum / series.len() as f64
}

#[derive(Clone, Debug, Default)]
pub struct SymbolMetrics {
    pub avg_price: f64,
    pub price_volatility: f64,
    pub funding_volatility: f64,
    pub latest_funding_abs: f64,
    pub avg_funding_abs: f64,
}
