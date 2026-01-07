mod calculation;

use std::cmp::Ordering;
use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;

use anyhow::Result;
use calculation::StrategyCalculation;
use chrono::{Timelike, Utc};
use common::{
    constant::{
        BINANCE_DEFAULT_FUNDING_INTERVAL_SECS, DEFAULT_STRATEGY_REBALANCE_THRESHOLD,
        DEFAULT_STRATEGY_TARGET_NOTIONAL_USD, DEFAULT_SYMBOLS,
    },
    now_ms, Event, FundingSnapshot, PriceCandle, StrategyTargetState, TargetPosition, Venue,
};
use datalayer::{SharedState, SymbolMetrics};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tokio::time::{interval, sleep, Duration, Instant, MissedTickBehavior};
use tracing::{info, warn};

const ACTIVE_SYMBOL_LIMIT: usize = 3;
const ACTIVE_WEIGHTS: [f64; ACTIVE_SYMBOL_LIMIT] = [0.45, 0.33, 0.22];
const MIN_NET_CARRY: f64 = 0.0002;
const SWITCHING_PENALTY: f64 = 0.02;
const MIN_SETTLEMENT_LEAD_MS: i64 = 10 * 60 * 1000;
const MAX_SETTLEMENT_LEAD_MS: i64 = 60 * 60 * 1000;
const PRICE_WINDOW_HOURS: usize = 120;
const PRICE_SEED_TIMEOUT_SECS: u64 = 180;
const PRICE_SEED_CHECK_MS: u64 = 500;
const MIN_ORDER_NOTIONAL_USD: f64 = 5.0;
const MIN_POSITION_NOTIONAL_USD: f64 = 5.0;
const NON_USDT_EQUITY_RATIO_LIMIT: f64 = 0.20;
const ACCOUNT_SEED_TIMEOUT_SECS: u64 = 60;
const ACCOUNT_SEED_CHECK_MS: u64 = 500;

struct BudgetTracker {
    spot_total: f64,
    perp_margin_total: f64,
    spot_remaining: f64,
    perp_margin_remaining: f64,
}

impl BudgetTracker {
    fn new(spot_budget_usd: f64, perp_budget_usd: f64) -> Self {
        let spot = spot_budget_usd.max(0.0);
        let perp = perp_budget_usd.max(0.0);
        Self {
            spot_total: spot,
            perp_margin_total: perp,
            spot_remaining: spot,
            perp_margin_remaining: perp,
        }
    }

    fn target_share(&self, weight: f64, leverage: f64) -> f64 {
        let weight = weight.clamp(0.0, 1.0);
        let leverage = leverage.max(1.0);
        let spot_share = self.spot_total * weight;
        let perp_share = (self.perp_margin_total * weight) * leverage;
        spot_share.min(perp_share).max(0.0)
    }

    fn reserve_notional(&mut self, desired_notional: f64, leverage: f64) -> Option<f64> {
        if desired_notional <= 0.0 {
            return Some(0.0);
        }
        let leverage = leverage.max(1.0);
        let spot_cap = self.spot_remaining;
        let perp_notional_cap = self.perp_margin_remaining * leverage;
        let available = spot_cap.min(perp_notional_cap);
        if available <= 0.0 {
            return None;
        }
        let actual = desired_notional.min(available);
        let scale = (actual / desired_notional).min(1.0);
        self.spot_remaining = (self.spot_remaining - actual).max(0.0);
        let margin_used = (actual / leverage).min(self.perp_margin_remaining);
        self.perp_margin_remaining = (self.perp_margin_remaining - margin_used).max(0.0);
        Some(scale)
    }
}

#[derive(Default)]
struct HourlyRebalanceGate {
    last_slot: Option<i64>,
}

impl HourlyRebalanceGate {
    fn should_trigger(&mut self, now: chrono::DateTime<Utc>) -> bool {
        if now.minute() < 50 {
            return false;
        }
        let slot = now.timestamp() / 3600;
        if self.last_slot == Some(slot) {
            return false;
        }
        self.last_slot = Some(slot);
        true
    }
}

#[derive(Clone)]
pub struct StrategyConfig {
    pub symbols: Vec<String>,
    pub target_notional_usd: f64,
    pub rebalance_threshold: f64,
    pub spot_budget_usd: f64,
    pub perp_budget_usd: f64,
}

impl Default for StrategyConfig {
    fn default() -> Self {
        Self {
            symbols: DEFAULT_SYMBOLS.iter().map(|s| s.to_string()).collect(),
            target_notional_usd: DEFAULT_STRATEGY_TARGET_NOTIONAL_USD,
            rebalance_threshold: DEFAULT_STRATEGY_REBALANCE_THRESHOLD,
            spot_budget_usd: DEFAULT_STRATEGY_TARGET_NOTIONAL_USD,
            perp_budget_usd: DEFAULT_STRATEGY_TARGET_NOTIONAL_USD,
        }
    }
}

pub fn spawn(
    mut strategy_rx: mpsc::Receiver<Event>,
    datalayer_tx: mpsc::Sender<Event>,
    state: SharedState,
    cfg: StrategyConfig,
) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let calculations = build_calculations(&cfg.symbols);
        let mut scores: HashMap<String, f64> = HashMap::new();
        let mut bootstrap_pending = true;
        let mut bootstrap_wait: Option<Pin<Box<dyn Future<Output = bool> + Send>>> = Some(
            Box::pin(wait_for_bootstrap_ready(state.clone(), cfg.symbols.clone())),
        );
        let mut hourly_gate = HourlyRebalanceGate::default();
        let mut timer = interval(Duration::from_secs(60));
        timer.set_missed_tick_behavior(MissedTickBehavior::Delay);
        loop {
            tokio::select! {
                maybe_event = strategy_rx.recv() => {
                    if maybe_event.is_none() {
                        break;
                    }
                }
                _ = timer.tick() => {
                    let now = Utc::now();
                    if hourly_gate.should_trigger(now) {
                        if let Err(err) = run_hourly_rebalance(
                            &cfg,
                            &state,
                            &datalayer_tx,
                            &calculations,
                            &mut scores,
                            true,
                        ).await {
                            warn!("strategy hourly rebalance failed: {err:?}");
                        }
                    }
                }
                has_positions = async {
                    if let Some(wait) = bootstrap_wait.as_mut() {
                        Some(wait.await)
                    } else {
                        None
                    }
                }, if bootstrap_pending && bootstrap_wait.is_some() => {
                    bootstrap_pending = false;
                    bootstrap_wait = None;
                    let has_positions = has_positions.unwrap_or(false);
                    if has_positions {
                        info!("strategy bootstrap skipped: existing positions detected");
                    } else {
                        info!("strategy bootstrap rebalance (no open positions detected)");
                        if let Err(err) = run_hourly_rebalance(
                            &cfg,
                            &state,
                            &datalayer_tx,
                            &calculations,
                            &mut scores,
                            false,
                        )
                        .await
                        {
                            warn!("strategy bootstrap rebalance failed: {err:?}");
                        }
                    }
                }
            }
        }
        Ok(())
    })
}

async fn maybe_emit_target(
    symbol: &str,
    cfg: &StrategyConfig,
    state: &SharedState,
    datalayer_tx: &mpsc::Sender<Event>,
    calc: Option<&StrategyCalculation>,
    metrics: &SymbolMetrics,
    price: &PriceSnapshot,
    weight: f64,
    max_price_vol: f64,
    budgets: Option<&mut BudgetTracker>,
    enforce_min_carry: bool,
) -> Result<()> {
    let calc = match calc {
        Some(calc) => calc,
        None => return Ok(()),
    };
    let funding_rate = state
        .funding_history(symbol)
        .last()
        .map(|f| f.funding_rate)
        .unwrap_or_default();
    let last_target = state
        .symbol_snapshot(symbol)
        .and_then(|(_, _, _, last_target, _, _)| last_target);
    let last_target_ref = last_target.as_ref();
    // Margin borrowing is disabled; treat carry as purely the funding leg.
    let margin_rate = 0.0;
    let net_carry = funding_rate - margin_rate;
    if enforce_min_carry && net_carry < MIN_NET_CARRY {
        if should_close_position(last_target_ref, cfg.rebalance_threshold) {
            let target = TargetPosition {
                ts_ms: now_ms(),
                symbol: symbol.to_string(),
                target_notional_usd: 0.0,
                spot_qty: 0.0,
                perp_qty: 0.0,
                funding_rate_at_signal: funding_rate,
                basis_bps_at_signal: price.basis_bps,
                leverage: 0.0,
                repay_margin: false,
            };
            datalayer_tx.send(Event::TargetPosition(target)).await?;
        }
        return Ok(());
    }

    let decision = calc.plan_order(
        price.spot_price,
        price.perp_price,
        funding_rate,
        margin_rate,
        last_target_ref,
        cfg,
        metrics,
        weight,
        max_price_vol,
    );
    let Some(mut decision) = decision else {
        return Ok(());
    };
    let mut desired_notional = decision.target_notional_usd;
    let mut applied_scale = 1.0;
    if let Some(budget) = budgets {
        let allocation = budget.target_share(weight, decision.leverage);
        if allocation <= 0.0 {
            info!(
                symbol,
                weight, "strategy allocation zero based on available budgets, skip signal"
            );
            return Ok(());
        }
        info!(
            symbol,
            weight,
            spot_target = budget.spot_total * weight,
            perp_target_nominal = budget.perp_margin_total * weight * decision.leverage,
            "strategy budget targets before scaling"
        );
        let base_notional = decision.target_notional_usd.max(1e-9);
        let share_scale = allocation / base_notional;
        if (share_scale - 1.0).abs() > 1e-6 {
            decision.target_notional_usd = allocation;
            decision.spot_qty *= share_scale;
            decision.perp_qty *= share_scale;
        }
        desired_notional = allocation;
        applied_scale = share_scale;
        info!(
            symbol,
            weight,
            target_notional = decision.target_notional_usd,
            allocation,
            share_scale,
            "strategy allocation after budget share"
        );
        let Some(scale) = budget.reserve_notional(desired_notional, decision.leverage) else {
            info!(
                symbol,
                desired_notional, "strategy budget exhausted, skip signal"
            );
            return Ok(());
        };
        if scale <= 0.0 {
            return Ok(());
        }
        applied_scale *= scale;
        if scale < 1.0 {
            decision.target_notional_usd *= scale;
            decision.spot_qty *= scale;
            decision.perp_qty *= scale;
        }
    }
    if decision.target_notional_usd < MIN_ORDER_NOTIONAL_USD {
        info!(
            symbol,
            actual_notional = decision.target_notional_usd,
            "strategy notional below venue minimum, skip signal"
        );
        return Ok(());
    }

    info!(
        symbol,
        weight,
        desired_notional,
        allocation_scale = applied_scale,
        actual_notional = decision.target_notional_usd,
        basis = decision.basis_bps,
        leverage = decision.leverage,
        net_carry_bps = decision.net_carry * 10_000.0,
        "strategy decision"
    );

    let target = TargetPosition {
        ts_ms: now_ms(),
        symbol: symbol.to_string(),
        target_notional_usd: decision.target_notional_usd,
        spot_qty: decision.spot_qty,
        perp_qty: decision.perp_qty,
        funding_rate_at_signal: decision.funding_rate,
        basis_bps_at_signal: decision.basis_bps,
        leverage: decision.leverage,
        repay_margin: false,
    };
    datalayer_tx.send(Event::TargetPosition(target)).await?;
    Ok(())
}

fn should_close_position(last: Option<&StrategyTargetState>, threshold: f64) -> bool {
    match last {
        Some(target) => target.spot_qty.abs() > threshold || target.perp_qty.abs() > threshold,
        None => false,
    }
}

fn build_calculations(symbols: &[String]) -> HashMap<String, StrategyCalculation> {
    symbols
        .iter()
        .cloned()
        .map(|symbol| {
            let calc = StrategyCalculation::new(symbol.clone());
            (symbol, calc)
        })
        .collect()
}

fn weight_for_symbol(symbol: &str, scores: &HashMap<String, f64>) -> Option<f64> {
    if scores.is_empty() {
        return None;
    }
    let mut ranked: Vec<(&String, &f64)> = scores.iter().collect();
    ranked.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(Ordering::Equal));
    for (idx, (sym, _)) in ranked.into_iter().enumerate() {
        if sym.as_str() == symbol && idx < ACTIVE_SYMBOL_LIMIT {
            return ACTIVE_WEIGHTS.get(idx).copied();
        }
    }
    None
}

async fn run_hourly_rebalance(
    cfg: &StrategyConfig,
    state: &SharedState,
    datalayer_tx: &mpsc::Sender<Event>,
    calculations: &HashMap<String, StrategyCalculation>,
    scores: &mut HashMap<String, f64>,
    enforce_settlement_window: bool,
) -> Result<()> {
    if !state.has_account_data() {
        info!("strategy rebalance skipped: account snapshot not ready");
        return Ok(());
    }
    let (mut spot_budget, mut perp_budget) = state.available_budgets();
    spot_budget = spot_budget.max(0.0);
    perp_budget = perp_budget.max(0.0);
    info!(
        spot_budget,
        perp_budget, "strategy budgets for current rebalance"
    );
    if let Some((ratio, non_usdt_equity, total_equity)) = state.non_usdt_equity_ratio() {
        if ratio > NON_USDT_EQUITY_RATIO_LIMIT {
            info!(
                ratio,
                non_usdt_equity,
                total_equity,
                "strategy rebalance skipped: non-usdt equity ratio above limit"
            );
            return Ok(());
        }
    }
    let mut budgets = BudgetTracker::new(spot_budget, perp_budget);
    let has_any_positions = state.has_open_positions_above(MIN_POSITION_NOTIONAL_USD);
    scores.clear();
    let mut all_scores: HashMap<String, f64> = HashMap::new();
    let mut eligible_scores: HashMap<String, f64> = HashMap::new();
    let mut prepared: Vec<(String, &StrategyCalculation, SymbolMetrics, PriceSnapshot, bool)> =
        Vec::new();
    let mut price_map: HashMap<String, PriceSnapshot> = HashMap::new();
    let mut metrics_considered = 0usize;
    let mut settlement_filtered = 0usize;
    let now = now_ms();
    for symbol in &cfg.symbols {
        let Some(calc) = calculations.get(symbol) else {
            continue;
        };
        let Some(spot_series) = state.price_window(symbol, Venue::Spot) else {
            continue;
        };
        if spot_series.len() < PRICE_WINDOW_HOURS {
            continue;
        }
        let Some(perp_series) = state.price_window(symbol, Venue::Perp) else {
            continue;
        };
        if perp_series.len() < PRICE_WINDOW_HOURS {
            continue;
        }
        metrics_considered += 1;
        let funding_history = state.funding_history(symbol);
        let mut passes_settlement = true;
        if enforce_settlement_window {
            if let Some(next_settlement) = state.next_settlement_ts(symbol) {
                let lead = next_settlement - now;
                if lead < MIN_SETTLEMENT_LEAD_MS || lead > MAX_SETTLEMENT_LEAD_MS {
                    settlement_filtered += 1;
                    passes_settlement = false;
                }
            }
        }
        let Some(metrics) = build_symbol_metrics(&spot_series, &funding_history) else {
            continue;
        };
        let Some(snapshot) = build_price_snapshot(&spot_series, &perp_series) else {
            continue;
        };
        price_map.insert(symbol.clone(), snapshot.clone());
        prepared.push((symbol.clone(), calc, metrics, snapshot, passes_settlement));
    }
    if prepared.is_empty() {
        if metrics_considered == 0 {
            info!("strategy hourly rebalance skipped: no metrics available");
        } else {
            info!(
                metrics_considered,
                settlement_filtered,
                "strategy hourly rebalance skipped: no symbols satisfied filters"
            );
        }
        return Ok(());
    }
    let mut max_latest = 0.0;
    let mut max_avg = 0.0;
    let mut max_price_vol = 0.0;
    for (_, _, metrics, _, _) in &prepared {
        if metrics.latest_funding_abs > max_latest {
            max_latest = metrics.latest_funding_abs;
        }
        if metrics.avg_funding_abs > max_avg {
            max_avg = metrics.avg_funding_abs;
        }
        if metrics.price_volatility > max_price_vol {
            max_price_vol = metrics.price_volatility;
        }
    }
    for (symbol, calc, metrics, _, passes_settlement) in &prepared {
        let latest_norm = if max_latest > 0.0 {
            metrics.latest_funding_abs / max_latest
        } else {
            0.0
        };
        let avg_norm = if max_avg > 0.0 {
            metrics.avg_funding_abs / max_avg
        } else {
            0.0
        };
        let mut score = calc.score(metrics, latest_norm, avg_norm);
        if has_any_positions && !has_open_position(state, symbol, cfg.rebalance_threshold) {
            score = (score - SWITCHING_PENALTY).max(0.0);
        }
        all_scores.insert(symbol.clone(), score);
        if *passes_settlement {
            eligible_scores.insert(symbol.clone(), score);
        }
    }
    let use_eligible = eligible_scores.len() >= ACTIVE_SYMBOL_LIMIT;
    let chosen_scores = if use_eligible {
        &eligible_scores
    } else {
        &all_scores
    };
    scores.extend(chosen_scores.iter().map(|(k, v)| (k.clone(), *v)));
    if enforce_settlement_window && !use_eligible && !eligible_scores.is_empty() {
        info!(
            eligible = eligible_scores.len(),
            total = all_scores.len(),
            "strategy using fallback ranking (settlement filter too strict)"
        );
    }
    let mut ranking: Vec<(&String, &f64)> = scores.iter().collect();
    ranking.sort_by(|a, b| b.1.partial_cmp(a.1).unwrap_or(Ordering::Equal));
    let top: Vec<(String, f64)> = ranking
        .iter()
        .take(ACTIVE_SYMBOL_LIMIT)
        .map(|(sym, score)| ((*sym).clone(), **score))
        .collect();
    info!(candidates = prepared.len(), ?top, "strategy hourly ranking");

    for (symbol, calc, metrics, snapshot, _) in prepared {
        let Some(weight) = weight_for_symbol(&symbol, scores) else {
            close_if_outside_top(&symbol, cfg, state, datalayer_tx, &snapshot).await?;
            continue;
        };
        maybe_emit_target(
            &symbol,
            cfg,
            state,
            datalayer_tx,
            Some(calc),
            &metrics,
            &snapshot,
            weight,
            max_price_vol,
            Some(&mut budgets),
            false,
        )
        .await?;
    }
    for symbol in &cfg.symbols {
        if weight_for_symbol(symbol, scores).is_some() {
            continue;
        }
        if let Some(snapshot) = price_map.get(symbol) {
            close_if_outside_top(symbol, cfg, state, datalayer_tx, snapshot).await?;
        }
    }
    Ok(())
}

async fn close_if_outside_top(
    symbol: &str,
    cfg: &StrategyConfig,
    state: &SharedState,
    datalayer_tx: &mpsc::Sender<Event>,
    price: &PriceSnapshot,
) -> Result<()> {
    let should_close = state
        .symbol_snapshot(symbol)
        .and_then(|(_, _, _, last_target, _, _)| last_target)
        .map(|target| {
            target.spot_qty.abs() > cfg.rebalance_threshold
                || target.perp_qty.abs() > cfg.rebalance_threshold
        })
        .unwrap_or(false);
    if !should_close {
        return Ok(());
    }
    info!(
        symbol,
        "strategy closing position (dropped from top ranking)"
    );
    let target = TargetPosition {
        ts_ms: now_ms(),
        symbol: symbol.to_string(),
        target_notional_usd: 0.0,
        spot_qty: 0.0,
        perp_qty: 0.0,
        funding_rate_at_signal: 0.0,
        basis_bps_at_signal: price.basis_bps,
        leverage: 0.0,
        repay_margin: false,
    };
    datalayer_tx.send(Event::TargetPosition(target)).await?;
    Ok(())
}

fn has_open_position(state: &SharedState, symbol: &str, threshold: f64) -> bool {
    state
        .symbol_snapshot(symbol)
        .and_then(|(_, _, _, last_target, _, _)| last_target)
        .map(|target| target.spot_qty.abs() > threshold || target.perp_qty.abs() > threshold)
        .unwrap_or(false)
}

#[derive(Clone)]
struct PriceSnapshot {
    spot_price: f64,
    perp_price: f64,
    basis_bps: f64,
}

fn build_symbol_metrics(
    candles: &[PriceCandle],
    funding_history: &[FundingSnapshot],
) -> Option<SymbolMetrics> {
    if candles.len() < 2 {
        return None;
    }
    let closes: Vec<f64> = candles.iter().map(|c| c.close).collect();
    let avg_price = closes.iter().copied().sum::<f64>() / closes.len() as f64;
    if avg_price <= 0.0 {
        return None;
    }
    let price_volatility = normalized_std(&closes, avg_price).max(0.0);
    let (funding_volatility, latest_funding_abs, avg_funding_abs) = funding_stats(funding_history);
    Some(SymbolMetrics {
        avg_price,
        price_volatility,
        funding_volatility,
        latest_funding_abs,
        avg_funding_abs,
    })
}

fn build_price_snapshot(
    spot_series: &[PriceCandle],
    perp_series: &[PriceCandle],
) -> Option<PriceSnapshot> {
    let spot_price = spot_series.last()?.close;
    let perp_price = perp_series.last()?.close;
    if spot_price <= 0.0 || perp_price <= 0.0 {
        return None;
    }
    let basis_bps = ((perp_price - spot_price) / spot_price) * 10_000.0;
    Some(PriceSnapshot {
        spot_price,
        perp_price,
        basis_bps,
    })
}

fn normalized_std(series: &[f64], mean: f64) -> f64 {
    if series.len() < 2 || mean.abs() <= f64::EPSILON {
        return 0.0;
    }
    let variance: f64 = series
        .iter()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (series.len() as f64 - 1.0);
    (variance.sqrt() / mean).max(0.0)
}

fn funding_stats(history: &[FundingSnapshot]) -> (f64, f64, f64) {
    if history.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    const DEFAULT_INTERVAL_HOURS: f64 = (BINANCE_DEFAULT_FUNDING_INTERVAL_SECS as f64) / 3600.0;
    const HOUR_MS: f64 = 60.0 * 60.0 * 1000.0;
    let mut normalized = Vec::with_capacity(history.len());
    let mut prev_ts: Option<i64> = None;
    for snapshot in history {
        let interval_ms = prev_ts
            .map(|ts| {
                let diff = (snapshot.ts_ms - ts).abs();
                if diff == 0 {
                    (DEFAULT_INTERVAL_HOURS * HOUR_MS) as i64
                } else {
                    diff
                }
            })
            .unwrap_or((DEFAULT_INTERVAL_HOURS * HOUR_MS) as i64);
        let interval_hours = (interval_ms as f64 / HOUR_MS).max(1e-6);
        normalized.push(snapshot.funding_rate / interval_hours);
        prev_ts = Some(snapshot.ts_ms);
    }
    let latest_abs = normalized.last().copied().unwrap_or(0.0).abs();
    let avg_abs = normalized.iter().map(|v| v.abs()).sum::<f64>() / normalized.len() as f64;
    if normalized.len() < 2 {
        return (0.0, latest_abs, avg_abs);
    }
    let mean = normalized.iter().sum::<f64>() / normalized.len() as f64;
    let variance: f64 = normalized
        .iter()
        .map(|value| {
            let diff = value - mean;
            diff * diff
        })
        .sum::<f64>()
        / (normalized.len() as f64 - 1.0);
    (variance.sqrt(), latest_abs, avg_abs)
}

fn count_missing_price_windows(state: &SharedState, symbols: &[String]) -> usize {
    let mut missing = 0usize;
    for symbol in symbols {
        for venue in [Venue::Spot, Venue::Perp] {
            let complete = state
                .price_window(symbol, venue)
                .map(|series| series.len() >= PRICE_WINDOW_HOURS)
                .unwrap_or(false);
            if !complete {
                missing += 1;
            }
        }
    }
    missing
}

async fn wait_for_initial_price_windows(state: SharedState, symbols: Vec<String>) {
    let start = Instant::now();
    let mut last_log = Instant::now();
    loop {
        let missing = count_missing_price_windows(&state, &symbols);
        if missing == 0 {
            info!("strategy detected price window seed complete");
            break;
        }
        if start.elapsed() >= Duration::from_secs(PRICE_SEED_TIMEOUT_SECS) {
            warn!(
                missing,
                "strategy waiting for price windows timed out, continuing bootstrap"
            );
            break;
        }
        if last_log.elapsed() >= Duration::from_secs(5) {
            info!(missing, "strategy waiting for price window seed");
            last_log = Instant::now();
        }
        sleep(Duration::from_millis(PRICE_SEED_CHECK_MS)).await;
    }
}

async fn wait_for_account_snapshot(state: SharedState) -> bool {
    let start = Instant::now();
    let mut last_log = Instant::now();
    loop {
        if state.has_account_data() {
            let has_positions = state.has_open_positions_above(MIN_POSITION_NOTIONAL_USD);
            return has_positions;
        }
        if start.elapsed() >= Duration::from_secs(ACCOUNT_SEED_TIMEOUT_SECS) {
            warn!(
                "strategy waiting for account snapshot timed out, assuming no existing positions"
            );
            return false;
        }
        if last_log.elapsed() >= Duration::from_secs(5) {
            info!("strategy waiting for account snapshot");
            last_log = Instant::now();
        }
        sleep(Duration::from_millis(ACCOUNT_SEED_CHECK_MS)).await;
    }
}

async fn wait_for_bootstrap_ready(state: SharedState, symbols: Vec<String>) -> bool {
    let (has_positions, _) = tokio::join!(
        wait_for_account_snapshot(state.clone()),
        wait_for_initial_price_windows(state, symbols)
    );
    has_positions
}
