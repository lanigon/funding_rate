use common::StrategyTargetState;
use datalayer::SymbolMetrics;

use crate::StrategyConfig;

const PRICE_VOL_WEIGHT: f64 = 0.10;
const FUNDING_VOL_WEIGHT: f64 = 0.133_333_333_333_333_33;
const FUNDING_AVG_WEIGHT: f64 = 0.133_333_333_333_333_33;
const FUNDING_LEVEL_WEIGHT: f64 = 0.633_333_333_333_333_3;
const MIN_TARGET_LEVERAGE: f64 = 2.0;
const MAX_TARGET_LEVERAGE: f64 = 9.0;

/// Encapsulates the calculated hedge direction for the current tick.
#[derive(Clone, Debug)]
pub struct OrderDecision {
    /// Desired notional exposure in USD terms.
    pub target_notional_usd: f64,
    /// Effective leverage implied by the volatility regime.
    pub leverage: f64,
    /// Funding minus margin rate used for carry decisions.
    pub net_carry: f64,
    /// Net spot quantity to trade.
    pub spot_qty: f64,
    /// Net perp quantity to trade.
    pub perp_qty: f64,
    /// Funding rate incorporated at decision time.
    pub funding_rate: f64,
    /// Basis between perp/spot in basis points.
    pub basis_bps: f64,
}

/// Maintains rolling series for a symbol and produces trade decisions.
#[derive(Clone, Debug)]
pub struct StrategyCalculation;

impl StrategyCalculation {
    pub fn new(_symbol: String) -> Self {
        Self
    }

    pub fn score(&self, metrics: &SymbolMetrics, latest_norm: f64, avg_norm: f64) -> f64 {
        let price_stability = stability_from_vol(metrics.price_volatility);
        let funding_stability = stability_from_vol(metrics.funding_volatility);
        let latest_norm = latest_norm.clamp(0.0, 1.0);
        let avg_norm = avg_norm.clamp(0.0, 1.0);
        (price_stability * PRICE_VOL_WEIGHT)
            + (funding_stability * FUNDING_VOL_WEIGHT)
            + (avg_norm * FUNDING_AVG_WEIGHT)
            + (latest_norm * FUNDING_LEVEL_WEIGHT)
    }

    pub fn plan_order(
        &self,
        spot_price: f64,
        perp_price: f64,
        funding_rate: f64,
        margin_rate: f64,
        last_target: Option<&StrategyTargetState>,
        cfg: &StrategyConfig,
        metrics: &SymbolMetrics,
        weight: f64,
        max_price_vol: f64,
    ) -> Option<OrderDecision> {
        if spot_price <= 0.0 || perp_price <= 0.0 {
            return None;
        }
        if metrics.avg_price <= 0.0 {
            return None;
        }
        let base_notional = cfg.target_notional_usd * weight.max(0.0);
        let vol = metrics.price_volatility.max(0.0);
        let leverage = leverage_from_volatility(vol, max_price_vol);
        let mut effective_notional = base_notional * (leverage / MIN_TARGET_LEVERAGE).max(1.0);
        let funding_bias = metrics.latest_funding_abs.max(metrics.funding_volatility);
        if funding_bias < 1e-5 {
            effective_notional *= 0.6;
        }
        let net_carry = funding_rate - margin_rate;
        if net_carry < 0.0 {
            let carry_boost = (net_carry.abs() * 10_000.0 / 10.0).clamp(1.0, 3.0);
            effective_notional *= carry_boost;
        }
        let spot_qty = effective_notional / spot_price;
        if !self.should_rebalance(spot_qty, last_target, cfg) {
            return None;
        }
        let perp_qty = -spot_qty;
        let basis_bps = if spot_price.abs() > f64::EPSILON {
            ((perp_price - spot_price) / spot_price) * 10_000.0
        } else {
            0.0
        };
        Some(OrderDecision {
            target_notional_usd: effective_notional,
            leverage,
            spot_qty,
            perp_qty,
            funding_rate,
            net_carry,
            basis_bps,
        })
    }

    fn should_rebalance(
        &self,
        desired_spot_qty: f64,
        last_target: Option<&StrategyTargetState>,
        cfg: &StrategyConfig,
    ) -> bool {
        match last_target {
            Some(target) => (target.spot_qty - desired_spot_qty).abs() > cfg.rebalance_threshold,
            None => true,
        }
    }
}

fn leverage_from_volatility(vol: f64, max_vol: f64) -> f64 {
    if max_vol <= 0.0 {
        return MAX_TARGET_LEVERAGE;
    }
    let ratio = (vol / max_vol).clamp(0.0, 1.0);
    let leverage = MAX_TARGET_LEVERAGE - ratio * (MAX_TARGET_LEVERAGE - MIN_TARGET_LEVERAGE);
    leverage.floor().clamp(MIN_TARGET_LEVERAGE, MAX_TARGET_LEVERAGE)
}

fn stability_from_vol(vol: f64) -> f64 {
    let clamped = vol.max(0.0);
    1.0 / (1.0 + clamped)
}
