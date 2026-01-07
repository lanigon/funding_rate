use common::{BookState, StrategyTargetState};

/// Basic liquidation guard that checks if the current position is within 1% of being liquidated.
pub struct RiskChecker {
    /// Maximum allowed drawdown ratio before we stop out.
    threshold_ratio: f64,
}

impl RiskChecker {
    pub fn new(threshold_ratio: f64) -> Self {
        Self { threshold_ratio }
    }

    /// Returns true if we should force-close the position based on the latest book prices.
    pub fn should_liquidate(
        &self,
        spot_book: &BookState,
        perp_book: &BookState,
        mark_price: Option<f64>,
        liquidation_price: Option<f64>,
        target: Option<&StrategyTargetState>,
    ) -> bool {
        let Some(target) = target else {
            return false;
        };
        if let (Some(mark), Some(liq)) = (mark_price, liquidation_price) {
            if liq > 0.0 {
                let distance = ((mark - liq) / liq).abs();
                if distance <= self.threshold_ratio {
                    return true;
                }
            }
        }
        // Estimate liquidation by comparing unrealized pnl ratio.
        let spot_mid = spot_book.mid;
        let perp_mid = perp_book.mid;
        if spot_mid <= 0.0 || perp_mid <= 0.0 {
            return false;
        }
        // If drift between actual vs target exceeds threshold_ratio, trigger liquidation.
        let spot_diff = (target.spot_qty / spot_mid).abs();
        let perp_diff = (target.perp_qty / perp_mid).abs();
        let imbalance_ratio = (spot_diff - perp_diff).abs() / (spot_diff.max(perp_diff)).max(1e-9);
        imbalance_ratio >= self.threshold_ratio
    }
}
