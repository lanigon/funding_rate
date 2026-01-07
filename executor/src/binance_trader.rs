use std::{collections::HashMap, sync::Arc};

use anyhow::{anyhow, Result};
use chrono::Utc;
use common::{
    Event, OrderBookSnapshot, OrderFilled, OrderGroupCreated, OrderRequested, TargetPosition, Venue,
};
use connector::{ConnectorConfig, MarketOrderParams, OrderResponse, RestClient, SymbolFilters};
use tokio::sync::{mpsc, Mutex};
use tokio::time::{sleep, Duration};
use tracing::warn;
use uuid::Uuid;

use crate::ExecutionRecord;

#[derive(Clone)]
pub struct BinanceTrader {
    client: Arc<RestClient>,
    store_tx: mpsc::Sender<Event>,
    applied_leverage: Arc<Mutex<HashMap<String, u32>>>,
    spot_filters: Arc<Mutex<HashMap<String, SymbolFilters>>>,
    perp_filters: Arc<Mutex<HashMap<String, SymbolFilters>>>,
}

const FEE_FETCH_RETRIES: usize = 5;
const FEE_FETCH_DELAY_MS: u64 = 200;

impl BinanceTrader {
    pub fn from_config(cfg: &ConnectorConfig, store_tx: mpsc::Sender<Event>) -> Self {
        Self {
            client: Arc::new(RestClient::from_config(cfg)),
            store_tx,
            applied_leverage: Arc::new(Mutex::new(HashMap::new())),
            spot_filters: Arc::new(Mutex::new(HashMap::new())),
            perp_filters: Arc::new(Mutex::new(HashMap::new())),
        }
    }

    pub async fn record_group(&self, group: &OrderGroupCreated) -> Result<()> {
        self.store_tx
            .send(Event::OrderGroupCreated(group.clone()))
            .await?;
        Ok(())
    }

    pub async fn execute_leg(
        &self,
        symbol: &str,
        venue: Venue,
        qty: f64,
        price_hint: f64,
        target: &TargetPosition,
        group_id: Uuid,
    ) -> Result<ExecutionRecord> {
        if venue == Venue::Perp {
            self.ensure_perp_leverage(symbol, target.leverage).await?;
        }
        let filters = self.symbol_filters(symbol, venue).await?;
        let mut abs_qty = filters.quantize_quantity(qty.abs());
        if abs_qty <= 0.0 {
            return Err(anyhow!(
                "order quantity rounded to zero for {symbol} on {venue:?}"
            ));
        }
        if let Some(min_qty) = filters.min_qty {
            if abs_qty < min_qty {
                return Err(anyhow!(
                    "order quantity {abs_qty} below exchange minimum {min_qty} for {symbol}"
                ));
            }
        }
        let mut price_reference = price_hint.abs();
        if price_reference <= 0.0 {
            return Err(anyhow!("invalid price hint for {symbol} on {venue:?}"));
        }
        let adjusted_price = filters.quantize_price(price_hint);
        if adjusted_price > 0.0 {
            price_reference = adjusted_price.abs();
        }
        if let Some(min_notional) = filters.min_notional {
            let mut notional = abs_qty * price_reference;
            if notional + 1e-8 < min_notional {
                let required_qty = filters.ceil_quantity(min_notional / price_reference);
                if required_qty <= 0.0 {
                    return Err(anyhow!(
                        "unable to satisfy min notional {min_notional} for {symbol}"
                    ));
                }
                abs_qty = required_qty;
                notional = abs_qty * price_reference;
                if notional + 1e-8 < min_notional {
                    return Err(anyhow!(
                        "order notional {notional} below exchange minimum {min_notional} for {symbol}"
                    ));
                }
            }
        }
        let adjusted_qty = if qty >= 0.0 { abs_qty } else { -abs_qty };
        let side = if adjusted_qty >= 0.0 { "BUY" } else { "SELL" };
        let params = MarketOrderParams::new(symbol.to_string(), side, abs_qty);
        let response = match venue {
            Venue::Spot => self.client.place_spot_taker_order(params.clone()).await,
            Venue::Perp => self.client.place_perp_taker_order(params).await,
        }
        .map_err(|err| anyhow!("rest order error: {err}"))?;
        let price_for_fee = if adjusted_price.abs() > 0.0 {
            adjusted_price.abs()
        } else {
            price_hint.abs()
        };
        let notional = adjusted_qty.abs() * price_for_fee;
        let fee_estimated = notional * 0.0004;
        let fee_paid = self
            .fetch_order_fee_usdt(symbol, venue, response.order_id)
            .await
            .unwrap_or(fee_estimated);
        let record = self.build_record(
            symbol,
            venue,
            adjusted_qty,
            adjusted_price,
            target,
            group_id,
            fee_estimated,
            fee_paid,
            response,
        );
        self.persist(&record).await?;
        Ok(record)
    }

    async fn ensure_perp_leverage(&self, symbol: &str, leverage: f64) -> Result<()> {
        if leverage <= 0.0 {
            return Ok(());
        }
        let target = leverage.floor().max(1.0).min(125.0) as u32;
        let mut guard = self.applied_leverage.lock().await;
        if guard.get(symbol).copied() == Some(target) {
            return Ok(());
        }
        self.client
            .set_perp_leverage(symbol, target)
            .await
            .map_err(|err| anyhow!("rest leverage error: {err}"))?;
        guard.insert(symbol.to_string(), target);
        Ok(())
    }

    async fn symbol_filters(&self, symbol: &str, venue: Venue) -> Result<SymbolFilters> {
        let cache = match venue {
            Venue::Spot => &self.spot_filters,
            Venue::Perp => &self.perp_filters,
        };
        if let Some(filters) = cache.lock().await.get(symbol).cloned() {
            return Ok(filters);
        }
        let fetched = match venue {
            Venue::Spot => self
                .client
                .fetch_spot_symbol_filters(symbol)
                .await
                .map_err(|err| anyhow!("fetch spot filters failed: {err}"))?,
            Venue::Perp => self
                .client
                .fetch_perp_symbol_filters(symbol)
                .await
                .map_err(|err| anyhow!("fetch perp filters failed: {err}"))?,
        };
        cache
            .lock()
            .await
            .insert(symbol.to_string(), fetched.clone());
        Ok(fetched)
    }

    pub async fn fetch_orderbook_snapshot(
        &self,
        symbol: &str,
        venue: Venue,
        depth: usize,
    ) -> Option<OrderBookSnapshot> {
        self.client
            .fetch_orderbook_snapshot(symbol, depth, venue)
            .await
            .ok()
    }

    fn build_record(
        &self,
        symbol: &str,
        venue: Venue,
        qty: f64,
        price_hint: f64,
        target: &TargetPosition,
        group_id: Uuid,
        fee_estimated: f64,
        fee_paid: f64,
        response: OrderResponse,
    ) -> ExecutionRecord {
        let child_order_id = Uuid::new_v4();
        let side = if qty >= 0.0 { "buy" } else { "sell" }.to_string();
        let abs_qty = qty.abs();
        let notional = abs_qty * price_hint;
        let requested = OrderRequested {
            ts_ms: Utc::now().timestamp_millis(),
            child_order_id,
            group_id,
            symbol: symbol.to_string(),
            venue,
            side: side.clone(),
            order_type: "market".to_string(),
            price: price_hint,
            qty,
            notional,
            funding_rate_at_order: target.funding_rate_at_signal,
            basis_bps_at_order: target.basis_bps_at_signal,
            fee_estimated,
            fee_asset: "USDT".to_string(),
            status: format!("submitted#{}", response.order_id),
        };
        let filled = OrderFilled {
            ts_ms: Utc::now().timestamp_millis(),
            child_order_id,
            group_id,
            symbol: symbol.to_string(),
            venue,
            filled_price: price_hint,
            filled_qty: qty,
            fee_paid,
            status: format!("filled#{}", response.status),
        };
        ExecutionRecord { requested, filled }
    }

    async fn fetch_order_fee_usdt(
        &self,
        symbol: &str,
        venue: Venue,
        order_id: i64,
    ) -> Option<f64> {
        for attempt in 0..FEE_FETCH_RETRIES {
            let fees = match venue {
                Venue::Spot => self.client.fetch_spot_order_fees(symbol, order_id).await,
                Venue::Perp => self.client.fetch_perp_order_fees(symbol, order_id).await,
            };
            match fees {
                Ok(entries) if !entries.is_empty() => {
                    if let Some(total) = self.sum_fee_usdt(&entries).await {
                        return Some(total);
                    }
                }
                Ok(_) => {}
                Err(err) => {
                    warn!(?err, symbol, ?venue, order_id, "fee fetch failed");
                }
            }
            if attempt + 1 < FEE_FETCH_RETRIES {
                sleep(Duration::from_millis(FEE_FETCH_DELAY_MS)).await;
            }
        }
        None
    }

    async fn sum_fee_usdt(&self, fees: &[(f64, String)]) -> Option<f64> {
        let mut total = 0.0;
        for (amount, asset) in fees {
            if *amount <= 0.0 {
                continue;
            }
            if is_stable_asset(asset) {
                total += amount;
                continue;
            }
            let symbol = format!("{asset}USDT");
            let price = self.client.fetch_spot_price(&symbol).await?;
            if price <= 0.0 {
                return None;
            }
            total += amount * price;
        }
        if total > 0.0 {
            Some(total)
        } else {
            None
        }
    }

    async fn persist(&self, record: &ExecutionRecord) -> Result<()> {
        self.store_tx
            .send(Event::OrderRequested(record.requested.clone()))
            .await?;
        self.store_tx
            .send(Event::OrderFilled(record.filled.clone()))
            .await?;
        Ok(())
    }
}

fn is_stable_asset(asset: &str) -> bool {
    matches!(
        asset.to_ascii_uppercase().as_str(),
        "USDT" | "BUSD" | "USDC" | "TUSD" | "FDUSD" | "USDP"
    )
}
