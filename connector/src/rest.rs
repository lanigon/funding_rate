use std::collections::HashMap;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::Duration;

use chrono::Utc;
use common::{
    constant::BINANCE_MAX_KLINE_LIMIT, FundingIncome, FundingSnapshot, HistoricalFundingPoint,
    LiquiditySample, MarginRateSnapshot, OrderBookSnapshot, Symbol, Venue,
};
use futures_util::future::try_join_all;
use hmac::{Hmac, Mac};
use rand::{rngs::StdRng, Rng, SeedableRng};
use reqwest::header::{HeaderMap, HeaderValue, CONTENT_TYPE};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use serde_json::{self, Value};
use serde_urlencoded;
use tokio::time::sleep;
use tracing::warn;

use crate::types::{
    KlinePoint, LimitOrderParams, MarketOrderParams, OrderRequest, OrderResponse, SymbolFilters,
};
use crate::utils::{base_price, convert_levels, parse_f64};
use crate::{BinanceCredentials, ConnectorConfig};

use sha2::Sha256;

type HmacSha256 = Hmac<Sha256>;

const HTTP_TIMEOUT_SECS: u64 = 10;
const MAX_FUNDING_LIMIT: usize = 1000;
const DEFAULT_RECV_WINDOW_MS: i64 = 5_000;
const SIGNED_MAX_RETRIES: usize = 3;
const TIMESTAMP_RETRY_DELAY_MS: u64 = 500;
const TIMESTAMP_ERROR_CODE: i64 = -1021;

#[derive(Debug, Deserialize)]
pub struct MarginInterestRecord {
    #[serde(rename = "asset")]
    pub asset: String,
    #[serde(rename = "principalAmount", alias = "principal")]
    pub principal_amount: String,
    #[serde(rename = "interest")]
    pub interest: String,
    #[serde(rename = "interestAccruedTime", alias = "interestAccuredTime")]
    pub interest_accrued_time: i64,
    #[serde(rename = "interestRate")]
    pub interest_rate: String,
}

#[derive(Debug, Deserialize)]
struct MarginInterestResponse {
    rows: Vec<MarginInterestRecord>,
}

#[derive(Debug, Deserialize)]
struct MarginRateResponse {
    rows: Vec<MarginRateRow>,
}

#[derive(Debug, Deserialize)]
struct MarginRateRow {
    asset: String,
    #[serde(rename = "interestRate")]
    interest_rate: String,
    #[serde(alias = "time", alias = "timestamp")]
    time: i64,
}

#[derive(Debug, Deserialize)]
struct CrossMarginAccountRaw {
    #[serde(rename = "marginLevel")]
    margin_level: Option<String>,
    #[serde(rename = "userAssets")]
    user_assets: Vec<CrossMarginAssetRaw>,
}

#[derive(Debug, Deserialize)]
struct CrossMarginAssetRaw {
    asset: String,
    borrowed: String,
}

#[derive(Clone, Debug)]
pub struct MarginAccountSnapshot {
    pub margin_level: f64,
    pub borrowed_assets: Vec<MarginBorrowedAsset>,
}

#[derive(Clone, Debug)]
pub struct MarginBorrowedAsset {
    pub asset: String,
    pub borrowed: f64,
}

#[derive(Debug, Deserialize)]
pub struct MarginAssetStatus {
    pub asset: String,
    #[serde(rename = "isBorrowable")]
    pub is_borrowable: bool,
    #[serde(rename = "isMortgageable")]
    pub is_mortgageable: bool,
    #[serde(rename = "userMinBorrow")]
    pub user_min_borrow: String,
}

#[derive(Debug, thiserror::Error)]
pub enum RestError {
    #[error("http error: {0}")]
    Http(#[from] reqwest::Error),
    #[error("api error {code}: {body}")]
    HttpStatus { code: u16, body: String },
    #[error("unexpected response: {0}")]
    InvalidPayload(&'static str),
    #[error("missing api credentials for signed request")]
    MissingCredentials,
}

#[derive(Clone, Copy, Debug)]
pub struct FuturesBalanceSnapshot {
    pub total_wallet_balance: f64,
    pub total_unrealized_profit: f64,
    pub total_margin_balance: f64,
    pub available_balance: f64,
}

#[derive(Clone, Debug)]
pub struct SpotBalanceSnapshot {
    pub total_balance: f64,
    pub usdt_free: f64,
    pub assets: Vec<SpotAssetBalance>,
}

#[derive(Clone, Copy, Debug, Default)]
pub struct SpotEquityBreakdown {
    pub total_usdt: f64,
    pub non_usdt_usdt: f64,
    pub usdt_balance: f64,
}

#[derive(Clone, Debug)]
pub struct SpotAssetBalance {
    pub asset: String,
    pub free: f64,
    pub locked: f64,
}

#[derive(Clone, Debug, Deserialize)]
struct SpotTradeFee {
    #[serde(rename = "orderId")]
    order_id: i64,
    #[serde(rename = "commission")]
    commission: String,
    #[serde(rename = "commissionAsset")]
    commission_asset: String,
}

#[derive(Clone, Debug, Deserialize)]
struct PerpTradeFee {
    #[serde(rename = "orderId")]
    order_id: i64,
    #[serde(rename = "commission")]
    commission: String,
    #[serde(rename = "commissionAsset")]
    commission_asset: String,
}

#[derive(Clone, Debug)]
pub struct TradeFill {
    pub trade_id: i64,
    pub order_id: i64,
    pub price: f64,
    pub qty: f64,
    pub quote_qty: f64,
    pub commission: f64,
    pub commission_asset: String,
    pub time: i64,
    pub side: String,
}

#[derive(Clone, Debug, Deserialize)]
struct SpotTradeRaw {
    #[serde(rename = "id")]
    trade_id: i64,
    #[serde(rename = "orderId")]
    order_id: i64,
    #[serde(rename = "price")]
    price: String,
    #[serde(rename = "qty")]
    qty: String,
    #[serde(rename = "quoteQty")]
    quote_qty: String,
    #[serde(rename = "commission")]
    commission: String,
    #[serde(rename = "commissionAsset")]
    commission_asset: String,
    #[serde(rename = "time")]
    time: i64,
    #[serde(rename = "isBuyer")]
    is_buyer: bool,
}

#[derive(Clone, Debug, Deserialize)]
struct PerpTradeRaw {
    #[serde(rename = "id")]
    trade_id: i64,
    #[serde(rename = "orderId")]
    order_id: i64,
    #[serde(rename = "price")]
    price: String,
    #[serde(rename = "qty")]
    qty: String,
    #[serde(rename = "quoteQty")]
    quote_qty: String,
    #[serde(rename = "commission")]
    commission: String,
    #[serde(rename = "commissionAsset")]
    commission_asset: String,
    #[serde(rename = "time")]
    time: i64,
    #[serde(rename = "side")]
    side: String,
}

pub struct RestClient {
    http: Client,
    rng: StdRng,
    spot_endpoint: String,
    futures_endpoint: String,
    kline_interval: String,
    depth_hint: usize,
    enable_http: bool,
    credentials: Option<BinanceCredentials>,
    recv_window: i64,
    time_offset_ms: AtomicI64,
}

#[derive(Deserialize)]
struct DepthRestResponse {
    #[serde(rename = "lastUpdateId")]
    last_update_id: i64,
    bids: Vec<[String; 2]>,
    asks: Vec<[String; 2]>,
}

#[derive(Deserialize)]
struct ExchangeInfoResponse {
    symbols: Vec<ExchangeInfoSymbol>,
}

#[derive(Deserialize)]
struct ExchangeInfoSymbol {
    symbol: String,
    filters: Vec<ExchangeInfoFilter>,
}

#[derive(Deserialize)]
struct ExchangeInfoFilter {
    #[serde(rename = "filterType")]
    filter_type: String,
    #[serde(rename = "minQty")]
    min_qty: Option<String>,
    #[serde(rename = "maxQty")]
    max_qty: Option<String>,
    #[serde(rename = "stepSize")]
    step_size: Option<String>,
    #[serde(rename = "tickSize")]
    tick_size: Option<String>,
    #[serde(rename = "minPrice")]
    min_price: Option<String>,
    #[serde(rename = "maxPrice")]
    max_price: Option<String>,
    #[serde(rename = "minNotional")]
    min_notional: Option<String>,
}

#[derive(Deserialize)]
struct ServerTimeResponse {
    #[serde(rename = "serverTime")]
    server_time: i64,
}

impl RestClient {
    pub fn from_config(cfg: &ConnectorConfig) -> Self {
        Self::with_mode(cfg, true)
    }

    pub fn new() -> Self {
        Self::from_config(&ConnectorConfig::default())
    }

    pub fn with_recv_window(mut self, recv_window: i64) -> Self {
        self.recv_window = recv_window;
        self
    }

    pub fn with_credentials(api_key: impl Into<String>, api_secret: impl Into<String>) -> Self {
        let mut cfg = ConnectorConfig::default();
        cfg.credentials = Some(BinanceCredentials {
            api_key: api_key.into(),
            api_secret: api_secret.into(),
        });
        Self::from_config(&cfg)
    }

    pub fn mock() -> Self {
        Self::with_mode(&ConnectorConfig::default(), false)
    }

    fn with_mode(cfg: &ConnectorConfig, enable_http: bool) -> Self {
        let http = Client::builder()
            .user_agent("tradeterminal-binance-connector")
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
            .build()
            .expect("failed to build reqwest client");
        Self {
            http,
            rng: StdRng::from_entropy(),
            spot_endpoint: cfg.spot_rest_endpoint.clone(),
            futures_endpoint: cfg.futures_rest_endpoint.clone(),
            kline_interval: cfg.kline_interval.clone(),
            depth_hint: cfg.depth,
            enable_http,
            credentials: cfg.credentials.clone(),
            recv_window: DEFAULT_RECV_WINDOW_MS,
            time_offset_ms: AtomicI64::new(0),
        }
    }

    pub async fn fetch_recent_klines(&mut self, symbol: &str, limit: usize) -> Vec<KlinePoint> {
        if !self.enable_http {
            return self.fallback_klines(symbol, limit);
        }
        match self.fetch_recent_klines_http(symbol, limit).await {
            Ok(data) if !data.is_empty() => data,
            Ok(_) => {
                warn!(symbol, "binance returned empty klines, using fallback");
                self.fallback_klines(symbol, limit)
            }
            Err(err) => {
                warn!(?err, symbol, "failed to fetch klines, using fallback");
                self.fallback_klines(symbol, limit)
            }
        }
    }

    pub async fn fetch_funding_rates(
        &mut self,
        symbol: &str,
        limit: usize,
    ) -> Vec<FundingSnapshot> {
        if !self.enable_http {
            return self.fallback_funding(symbol, limit);
        }
        match self.fetch_funding_rates_http(symbol, limit).await {
            Ok(data) if !data.is_empty() => data,
            Ok(_) => {
                warn!(symbol, "binance returned empty funding, using fallback");
                self.fallback_funding(symbol, limit)
            }
            Err(err) => {
                warn!(?err, symbol, "failed to fetch funding, using fallback");
                self.fallback_funding(symbol, limit)
            }
        }
    }

    pub async fn fetch_income_history(
        &mut self,
        start_time: Option<i64>,
        limit: usize,
        income_type: Option<&str>,
    ) -> Vec<FundingIncome> {
        if !self.enable_http {
            return Vec::new();
        }
        let url = format!("{}/fapi/v1/income", self.futures_endpoint);
        let mut params = self.base_signed_params(None);
        if let Some(start) = start_time {
            params.push(("startTime".into(), start.to_string()));
        }
        params.push(("limit".into(), limit.min(1000).max(1).to_string()));
        if let Some(kind) = income_type {
            params.push(("incomeType".into(), kind.to_string()));
        }
        match self
            .signed_get_json::<Vec<IncomeRecordRaw>>(&url, params)
            .await
        {
            Ok(rows) => rows
                .into_iter()
                .filter_map(|row| FundingIncome::try_from(row).ok())
                .collect(),
            Err(err) => {
                warn!(?err, "failed to fetch income history");
                Vec::new()
            }
        }
    }

    pub async fn fetch_spot_trades(
        &self,
        symbol: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        from_id: Option<i64>,
        limit: usize,
    ) -> Result<Vec<TradeFill>, RestError> {
        if !self.enable_http {
            return Ok(Vec::new());
        }
        let url = format!("{}/api/v3/myTrades", self.spot_endpoint);
        let mut params = self.base_signed_params(None);
        params.push(("symbol".into(), symbol.to_string()));
        if let Some(start) = start_time {
            params.push(("startTime".into(), start.to_string()));
        }
        if let Some(end) = end_time {
            params.push(("endTime".into(), end.to_string()));
        }
        if let Some(from_id) = from_id {
            params.push(("fromId".into(), from_id.to_string()));
        }
        params.push(("limit".into(), limit.min(1000).max(1).to_string()));
        let rows: Vec<SpotTradeRaw> = self.signed_get_json(&url, params).await?;
        Ok(rows
            .into_iter()
            .map(|row| TradeFill {
                trade_id: row.trade_id,
                order_id: row.order_id,
                price: row.price.parse::<f64>().unwrap_or(0.0),
                qty: row.qty.parse::<f64>().unwrap_or(0.0),
                quote_qty: row.quote_qty.parse::<f64>().unwrap_or(0.0),
                commission: row.commission.parse::<f64>().unwrap_or(0.0),
                commission_asset: row.commission_asset,
                time: row.time,
                side: if row.is_buyer {
                    "buy".to_string()
                } else {
                    "sell".to_string()
                },
            })
            .collect())
    }

    pub async fn fetch_perp_trades(
        &self,
        symbol: &str,
        start_time: Option<i64>,
        end_time: Option<i64>,
        from_id: Option<i64>,
        limit: usize,
    ) -> Result<Vec<TradeFill>, RestError> {
        if !self.enable_http {
            return Ok(Vec::new());
        }
        let url = format!("{}/fapi/v1/userTrades", self.futures_endpoint);
        let mut params = self.base_signed_params(None);
        params.push(("symbol".into(), symbol.to_string()));
        if let Some(start) = start_time {
            params.push(("startTime".into(), start.to_string()));
        }
        if let Some(end) = end_time {
            params.push(("endTime".into(), end.to_string()));
        }
        if let Some(from_id) = from_id {
            params.push(("fromId".into(), from_id.to_string()));
        }
        params.push(("limit".into(), limit.min(1000).max(1).to_string()));
        let rows: Vec<PerpTradeRaw> = self.signed_get_json(&url, params).await?;
        Ok(rows
            .into_iter()
            .map(|row| TradeFill {
                trade_id: row.trade_id,
                order_id: row.order_id,
                price: row.price.parse::<f64>().unwrap_or(0.0),
                qty: row.qty.parse::<f64>().unwrap_or(0.0),
                quote_qty: row.quote_qty.parse::<f64>().unwrap_or(0.0),
                commission: row.commission.parse::<f64>().unwrap_or(0.0),
                commission_asset: row.commission_asset,
                time: row.time,
                side: row.side.to_ascii_lowercase(),
            })
            .collect())
    }

    pub async fn fetch_spot_order_fees(
        &self,
        symbol: &str,
        order_id: i64,
    ) -> Result<Vec<(f64, String)>, RestError> {
        if !self.enable_http {
            return Ok(Vec::new());
        }
        let url = format!("{}/api/v3/myTrades", self.spot_endpoint);
        let mut params = self.base_signed_params(None);
        params.push(("symbol".into(), symbol.to_string()));
        params.push(("orderId".into(), order_id.to_string()));
        params.push(("limit".into(), "1000".into()));
        let rows: Vec<SpotTradeFee> = self.signed_get_json(&url, params).await?;
        let mut out = Vec::new();
        for row in rows {
            if row.order_id != order_id {
                continue;
            }
            let amount = row.commission.parse::<f64>().unwrap_or(0.0);
            if amount <= 0.0 {
                continue;
            }
            out.push((amount, row.commission_asset));
        }
        Ok(out)
    }

    pub async fn fetch_perp_order_fees(
        &self,
        symbol: &str,
        order_id: i64,
    ) -> Result<Vec<(f64, String)>, RestError> {
        if !self.enable_http {
            return Ok(Vec::new());
        }
        let url = format!("{}/fapi/v1/userTrades", self.futures_endpoint);
        let mut params = self.base_signed_params(None);
        params.push(("symbol".into(), symbol.to_string()));
        params.push(("orderId".into(), order_id.to_string()));
        params.push(("limit".into(), "1000".into()));
        let rows: Vec<PerpTradeFee> = self.signed_get_json(&url, params).await?;
        let mut out = Vec::new();
        for row in rows {
            if row.order_id != order_id {
                continue;
            }
            let amount = row.commission.parse::<f64>().unwrap_or(0.0);
            if amount <= 0.0 {
                continue;
            }
            out.push((amount, row.commission_asset));
        }
        Ok(out)
    }

    pub async fn fetch_position_risk(&mut self) -> Vec<PositionRisk> {
        let url = format!("{}/fapi/v3/positionRisk", self.futures_endpoint);
        let params = self.base_signed_params(None);
        match self
            .signed_get_json::<Vec<RawPositionRisk>>(&url, params)
            .await
        {
            Ok(rows) => rows
                .into_iter()
                .filter_map(|row| PositionRisk::try_from(row).ok())
                .collect(),
            Err(err) => {
                warn!(?err, "failed to fetch position risk");
                Vec::new()
            }
        }
    }

    pub async fn fetch_futures_balances(&mut self) -> Option<FuturesBalanceSnapshot> {
        if !self.enable_http {
            return None;
        }
        let url = format!("{}/fapi/v2/account", self.futures_endpoint);
        let params = self.base_signed_params(None);
        match self.signed_get_json::<Value>(&url, params).await {
            Ok(body) => {
                let total_wallet_balance = body
                    .get("totalWalletBalance")
                    .and_then(parse_f64)
                    .unwrap_or(0.0);
                let total_unrealized_profit = body
                    .get("totalUnrealizedProfit")
                    .and_then(parse_f64)
                    .unwrap_or(0.0);
                let total_margin_balance = body
                    .get("totalMarginBalance")
                    .and_then(parse_f64)
                    .unwrap_or(total_wallet_balance + total_unrealized_profit);
                let available_balance = body
                    .get("availableBalance")
                    .and_then(parse_f64)
                    .unwrap_or(total_margin_balance);
                Some(FuturesBalanceSnapshot {
                    total_wallet_balance,
                    total_unrealized_profit,
                    total_margin_balance,
                    available_balance,
                })
            }
            Err(err) => {
                warn!(?err, "failed to fetch futures account balances");
                None
            }
        }
    }

    pub async fn fetch_spot_balances(&mut self) -> Option<SpotBalanceSnapshot> {
        if !self.enable_http {
            return None;
        }
        let url = format!("{}/api/v3/account", self.spot_endpoint);
        let params = self.base_signed_params(None);
        match self.signed_get_json::<Value>(&url, params).await {
            Ok(body) => {
                let balances = body.get("balances")?.as_array()?;
                let mut total_balance = 0.0;
                let mut usdt_free = 0.0;
                let mut assets = Vec::new();
                for bal in balances {
                    let asset = bal.get("asset").and_then(|v| v.as_str()).unwrap_or("");
                    let free = bal.get("free").and_then(parse_f64).unwrap_or(0.0);
                    let locked = bal.get("locked").and_then(parse_f64).unwrap_or(0.0);
                    let total = free + locked;
                    total_balance += total;
                    if asset == "USDT" {
                        usdt_free = free;
                    }
                    if total > 0.0 && !asset.is_empty() {
                        assets.push(SpotAssetBalance {
                            asset: asset.to_string(),
                            free,
                            locked,
                        });
                    }
                }
                Some(SpotBalanceSnapshot {
                    total_balance,
                    usdt_free,
                    assets,
                })
            }
            Err(err) => {
                warn!(?err, "failed to fetch spot account balances");
                None
            }
        }
    }

    pub async fn fetch_spot_equity_breakdown(&mut self) -> Option<SpotEquityBreakdown> {
        let balances = self.fetch_spot_balances().await?;
        Some(self.spot_equity_breakdown_from_balances(&balances).await)
    }

    pub(crate) async fn spot_equity_breakdown_from_balances(
        &self,
        balances: &SpotBalanceSnapshot,
    ) -> SpotEquityBreakdown {
        let mut total = 0.0;
        let mut non_usdt_total = 0.0;
        let mut usdt_balance = 0.0;
        for bal in &balances.assets {
            let qty = bal.free + bal.locked;
            if qty <= 0.0 {
                continue;
            }
            if bal.asset == "USDT" {
                total += qty;
                usdt_balance += qty;
                continue;
            }
            let symbol = format!("{}USDT", bal.asset);
            if let Some(price) = self.fetch_spot_price(&symbol).await {
                if price > 0.0 {
                    let value = qty * price;
                    total += value;
                    non_usdt_total += value;
                }
            }
        }
        SpotEquityBreakdown {
            total_usdt: total,
            non_usdt_usdt: non_usdt_total,
            usdt_balance,
        }
    }

    pub async fn fetch_liquidity_sample(&mut self, symbol: &str) -> LiquiditySample {
        if !self.enable_http {
            return self.fallback_liquidity(symbol);
        }
        match self.fetch_liquidity_sample_http(symbol).await {
            Ok(sample) => sample,
            Err(err) => {
                warn!(?err, symbol, "failed to fetch depth, using fallback");
                self.fallback_liquidity(symbol)
            }
        }
    }

    pub async fn fetch_margin_rate(&mut self, asset: &str) -> Option<MarginRateSnapshot> {
        if !self.enable_http {
            return None;
        }
        let url = format!("{}/sapi/v1/margin/interestRateHistory", self.spot_endpoint);
        let mut params = self.base_signed_params(None);
        params.push(("asset".into(), asset.to_string()));
        params.push(("limit".into(), "1".into()));
        let response = self
            .signed_get_json::<MarginRateResponse>(&url, params)
            .await;
        match response {
            Ok(resp) => resp.rows.into_iter().last().and_then(|row| {
                let rate = row.interest_rate.parse::<f64>().ok()?;
                Some(MarginRateSnapshot {
                    ts_ms: row.time,
                    asset: row.asset,
                    interest_rate: rate,
                })
            }),
            Err(err) => {
                warn!(?err, ?asset, "failed to fetch margin interest rate");
                None
            }
        }
    }

    pub async fn fetch_margin_account(&mut self) -> Option<MarginAccountSnapshot> {
        if !self.enable_http {
            return None;
        }
        let url = format!("{}/sapi/v1/margin/account", self.spot_endpoint);
        let params = self.base_signed_params(None);
        match self
            .signed_get_json::<CrossMarginAccountRaw>(&url, params)
            .await
        {
            Ok(info) => {
                let margin_level = info
                    .margin_level
                    .and_then(|value| value.parse::<f64>().ok())
                    .unwrap_or(0.0);
                let borrowed_assets = info
                    .user_assets
                    .into_iter()
                    .filter_map(|entry| {
                        entry
                            .borrowed
                            .parse::<f64>()
                            .ok()
                            .filter(|borrowed| *borrowed > 0.0)
                            .map(|borrowed| MarginBorrowedAsset {
                                asset: entry.asset,
                                borrowed,
                            })
                    })
                    .collect::<Vec<_>>();
                Some(MarginAccountSnapshot {
                    margin_level,
                    borrowed_assets,
                })
            }
            Err(err) => {
                warn!(?err, "failed to fetch cross margin account");
                None
            }
        }
    }

    pub async fn fetch_spot_price(&self, symbol: &str) -> Option<f64> {
        if !self.enable_http {
            return Some(base_price(symbol));
        }
        let url = format!("{}/api/v3/ticker/price", self.spot_endpoint);
        let resp = self
            .http
            .get(&url)
            .query(&[("symbol", symbol)])
            .send()
            .await;
        match resp {
            Ok(resp) => match resp.json::<PriceTicker>().await {
                Ok(ticker) => ticker.price.parse::<f64>().ok(),
                Err(err) => {
                    warn!(?err, ?symbol, "failed to decode spot ticker");
                    None
                }
            },
            Err(err) => {
                warn!(?err, ?symbol, "failed to fetch spot ticker");
                None
            }
        }
    }

    pub async fn fetch_orderbook_snapshot(
        &self,
        symbol: &str,
        depth: usize,
        venue: Venue,
    ) -> Result<OrderBookSnapshot, RestError> {
        if !self.enable_http {
            return Err(RestError::InvalidPayload(
                "orderbook snapshot requested in mock mode",
            ));
        }
        match self
            .fetch_orderbook_snapshot_http(symbol, depth, venue)
            .await
        {
            Ok(snapshot) => Ok(snapshot),
            Err(err) => {
                warn!(?err, symbol, ?venue, "failed to fetch depth snapshot");
                Err(err)
            }
        }
    }

    pub async fn fetch_spot_symbol_filters(
        &self,
        symbol: &str,
    ) -> Result<SymbolFilters, RestError> {
        let url = format!("{}/api/v3/exchangeInfo", self.spot_endpoint);
        self.fetch_symbol_filters(&url, symbol).await
    }

    pub async fn fetch_perp_symbol_filters(
        &self,
        symbol: &str,
    ) -> Result<SymbolFilters, RestError> {
        let url = format!("{}/fapi/v1/exchangeInfo", self.futures_endpoint);
        self.fetch_symbol_filters(&url, symbol).await
    }

    async fn fetch_symbol_filters(
        &self,
        url: &str,
        symbol: &str,
    ) -> Result<SymbolFilters, RestError> {
        let resp = self
            .http
            .get(url)
            .query(&[("symbol", symbol)])
            .send()
            .await?
            .error_for_status()?;
        let payload: ExchangeInfoResponse = resp.json().await?;
        let symbol_info = payload
            .symbols
            .into_iter()
            .find(|entry| entry.symbol.eq_ignore_ascii_case(symbol))
            .ok_or(RestError::InvalidPayload("symbol filters missing"))?;
        Ok(build_symbol_filters(symbol_info))
    }

    async fn fetch_recent_klines_http(
        &self,
        symbol: &str,
        limit: usize,
    ) -> Result<Vec<KlinePoint>, RestError> {
        let url = format!("{}/api/v3/klines", self.spot_endpoint);
        let limited = limit.min(BINANCE_MAX_KLINE_LIMIT);
        let limit_str = limited.to_string();
        let resp = self
            .http
            .get(url)
            .query(&[
                ("symbol", symbol),
                ("interval", self.kline_interval.as_str()),
                ("limit", limit_str.as_str()),
            ])
            .send()
            .await?
            .error_for_status()?;
        let rows: Vec<Vec<Value>> = resp.json().await?;
        let mut klines = Vec::with_capacity(rows.len());
        for entry in rows {
            if entry.len() < 7 {
                continue;
            }
            let open_time = entry.get(0).and_then(Value::as_i64);
            let close_time = entry.get(6).and_then(Value::as_i64);
            let open = entry.get(1).and_then(parse_f64);
            let high = entry.get(2).and_then(parse_f64);
            let low = entry.get(3).and_then(parse_f64);
            let close = entry.get(4).and_then(parse_f64);
            let volume = entry.get(5).and_then(parse_f64);
            if let (
                Some(open_time),
                Some(close_time),
                Some(open),
                Some(high),
                Some(low),
                Some(close),
                Some(volume),
            ) = (open_time, close_time, open, high, low, close, volume)
            {
                klines.push(KlinePoint {
                    open_time,
                    close_time,
                    open,
                    high,
                    low,
                    close,
                    volume,
                });
            }
        }
        if klines.is_empty() {
            return Err(RestError::InvalidPayload("empty klines"));
        }
        Ok(klines)
    }

    async fn fetch_funding_rates_http(
        &self,
        symbol: &str,
        limit: usize,
    ) -> Result<Vec<FundingSnapshot>, RestError> {
        #[derive(Deserialize)]
        struct FundingRateResponse {
            #[serde(rename = "fundingTime")]
            funding_time: i64,
            #[serde(rename = "fundingRate")]
            funding_rate: String,
        }
        let url = format!("{}/fapi/v1/fundingRate", self.futures_endpoint);
        let limited = limit.min(MAX_FUNDING_LIMIT);
        let limit_str = limited.to_string();
        let resp = self
            .http
            .get(url)
            .query(&[("symbol", symbol), ("limit", limit_str.as_str())])
            .send()
            .await?
            .error_for_status()?;
        let rows: Vec<FundingRateResponse> = resp.json().await?;
        if rows.is_empty() {
            return Err(RestError::InvalidPayload("empty funding"));
        }
        let data = rows
            .into_iter()
            .filter_map(|row| {
                row.funding_rate
                    .parse::<f64>()
                    .ok()
                    .map(|funding_rate| FundingSnapshot {
                        ts_ms: row.funding_time,
                        symbol: symbol.to_string(),
                        funding_rate,
                    })
            })
            .collect::<Vec<_>>();
        if data.is_empty() {
            return Err(RestError::InvalidPayload("unable to parse funding"));
        }
        Ok(data)
    }

    async fn fetch_liquidity_sample_http(
        &self,
        symbol: &str,
    ) -> Result<LiquiditySample, RestError> {
        let snapshot = self
            .fetch_orderbook_snapshot_http(symbol, self.depth_hint, Venue::Spot)
            .await?;
        let total_depth: f64 = snapshot
            .bids
            .iter()
            .chain(snapshot.asks.iter())
            .map(|lvl| lvl.size)
            .sum();
        if total_depth == 0.0 {
            return Err(RestError::InvalidPayload("empty depth"));
        }
        Ok(LiquiditySample {
            ts_ms: snapshot.ts_ms,
            symbol: snapshot.symbol,
            venue: Venue::Spot,
            total_depth,
        })
    }

    async fn fetch_orderbook_snapshot_http(
        &self,
        symbol: &str,
        depth: usize,
        venue: Venue,
    ) -> Result<OrderBookSnapshot, RestError> {
        let (endpoint, path) = match venue {
            Venue::Spot => (&self.spot_endpoint, "/api/v3/depth"),
            Venue::Perp => (&self.futures_endpoint, "/fapi/v1/depth"),
        };
        let url = format!("{}{}", endpoint, path);
        let limit = depth.min(1000);
        let limit_str = limit.to_string();
        let resp = self
            .http
            .get(url)
            .query(&[("symbol", symbol), ("limit", limit_str.as_str())])
            .send()
            .await?
            .error_for_status()?;
        let depth: DepthRestResponse = resp.json().await?;
        if depth.bids.is_empty() && depth.asks.is_empty() {
            return Err(RestError::InvalidPayload("empty depth"));
        }
        Ok(OrderBookSnapshot {
            ts_ms: Utc::now().timestamp_millis(),
            symbol: symbol.to_string(),
            venue,
            last_update_id: depth.last_update_id,
            bids: convert_levels(depth.bids),
            asks: convert_levels(depth.asks),
        })
    }

    fn fallback_klines(&mut self, symbol: &str, limit: usize) -> Vec<KlinePoint> {
        let mut klines = Vec::with_capacity(limit);
        let mut last_close = base_price(symbol);
        let now = Utc::now().timestamp_millis();
        for idx in 0..limit {
            let open_time = now - (limit as i64 - idx as i64) * 60_000;
            let open = last_close * (1.0 + self.rng.gen_range(-0.001..0.001));
            let high = open * (1.0 + self.rng.gen_range(0.0..0.002));
            let low = open * (1.0 - self.rng.gen_range(0.0..0.002));
            let close = ((high + low) / 2.0) * (1.0 + self.rng.gen_range(-0.0005..0.0005));
            let volume = self.rng.gen_range(50.0..500.0);
            klines.push(KlinePoint {
                open_time,
                close_time: open_time + 60_000,
                open,
                high,
                low,
                close,
                volume,
            });
            last_close = close;
        }
        klines
    }

    fn fallback_funding(&mut self, symbol: &str, limit: usize) -> Vec<FundingSnapshot> {
        let mut data = Vec::with_capacity(limit);
        let now = Utc::now().timestamp_millis();
        for idx in 0..limit {
            let ts_ms = now - (limit as i64 - idx as i64) * 8 * 60 * 60 * 1000;
            let funding_rate = self.rng.gen_range(-0.001..0.001);
            data.push(FundingSnapshot {
                ts_ms,
                symbol: symbol.to_string(),
                funding_rate,
            });
        }
        data
    }

    fn fallback_liquidity(&mut self, symbol: &str) -> LiquiditySample {
        let venue = if self.rng.gen_bool(0.5) {
            Venue::Spot
        } else {
            Venue::Perp
        };
        LiquiditySample {
            ts_ms: Utc::now().timestamp_millis(),
            symbol: symbol.to_string(),
            venue,
            total_depth: self.rng.gen_range(5_000.0..100_000.0),
        }
    }
}

#[derive(Debug, Deserialize)]
struct RawPositionRisk {
    symbol: String,
    #[serde(rename = "positionAmt")]
    position_amt: String,
    #[serde(rename = "markPrice")]
    mark_price: String,
    #[serde(rename = "liquidationPrice")]
    liquidation_price: String,
    #[serde(rename = "leverage")]
    leverage: Option<String>,
}

#[derive(Clone, Debug)]
pub struct PositionRisk {
    pub symbol: String,
    pub position_amt: f64,
    pub mark_price: f64,
    pub liquidation_price: f64,
    pub leverage: f64,
}

impl TryFrom<RawPositionRisk> for PositionRisk {
    type Error = ();

    fn try_from(raw: RawPositionRisk) -> Result<Self, Self::Error> {
        let position_amt = raw.position_amt.parse::<f64>().map_err(|_| ())?;
        let mark_price = raw.mark_price.parse::<f64>().map_err(|_| ())?;
        let liquidation_price = raw.liquidation_price.parse::<f64>().unwrap_or(0.0);
        let leverage = raw
            .leverage
            .as_deref()
            .unwrap_or("0")
            .parse::<f64>()
            .unwrap_or(0.0);
        Ok(Self {
            symbol: raw.symbol,
            position_amt,
            mark_price,
            liquidation_price,
            leverage,
        })
    }
}

#[derive(Debug, Deserialize)]
struct IncomeRecordRaw {
    symbol: String,
    #[serde(rename = "incomeType")]
    income_type: String,
    income: String,
    #[serde(rename = "time")]
    time: i64,
}

impl TryFrom<IncomeRecordRaw> for FundingIncome {
    type Error = ();

    fn try_from(raw: IncomeRecordRaw) -> Result<Self, Self::Error> {
        if raw.income_type != "FUNDING_FEE" {
            return Err(());
        }
        let amount = raw.income.parse::<f64>().map_err(|_| ())?;
        Ok(FundingIncome {
            ts_ms: raw.time,
            symbol: raw.symbol,
            amount,
        })
    }
}

#[derive(Debug, Deserialize)]
struct PriceTicker {
    price: String,
}

pub async fn fetch_recent_klines(symbol: &str, limit: usize) -> Vec<KlinePoint> {
    RestClient::new().fetch_recent_klines(symbol, limit).await
}

pub async fn generate_mock_funding_history(
    symbols: &[Symbol],
    points: usize,
) -> HashMap<Symbol, Vec<HistoricalFundingPoint>> {
    let mut client = RestClient::new();
    let mut data = HashMap::new();
    for symbol in symbols {
        let entries = client
            .fetch_funding_rates(symbol, points)
            .await
            .into_iter()
            .map(|snap| HistoricalFundingPoint {
                ts_ms: snap.ts_ms,
                funding_rate: snap.funding_rate,
            })
            .collect();
        data.insert(symbol.clone(), entries);
    }
    data
}

pub async fn generate_mock_liquidity_samples(
    symbols: &[Symbol],
) -> HashMap<Symbol, LiquiditySample> {
    let mut client = RestClient::new();
    let mut map = HashMap::new();
    for symbol in symbols {
        map.insert(symbol.clone(), client.fetch_liquidity_sample(symbol).await);
    }
    map
}

impl RestClient {
    fn require_credentials(&self) -> Result<&BinanceCredentials, RestError> {
        self.credentials
            .as_ref()
            .ok_or(RestError::MissingCredentials)
    }

    fn base_signed_params(&self, recv_window: Option<i64>) -> Vec<(String, String)> {
        let window = recv_window.unwrap_or(self.recv_window);
        vec![
            ("timestamp".into(), self.current_timestamp().to_string()),
            ("recvWindow".into(), window.to_string()),
        ]
    }

    async fn submit_order(
        &self,
        url: String,
        request: OrderRequest,
        allow_reduce_only: bool,
    ) -> Result<OrderResponse, RestError> {
        let OrderRequest {
            symbol,
            side,
            order_type,
            quantity,
            price,
            time_in_force,
            reduce_only,
            recv_window,
        } = request;
        let mut params = self.base_signed_params(recv_window);
        params.push(("symbol".into(), symbol));
        params.push(("side".into(), side));
        params.push(("type".into(), order_type));
        params.push(("quantity".into(), quantity.to_string()));
        if let Some(price) = price {
            params.push(("price".into(), price.to_string()));
        }
        if let Some(tif) = time_in_force {
            params.push(("timeInForce".into(), tif));
        }
        if allow_reduce_only && reduce_only {
            params.push(("reduceOnly".into(), "true".into()));
        }
        self.signed_post_json(&url, params).await
    }

    async fn signed_post_json<T: DeserializeOwned>(
        &self,
        url: &str,
        mut params: Vec<(String, String)>,
    ) -> Result<T, RestError> {
        let creds = self.require_credentials()?;
        for attempt in 0..SIGNED_MAX_RETRIES {
            self.refresh_timestamp(&mut params);
            let query = serde_urlencoded::to_string(&params)
                .map_err(|_| RestError::InvalidPayload("encode params"))?;
            let signature = sign_payload(&creds.api_secret, &query);
            let body = format!("{query}&signature={signature}");
            let mut headers = HeaderMap::new();
            headers.insert(
                "X-MBX-APIKEY",
                HeaderValue::from_str(&creds.api_key)
                    .map_err(|_| RestError::InvalidPayload("api key"))?,
            );
            headers.insert(
                CONTENT_TYPE,
                HeaderValue::from_static("application/x-www-form-urlencoded"),
            );
            let resp = self
                .http
                .post(url)
                .headers(headers)
                .body(body)
                .send()
                .await?;
            let status = resp.status();
            let payload = resp.text().await?;
            if status.is_success() {
                let data = serde_json::from_str(&payload)
                    .map_err(|_| RestError::InvalidPayload("decode response"))?;
                return Ok(data);
            }
            let code = status.as_u16();
            if attempt + 1 < SIGNED_MAX_RETRIES && Self::is_timestamp_error(code, &payload) {
                if self.sync_server_time().await.is_ok() {
                    continue;
                }
                sleep(Duration::from_millis(TIMESTAMP_RETRY_DELAY_MS)).await;
                continue;
            }
            return Err(RestError::HttpStatus {
                code,
                body: payload,
            });
        }
        Err(RestError::HttpStatus {
            code: 400,
            body: "timestamp retry exhausted".to_string(),
        })
    }

    async fn signed_get_json<T: DeserializeOwned>(
        &self,
        url: &str,
        mut params: Vec<(String, String)>,
    ) -> Result<T, RestError> {
        let creds = self.require_credentials()?;
        for attempt in 0..SIGNED_MAX_RETRIES {
            self.refresh_timestamp(&mut params);
            let query = serde_urlencoded::to_string(&params)
                .map_err(|_| RestError::InvalidPayload("encode params"))?;
            let signature = sign_payload(&creds.api_secret, &query);
            let mut headers = HeaderMap::new();
            headers.insert(
                "X-MBX-APIKEY",
                HeaderValue::from_str(&creds.api_key)
                    .map_err(|_| RestError::InvalidPayload("api key"))?,
            );
            let full_url = format!("{url}?{query}&signature={signature}");
            let resp = self.http.get(full_url).headers(headers).send().await?;
            let status = resp.status();
            let payload = resp.text().await?;
            if status.is_success() {
                match serde_json::from_str(&payload) {
                    Ok(data) => return Ok(data),
                    Err(err) => {
                        warn!(
                            ?err,
                            endpoint = url,
                            payload = %Self::truncate_payload(&payload, 2048),
                            "failed to decode response"
                        );
                        return Err(RestError::InvalidPayload("decode response"));
                    }
                }
            }
            let code = status.as_u16();
            if attempt + 1 < SIGNED_MAX_RETRIES && Self::is_timestamp_error(code, &payload) {
                if self.sync_server_time().await.is_ok() {
                    continue;
                }
                sleep(Duration::from_millis(TIMESTAMP_RETRY_DELAY_MS)).await;
                continue;
            }
            return Err(RestError::HttpStatus {
                code,
                body: payload,
            });
        }
        Err(RestError::HttpStatus {
            code: 400,
            body: "timestamp retry exhausted".to_string(),
        })
    }

    fn truncate_payload(payload: &str, max_len: usize) -> String {
        if payload.len() <= max_len {
            return payload.to_string();
        }
        let mut out = payload.chars().take(max_len).collect::<String>();
        out.push_str("...");
        out
    }
}

impl RestClient {
    fn refresh_timestamp(&self, params: &mut Vec<(String, String)>) {
        let ts = self.current_timestamp();
        if let Some(entry) = params.iter_mut().find(|(key, _)| key == "timestamp") {
            entry.1 = ts.to_string();
        } else {
            params.push(("timestamp".into(), ts.to_string()));
        }
    }

    fn current_timestamp(&self) -> i64 {
        Utc::now().timestamp_millis() + self.time_offset_ms.load(Ordering::Relaxed)
    }

    async fn sync_server_time(&self) -> Result<(), RestError> {
        let url = format!("{}/fapi/v1/time", self.futures_endpoint);
        let resp = self.http.get(&url).send().await?;
        let status = resp.status();
        let payload = resp.text().await?;
        if !status.is_success() {
            return Err(RestError::HttpStatus {
                code: status.as_u16(),
                body: payload,
            });
        }
        let data: ServerTimeResponse = serde_json::from_str(&payload)
            .map_err(|_| RestError::InvalidPayload("decode server time"))?;
        let local = Utc::now().timestamp_millis();
        self.time_offset_ms
            .store(data.server_time - local, Ordering::Relaxed);
        Ok(())
    }

    fn is_timestamp_error(code: u16, body: &str) -> bool {
        if code != 400 {
            return false;
        }
        match serde_json::from_str::<Value>(body).ok() {
            Some(Value::Object(map)) => map
                .get("code")
                .and_then(|v| v.as_i64())
                .map(|v| v == TIMESTAMP_ERROR_CODE)
                .unwrap_or(false),
            _ => false,
        }
    }
}

impl RestClient {
    /// Spot venue base order submission (single order).
    pub async fn place_spot_order(
        &self,
        request: OrderRequest,
    ) -> Result<OrderResponse, RestError> {
        let url = format!("{}/api/v3/order", self.spot_endpoint);
        self.submit_order(url, request, false).await
    }

    /// Spot venue taker (market) order helpers.
    pub async fn place_spot_taker_order(
        &self,
        order: MarketOrderParams,
    ) -> Result<OrderResponse, RestError> {
        self.place_spot_order(order.into()).await
    }

    /// Spot venue maker (limit) order helpers.
    pub async fn place_spot_maker_order(
        &self,
        order: LimitOrderParams,
    ) -> Result<OrderResponse, RestError> {
        self.place_spot_order(order.into()).await
    }

    /// There is no native batch API for spot orders, so fire requests concurrently.
    pub async fn place_spot_batch_taker_orders(
        &self,
        orders: Vec<MarketOrderParams>,
    ) -> Result<Vec<OrderResponse>, RestError> {
        let tasks = orders
            .into_iter()
            .map(|order| self.place_spot_taker_order(order));
        try_join_all(tasks).await
    }

    /// There is no native batch API for spot orders, so fire requests concurrently.
    pub async fn place_spot_batch_maker_orders(
        &self,
        orders: Vec<LimitOrderParams>,
    ) -> Result<Vec<OrderResponse>, RestError> {
        let tasks = orders
            .into_iter()
            .map(|order| self.place_spot_maker_order(order));
        try_join_all(tasks).await
    }
}

impl RestClient {
    /// Futures/perp base order submission (single order).
    pub async fn place_perp_order(
        &self,
        request: OrderRequest,
    ) -> Result<OrderResponse, RestError> {
        let url = format!("{}/fapi/v1/order", self.futures_endpoint);
        self.submit_order(url, request, true).await
    }

    /// Futures/perp venue taker (market) order helpers.
    pub async fn place_perp_taker_order(
        &self,
        order: MarketOrderParams,
    ) -> Result<OrderResponse, RestError> {
        self.place_perp_order(order.into()).await
    }

    /// Futures/perp venue maker (limit) order helpers.
    pub async fn place_perp_maker_order(
        &self,
        order: LimitOrderParams,
    ) -> Result<OrderResponse, RestError> {
        self.place_perp_order(order.into()).await
    }

    pub async fn place_perp_batch_taker_orders(
        &self,
        orders: Vec<MarketOrderParams>,
    ) -> Result<Vec<OrderResponse>, RestError> {
        let mut responses = Vec::with_capacity(orders.len());
        for order in orders {
            responses.push(self.place_perp_taker_order(order).await?);
        }
        Ok(responses)
    }

    pub async fn place_perp_batch_maker_orders(
        &self,
        orders: Vec<LimitOrderParams>,
    ) -> Result<Vec<OrderResponse>, RestError> {
        let mut responses = Vec::with_capacity(orders.len());
        for order in orders {
            responses.push(self.place_perp_maker_order(order).await?);
        }
        Ok(responses)
    }
}

impl RestClient {
    /// Margin account base order submission (single order).
    pub async fn place_margin_order(
        &self,
        request: OrderRequest,
    ) -> Result<OrderResponse, RestError> {
        let url = format!("{}/sapi/v1/margin/order", self.spot_endpoint);
        self.submit_order(url, request, true).await
    }

    /// Margin venue taker (market) order helpers.
    pub async fn place_margin_taker_order(
        &self,
        order: MarketOrderParams,
    ) -> Result<OrderResponse, RestError> {
        self.place_margin_order(order.into()).await
    }

    /// Margin venue maker (limit) order helpers.
    pub async fn place_margin_maker_order(
        &self,
        order: LimitOrderParams,
    ) -> Result<OrderResponse, RestError> {
        self.place_margin_order(order.into()).await
    }

    /// Margin endpoints also lack batch APIs, so use concurrent POSTs.
    pub async fn place_margin_batch_taker_orders(
        &self,
        orders: Vec<MarketOrderParams>,
    ) -> Result<Vec<OrderResponse>, RestError> {
        let tasks = orders
            .into_iter()
            .map(|order| self.place_margin_taker_order(order));
        try_join_all(tasks).await
    }

    /// Margin endpoints also lack batch APIs, so use concurrent POSTs.
    pub async fn place_margin_batch_maker_orders(
        &self,
        orders: Vec<LimitOrderParams>,
    ) -> Result<Vec<OrderResponse>, RestError> {
        let tasks = orders
            .into_iter()
            .map(|order| self.place_margin_maker_order(order));
        try_join_all(tasks).await
    }

    pub async fn set_perp_leverage(&self, symbol: &str, leverage: u32) -> Result<Value, RestError> {
        let target = leverage.max(1).min(125);
        let url = format!("{}/fapi/v1/leverage", self.futures_endpoint);
        let mut params = self.base_signed_params(None);
        params.push(("symbol".into(), symbol.to_string()));
        params.push(("leverage".into(), target.to_string()));
        self.signed_post_json(&url, params).await
    }

    pub async fn borrow_margin_asset(&self, asset: &str, amount: f64) -> Result<Value, RestError> {
        let url = format!("{}/sapi/v1/margin/loan", self.spot_endpoint);
        let mut params = self.base_signed_params(None);
        params.push(("asset".into(), asset.to_string()));
        params.push(("amount".into(), amount.to_string()));
        self.signed_post_json(&url, params).await
    }

    pub async fn repay_margin_asset(&self, asset: &str, amount: f64) -> Result<Value, RestError> {
        let url = format!("{}/sapi/v1/margin/repay", self.spot_endpoint);
        let mut params = self.base_signed_params(None);
        params.push(("asset".into(), asset.to_string()));
        params.push(("amount".into(), amount.to_string()));
        self.signed_post_json(&url, params).await
    }

    pub async fn fetch_margin_assets(&self) -> Result<Vec<MarginAssetStatus>, RestError> {
        let url = format!("{}/sapi/v1/margin/allAssets", self.spot_endpoint);
        let params = self.base_signed_params(None);
        self.signed_get_json(&url, params).await
    }

    pub async fn fetch_margin_interest_history(
        &self,
        asset: &str,
        start: i64,
        end: i64,
    ) -> Result<Vec<MarginInterestRecord>, RestError> {
        let url = format!("{}/sapi/v1/margin/interestHistory", self.spot_endpoint);
        let mut params = self.base_signed_params(None);
        params.push(("asset".into(), asset.to_string()));
        params.push(("startTime".into(), start.to_string()));
        params.push(("endTime".into(), end.to_string()));
        let resp: MarginInterestResponse = self.signed_get_json(&url, params).await?;
        Ok(resp.rows)
    }
}

fn sign_payload(secret: &str, payload: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key size");
    mac.update(payload.as_bytes());
    let result = mac.finalize();
    hex::encode(result.into_bytes())
}

fn build_symbol_filters(symbol: ExchangeInfoSymbol) -> SymbolFilters {
    let mut filters = SymbolFilters::default();
    for filter in symbol.filters {
        let ExchangeInfoFilter {
            filter_type,
            min_qty,
            max_qty,
            step_size,
            tick_size,
            min_price,
            max_price,
            min_notional,
        } = filter;
        match filter_type.as_str() {
            "LOT_SIZE" | "MARKET_LOT_SIZE" => {
                let parsed_step = parse_opt_f64(step_size);
                let parsed_min = parse_opt_f64(min_qty);
                let parsed_max = parse_opt_f64(max_qty);
                if filters.step_size.is_none() {
                    filters.step_size = parsed_step;
                }
                if filters.min_qty.is_none() {
                    filters.min_qty = parsed_min;
                }
                if filters.max_qty.is_none() {
                    filters.max_qty = parsed_max;
                }
            }
            "PRICE_FILTER" => {
                let parsed_tick = parse_opt_f64(tick_size);
                let parsed_min_price = parse_opt_f64(min_price);
                let parsed_max_price = parse_opt_f64(max_price);
                if filters.tick_size.is_none() {
                    filters.tick_size = parsed_tick;
                }
                if filters.min_price.is_none() {
                    filters.min_price = parsed_min_price;
                }
                if filters.max_price.is_none() {
                    filters.max_price = parsed_max_price;
                }
            }
            "MIN_NOTIONAL" | "NOTIONAL" => {
                if filters.min_notional.is_none() {
                    filters.min_notional = parse_opt_f64(min_notional);
                }
            }
            _ => {}
        }
    }
    filters
}

fn parse_opt_f64(value: Option<String>) -> Option<f64> {
    value.and_then(|raw| raw.parse::<f64>().ok())
}
