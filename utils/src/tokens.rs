use std::collections::{HashMap, HashSet};
use std::time::Duration;

use anyhow::Result;
use common::constant::{BINANCE_FUTURES_API_BASE, BINANCE_SPOT_API_BASE};
use reqwest::Client;
use serde::Deserialize;

const HTTP_TIMEOUT_SECS: u64 = 15;
const SPOT_MAKER_FEE_VIP0: f64 = 0.0010;
const SPOT_TAKER_FEE_VIP0: f64 = 0.0010;
const PERP_MAKER_FEE_VIP0: f64 = 0.0002;
const PERP_TAKER_FEE_VIP0: f64 = 0.0005;

/// Basic trading metadata for a symbol.
#[derive(Clone, Debug)]
pub struct TokenInfo {
    pub symbol: String,
    pub has_spot: bool,
    pub has_perp: bool,
    pub margin_enabled: bool,
    pub spot_taker_fee: Option<f64>,
    pub spot_maker_fee: Option<f64>,
    pub perp_taker_fee: Option<f64>,
    pub perp_maker_fee: Option<f64>,
    pub funding_interval_secs: Option<i64>,
}

pub async fn fetch_all_tokens() -> Result<Vec<TokenInfo>> {
    let client = http_client();
    let (spot, perp) = tokio::try_join!(fetch_spot_symbols(&client), fetch_perp_symbols(&client))?;

    let universe: HashSet<String> = spot.keys().cloned().chain(perp.keys().cloned()).collect();

    let mut out: Vec<TokenInfo> = universe
        .into_iter()
        .map(|symbol| {
            let spot_entry = spot.get(&symbol);
            let perp_entry = perp.get(&symbol);
            let spot_meta = spot_entry.map(|info| (info.is_spot_allowed, info.margin_allowed));
            let has_spot = spot_meta.map(|meta| meta.0).unwrap_or(false);
            let has_perp = perp_entry.is_some();
            TokenInfo {
                symbol,
                has_spot,
                has_perp,
                margin_enabled: spot_meta.map(|meta| meta.1).unwrap_or(false),
                spot_taker_fee: if has_spot {
                    Some(SPOT_TAKER_FEE_VIP0)
                } else {
                    None
                },
                spot_maker_fee: if has_spot {
                    Some(SPOT_MAKER_FEE_VIP0)
                } else {
                    None
                },
                perp_taker_fee: if has_perp {
                    Some(PERP_TAKER_FEE_VIP0)
                } else {
                    None
                },
                perp_maker_fee: if has_perp {
                    Some(PERP_MAKER_FEE_VIP0)
                } else {
                    None
                },
                funding_interval_secs: perp_entry.map(|info| info.funding_interval_secs),
            }
        })
        .filter(|token| token.symbol.ends_with("USDT"))
        .collect();
    out.sort_by(|a, b| a.symbol.cmp(&b.symbol));
    Ok(out)
}

#[derive(Deserialize)]
struct SpotExchangeResponse {
    symbols: Vec<SpotSymbol>,
}

#[derive(Deserialize)]
struct SpotSymbol {
    symbol: String,
    status: String,
    #[serde(rename = "isMarginTradingAllowed")]
    is_margin_trading_allowed: bool,
    #[serde(rename = "isSpotTradingAllowed")]
    is_spot_trading_allowed: bool,
}

#[derive(Clone, Debug)]
struct SymbolFeeInfo {
    funding_interval_secs: i64,
    is_spot_allowed: bool,
    margin_allowed: bool,
}

#[derive(Deserialize)]
struct PerpExchangeResponse {
    symbols: Vec<PerpSymbol>,
}

#[derive(Deserialize)]
struct PerpSymbol {
    symbol: String,
    status: String,
}

fn http_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
        .user_agent("tradeterminal-fetch-token")
        .build()
        .expect("http client")
}

async fn fetch_spot_symbols(client: &Client) -> Result<HashMap<String, SymbolFeeInfo>> {
    let url = format!("{}/api/v3/exchangeInfo", BINANCE_SPOT_API_BASE);
    let resp: SpotExchangeResponse = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let mut map = HashMap::new();
    for symbol in resp.symbols {
        if symbol.status != "TRADING" {
            continue;
        }
        map.insert(
            symbol.symbol,
            SymbolFeeInfo {
                funding_interval_secs: 0,
                is_spot_allowed: symbol.is_spot_trading_allowed,
                margin_allowed: symbol.is_margin_trading_allowed,
            },
        );
    }
    Ok(map)
}

async fn fetch_perp_symbols(client: &Client) -> Result<HashMap<String, SymbolFeeInfo>> {
    let url = format!("{}/fapi/v1/exchangeInfo", BINANCE_FUTURES_API_BASE);
    let resp: PerpExchangeResponse = client
        .get(url)
        .send()
        .await?
        .error_for_status()?
        .json()
        .await?;
    let mut map = HashMap::new();
    for symbol in resp.symbols {
        if symbol.status != "TRADING" {
            continue;
        }
        map.insert(
            symbol.symbol,
            SymbolFeeInfo {
                funding_interval_secs: 8 * 60 * 60,
                is_spot_allowed: true,
                margin_allowed: false,
            },
        );
    }
    Ok(map)
}
