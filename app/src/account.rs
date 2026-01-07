#![allow(dead_code)]

use std::env;
use std::time::Duration;

use anyhow::{Context, Result};
use chrono::Utc;
use common::{
    constant::{BINANCE_FUTURES_API_BASE, BINANCE_SPOT_API_BASE},
    logger,
};
use dotenvy::dotenv;
use hmac::{Hmac, Mac};
use reqwest::Client;
use serde::de::DeserializeOwned;
use serde::Deserialize;
use sha2::Sha256;
use tracing::info;

const HTTP_TIMEOUT_SECS: u64 = 15;
const RECV_WINDOW_MS: i64 = 5_000;

type HmacSha256 = Hmac<Sha256>;

pub struct BinanceAccountClient {
    http: Client,
    api_key: String,
    api_secret: String,
}

impl BinanceAccountClient {
    pub fn new_from_env() -> Result<Self> {
        let api_key = env::var("BINANCE_API_KEY").context("missing BINANCE_API_KEY")?;
        let api_secret = env::var("BINANCE_API_SECRET").context("missing BINANCE_API_SECRET")?;
        let http = Client::builder()
            .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
            .user_agent("tradeterminal-account-fetch")
            .build()
            .context("failed to build http client")?;
        Ok(Self {
            http,
            api_key,
            api_secret,
        })
    }

    pub async fn fetch_futures_account(&self) -> Result<FuturesAccount> {
        let url = format!("{}/fapi/v2/account", BINANCE_FUTURES_API_BASE);
        self.signed_get(&url, vec![]).await
    }

    pub async fn fetch_spot_account(&self) -> Result<SpotAccount> {
        let url = format!("{}/api/v3/account", BINANCE_SPOT_API_BASE);
        self.signed_get(&url, vec![]).await
    }

    async fn signed_get<T>(&self, url: &str, mut params: Vec<(String, String)>) -> Result<T>
    where
        T: DeserializeOwned,
    {
        params.push((
            "timestamp".into(),
            Utc::now().timestamp_millis().to_string(),
        ));
        params.push(("recvWindow".into(), RECV_WINDOW_MS.to_string()));
        let query = serde_urlencoded::to_string(&params).context("encode params")?;
        let signature = sign_payload(&self.api_secret, &query);
        params.push(("signature".into(), signature));
        let resp = self
            .http
            .get(url)
            .header("X-MBX-APIKEY", &self.api_key)
            .query(&params)
            .send()
            .await?
            .error_for_status()?;
        Ok(resp.json::<T>().await?)
    }
}

#[derive(Debug, Deserialize)]
pub struct FuturesAccount {
    #[serde(
        rename = "totalWalletBalance",
        default,
        deserialize_with = "str_or_num_to_f64"
    )]
    pub total_wallet_balance: f64,
    #[serde(
        rename = "availableBalance",
        default,
        deserialize_with = "str_or_num_to_f64"
    )]
    pub available_balance: f64,
    #[serde(
        rename = "maxWithdrawAmount",
        default,
        deserialize_with = "str_or_num_to_f64"
    )]
    pub max_withdraw_amount: f64,
    #[serde(default)]
    pub assets: Vec<FuturesAsset>,
    #[serde(default)]
    pub positions: Vec<FuturesPosition>,
}

#[derive(Debug, Deserialize)]
pub struct FuturesAsset {
    pub asset: String,
    #[serde(
        rename = "walletBalance",
        deserialize_with = "str_or_num_to_f64",
        default
    )]
    pub wallet_balance: f64,
    #[serde(
        rename = "unrealizedProfit",
        deserialize_with = "str_or_num_to_f64",
        default
    )]
    pub unrealized_profit: f64,
    #[serde(
        rename = "marginBalance",
        deserialize_with = "str_or_num_to_f64",
        default
    )]
    pub margin_balance: f64,
    #[serde(
        rename = "maintMargin",
        deserialize_with = "str_or_num_to_f64",
        default
    )]
    pub maintenance_margin: f64,
}

#[derive(Debug, Deserialize)]
pub struct FuturesPosition {
    pub symbol: String,
    #[serde(
        rename = "positionAmt",
        deserialize_with = "str_or_num_to_f64",
        default
    )]
    pub position_amt: f64,
    #[serde(rename = "entryPrice", deserialize_with = "str_or_num_to_f64", default)]
    pub entry_price: f64,
    #[serde(
        rename = "unRealizedProfit",
        deserialize_with = "str_or_num_to_f64",
        default
    )]
    pub unrealized_profit: f64,
    #[serde(
        rename = "liquidationPrice",
        deserialize_with = "str_or_num_to_f64",
        default
    )]
    pub liquidation_price: f64,
    #[serde(rename = "marginType", default)]
    pub margin_type: String,
    #[serde(
        rename = "isolatedMargin",
        deserialize_with = "str_or_num_to_f64",
        default
    )]
    pub isolated_margin: f64,
}

#[derive(Debug, Deserialize)]
pub struct SpotAccount {
    pub balances: Vec<SpotBalance>,
}

#[derive(Debug, Deserialize)]
pub struct SpotBalance {
    pub asset: String,
    #[serde(deserialize_with = "str_or_num_to_f64", default)]
    pub free: f64,
    #[serde(deserialize_with = "str_or_num_to_f64", default)]
    pub locked: f64,
}

fn str_or_num_to_f64<'de, D>(deserializer: D) -> std::result::Result<f64, D::Error>
where
    D: serde::Deserializer<'de>,
{
    let value = serde_json::Value::deserialize(deserializer)?;
    match value {
        serde_json::Value::String(s) => s
            .parse::<f64>()
            .map_err(|err| serde::de::Error::custom(format!("{err}"))),
        serde_json::Value::Number(num) => num
            .as_f64()
            .ok_or_else(|| serde::de::Error::custom("invalid number")),
        serde_json::Value::Null => Ok(0.0),
        _ => Err(serde::de::Error::custom("unexpected type")),
    }
}

fn sign_payload(secret: &str, payload: &str) -> String {
    let mut mac = HmacSha256::new_from_slice(secret.as_bytes()).expect("HMAC accepts any key size");
    mac.update(payload.as_bytes());
    let result = mac.finalize();
    hex::encode(result.into_bytes())
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    logger::init_logging();
    let client = BinanceAccountClient::new_from_env()?;
    let futures = client.fetch_futures_account().await?;
    info!(
        wallet = futures.total_wallet_balance,
        available = futures.available_balance,
        max_withdraw = futures.max_withdraw_amount,
        "futures wallet snapshot"
    );
    for asset in futures.assets.iter().filter(|a| a.wallet_balance != 0.0) {
        info!(
            asset = %asset.asset,
            wallet = asset.wallet_balance,
            margin = asset.margin_balance,
            maintenance = asset.maintenance_margin,
            unrealized = asset.unrealized_profit,
            "futures asset"
        );
    }
    let active_positions: Vec<_> = futures
        .positions
        .iter()
        .filter(|pos| pos.position_amt.abs() > 0.0)
        .collect();
    if active_positions.is_empty() {
        info!("no open futures positions");
    } else {
        info!("active futures positions");
        for pos in active_positions {
            info!(
                symbol = %pos.symbol,
                qty = pos.position_amt,
                entry = pos.entry_price,
                liq = pos.liquidation_price,
                unrealized = pos.unrealized_profit,
                margin_type = %pos.margin_type,
                "futures position"
            );
        }
    }

    let spot = client.fetch_spot_account().await?;
    info!("spot balances with size");
    for bal in spot
        .balances
        .iter()
        .filter(|b| b.free > 0.0 || b.locked > 0.0)
    {
        info!(
            asset = %bal.asset,
            free = bal.free,
            locked = bal.locked,
            "spot balance"
        );
    }
    let spot_total: f64 = spot.balances.iter().map(|bal| bal.free + bal.locked).sum();
    info!(
        total_spot = spot_total,
        total_futures = futures.total_wallet_balance,
        total_equity = futures.total_wallet_balance + spot_total,
        "total spot + futures equity"
    );
    Ok(())
}
