use std::collections::HashMap;
use std::time::Duration;

use anyhow::Result;
use chrono::Utc;
pub use common::price::PriceCandle;
use common::{
    constant::{BINANCE_FUTURES_API_BASE, BINANCE_MAX_KLINE_LIMIT, BINANCE_SPOT_API_BASE},
    Venue,
};
use reqwest::Client;
use serde::Serialize;
use serde_json::Value;

use crate::time::rolling_window_hours;

const HTTP_TIMEOUT_SECS: u64 = 15;
const HOURS_IN_LOOKBACK: i64 = 24 * 90;
const HOUR_MS: i64 = 60 * 60 * 1000;

pub async fn fetch_hourly_price_history(
    symbols: &[String],
    venue: Venue,
) -> Result<HashMap<String, Vec<PriceCandle>>> {
    let client = http_client();
    let (start, end) = rolling_window_hours(HOURS_IN_LOOKBACK);
    let base_url = match venue {
        Venue::Spot => BINANCE_SPOT_API_BASE,
        Venue::Perp => BINANCE_FUTURES_API_BASE,
    };
    let mut out = HashMap::new();
    for symbol in symbols {
        let candles = fetch_symbol_klines(&client, base_url, symbol, start, end, venue).await?;
        out.insert(symbol.clone(), candles);
    }
    Ok(out)
}

pub async fn fetch_hourly_range(
    symbol: &str,
    venue: Venue,
    start_time: i64,
    end_time: i64,
) -> Result<Vec<PriceCandle>> {
    let client = http_client();
    let base_url = match venue {
        Venue::Spot => BINANCE_SPOT_API_BASE,
        Venue::Perp => BINANCE_FUTURES_API_BASE,
    };
    fetch_symbol_klines(&client, base_url, symbol, start_time, end_time, venue).await
}

pub async fn fetch_recent_hourly_candles(
    symbol: &str,
    venue: Venue,
    hours: i64,
) -> Result<Vec<PriceCandle>> {
    let end = align_to_hour(Utc::now().timestamp_millis());
    let start = end - hours.max(1) * HOUR_MS;
    fetch_hourly_range(symbol, venue, start, end).await
}

#[derive(Serialize)]
struct KlineQuery<'a> {
    symbol: &'a str,
    interval: &'a str,
    #[serde(rename = "startTime")]
    start_time: i64,
    #[serde(rename = "endTime")]
    end_time: i64,
    limit: usize,
}

fn http_client() -> Client {
    Client::builder()
        .timeout(Duration::from_secs(HTTP_TIMEOUT_SECS))
        .user_agent("tradeterminal-fetch-price")
        .build()
        .expect("http client")
}

async fn fetch_symbol_klines(
    client: &Client,
    base_url: &str,
    symbol: &str,
    start: i64,
    end: i64,
    venue: Venue,
) -> Result<Vec<PriceCandle>> {
    let mut candles = Vec::new();
    let mut cursor = start;
    let path = match venue {
        Venue::Spot => "api/v3/klines",
        Venue::Perp => "fapi/v1/klines",
    };
    while cursor < end {
        let url = format!("{}/{}", base_url, path);
        let limit = BINANCE_MAX_KLINE_LIMIT.min(((end - cursor) / HOUR_MS).max(1) as usize);
        let params = KlineQuery {
            symbol,
            interval: "1h",
            start_time: cursor,
            end_time: end,
            limit,
        };
        let resp: Vec<Vec<Value>> = client
            .get(&url)
            .query(&params)
            .send()
            .await?
            .error_for_status()?
            .json()
            .await?;
        if resp.is_empty() {
            break;
        }
        for row in &resp {
            if row.len() < 7 {
                continue;
            }
            let open_time = row[0].as_i64().unwrap_or(0);
            let open = parse_f64(&row[1]);
            let high = parse_f64(&row[2]);
            let low = parse_f64(&row[3]);
            let close = parse_f64(&row[4]);
            let volume = parse_f64(&row[5]);
            let close_time = row[6].as_i64().unwrap_or(open_time + HOUR_MS);
            if let (Some(open), Some(high), Some(low), Some(close), Some(volume)) =
                (open, high, low, close, volume)
            {
                candles.push(PriceCandle {
                    venue,
                    symbol: symbol.to_string(),
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
        let last = resp
            .last()
            .and_then(|row| row.get(6))
            .and_then(|val| val.as_i64())
            .unwrap_or(cursor);
        if last <= cursor {
            break;
        }
        cursor = last + 1;
    }
    Ok(candles)
}

fn parse_f64(value: &Value) -> Option<f64> {
    match value {
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Number(num) => num.as_f64(),
        _ => None,
    }
}

pub fn align_to_hour(ts_ms: i64) -> i64 {
    if ts_ms <= 0 {
        return 0;
    }
    let remainder = ts_ms % HOUR_MS;
    ts_ms - remainder
}
