use std::cmp::Ordering;
use std::fs;
use std::path::Path;

use anyhow::Result;
use chrono::Utc;
use common::Symbol;
use connector::{
    fetch_recent_klines, generate_mock_funding_history, generate_mock_liquidity_samples,
};
use serde::Serialize;
use tracing::info;

const TOP_K: usize = 3;
const HISTORY_POINTS: usize = 21;

#[derive(Serialize)]
struct UniverseEntry {
    symbol: Symbol,
    funding_score: f64,
    liquidity_score: f64,
    avg_close: f64,
    as_of_ms: i64,
}

pub async fn run_research() -> Result<()> {
    let symbols = vec![
        "BTCUSDT".to_string(),
        "ETHUSDT".to_string(),
        "SOLUSDT".to_string(),
        "BNBUSDT".to_string(),
    ];

    let funding_history = generate_mock_funding_history(&symbols, HISTORY_POINTS).await;
    let liquidity = generate_mock_liquidity_samples(&symbols).await;

    let mut scored = Vec::new();
    for symbol in &symbols {
        let funding_points = funding_history
            .get(symbol)
            .map(|series| {
                series.iter().map(|p| p.funding_rate.abs()).sum::<f64>()
                    / series.len().max(1) as f64
            })
            .unwrap_or_default();
        let liquidity_score = liquidity
            .get(symbol)
            .map(|sample| sample.total_depth)
            .unwrap_or_default();
        let klines = fetch_recent_klines(symbol, 50).await;
        let avg_close = if klines.is_empty() {
            0.0
        } else {
            klines.iter().map(|k| k.close).sum::<f64>() / klines.len() as f64
        };
        let total = funding_points * 10_000.0 + liquidity_score + avg_close / 100.0;
        scored.push((
            symbol.clone(),
            total,
            funding_points,
            liquidity_score,
            avg_close,
        ));
    }

    scored.sort_by(|a, b| b.1.partial_cmp(&a.1).unwrap_or(Ordering::Equal));

    let as_of = Utc::now().timestamp_millis();
    let universe: Vec<UniverseEntry> = scored
        .into_iter()
        .take(TOP_K)
        .map(
            |(symbol, _, funding_score, liquidity_score, avg_close)| UniverseEntry {
                symbol,
                funding_score,
                liquidity_score,
                avg_close,
                as_of_ms: as_of,
            },
        )
        .collect();

    let body = serde_json::to_string_pretty(&universe)?;
    let path = Path::new("universe.json");
    fs::write(path, &body)?;
    info!(?path, "universe written");
    info!(body = %body, "universe selection emitted");
    Ok(())
}
