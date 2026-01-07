#![allow(dead_code)]

use anyhow::{anyhow, Result};
use common::{
    constant::{
        DEFAULT_ORDER_ACCOUNT_LISTEN_WINDOW_SECS, DEFAULT_ORDER_PERP_LEVERAGE,
        DEFAULT_ORDER_TOTAL_NOTIONAL_USD,
    },
    logger, AccountSnapshot, Event, Venue,
};
use connector::{
    BinanceCredentials, BinanceWebsocketClient, ConnectorConfig, OrderRequest, OrderResponse,
    RestClient, SymbolFilters,
};
use dotenvy::dotenv;
use std::env;
use tokio::sync::{mpsc, oneshot};
use tokio::time::{sleep, Duration};
use tracing::warn;

/// 通过订单簿确定价位，下单后继续监听账户 websocket 并把快照打印出来，方便快速验证流程。
/// 新增一个美元额度参数，按照设定的合约杠杆拆分成“现货多 + 合约空”的对冲头寸。
pub async fn stream_then_order() -> Result<(AccountSnapshot, OrderResponse, OrderResponse)> {
    let api_key = env::var("BINANCE_API_KEY").map_err(|_| anyhow!("BINANCE_API_KEY not set"))?;
    let api_secret =
        env::var("BINANCE_API_SECRET").map_err(|_| anyhow!("BINANCE_API_SECRET not set"))?;
    let symbol = "SUIUSDT".to_string();
    let cfg = ConnectorConfig {
        symbols: vec![symbol.clone()],
        credentials: Some(BinanceCredentials {
            api_key,
            api_secret,
        }),
        ..Default::default()
    };

    let (tx, rx) = mpsc::channel(32);
    let ws_client = BinanceWebsocketClient::new(cfg.clone(), tx);
    let ws_handle = tokio::spawn(async move {
        ws_client.subscribe_account_updates().await;
    });

    let (snapshot_tx, snapshot_rx) = oneshot::channel();
    let printer_handle = tokio::spawn(async move {
        let mut rx = rx;
        let mut first_snapshot_tx = Some(snapshot_tx);
        while let Some(event) = rx.recv().await {
            if let Event::AccountSnapshot(snapshot) = event {
                println!("snapshot: {:?}", snapshot);
                println!(
                    "[account] {} equity {:.2} USD ({} positions) @ {}",
                    snapshot.account_id,
                    snapshot.equity_usd,
                    snapshot.positions.len(),
                    snapshot.updated_ms
                );
                for position in &snapshot.positions {
                    println!(
                        "    - {} spot {:+.4} perp {:+.4}",
                        position.symbol, position.spot_qty, position.perp_qty
                    );
                }
                if let Some(tx) = first_snapshot_tx.take() {
                    let _ = tx.send(snapshot.clone());
                }
            }
        }
    });

    let snapshot = snapshot_rx
        .await
        .map_err(|_| anyhow!("websocket stream ended before first account snapshot"))?;

    let total_notional = env::var("TOTAL_NOTIONAL_USD")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(DEFAULT_ORDER_TOTAL_NOTIONAL_USD);
    let perp_leverage = env::var("PERP_LEVERAGE")
        .ok()
        .and_then(|v| v.parse::<f64>().ok())
        .unwrap_or(DEFAULT_ORDER_PERP_LEVERAGE);
    if total_notional <= 0.0 || perp_leverage <= 0.0 {
        return Err(anyhow!("invalid TOTAL_NOTIONAL_USD or PERP_LEVERAGE"));
    }
    let spot_share = perp_leverage / (perp_leverage + 1.0);
    let hedge_notional = total_notional * spot_share;
    let perp_margin = total_notional - hedge_notional;
    println!(
        "[alloc] total {:.4} USD -> spot exposure {:.4} USD, perp margin {:.4} USD (target hedge {:.4} USD, leverage {:.2}x)",
        total_notional, hedge_notional, perp_margin, hedge_notional, perp_leverage
    );

    let mut rest_client = RestClient::from_config(&cfg);
    let spot_filters = match rest_client.fetch_spot_symbol_filters(&symbol).await {
        Ok(filters) => filters,
        Err(err) => fallback_filters(&symbol, Venue::Spot)
            .ok_or_else(|| anyhow!("failed to fetch spot filters: {err}"))?,
    };
    let perp_filters = match rest_client.fetch_perp_symbol_filters(&symbol).await {
        Ok(filters) => filters,
        Err(err) => fallback_filters(&symbol, Venue::Perp)
            .ok_or_else(|| anyhow!("failed to fetch perp filters: {err}"))?,
    };
    let spot_depth = rest_client
        .fetch_orderbook_snapshot(&symbol, 5, Venue::Spot)
        .await
        .map_err(|err| anyhow!("failed to fetch spot orderbook: {err}"))?;
    let perp_depth = rest_client
        .fetch_orderbook_snapshot(&symbol, 5, Venue::Perp)
        .await
        .map_err(|err| anyhow!("failed to fetch perp orderbook: {err}"))?;

    let best_ask = spot_depth
        .asks
        .first()
        .ok_or_else(|| anyhow!("spot orderbook missing asks"))?;
    let best_bid = perp_depth
        .bids
        .first()
        .ok_or_else(|| anyhow!("perp orderbook missing bids"))?;

    let spot_price = spot_filters.quantize_price(best_ask.price);
    let perp_price = perp_filters.quantize_price(best_bid.price);
    let spot_qty_raw = (hedge_notional / spot_price).max(0.0);
    let perp_qty_raw = (hedge_notional / perp_price).max(0.0);
    let spot_qty = apply_filters(spot_qty_raw, spot_price, &spot_filters, "spot")?;
    let perp_qty = apply_filters(perp_qty_raw, perp_price, &perp_filters, "perp")?;
    println!(
        "[order] spot BUY {} qty {:.6} (market, ref {:.4})",
        symbol, spot_qty, spot_price
    );
    println!(
        "[order] perp SELL {} qty {:.6} (market, ref {:.4})",
        symbol, perp_qty, perp_price
    );

    let spot_order = OrderRequest::market(symbol.clone(), "BUY", spot_qty);
    let spot_response = rest_client.place_spot_order(spot_order).await?;
    let perp_order = OrderRequest::market(symbol.clone(), "SELL", perp_qty);
    let perp_response = rest_client.place_perp_order(perp_order).await?;
    println!(
        "[order] spot order_id {} status {} | perp order_id {} status {}",
        spot_response.order_id, spot_response.status, perp_response.order_id, perp_response.status
    );

    println!(
        "[websocket] listening for {} seconds to capture account updates...",
        DEFAULT_ORDER_ACCOUNT_LISTEN_WINDOW_SECS
    );
    sleep(Duration::from_secs(
        DEFAULT_ORDER_ACCOUNT_LISTEN_WINDOW_SECS,
    ))
    .await;

    ws_handle.abort();
    printer_handle.abort();

    Ok((snapshot, spot_response, perp_response))
}

pub async fn borrow_margin_and_sell_and_buy_perp() -> Result<()> {
    let api_key = env::var("BINANCE_API_KEY").map_err(|_| anyhow!("BINANCE_API_KEY not set"))?;
    let api_secret =
        env::var("BINANCE_API_SECRET").map_err(|_| anyhow!("BINANCE_API_SECRET not set"))?;
    let symbol = "SUIUSDT".to_string();
    const TARGET_NOTIONAL_USD: f64 = 6.0;
    const MARGIN_LEVERAGE: f64 = 5.0;
    const PERP_LEVERAGE: f64 = 5.0;
    println!(
        "[setup] symbol {symbol}, target_notional {:.2} USD, margin_leverage {:.1}x, perp_leverage {:.1}x",
        TARGET_NOTIONAL_USD, MARGIN_LEVERAGE, PERP_LEVERAGE
    );

    let cfg = ConnectorConfig {
        symbols: vec![symbol.clone()],
        credentials: Some(BinanceCredentials {
            api_key,
            api_secret,
        }),
        ..Default::default()
    };
    let mut rest_client = RestClient::from_config(&cfg);

    let spot_filters = rest_client
        .fetch_spot_symbol_filters(&symbol)
        .await
        .or_else(|err| {
            warn!("failed to fetch spot filters: {err:?}, using fallback");
            fallback_filters(&symbol, Venue::Spot)
                .ok_or_else(|| anyhow!("no fallback spot filters for {symbol}"))
        })?;
    let perp_filters = rest_client
        .fetch_perp_symbol_filters(&symbol)
        .await
        .or_else(|err| {
            warn!("failed to fetch perp filters: {err:?}, using fallback");
            fallback_filters(&symbol, Venue::Perp)
                .ok_or_else(|| anyhow!("no fallback perp filters for {symbol}"))
        })?;

    let spot_depth = rest_client
        .fetch_orderbook_snapshot(&symbol, 5, Venue::Spot)
        .await?;
    let perp_depth = rest_client
        .fetch_orderbook_snapshot(&symbol, 5, Venue::Perp)
        .await?;
    let spot_price = spot_filters
        .quantize_price(
            spot_depth
                .asks
                .first()
                .ok_or_else(|| anyhow!("spot book missing asks"))?
                .price,
        )
        .max(1e-8);
    let perp_price = perp_filters
        .quantize_price(
            perp_depth
                .bids
                .first()
                .ok_or_else(|| anyhow!("perp book missing bids"))?
                .price,
        )
        .max(1e-8);
    let raw_qty = TARGET_NOTIONAL_USD / spot_price;
    let spot_qty = apply_filters(raw_qty, spot_price, &spot_filters, "spot")?;
    let perp_qty = apply_filters(raw_qty, perp_price, &perp_filters, "perp")?;
    println!(
        "[calc] spot_price {:.4}, perp_price {:.4}, qty spot {:.6}, qty perp {:.6}",
        spot_price, perp_price, spot_qty, perp_qty
    );

    let asset = symbol.trim_end_matches("USDT");
    println!(
        "[margin] borrowing {:.6} {} (configured leverage {:.1}x)",
        spot_qty, asset, MARGIN_LEVERAGE
    );
    rest_client
        .borrow_margin_asset(asset, spot_qty)
        .await
        .map_err(|err| anyhow!("margin borrow failed: {err:?}"))?;

    let sell_order = OrderRequest::market(symbol.clone(), "SELL", spot_qty);
    let sell_resp = rest_client
        .place_margin_order(sell_order)
        .await
        .map_err(|err| anyhow!("margin sell failed: {err:?}"))?;
    println!(
        "[margin] SELL {} qty {:.6} -> status {}",
        symbol, spot_qty, sell_resp.status
    );

    println!(
        "[perp] buying {:.6} contracts (target leverage {:.1}x)",
        perp_qty, PERP_LEVERAGE
    );
    let buy_order = OrderRequest::market(symbol.clone(), "BUY", perp_qty);
    let buy_resp = rest_client
        .place_perp_order(buy_order)
        .await
        .map_err(|err| anyhow!("perp buy failed: {err:?}"))?;
    println!(
        "[perp] BUY {} qty {:.6} -> status {}",
        symbol, perp_qty, buy_resp.status
    );
    println!("[done] margin short + perp long executed");
    Ok(())
}

fn apply_filters(qty: f64, price: f64, filters: &SymbolFilters, venue_label: &str) -> Result<f64> {
    let adjusted = filters.quantize_quantity(qty);
    if adjusted <= 0.0 {
        return Err(anyhow!(
            "{} quantity rounded down to zero, increase TOTAL_NOTIONAL_USD",
            venue_label
        ));
    }
    if let Some(min_qty) = filters.min_qty {
        if adjusted + 1e-12 < min_qty {
            return Err(anyhow!(
                "{} quantity {:.8} below minimum lot size {:.8}",
                venue_label,
                adjusted,
                min_qty
            ));
        }
    }
    if let Some(max_qty) = filters.max_qty {
        if adjusted > max_qty + 1e-12 {
            return Err(anyhow!(
                "{} quantity {:.8} exceeds max lot size {:.8}",
                venue_label,
                adjusted,
                max_qty
            ));
        }
    }
    if let Some(min_notional) = filters.min_notional {
        let notional = adjusted * price;
        if notional + 1e-8 < min_notional {
            return Err(anyhow!(
                "{} notional {:.8} below minimum {:.8}",
                venue_label,
                notional,
                min_notional
            ));
        }
    }
    Ok(adjusted)
}

fn fallback_filters(symbol: &str, venue: Venue) -> Option<SymbolFilters> {
    match (symbol, venue) {
        ("SUIUSDT", Venue::Spot) | ("SUIUSDT", Venue::Perp) => Some(SymbolFilters {
            step_size: Some(0.1),
            min_qty: Some(0.1),
            max_qty: None,
            tick_size: Some(0.0001),
            min_price: None,
            max_price: None,
            min_notional: Some(5.0),
        }),
        _ => None,
    }
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    logger::init_logging();
    // let (snapshot, spot_order, perp_order) = stream_then_order().await?;
    // println!(
    //     "Received account snapshot at {} with equity {:.2} USD",
    //     snapshot.updated_ms, snapshot.equity_usd
    // );
    // println!(
    //     "Spot order {} status {}, Perp order {} status {}",
    //     spot_order.order_id, spot_order.status, perp_order.order_id, perp_order.status
    // );
    borrow_margin_and_sell_and_buy_perp().await?;
    Ok(())
}
