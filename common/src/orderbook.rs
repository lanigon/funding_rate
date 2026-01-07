use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::{Symbol, Venue};

/// L2 price level representation (price + size stay as f64 per requirements).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Level {
    pub price: f64,
    pub size: f64,
}

impl Level {
    pub fn new(price: f64, size: f64) -> Self {
        Self { price, size }
    }
}

/// Full snapshot of venue depth that seeds the builder.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderBookSnapshot {
    pub ts_ms: i64,
    pub symbol: Symbol,
    pub venue: Venue,
    pub last_update_id: i64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// Incremental change to the book (upsert/delete semantics based on size).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderBookDelta {
    pub ts_ms: i64,
    pub symbol: Symbol,
    pub venue: Venue,
    pub first_update_id: i64,
    pub final_update_id: i64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
}

/// Top-N export shared downstream (strategy/datalayer/metrics).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderBookTopN {
    pub ts_ms: i64,
    pub symbol: Symbol,
    pub venue: Venue,
    pub update_id: i64,
    pub bids: Vec<Level>,
    pub asks: Vec<Level>,
    pub best_bid: f64,
    pub best_ask: f64,
    pub mid: f64,
    pub spread_bps: f64,
}

/// Cached Top-N state stored inside datalayer shared memory.
#[derive(Clone, Debug, Default, Serialize, Deserialize)]
pub struct BookState {
    /// Symbol this snapshot belongs to.
    pub symbol: Symbol,
    /// Venue identifier (spot/perp) for the book.
    pub venue: Venue,
    /// Millisecond timestamp when the snapshot was produced.
    pub ts_ms: i64,
    /// Top bid ladder exported for downstream consumers.
    pub bids: Vec<Level>,
    /// Top ask ladder exported for downstream consumers.
    pub asks: Vec<Level>,
    /// Current best bid price.
    pub best_bid: f64,
    /// Current best ask price.
    pub best_ask: f64,
    /// Mid price derived from the best bid/ask.
    pub mid: f64,
    /// Spread in basis points (ask-bid)/mid.
    pub spread_bps: f64,
}

#[derive(Clone, Copy, Debug, Serialize, Deserialize)]
pub enum QuoteSide {
    Bid,
    Ask,
}

#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct Quote {
    /// Side of the quote (bid or ask).
    pub side: QuoteSide,
    /// Requested/filled quantity.
    pub qty: f64,
    /// VWAP price for the requested size.
    pub avg_price: f64,
    /// Aggregate notional corresponding to the quote.
    pub notional: f64,
}

impl BookState {
    /// Returns a simple VWAP-style quote for the provided side/qty if liquidity exists.
    pub fn quote(&self, side: QuoteSide, qty: f64) -> Option<Quote> {
        if qty <= 0.0 {
            return None;
        }
        let mut remaining = qty;
        let mut notional = 0.0;
        let levels = match side {
            QuoteSide::Bid => &self.bids,
            QuoteSide::Ask => &self.asks,
        };
        for level in levels {
            if remaining <= 0.0 {
                break;
            }
            let slice = remaining.min(level.size);
            notional += slice * level.price;
            remaining -= slice;
        }
        if remaining > 1e-6 {
            return None;
        }
        Some(Quote {
            side,
            qty,
            avg_price: notional / qty,
            notional,
        })
    }
}

/// Mutable order book helper that applies exchange snapshots/deltas.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct OrderBookBuilder {
    symbol: Symbol,
    venue: Venue,
    price_scale: f64,
    bids: BTreeMap<i64, f64>,
    asks: BTreeMap<i64, f64>,
    pub last_update_id: i64,
}

impl OrderBookBuilder {
    pub fn new(symbol: Symbol, venue: Venue, price_scale: f64) -> Self {
        Self {
            symbol,
            venue,
            price_scale,
            bids: BTreeMap::new(),
            asks: BTreeMap::new(),
            last_update_id: 0,
        }
    }

    pub fn symbol(&self) -> &Symbol {
        &self.symbol
    }

    pub fn venue(&self) -> Venue {
        self.venue
    }

    pub fn price_scale(&self) -> f64 {
        self.price_scale
    }

    pub fn apply_snapshot(&mut self, snapshot: &OrderBookSnapshot) {
        self.bids.clear();
        self.asks.clear();
        for lvl in &snapshot.bids {
            let key = price_key(lvl.price, self.price_scale);
            self.bids.insert(key, lvl.size);
        }
        for lvl in &snapshot.asks {
            let key = price_key(lvl.price, self.price_scale);
            self.asks.insert(key, lvl.size);
        }
        self.last_update_id = snapshot.last_update_id;
    }

    pub fn apply_delta(&mut self, delta: &OrderBookDelta) {
        for lvl in &delta.bids {
            let key = price_key(lvl.price, self.price_scale);
            if lvl.size == 0.0 {
                self.bids.remove(&key);
            } else {
                self.bids.insert(key, lvl.size);
            }
        }
        for lvl in &delta.asks {
            let key = price_key(lvl.price, self.price_scale);
            if lvl.size == 0.0 {
                self.asks.remove(&key);
            } else {
                self.asks.insert(key, lvl.size);
            }
        }
        self.last_update_id = delta.final_update_id;
    }

    pub fn top_n(&self, n: usize, ts_ms: i64) -> OrderBookTopN {
        let mut bids = Vec::with_capacity(n);
        for (price_key, size) in self.bids.iter().rev().take(n) {
            let price = (*price_key as f64) / self.price_scale;
            bids.push(Level::new(price, *size));
        }
        let mut asks = Vec::with_capacity(n);
        for (price_key, size) in self.asks.iter().take(n) {
            let price = (*price_key as f64) / self.price_scale;
            asks.push(Level::new(price, *size));
        }
        let best_bid = bids.first().map(|lvl| lvl.price).unwrap_or(0.0);
        let best_ask = asks.first().map(|lvl| lvl.price).unwrap_or(0.0);
        let mid = if best_bid > 0.0 && best_ask > 0.0 {
            (best_bid + best_ask) / 2.0
        } else {
            0.0
        };
        let spread_bps = if mid > 0.0 {
            ((best_ask - best_bid) / mid) * 10_000.0
        } else {
            0.0
        };

        OrderBookTopN {
            ts_ms,
            symbol: self.symbol.clone(),
            venue: self.venue,
            update_id: self.last_update_id,
            bids,
            asks,
            best_bid,
            best_ask,
            mid,
            spread_bps,
        }
    }

    pub fn indicative_mid(&self) -> f64 {
        let mut best_bid = None;
        for (price_key, _) in self.bids.iter().rev().take(1) {
            best_bid = Some((*price_key as f64) / self.price_scale);
        }
        let mut best_ask = None;
        for (price_key, _) in self.asks.iter().take(1) {
            best_ask = Some((*price_key as f64) / self.price_scale);
        }
        match (best_bid, best_ask) {
            (Some(bid), Some(ask)) if bid > 0.0 && ask > 0.0 => (bid + ask) / 2.0,
            (Some(bid), None) => bid,
            (None, Some(ask)) => ask,
            _ => 1.0,
        }
    }
}

/// Converts floating price into a BTree-friendly integer key.
pub fn price_key(price: f64, price_scale: f64) -> i64 {
    (price * price_scale).round() as i64
}
