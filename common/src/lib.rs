use chrono::{DateTime, Utc};
use serde::{Deserialize, Serialize};

pub mod account;
pub mod constant;
pub mod event;
pub mod logger;
pub mod order;
pub mod orderbook;
pub mod price;

pub use account::*;
pub use constant::*;
pub use event::*;
pub use order::*;
pub use orderbook::*;
pub use price::*;

pub type Symbol = String;

#[derive(Clone, Copy, Debug, Serialize, Deserialize, Eq, PartialEq, Hash)]
pub enum Venue {
    Spot,
    Perp,
}

impl std::fmt::Display for Venue {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            Venue::Spot => write!(f, "spot"),
            Venue::Perp => write!(f, "perp"),
        }
    }
}

impl Default for Venue {
    fn default() -> Self {
        Venue::Spot
    }
}

pub fn now_ms() -> i64 {
    let now: DateTime<Utc> = Utc::now();
    now.timestamp_millis()
}
