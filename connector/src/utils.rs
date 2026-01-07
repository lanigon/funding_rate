use common::Level;
use serde_json::Value;

pub fn parse_f64(value: &Value) -> Option<f64> {
    match value {
        Value::String(s) => s.parse::<f64>().ok(),
        Value::Number(num) => num.as_f64(),
        _ => None,
    }
}

pub fn convert_levels(levels: Vec<[String; 2]>) -> Vec<Level> {
    levels
        .into_iter()
        .filter_map(|pair| {
            let [price, size] = pair;
            let price = price.parse::<f64>().ok()?;
            let size = size.parse::<f64>().ok()?;
            Some(Level::new(price, size))
        })
        .collect()
}

pub fn base_price(symbol: &str) -> f64 {
    match symbol {
        "BTCUSDT" => 30_000.0,
        "ETHUSDT" => 2_000.0,
        "SOLUSDT" => 60.0,
        "BNBUSDT" => 300.0,
        _ => 1_000.0,
    }
}
