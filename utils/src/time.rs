use chrono::{Duration, Utc};

/// Returns `(start_ms, end_ms)` covering the last `hours_lookback` hours.
pub fn rolling_window_hours(hours_lookback: i64) -> (i64, i64) {
    let end = Utc::now().timestamp_millis();
    let start = (Utc::now() - Duration::hours(hours_lookback)).timestamp_millis();
    (start, end)
}
