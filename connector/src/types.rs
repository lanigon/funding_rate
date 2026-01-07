use serde::Deserialize;

/// 单根 K 线数据。
#[derive(Clone, Debug)]
pub struct KlinePoint {
    /// 开始时间（毫秒）。
    pub open_time: i64,
    /// 结束时间（毫秒）。
    pub close_time: i64,
    /// 开盘价。
    pub open: f64,
    /// 最高价。
    pub high: f64,
    /// 最低价。
    pub low: f64,
    /// 收盘价。
    pub close: f64,
    /// 成交量。
    pub volume: f64,
}

/// Request payload for提交 Binance 签名下单的通用结构。
#[derive(Clone, Debug)]
pub struct OrderRequest {
    /// 交易对符号，例如 `BTCUSDT`。
    pub symbol: String,
    /// 方向，`BUY` 或 `SELL`。
    pub side: String,
    /// 订单类型，例如 `MARKET`、`LIMIT`。
    pub order_type: String,
    /// 下单数量，单位依交易对定义（币或张）。
    pub quantity: f64,
    /// 限价单价格，市价单保持为 `None`。
    pub price: Option<f64>,
    /// 时效（time-in-force），如 `GTC`、`IOC`。
    pub time_in_force: Option<String>,
    /// 是否仅减仓（仅部分接口支持）。
    pub reduce_only: bool,
    /// API 自定义 `recvWindow`，不设置时使用客户端默认值。
    pub recv_window: Option<i64>,
}

impl OrderRequest {
    pub fn market(symbol: impl Into<String>, side: impl Into<String>, quantity: f64) -> Self {
        Self {
            symbol: symbol.into(),
            side: side.into(),
            order_type: "MARKET".into(),
            quantity,
            price: None,
            time_in_force: None,
            reduce_only: false,
            recv_window: None,
        }
    }

    pub fn limit(
        symbol: impl Into<String>,
        side: impl Into<String>,
        quantity: f64,
        price: f64,
        tif: impl Into<String>,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            side: side.into(),
            order_type: "LIMIT".into(),
            quantity,
            price: Some(price),
            time_in_force: Some(tif.into()),
            reduce_only: false,
            recv_window: None,
        }
    }
}

/// Binance 订单响应中常用的字段子集。
#[derive(Debug, Deserialize)]
pub struct OrderResponse {
    /// 交易对符号。
    #[serde(rename = "symbol")]
    pub symbol: String,
    /// 平台订单 ID。
    #[serde(rename = "orderId")]
    pub order_id: i64,
    /// 订单状态。
    #[serde(rename = "status")]
    pub status: String,
}

/// Binance 交易对精度与交易限制（源自 exchangeInfo）。
#[derive(Clone, Debug, Default)]
pub struct SymbolFilters {
    /// LOT_SIZE/MARKET_LOT_SIZE -> stepSize。
    pub step_size: Option<f64>,
    /// LOT_SIZE/MARKET_LOT_SIZE -> minQty。
    pub min_qty: Option<f64>,
    /// LOT_SIZE/MARKET_LOT_SIZE -> maxQty。
    pub max_qty: Option<f64>,
    /// PRICE_FILTER -> tickSize。
    pub tick_size: Option<f64>,
    /// PRICE_FILTER -> minPrice。
    pub min_price: Option<f64>,
    /// PRICE_FILTER -> maxPrice。
    pub max_price: Option<f64>,
    /// MIN_NOTIONAL/notional filters。
    pub min_notional: Option<f64>,
}

impl SymbolFilters {
    /// 按照 stepSize 向下取整数量，避免触发 LOT_SIZE。
    pub fn quantize_quantity(&self, qty: f64) -> f64 {
        match self.step_size {
            Some(step) if step > 0.0 => floor_to_step(qty, step),
            _ => qty,
        }
    }

    /// 按照 stepSize 向上取整数量，确保满足 NOTIONAL/LOT_SIZE。
    pub fn ceil_quantity(&self, qty: f64) -> f64 {
        match self.step_size {
            Some(step) if step > 0.0 => ceil_to_step(qty, step),
            _ => qty,
        }
    }

    /// 根据 tickSize 四舍五入价格，确保落在合法网格。
    pub fn quantize_price(&self, price: f64) -> f64 {
        match self.tick_size {
            Some(tick) if tick > 0.0 => round_to_step(price, tick),
            _ => price,
        }
    }
}

fn floor_to_step(value: f64, step: f64) -> f64 {
    if value <= 0.0 {
        return 0.0;
    }
    let steps = (value / step).floor();
    normalize_float(steps * step)
}

fn ceil_to_step(value: f64, step: f64) -> f64 {
    if value <= 0.0 {
        return 0.0;
    }
    let steps = (value / step).ceil();
    normalize_float(steps * step)
}

fn round_to_step(value: f64, step: f64) -> f64 {
    if value == 0.0 {
        return 0.0;
    }
    let steps = (value / step).round();
    normalize_float(steps * step)
}

fn normalize_float(value: f64) -> f64 {
    (value * 1e12).round() / 1e12
}

/// 市价（taker）订单参数，方便构建 `OrderRequest`。
#[derive(Clone, Debug)]
pub struct MarketOrderParams {
    /// 交易对符号。
    pub symbol: String,
    /// 方向。
    pub side: String,
    /// 数量。
    pub quantity: f64,
    /// 是否仅减仓。
    pub reduce_only: bool,
    /// 自定义 `recvWindow`。
    pub recv_window: Option<i64>,
}

impl MarketOrderParams {
    pub fn new(symbol: impl Into<String>, side: impl Into<String>, quantity: f64) -> Self {
        Self {
            symbol: symbol.into(),
            side: side.into(),
            quantity,
            reduce_only: false,
            recv_window: None,
        }
    }

    pub fn with_reduce_only(mut self, reduce_only: bool) -> Self {
        self.reduce_only = reduce_only;
        self
    }

    pub fn with_recv_window(mut self, recv_window: i64) -> Self {
        self.recv_window = Some(recv_window);
        self
    }
}

/// 限价（maker）订单参数，方便构建 `OrderRequest`。
#[derive(Clone, Debug)]
pub struct LimitOrderParams {
    /// 交易对符号。
    pub symbol: String,
    /// 方向。
    pub side: String,
    /// 数量。
    pub quantity: f64,
    /// 限价价格。
    pub price: f64,
    /// time-in-force 选项，如 `GTC`。
    pub time_in_force: String,
    /// 是否仅减仓。
    pub reduce_only: bool,
    /// 自定义 `recvWindow`。
    pub recv_window: Option<i64>,
}

impl LimitOrderParams {
    pub fn new(
        symbol: impl Into<String>,
        side: impl Into<String>,
        quantity: f64,
        price: f64,
        time_in_force: impl Into<String>,
    ) -> Self {
        Self {
            symbol: symbol.into(),
            side: side.into(),
            quantity,
            price,
            time_in_force: time_in_force.into(),
            reduce_only: false,
            recv_window: None,
        }
    }

    pub fn with_reduce_only(mut self, reduce_only: bool) -> Self {
        self.reduce_only = reduce_only;
        self
    }

    pub fn with_recv_window(mut self, recv_window: i64) -> Self {
        self.recv_window = Some(recv_window);
        self
    }
}

impl From<MarketOrderParams> for OrderRequest {
    fn from(value: MarketOrderParams) -> Self {
        let MarketOrderParams {
            symbol,
            side,
            quantity,
            reduce_only,
            recv_window,
        } = value;
        Self {
            symbol,
            side,
            order_type: "MARKET".into(),
            quantity,
            price: None,
            time_in_force: None,
            reduce_only,
            recv_window,
        }
    }
}

impl From<LimitOrderParams> for OrderRequest {
    fn from(value: LimitOrderParams) -> Self {
        let LimitOrderParams {
            symbol,
            side,
            quantity,
            price,
            time_in_force,
            reduce_only,
            recv_window,
        } = value;
        Self {
            symbol,
            side,
            order_type: "LIMIT".into(),
            quantity,
            price: Some(price),
            time_in_force: Some(time_in_force),
            reduce_only,
            recv_window,
        }
    }
}
