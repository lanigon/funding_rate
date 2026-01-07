use sea_orm::entity::prelude::*;
use uuid::Uuid;

pub mod order_groups {
    use super::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "order_groups")]
    pub struct Model {
        /// Unique identifier shared by every child order in the group.
        #[sea_orm(primary_key, auto_increment = false)]
        pub group_id: Uuid,
        /// Millisecond timestamp produced by the strategy when emitting the target.
        pub ts_ms: i64,
        /// Trading symbol for the hedge pair (e.g. BTCUSDT).
        pub symbol: String,
        /// Requested notional exposure in USD terms.
        pub target_notional: f64,
        /// Funding snapshot observed when the signal fired.
        pub funding_rate_at_signal: f64,
        /// Spot/perp basis at signal time (basis points).
        pub basis_bps_at_signal: f64,
        /// Lifecycle state for the order group (created/filled/etc).
        pub status: String,
        /// Free-form note used for debugging or reconciliation.
        pub note: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {
        #[sea_orm(has_many = "super::orders::Entity")]
        Orders,
    }

    impl Related<super::orders::Entity> for Entity {
        fn to() -> RelationDef {
            Relation::Orders.def()
        }
    }

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod orders {
    use super::*;

    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "orders")]
    pub struct Model {
        /// Unique identifier for the specific child order/leg.
        #[sea_orm(primary_key, auto_increment = false)]
        pub id: Uuid,
        /// Foreign key pointing to `order_groups`.
        pub group_id: Uuid,
        /// Millisecond timestamp when the order was created/observed.
        pub ts_ms: i64,
        /// Trading symbol for the order.
        pub symbol: String,
        /// Venue string (spot or perp) recorded for the leg.
        pub venue: String,
        /// Side of the order: buy or sell.
        pub side: String,
        /// Order type hint (market/limit/etc).
        pub order_type: String,
        /// Price attached to the request or fill.
        pub price: f64,
        /// Quantity of base asset requested or filled.
        pub qty: f64,
        /// Total notional (price * qty).
        pub notional: f64,
        /// Quote currency exposure associated with the request (e.g. USDT amount).
        pub quote_qty: f64,
        /// Actual base asset amount confirmed via downstream fills.
        pub qty_executed: f64,
        /// Funding rate snapshot when the order was sent.
        pub funding_rate_at_order: f64,
        /// Basis captured at the time of order placement.
        pub basis_bps_at_order: f64,
        /// Estimated fees used for budgeting prior to fills.
        pub fee_estimated: f64,
        /// Actual fees paid according to fills.
        pub fee_paid: f64,
        /// Fee currency (usually USDT in this mock system).
        pub fee_asset: String,
        /// Order status indicator (new/filled/etc).
        pub status: String,
        /// Optional note for debugging or reconciliation.
        pub note: String,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {
        #[sea_orm(
            belongs_to = "super::order_groups::Entity",
            from = "Column::GroupId",
            to = "super::order_groups::Column::GroupId"
        )]
        OrderGroups,
    }

    impl ActiveModelBehavior for ActiveModel {}

    impl Related<super::order_groups::Entity> for Entity {
        fn to() -> RelationDef {
            Relation::OrderGroups.def()
        }
    }
}

pub mod token_pairs {
    use super::*;

    /// Static metadata for each tradable symbol (fees, availability).
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "token_pairs")]
    pub struct Model {
        /// Symbol identifier, e.g. "BTCUSDT".
        #[sea_orm(primary_key, auto_increment = false)]
        pub symbol: String,
        /// Whether a spot venue is available for trading this symbol.
        pub has_spot: bool,
        /// Spot taker fee ratio (e.g. 0.0004).
        pub spot_taker_fee: f64,
        /// Spot maker fee ratio.
        pub spot_maker_fee: f64,
        /// Perpetual taker fee ratio.
        pub perp_taker_fee: f64,
        /// Perpetual maker fee ratio.
        pub perp_maker_fee: f64,
        /// Funding settlement interval in seconds.
        pub funding_interval_secs: i64,
        /// Whether margin (借币) is enabled for this token.
        pub margin_enabled: bool,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod funding_income {
    use super::*;

    /// Funding fee income snapshots recorded per symbol/settlement.
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "funding_income")]
    pub struct Model {
        #[sea_orm(primary_key, auto_increment = false)]
        pub symbol: String,
        #[sea_orm(primary_key, auto_increment = false)]
        pub ts_ms: i64,
        pub amount: f64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod funding_rates {
    use super::*;

    /// Historical funding entries captured per symbol.
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "funding_rates")]
    pub struct Model {
        /// Symbol identifier, part of the composite primary key.
        #[sea_orm(primary_key, auto_increment = false)]
        pub symbol: String,
        /// Timestamp (ms) for the funding record, part of the composite key.
        #[sea_orm(primary_key, auto_increment = false)]
        pub ts_ms: i64,
        /// Observed funding rate for the window.
        pub funding_rate: f64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod price_candles {
    use super::*;

    /// Hourly OHLCV snapshot stored for analytics.
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "price_candles")]
    pub struct Model {
        /// Venue identifier (spot/perp) for the candle.
        #[sea_orm(primary_key, auto_increment = false)]
        pub venue: String,
        /// Symbol identifier (composite primary key).
        #[sea_orm(primary_key, auto_increment = false)]
        pub symbol: String,
        /// Candle open timestamp (ms), composite primary key.
        #[sea_orm(primary_key, auto_increment = false)]
        pub open_time: i64,
        /// Candle close timestamp (ms).
        pub close_time: i64,
        pub open: f64,
        pub high: f64,
        pub low: f64,
        pub close: f64,
        pub volume: f64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}

pub mod margin_interest {
    use super::*;

    /// Historical margin interest accruals per asset.
    #[derive(Clone, Debug, PartialEq, DeriveEntityModel)]
    #[sea_orm(table_name = "margin_interest")]
    pub struct Model {
        /// Borrowed asset symbol.
        #[sea_orm(primary_key, auto_increment = false)]
        pub asset: String,
        /// Accrual timestamp (ms).
        #[sea_orm(primary_key, auto_increment = false)]
        pub ts_ms: i64,
        /// Interest rate applied for the period.
        pub interest_rate: f64,
        /// Interest amount (quote currency).
        pub interest: f64,
        /// Principal amount outstanding.
        pub principal: f64,
    }

    #[derive(Copy, Clone, Debug, EnumIter, DeriveRelation)]
    pub enum Relation {}

    impl ActiveModelBehavior for ActiveModel {}
}
