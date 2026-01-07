use std::env;

use anyhow::Result;
use common::{Event, FundingIncome, OrderFilled, OrderGroupCreated, OrderRequested};
use sea_orm::sea_query::OnConflict;
use sea_orm::ActiveValue::Set;
use sea_orm::{ActiveModelTrait, Database, DatabaseConnection, EntityTrait, IntoActiveModel};
use tokio::sync::mpsc;
use tokio::task::JoinHandle;
use tracing::{info, warn};

pub mod entities;
pub mod migration;

use entities::{funding_income, order_groups, orders};

#[derive(Clone, Default)]
pub struct StoreConfig {
    pub database_url: Option<String>,
}

pub fn spawn(mut store_rx: mpsc::Receiver<Event>, cfg: StoreConfig) -> JoinHandle<Result<()>> {
    tokio::spawn(async move {
        let db = match resolve_connection(&cfg).await {
            Ok(conn) => conn,
            Err(err) => {
                warn!("store running without database: {err}");
                None
            }
        };
        while let Some(event) = store_rx.recv().await {
            if let Some(conn) = db.as_ref() {
                if let Err(err) = persist_event(conn, event).await {
                    warn!("store persist error: {err:?}");
                }
            }
        }
        Ok(())
    })
}

async fn resolve_connection(cfg: &StoreConfig) -> Result<Option<DatabaseConnection>> {
    let url = cfg
        .database_url
        .clone()
        .or_else(|| env::var("DATABASE_URL").ok());
    if let Some(url) = url {
        let db = Database::connect(&url).await?;
        migration::run_migrations(&db).await?;
        info!("store connected to postgres");
        Ok(Some(db))
    } else {
        Ok(None)
    }
}

async fn persist_event(db: &DatabaseConnection, event: Event) -> Result<()> {
    match event {
        Event::OrderGroupCreated(group) => insert_group(db, group).await?,
        Event::OrderRequested(req) => insert_order(db, req).await?,
        Event::OrderFilled(fill) => mark_filled(db, fill).await?,
        Event::FundingIncome(income) => insert_funding_income(db, income).await?,
        _ => {}
    }
    Ok(())
}

async fn insert_group(db: &DatabaseConnection, group: OrderGroupCreated) -> Result<()> {
    let model = order_groups::ActiveModel {
        group_id: Set(group.group_id),
        ts_ms: Set(group.ts_ms),
        symbol: Set(group.symbol),
        target_notional: Set(group.target_notional_usd),
        funding_rate_at_signal: Set(group.funding_rate_at_signal),
        basis_bps_at_signal: Set(group.basis_bps_at_signal),
        status: Set(group.status),
        note: Set(String::new()),
    };
    model.insert(db).await?;
    Ok(())
}

async fn insert_order(db: &DatabaseConnection, req: OrderRequested) -> Result<()> {
    let model = orders::ActiveModel {
        id: Set(req.child_order_id),
        group_id: Set(req.group_id),
        ts_ms: Set(req.ts_ms),
        symbol: Set(req.symbol),
        venue: Set(format!("{:?}", req.venue)),
        side: Set(req.side),
        order_type: Set(req.order_type),
        price: Set(req.price),
        qty: Set(req.qty),
        notional: Set(req.notional),
        quote_qty: Set(req.notional),
        qty_executed: Set(0.0),
        funding_rate_at_order: Set(req.funding_rate_at_order),
        basis_bps_at_order: Set(req.basis_bps_at_order),
        fee_estimated: Set(req.fee_estimated),
        fee_paid: Set(0.0),
        fee_asset: Set(req.fee_asset),
        status: Set(req.status),
        note: Set(String::new()),
    };
    model.insert(db).await?;
    Ok(())
}

async fn mark_filled(db: &DatabaseConnection, fill: OrderFilled) -> Result<()> {
    let mut model: orders::ActiveModel = if let Some(existing) =
        orders::Entity::find_by_id(fill.child_order_id)
            .one(db)
            .await?
    {
        existing.into_active_model()
    } else {
        orders::ActiveModel {
            id: Set(fill.child_order_id),
            group_id: Set(fill.group_id),
            ts_ms: Set(fill.ts_ms),
            symbol: Set(fill.symbol),
            venue: Set(format!("{:?}", fill.venue)),
            side: Set(String::new()),
            order_type: Set(String::new()),
            price: Set(fill.filled_price),
            qty: Set(fill.filled_qty),
            notional: Set(fill.filled_qty.abs() * fill.filled_price),
            quote_qty: Set(fill.filled_qty.abs() * fill.filled_price),
            qty_executed: Set(fill.filled_qty),
            funding_rate_at_order: Set(0.0),
            basis_bps_at_order: Set(0.0),
            fee_estimated: Set(fill.fee_paid),
            fee_paid: Set(fill.fee_paid),
            fee_asset: Set("USDT".to_string()),
            status: Set(fill.status.clone()),
            note: Set(String::new()),
        }
    };
    model.fee_paid = Set(fill.fee_paid);
    model.status = Set(fill.status);
    model.price = Set(fill.filled_price);
    model.qty = Set(fill.filled_qty);
    model.qty_executed = Set(fill.filled_qty);
    model.notional = Set(fill.filled_qty.abs() * fill.filled_price);
    model.quote_qty = Set(fill.filled_qty.abs() * fill.filled_price);
    model.ts_ms = Set(fill.ts_ms);
    model.update(db).await?;
    Ok(())
}

async fn insert_funding_income(db: &DatabaseConnection, income: FundingIncome) -> Result<()> {
    let model = funding_income::ActiveModel {
        symbol: Set(income.symbol),
        ts_ms: Set(income.ts_ms),
        amount: Set(income.amount),
    };
    funding_income::Entity::insert(model)
        .on_conflict(
            OnConflict::columns([funding_income::Column::Symbol, funding_income::Column::TsMs])
                .update_column(funding_income::Column::Amount)
                .to_owned(),
        )
        .exec(db)
        .await?;
    Ok(())
}
