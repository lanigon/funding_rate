use sea_orm::DbErr;
use sea_orm::{ConnectionTrait, DatabaseConnection, Schema, Statement};

use crate::entities::{funding_income, order_groups, orders, token_pairs};

pub async fn run_migrations(db: &DatabaseConnection) -> Result<(), DbErr> {
    let backend = db.get_database_backend();
    let schema = Schema::new(backend);

    let mut order_groups_stmt = schema.create_table_from_entity(order_groups::Entity);
    order_groups_stmt.if_not_exists();
    db.execute(backend.build(&order_groups_stmt)).await?;

    let mut orders_stmt = schema.create_table_from_entity(orders::Entity);
    orders_stmt.if_not_exists();
    db.execute(backend.build(&orders_stmt)).await?;

    let mut token_pairs_stmt = schema.create_table_from_entity(token_pairs::Entity);
    token_pairs_stmt.if_not_exists();
    db.execute(backend.build(&token_pairs_stmt)).await?;

    let mut funding_income_stmt = schema.create_table_from_entity(funding_income::Entity);
    funding_income_stmt.if_not_exists();
    db.execute(backend.build(&funding_income_stmt)).await?;

    create_partitioned_funding_table(db).await?;
    create_partitioned_price_table(db).await?;
    create_margin_interest_table(db).await?;
    create_indexes(db).await?;

    Ok(())
}

async fn create_partitioned_funding_table(db: &DatabaseConnection) -> Result<(), DbErr> {
    let backend = db.get_database_backend();
    let create_parent = r#"
        CREATE TABLE IF NOT EXISTS funding_rates (
            symbol TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            funding_rate DOUBLE PRECISION NOT NULL,
            PRIMARY KEY (symbol, ts_ms)
        ) PARTITION BY RANGE (ts_ms);
    "#;
    db.execute(Statement::from_string(backend, create_parent.to_string()))
        .await?;
    let create_default =
        "CREATE TABLE IF NOT EXISTS funding_rates_default PARTITION OF funding_rates DEFAULT;";
    db.execute(Statement::from_string(backend, create_default.to_string()))
        .await?;
    Ok(())
}

async fn create_partitioned_price_table(db: &DatabaseConnection) -> Result<(), DbErr> {
    let backend = db.get_database_backend();
    let create_parent = r#"
        CREATE TABLE IF NOT EXISTS price_candles (
            venue TEXT NOT NULL,
            symbol TEXT NOT NULL,
            open_time BIGINT NOT NULL,
            close_time BIGINT NOT NULL,
            open DOUBLE PRECISION NOT NULL,
            high DOUBLE PRECISION NOT NULL,
            low DOUBLE PRECISION NOT NULL,
            close DOUBLE PRECISION NOT NULL,
            volume DOUBLE PRECISION NOT NULL,
            PRIMARY KEY (venue, symbol, open_time)
        ) PARTITION BY RANGE (open_time);
    "#;
    db.execute(Statement::from_string(backend, create_parent.to_string()))
        .await?;
    let create_default =
        "CREATE TABLE IF NOT EXISTS price_candles_default PARTITION OF price_candles DEFAULT;";
    db.execute(Statement::from_string(backend, create_default.to_string()))
        .await?;
    Ok(())
}

async fn create_margin_interest_table(db: &DatabaseConnection) -> Result<(), DbErr> {
    let backend = db.get_database_backend();
    let create_stmt = r#"
        CREATE TABLE IF NOT EXISTS margin_interest (
            asset TEXT NOT NULL,
            ts_ms BIGINT NOT NULL,
            interest_rate DOUBLE PRECISION NOT NULL,
            interest DOUBLE PRECISION NOT NULL,
            principal DOUBLE PRECISION NOT NULL,
            PRIMARY KEY (asset, ts_ms)
        );
    "#;
    db.execute(Statement::from_string(backend, create_stmt.to_string()))
        .await?;
    Ok(())
}

async fn create_indexes(db: &DatabaseConnection) -> Result<(), DbErr> {
    let backend = db.get_database_backend();
    let create_price_symbol_idx = r#"
        CREATE INDEX IF NOT EXISTS idx_price_candles_symbol_time
        ON price_candles (symbol, open_time);
    "#;
    db.execute(Statement::from_string(
        backend,
        create_price_symbol_idx.to_string(),
    ))
    .await?;

    let create_funding_idx = r#"
        CREATE INDEX IF NOT EXISTS idx_funding_rates_symbol_time
        ON funding_rates (symbol, ts_ms);
    "#;
    db.execute(Statement::from_string(
        backend,
        create_funding_idx.to_string(),
    ))
    .await?;

    let create_margin_idx = r#"
        CREATE INDEX IF NOT EXISTS idx_margin_interest_asset_time
        ON margin_interest (asset, ts_ms);
    "#;
    db.execute(Statement::from_string(
        backend,
        create_margin_idx.to_string(),
    ))
    .await?;
    Ok(())
}
