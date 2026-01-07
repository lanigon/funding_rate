#![allow(dead_code)]

use anyhow::Result;
use common::logger;
use dotenvy::dotenv;
use tracing::info;
use utils::db::{connect_db, persist_tokens};
use utils::tokens::fetch_all_tokens;

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    logger::init_logging();
    let tokens = fetch_all_tokens().await?;
    let db = connect_db().await?;
    persist_tokens(&db, &tokens).await?;
    info!(tokens = tokens.len(), "persisted token metadata");
    Ok(())
}
