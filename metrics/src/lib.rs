use anyhow::Result;
use axum::{extract::State as AxumState, routing::get, Router};
use common::SharedStateSnapshot;
use datalayer::SharedState;
use tokio::{net::TcpListener, task::JoinHandle};
use tracing::info;

#[derive(Clone)]
struct MetricsState {
    shared: SharedState,
}

pub fn spawn(state: SharedState) -> JoinHandle<Result<()>> {
    let app_state = MetricsState { shared: state };
    tokio::spawn(async move {
        let app = Router::new()
            .route("/metrics", get(metrics_handler))
            .with_state(app_state);
        info!("metrics server listening on 0.0.0.0:8000");
        let listener = TcpListener::bind("0.0.0.0:8000").await?;
        axum::serve(listener, app.into_make_service()).await?;
        Ok(())
    })
}

async fn metrics_handler(AxumState(state): AxumState<MetricsState>) -> String {
    let snapshot = state.shared.snapshot();
    render(snapshot)
}

fn render(snapshot: SharedStateSnapshot) -> String {
    let mut out = String::new();
    out.push_str("# HELP order_count_total Total orders observed\n");
    out.push_str("# TYPE order_count_total counter\n");
    out.push_str(&format!(
        "order_count_total {}\n",
        snapshot.counters.order_count_total
    ));
    out.push_str("# HELP fee_cumulative Estimated cumulative fees\n");
    out.push_str("# TYPE fee_cumulative counter\n");
    out.push_str(&format!("fee_cumulative {}\n", snapshot.counters.fee_cum));
    out.push_str("# HELP account_equity_usd Account equity snapshot\n");
    out.push_str("# TYPE account_equity_usd gauge\n");
    for (account_id, account) in &snapshot.accounts {
        out.push_str(&format!(
            "account_equity_usd{{account=\"{account_id}\"}} {}\n",
            account.equity_usd
        ));
    }
    out.push_str("# HELP funding_income_total Cumulative funding income\n");
    out.push_str("# TYPE funding_income_total counter\n");
    out.push_str(&format!(
        "funding_income_total {}\n",
        snapshot.counters.funding_income_total
    ));
    out
}
