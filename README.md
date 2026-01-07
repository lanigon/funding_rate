# Funding Rate Arbitrage Workspace

Rust workspace with mock funding-rate arbitrage pipeline. The layout is fully event-driven and each module communicates via `tokio::sync::mpsc` channels.

## Crates

| Crate | Description |
| --- | --- |
| `common` | Shared structs (`Event`, `Level`, orders, funding, helpers). |
| `connector` | Mock order book + funding generator with `OrderBookBuilder` (snapshot + delta). |
| `datalayer` | Central router/state keeper. Holds top-N books, latest funding, targets, counters and fans out to strategy/executor/store. |
| `strategy` | Reads shared state, estimates slippage with top-N depth and emits `TargetPosition` events when drift > threshold. |
| `executor` | Spawns mock legs (spot/perp) concurrently, emits order lifecycle events. |
| `store` | SeaORM + Postgres writer (tables `order_groups`, `orders`). Includes schema bootstrap via SeaORM `Schema`. |
| `metrics` | Axum HTTP server exposing `/metrics` (Prometheus format) derived from the shared state. |
| `research` | Offline symbol selection. Generates mock funding/liquidity histories through `connector` helpers and produces `universe.json`. |
| `app` | Runtime entry point. Loads universe, spawns the connector/datalayer/strategy/executor/store/metrics stack. |

## Getting Started

1. **Infrastructure**
   ```bash
   docker compose up -d
   # services:
   # - Postgres  (localhost:5432)
   # - Prometheus(localhost:9090) -> monitoring/prometheus.yml
   # - Grafana   (localhost:3000)
   ```

2. **Environment**
   ```bash
   export DATABASE_URL=postgres://postgres:postgres@localhost:5432/arbitrage
   ```

3. **Research universe**
   ```bash
   cargo run -p app -- --research
   # writes universe.json (top symbols with mock scores)
   ```

4. **Run app**
   ```bash
   cargo run -p app
   ```

5. **Observability**
   - `http://localhost:8000/metrics` (served by `metrics` crate)
   - Prometheus scrapes `host.docker.internal:8000` by default (Linux alternative noted in `monitoring/prometheus.yml`).
   - Grafana listens on `localhost:3000` (credentials admin/admin by default).

## Notes

- Connector emits initial snapshots (20 levels) followed by 1s deltas per symbol/venue and periodic funding snapshots.
- Datalayer updates the shared `State` after every event and fan-outs: strategy via `try_send`, executor/store via `send().await`.
- Store automatically creates the required tables when a database is reachable. If no DB URL is provided, events are logged but not persisted.
- All modules avoid traits for composition—plain structs, functions, and channels are used end-to-end.
