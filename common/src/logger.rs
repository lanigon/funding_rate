use std::{
    fs::{self, OpenOptions},
    path::PathBuf,
    sync::{Arc, OnceLock},
};

use chrono::Local;
use console_subscriber::ConsoleLayer;
use tracing::Level;
use tracing_subscriber::{
    filter::Targets, fmt, fmt::writer::BoxMakeWriter, layer::SubscriberExt,
    util::SubscriberInitExt, EnvFilter, Layer,
};

static LOGGER_ONCE: OnceLock<()> = OnceLock::new();
const LOG_DIR: &str = "logs";
const DEFAULT_FILTER: &str = "info,sqlx::query=off,sqlx::postgres::notice=off";

/// 初始化全局 tracing 日志订阅者（幂等，可多次调用）。
pub fn init_logging() {
    LOGGER_ONCE.get_or_init(|| {
        let build_env_filter =
            || EnvFilter::try_from_default_env().unwrap_or_else(|_| EnvFilter::new(DEFAULT_FILTER));
        let build_fmt_layer = || {
            fmt::layer()
                .with_target(true)
                .with_level(true)
                .with_thread_ids(false)
                .with_thread_names(false)
                .with_ansi(false)
                .with_writer(prepare_log_writer())
        };

        let env_filter = build_env_filter();
        let fmt_layer = build_fmt_layer().with_filter(env_filter);

        if console_enabled() {
            let console_addr = std::env::var("TOKIO_CONSOLE_BIND")
                .unwrap_or_else(|_| "127.0.0.1:6669".to_string());
            eprintln!("tokio-console enabled via TOKIO_CONSOLE (connect to {console_addr})");
            let console_layer = ConsoleLayer::builder().with_default_env().spawn();
            let console_filter = Targets::new()
                .with_target("tokio", Level::TRACE)
                .with_target("runtime", Level::TRACE);
            tracing_subscriber::registry()
                .with(fmt_layer)
                .with(console_layer.with_filter(console_filter))
                .init();
        } else {
            tracing_subscriber::registry()
                .with(fmt_layer)
                .init();
        }
    });
}

fn prepare_log_writer() -> BoxMakeWriter {
    let log_dir = PathBuf::from(LOG_DIR);
    fs::create_dir_all(&log_dir).expect("failed to create logs directory");
    let date_prefix = Local::now().format("%Y-%m-%d").to_string();
    let log_path = Arc::new(log_dir.join(format!("{date_prefix}.log")));
    BoxMakeWriter::new({
        let log_path = Arc::clone(&log_path);
        move || {
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&*log_path)
                .expect("failed to open log file")
        }
    })
}

fn console_enabled() -> bool {
    std::env::var("TOKIO_CONSOLE")
        .map(|value| value != "0")
        .unwrap_or(true)
}
