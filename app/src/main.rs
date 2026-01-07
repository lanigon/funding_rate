use anyhow::Result;
use clap::{Args, Parser, Subcommand};
use common::logger;
use dotenvy::dotenv;
use engine::{run, EngineOptions};

pub mod account;
pub mod fetch_fee;
pub mod fetch_margin;
pub mod fetch_price;
pub mod fetch_token;
pub mod order;
pub mod select;

#[derive(Parser)]
#[command(name = "tradeterminal", about = "Workspace management CLI")]
struct Cli {
    #[command(subcommand)]
    command: Option<Command>,
}

#[derive(Subcommand, Debug, Clone)]
enum Command {
    /// Run the live engine (connector + strategy + executor)
    Run(RunArgs),
    /// Inspect database data and print strategy recommendations
    Select,
}

#[derive(Args, Debug, Clone, Default)]
struct RunArgs {
    /// Run the offline research pipeline instead of the live engine.
    #[arg(long)]
    research: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    dotenv().ok();
    logger::init_logging();
    let cli = Cli::parse();
    match cli
        .command
        .unwrap_or_else(|| Command::Run(RunArgs::default()))
    {
        Command::Run(args) => {
            let opts = EngineOptions {
                run_research: args.research,
            };
            run(opts).await
        }
        Command::Select => select::run().await,
    }
}
