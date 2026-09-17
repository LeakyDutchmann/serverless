use serverless::routes::net::handle_connection;
use serverless::database::connection::connect;
use serverless::scheduler::{model::Scheduler, types::Job, shutdown::Shutdown};

use tokio::net::TcpListener;
use wasmtime::Engine;
use tokio::select;
use tokio::signal;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("0.0.0.0:8080").await.expect("Failed to bind to port 8080");
    let (tx, rx) = tokio::sync::mpsc::channel::<Job>(1024);
    let (shutdown_tx, mut shutdown_rx) = tokio::sync::mpsc::channel::<Shutdown>(1024);
    dotenvy::dotenv().ok();
    let db_url = std::env::var("DATABASE_URL").expect("DATABASE_URL not set");
    //using .expect() here to make sure program stops if connection failed.
    let db_pool = connect(&db_url, 10).await.expect("Failed to connect to a database, panicking!");
    
    //Start scheduler and spawn workers!
    let mut scheduler = Scheduler::initialize(4, 20, rx, db_pool.clone(), shutdown_tx).await;
    scheduler.run().await;
    println!("Scheduler is running successfully");

    let wasm_engine = Engine::default();
    loop {
        select! {
            Some(signal) = shutdown_rx.recv() => {
                println!("Received shutdown signal after: {:?} for reason: {}", signal.instant, signal.reason);
                break;
            }
            _ = signal::ctrl_c() => {
                println!("Received Ctrl+C signal");
                break;
            }
            connection = listener.accept() => {
                match connection {
                    Ok((stream, _)) => {
                        let tx_cloned = tx.clone();
                        let pool_cloned = db_pool.clone();
                        let engine_cloned = wasm_engine.clone();
                        tokio::spawn(async move {
                            println!("New connection!");
                            handle_connection(stream, pool_cloned, tx_cloned, engine_cloned).await;
                        });
                    }
                    Err(e) => {
                        println!("Failed to accept connection: {}", e);
                    }
                }
            }
        }
    }
    scheduler.shutdown().await;
    println!("Server is down");
}
