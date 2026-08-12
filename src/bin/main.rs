use serverless::routes::net::handle_connection;
use serverless::database::connection::connect;


use tokio::net::{TcpListener, TcpStream};
use tokio::sync::mpsc::Sender;

#[tokio::main]
async fn main() {
    let listener = TcpListener::bind("0.0.0.0:8080").await.unwrap();
    let (tx, mut rx) = tokio::sync::mpsc::channel::<String>(1024);
    
    dotenvy::dotenv().ok();
    let db_url = std::env::var("DATABASE_URL").unwrap();
    //using .expect() here to make sure program stops if connection failed.
    let db_pool = connect(&db_url, 10).await.expect("Failed to connect to a database, panicking!");

    //Spawn workers
    
    loop {
        let (stream, _) = listener.accept().await.unwrap();
        let tx_cloned = tx.clone();
        let pool_cloned = db_pool.clone();
        tokio::spawn(async move {
            handle_connection(stream, pool_cloned, tx_cloned).await;
        });
    }
}
