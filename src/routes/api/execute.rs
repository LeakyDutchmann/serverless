use crate::workers::model::Message;
use crate::http::response::{Response, StatusCode, send};

use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

pub async fn execute(mut stream: TcpStream, path: &str, tx: Sender<String>) {
    let result = tx.send(path.to_string()).await;
    match result {
        Ok(_) => {
            let response = Response::text(StatusCode::Ok, format!("The job {} is being processed from now", &path));
            send(&mut stream, &response).await;
        }
        Err(e) => {
            eprintln!("Failed to send job: {}", e);
            let response = Response::text(StatusCode::IntServerError, "Failed to send job".to_string());
            send(&mut stream, &response).await;
        },
    }
}