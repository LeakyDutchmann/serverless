use crate::scheduler::model::Job;
use crate::http::response::{Response, StatusCode, send};
use crate::http::utils::get_body_idx;

use tokio::net::TcpStream;
use tokio::sync::mpsc::Sender;

pub async fn execute(stream: TcpStream, buffer: &[u8], path: &str, tx: Sender<Job>) {
    let input = get_input(&buffer);
    let wrapper: serde_json::Value = serde_json::from_slice(&input).unwrap();
    let inner = wrapper.get("input").unwrap_or_default();
    let raw_input = serde_json::to_vec(inner).unwrap();
    let job = Job {
        path: path.to_string(),
        input: raw_input,
        stream,
    };
    let _ = tx.send(job).await;
}

pub fn get_input(buffer: &[u8]) -> Vec<u8> {
    if let Some(body_idx) = get_body_idx(String::from_utf8_lossy(buffer).as_ref()) {
        buffer[body_idx..].to_vec()
    } else {
        Vec::new()
    }
}