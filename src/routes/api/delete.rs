use tokio::net::TcpStream;
use crate::http::response::{Response, StatusCode};

pub async fn delete(buffer: &[u8]) -> Response{
    Response::new(StatusCode::BadRequest, vec![], Some("Delete endpoint is not implemented yet bruh, sorry".to_string()), false)
}