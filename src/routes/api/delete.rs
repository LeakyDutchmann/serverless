use crate::http::response::{Response, StatusCode};

pub async fn delete(_buffer: &[u8]) -> Response{
    Response::new(StatusCode::BadRequest, vec![], Some("Delete endpoint is not implemented yet bruh, sorry".to_string()), false)
}