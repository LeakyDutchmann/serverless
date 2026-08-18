use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub enum StatusCode {
    Ok,
    NotFound,
    IntServerError,
    Unauthorized,
    BadRequest,
}


pub struct Response {
    pub resp: String,
}

impl Response {
    pub fn json(status: StatusCode, result: Vec<u8>, reason: Option<String>) -> Response {
        let status_line = match status {
            StatusCode::Ok => "HTTP/1.1 200 Ok",
            StatusCode::NotFound => "HTTP/1.1 404 Not Found",
            StatusCode::IntServerError => "HTTP/1.1 500 Internal Server Error",
            StatusCode::Unauthorized => "HTTP/1.1 401 Unauthorized",
            StatusCode::BadRequest => "HTTP/1.1 400 Bad Request",
        };
        if let Some(reason) = reason {
            let reason_js = serde_json::json!({"reason": reason}).to_string();
            let len = reason_js.len();
            let line = format!("{}\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}", status_line, len, reason_js);
            Response {
                resp: line,
            }
        } else {
            let body = serde_json::json!({
                "result": serde_json::from_slice::<serde_json::Value>(&result).unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&result).to_string()))
            });
            let body = body.to_string();
            let len = body.len();
            let line = format!("{}\r\nContent-Length: {}\r\nContent-Type: application/json\r\n\r\n{}", status_line, len, body);
            Response {
                resp: line,
            }
        }
    }
}

pub async fn send(stream: &mut TcpStream, response: &Response) {
    let _ = stream.write_all(response.resp.as_bytes()).await;
    let _ = stream.flush().await;
}
