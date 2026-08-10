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
    pub fn text(status: StatusCode, body: String) -> Response {
        let status_line = match status {
            StatusCode::Ok => "HTTP/1.1 200 Ok",
            StatusCode::NotFound => "HTTP/1.1 404 Not Found",
            StatusCode::IntServerError => "HTTP/1.1 500 Internal Server Error",
            StatusCode::Unauthorized => "HTTP/1.1 401 Unauthorized",
            StatusCode::BadRequest => "HTTP/1.1 400 Bad Request",
        };
        let len = body.len();
        let line = format!("{}\r\nContent-Length: {}\r\nContent-Type: plain/text\r\n\r\n", status_line, len);
        Response {
            resp: line + &body,
        }
    }
}

pub async fn send(stream: &mut TcpStream, response: &Response) {
    let _ = stream.write_all(response.resp.as_bytes()).await;
    let _ = stream.flush().await;
}
