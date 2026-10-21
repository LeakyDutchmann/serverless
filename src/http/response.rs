use tokio::io::AsyncWriteExt;
use tokio::net::TcpStream;

pub enum StatusCode {
    Ok,
    NotFound,
    IntServerError,
    Unauthorized,
    BadRequest,
}

impl StatusCode {
    fn as_str(&self) -> &str {
        match &self {
            StatusCode::BadRequest => {
                "HTTP/1.1 400 Bad Request"
            },
            StatusCode::IntServerError => {
                "HTTP/1.1 500 Internal Server Error"
            },
            StatusCode::NotFound => {
                "HTTP/1.1 404 Not Found"
            },
            StatusCode::Ok => {
                "HTTP/1.1 200 Ok"
            },
            StatusCode::Unauthorized => {
                "HTTP/1.1 401 Unauthorized"
            }
        }
    }
}


pub struct Response {
    pub status: StatusCode,
    pub headers: Vec<(String, String)>,
    pub body: Vec<u8>,
}

impl Response {
    pub fn new(status: StatusCode, result: Vec<u8>, reason: Option<String>, keep_alive: bool) -> Response {
        if let Some(reason) = reason {
            let reason_js = serde_json::json!({"reason": reason}).to_string();
            let len = reason_js.len();
            let mut headers = Vec::new();
            headers.push(("Content-Length".to_string(), format!("{}", len)));
            headers.push(("Content-Type".to_string(), "application/json".to_string()));
            if keep_alive {
                headers.push(("Connection".to_string(), "keep-alive".to_string()));
                headers.push(("Keep-Alive".to_string(), "timeout=5, max=100".to_string()));
            } else {
                headers.push(("Connection".to_string(), "close".to_string()));
            }
            Response {
                status,
                headers,
                body: reason_js.as_bytes().to_vec()
            }
        } else {
            let body = serde_json::json!({
                "result": serde_json::from_slice::<serde_json::Value>(&result).unwrap_or_else(|_| serde_json::Value::String(String::from_utf8_lossy(&result).to_string()))
            });
            let body = body.to_string();
            let len = body.len();
            let mut headers = Vec::new();
            headers.push(("Content-Length".to_string(), format!("{}", len)));
            headers.push(("Content-Type".to_string(), "application/json".to_string()));
            if keep_alive {
                headers.push(("Connection".to_string(), "keep-alive".to_string()))
            } else {
                headers.push(("Connection".to_string(), "close".to_string()))
            }
            Response {
                status,
                headers,
                body: body.as_bytes().to_vec()
            }
        }
    }
    pub fn as_bytes(&self) -> Vec<u8> {
        let status_line = &self.status.as_str();
        let mut line = format!("{}\r\n", status_line);
        for (header, val) in &self.headers {
            let n_h = header.to_owned() + ": " + val + "\r\n";
            line += &n_h;
        }
        line += "\r\n";
        
        let mut headers = line.as_bytes().to_vec();
        headers.extend_from_slice(self.body.as_slice());
        headers
    }
}

pub async fn send(stream: &mut TcpStream, response: &Response) -> anyhow::Result<()> {
    stream.write_all(response.as_bytes().as_slice()).await?;
    stream.flush().await?;
    Ok(())
}
