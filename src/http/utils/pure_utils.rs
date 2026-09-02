pub fn split_in_lines(string: String) -> Vec<String> {
    string.lines().map(|l| l.to_string()).collect::<Vec<String>>()
}

pub fn get_body_idx(string_buffer: &str) -> Option<usize> {
    let raw = string_buffer.find("\r\n\r\n")?;
    Some(raw + 4)
}

pub fn find_headers_end(buf: &[u8]) -> Option<usize> {
    buf.windows(4).position(|w| w == b"\r\n\r\n").map(|w| w + 4)
}

pub fn get_body_len(string_buffer: &str) -> Option<usize> {
    let lines: Vec<String> = string_buffer.lines().map(|l| l.to_string()).collect();
    for line in lines {
        if line.starts_with("Content-Length") {
            let parts: Vec<&str> = line.split(":").collect();
            if parts.len() == 2 {
                let result = usize::from_str_radix(parts[1].trim(), 10);
                if let Ok(len) = result {
                    return Some(len);
                } else {
                    let err = result.err().unwrap();
                    println!("Failedd to parse content-length: {}", err);
                    return None;
                }
            }
        }
    }
    None
}

pub fn get_function_name(path: &str) -> String {
    let new = path.split('/').last().unwrap_or("").to_string();
    println!("path: {path}");
    new
}

pub fn parse_request_line(request: &str) -> Option<(String, String)> {
    let line = request.lines().next().unwrap_or("");
    if line.is_empty() {
        return None;
    }
    let parts: Vec<&str> = line.split_whitespace().collect();
    let method = parts[0];
    if parts.len() < 2 {
        return None;
    }
    Some((method.to_string(), parts[1].to_string()))
}

pub fn get_input(buffer: &[u8]) -> Result<Vec<u8>, anyhow::Error> {
    let string = String::from_utf8_lossy(buffer).to_string();
    let len = get_body_len(&string);
    if len.is_none() {
        return Err(anyhow::anyhow!("No body length found"));
    }
    let len = len.unwrap();
    if let Some(body_idx) = get_body_idx(&string) {
        Ok(buffer[body_idx..body_idx + len].to_vec())
    } else {
        Err(anyhow::anyhow!("No body index found"))
    }
}