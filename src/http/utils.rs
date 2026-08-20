pub fn split_in_lines(string: String) -> Vec<String> {
    string.lines().map(|l| l.to_string()).collect::<Vec<String>>()
}

pub fn get_body_idx(string_buffer: &str) -> Option<usize> {
    let raw = string_buffer.find("\r\n\r\n")?;
    Some(raw + 4)
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