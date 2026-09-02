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

#[cfg(test)]
mod test {
    use super::*;
    #[test]
    fn function_name_parser() {
        assert_eq!(get_function_name("/"), "".to_string());
        assert_eq!(get_function_name("/foo"), "foo".to_string());
        assert_eq!(get_function_name("/foo/bar"), "bar".to_string());
    }

    #[test]
    fn body_len_parser() {
        let headers = "GET /test HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: 10\r\n\r\n".to_string();
        assert_eq!(get_body_len(&headers), Some(10));
        let no_len = "GET /test HTTP/1.1\r\nContent-Type: application/json\r\n\r\n".to_string();
        assert_eq!(get_body_len(&no_len), None);
        let invalid = "GET /test HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: invalid\r\n\r\n".to_string();
        assert_eq!(get_body_len(&invalid), None);
        let invalid_len = "GET /test HTTP/1.1\r\nContent-Type: application/json\r\nContent-Length: -10\r\n\r\n".to_string();
        assert_eq!(get_body_len(&invalid_len), None);
    }
}