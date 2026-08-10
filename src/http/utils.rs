pub fn split_in_lines(string: String) -> Vec<String> {
    string.lines().map(|l| l.to_string()).collect::<Vec<String>>()
}

pub fn get_body_idx(string_buffer: &str) -> Option<usize> {
    let raw = string_buffer.find("\r\n\r\n")?;
    Some(raw + 4)
}