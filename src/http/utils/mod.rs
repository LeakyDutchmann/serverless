pub mod tcp;
pub mod pure_utils;

pub use pure_utils::*;
pub use tcp::*;

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