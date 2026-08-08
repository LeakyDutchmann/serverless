pub enum Route {
    POST,
    DELETE,
    Unexpected(String),
}

impl Route {
    pub fn from_buffer(buffer: &[u8]) -> Route {
        if buffer.starts_with(b"POST") {
            return Route::POST;
        }
        if buffer.starts_with(b"DELETE") {
            return Route::DELETE;
        }
        Route::Unexpected(String::from_utf8_lossy(buffer).into_owned())
    }
}