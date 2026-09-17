
pub struct Shutdown {
    pub reason: String,
    pub instant: tokio::time::Instant,
}