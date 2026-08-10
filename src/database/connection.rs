use sqlx::mysql::{MySqlPool, MySqlPoolOptions};
use std::time::Duration;

pub async fn connect(path: &str, max_attempts: usize) -> Option<MySqlPool> {
    for attempt in 1..=max_attempts {
        let poll = MySqlPoolOptions::new()
            .max_connections(50)
            .idle_timeout(Duration::from_secs(15))
            .connect(path)
            .await;
        match poll {
            Ok(pool) => {
                println!("Successfully connected to database on attempt {}", attempt);
                return Some(pool);
            },
            Err(e) => {
                println!("Attempt {}: Failed to connect to database: {}", attempt, e);
                tokio::time::sleep(Duration::from_secs(1)).await;
                continue;
            },
        };
    }
    println!("Failed to connect to database after {} attempts", max_attempts);
    None
}