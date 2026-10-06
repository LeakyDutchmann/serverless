use super::model::{MetricsPacket, MetricsRecord};

use tokio::sync::mpsc::Receiver;
use sqlx::{MySqlPool, FromRow};
use tokio::task::JoinHandle;
use tokio::time::Duration;

pub async fn start_metrics_loop(
    mut rx: Receiver<(String, MetricsPacket)>,
    db: MySqlPool,
) -> JoinHandle<()> {
    let task = tokio::spawn(async move {
        let db = db.clone();
        while let Some((path, metrics)) = rx.recv().await {
            let result = sqlx::query("SELECT * FROM metrics WHERE path = ?")
                .bind(&path)
                .fetch_optional(&db.clone())
                .await;
            let row = match result {
                Ok(opt) => { opt }
                Err(e) => { 
                    println!("Error: {:?}", e);
                    continue 
                }
            };
            let new_record = if let Some(row) = row {
                let record = MetricsRecord::from_row(&row);
                if let Ok(metrics_packet) = record {
                    update_record(metrics_packet, metrics)
                } else {
                    println!("[line: 26] Failed to serialize MetricsRecord from row: {:?}", record.unwrap_err());
                    continue
                }
            } else {
                MetricsRecord::from_packet(&metrics)
            };
            let db_pool = db.clone();
            tokio::spawn(async move {
                let r = new_record;
                let result = sqlx::query("INSERT INTO metrics (path, invocations, successes, failures, cold_starts, fuel_used_total, memory_peak, memory_min, memory_mean, duration_min, duration_max, duration_mean) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)")
                    .bind(&path)
                    .bind(r.invocations)
                    .bind(r.successes)
                    .bind(r.failures)
                    .bind(r.cold_starts)
                    .bind(r.fuel_used_total)
                    .bind(r.memory_peak)
                    .bind(r.memory_min)
                    .bind(r.memory_mean)
                    .bind(r.duration_min.as_millis() as u64)
                    .bind(r.duration_max.as_millis() as u64)
                    .bind(r.duration_mean.as_millis() as u64)
                    .execute(&db_pool)
                    .await;
                match result {
                    Ok(_) => {}
                    Err(e) => {
                        eprintln!("Failed to insert metrics: {}", e);
                    }
                }
            });
        }
    });
    task
}



fn update_record(record: MetricsRecord, packet: MetricsPacket) -> MetricsRecord {
    let mut r = record;
    let p = packet;
    
    r.memory_mean = (r.invocations as f64 * r.memory_mean + p.invocations as f64 * p.memory_mean) / (r.invocations + p.invocations) as f64;
    r.duration_mean = merge_duration(r.duration_mean, r.invocations, p.duration_mean, p.invocations);
    
    r.invocations += p.invocations;
    r.successes += p.successes;
    r.failures += p.failures;
    r.cold_starts += p.cold_starts;
    r.fuel_used_total += p.fuel_used_total;
    r.memory_peak = r.memory_peak.max(p.memory_peak);
    r.memory_min = r.memory_min.min(p.memory_min);
    r.duration_max = r.duration_max.max(p.duration_max);
    r.duration_min = r.duration_min.min(p.duration_min);
    return r;
}

fn merge_duration(r_mean: Duration, r_count: u64, p_mean: Duration, p_count: u64) -> Duration {
    let r_ns = r_mean.as_nanos() * r_count as u128;
    let p_ns = p_mean.as_nanos() * p_count as u128;
    let combined_ns = r_ns + p_ns;
    let combined_count = r_count + p_count;
    if combined_count == 0 {
        return Duration::from_nanos(0);
    }
    let new_mean_ns = combined_ns / combined_count as u128;
    Duration::from_nanos(new_mean_ns as u64)
}