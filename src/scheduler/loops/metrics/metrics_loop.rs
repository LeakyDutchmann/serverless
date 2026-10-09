use super::model::{MetricsPacket, MetricsRecord};

use tokio::sync::mpsc::Receiver;
use sqlx::{MySqlPool, FromRow};
use tokio::task::JoinHandle;
use tokio::time::{Duration, interval};
use tokio::select;
use std::sync::Arc;
use tokio::sync::RwLock;
use std::collections::HashMap;
use anyhow::anyhow;

pub fn start_metrics_loop(
    mut rx: Receiver<(String, MetricsPacket)>,
    db: MySqlPool,
) -> JoinHandle<()> {
    let task = tokio::spawn(async move {
        let db_pool = db.clone();
        let mut flush_timeout = interval(Duration::from_secs(15));
        let queue: Arc<RwLock<HashMap<String, MetricsRecord>>> = Arc::new(RwLock::new(HashMap::new()));
        loop {
            let q_map = queue.read().await;
            if q_map.len() >= 100 {
                handle_queue_flush(db_pool.clone(), Arc::clone(&queue)).await;
            }
            drop(q_map);
            
            select! {
                Some((path, metrics)) = rx.recv() => {
                    println!("Metrics: received record for path: {}", path);
                    let mut map = queue.write().await;
                    if let Some(record) = map.get_mut(&path) {
                        update_record(record, metrics);
                    } else {
                        let record = MetricsRecord::from_packet(&metrics);
                        map.insert(path, record);
                    }
                }
                _ = flush_timeout.tick() => {
                    println!("Metrics flush timeout");
                    handle_queue_flush(db_pool.clone(), Arc::clone(&queue)).await;
                } 
            }
        }
    });
    task
}

async fn handle_queue_flush(db_pool: MySqlPool, arc_map: Arc<RwLock<HashMap<String, MetricsRecord>>>) {
    let mut map = arc_map.write().await;
    for (path, record) in map.drain() {
        println!("Metrics flush: upserting record for path: {}", path);
        match upsert_record(path.clone(), record.clone(), db_pool.clone()).await {
            Ok(_) => {println!("Metrics upserted successfully");},
            Err(e) => {
                println!("MetricsErr: Failed to update metrics db record: {}", e)
            }
        }
    }
}


async fn upsert_record(path: String, metrics: MetricsRecord, db: MySqlPool) -> anyhow::Result<()> {
    let result = sqlx::query("SELECT * FROM metrics WHERE path = ?")
        .bind(&path)
        .fetch_optional(&db)
        .await;
    let row = match result {
        Ok(opt) => { opt }
        Err(e) => { 
            // println!("Error: {:?}", e);
            return Err(anyhow!("Failed to fetch MySql row: {}", e));
        }
    };
    let new_record = if let Some(row) = row {
        let record = MetricsRecord::from_row(&row);
        if let Ok(metrics_packet) = record {
            merge_records(metrics_packet, metrics)
        } else {
            // println!("[line: 26] Failed to serialize MetricsRecord from row: {:?}", record.unwrap_err());
            return Err(anyhow!("Failed to serialize MetricsRecord from row: {}", record.unwrap_err()));
        }
    } else {
        metrics
    };
    let r = new_record;
    let result = sqlx::query(
                "INSERT INTO metrics (path, invocations, successes, failures, cold_starts, fuel_used_total, memory_peak, memory_min, memory_mean, duration_min, duration_max, duration_mean) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?) \
                ON DUPLICATE KEY UPDATE \
                invocations = VALUES(invocations), \
                successes = VALUES(successes), \
                failures = VALUES(failures), \
                cold_starts = VALUES(cold_starts), \
                fuel_used_total = VALUES(fuel_used_total), \
                memory_peak = VALUES(memory_peak), \
                memory_min = VALUES(memory_min), \
                memory_mean = VALUES(memory_mean), \
                duration_min = VALUES(duration_min), \
                duration_max = VALUES(duration_max), \
                duration_mean = VALUES(duration_mean)"
        )
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
        .execute(&db)
        .await;
    match result {
        Ok(_) => {
            println!("Metrics inserted successfully");
        }
        Err(e) => {
            eprintln!("Failed to insert metrics: {}", e);
        }
    }
    Ok(())
}

fn merge_records(mut r_1: MetricsRecord, r_2: MetricsRecord) -> MetricsRecord {
    r_1.memory_mean = (r_1.invocations as f64 * r_1.memory_mean + r_2.invocations as f64 * r_2.memory_mean) / (r_1.invocations + r_2.invocations) as f64;
    r_1.duration_mean = merge_duration(r_1.duration_mean, r_1.invocations, r_2.duration_mean, r_2.invocations);

    r_1.invocations += r_2.invocations;
    r_1.successes += r_2.successes;
    r_1.failures += r_2.failures;
    r_1.cold_starts += r_2.cold_starts;

    r_1.fuel_used_total += r_2.fuel_used_total;
    r_1.memory_peak = r_1.memory_peak.max(r_2.memory_peak);
    r_1.memory_min = r_1.memory_min.min(r_2.memory_min);
    r_1.duration_max = r_1.duration_max.max(r_2.duration_max);
    r_1.duration_min = r_1.duration_min.min(r_2.duration_min);
    return r_1;
}

fn update_record(record: &mut MetricsRecord, packet: MetricsPacket)  {
    let r = record;
    let p = packet;
    
    r.memory_mean = (r.invocations as f64 * r.memory_mean + 1 as f64 * p.memory_mean) / (r.invocations + 1) as f64;
    r.duration_mean = merge_duration(r.duration_mean, r.invocations, p.duration_mean, 1);

    if p.success {
        r.successes += 1;
    } else {
        r.failures += 1;
    }
    if p.cold_start {
        r.cold_starts += 1;
    }
    
    r.invocations += 1;
    r.fuel_used_total += p.fuel_used_total;
    r.memory_peak = r.memory_peak.max(p.memory_peak);
    r.memory_min = r.memory_min.min(p.memory_min);
    r.duration_max = r.duration_max.max(p.duration_max);
    r.duration_min = r.duration_min.min(p.duration_min);
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