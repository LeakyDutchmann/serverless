use tokio::time::{Duration, Instant};
use tokio::sync::RwLock;
use std::sync::Arc;
use std::collections::HashMap;
use wasmtime::Store;
use super::super::wasm_imports::model::CallerTable;

#[derive(Debug, Clone)]
pub struct MetricsPacket {
    pub invocations: u64,
    pub successes: u64,
    pub failures: u64,
    pub cold_starts: u64,
    pub fuel_used_total: u64,
    pub memory_peak: u64,
    pub memory_min: u64,
    pub duration_min: Duration,
    pub duration_max: Duration,
    pub duration_mean: Duration,
    pub window_start: Instant,
    pub window_end: Instant,
}

pub async fn update_metrics(metrics_map: Arc<RwLock<HashMap<String, MetricsPacket>>>, handling_started: Instant, store: &Store<CallerTable>, path: String, fuel_init: u64, cold: bool) {
    let handling_ended = Instant::now();
    let duration = handling_ended - handling_started;
    let fuel_used = match store.get_fuel() {
        Ok(fuel) => {
            if fuel < fuel_init {
                fuel_init - fuel
            } else {
                fuel_init
            }
        },
        Err(e) => {
            println!("MetricsErr: failed to fetch fuel from store: {:?}", e);
            0
        },
    };
    if let Some(m) = metrics_map.write().await.get_mut(&path) {
        m.invocations += 1;
        m.successes += 1;
        m.duration_min = m.duration_min.min(duration);
        m.duration_max = m.duration_max.max(duration);
        m.duration_mean = Duration::from_millis(123);
        m.window_end = handling_ended;
        m.memory_peak = m.memory_peak.max(store.data().memory_usage.peak);
        m.memory_min = m.memory_min.min(store.data().memory_usage.min);
        m.fuel_used_total += fuel_used;
        if cold {
            m.cold_starts += 1;
        }
        println!("metrics: {:?}", m);
    } else {
        let mut measurements = MetricsPacket {
            invocations: 1,
            successes: 1,
            failures: 0,
            cold_starts: 0,
            fuel_used_total: fuel_used,
            duration_min: duration,
            duration_max: duration,
            duration_mean: duration,
            window_start: handling_started,
            window_end: handling_ended,
            memory_peak: store.data().memory_usage.peak,
            memory_min: store.data().memory_usage.min,
        };
        if cold {
            measurements.cold_starts += 1;
        }
        println!("metrics: {:?}", measurements);
        metrics_map.write().await.insert(path.clone(), measurements);
    }
}