use sqlx::{Row, FromRow};
use sqlx::mysql::MySqlRow;
use tokio::time::{Duration, Instant};
use tokio::sync::RwLock;
use std::sync::Arc;
use std::collections::HashMap;
use wasmtime::Store;
use crate::workers::main_loop::wasm_imports::model::CallerTable;

#[derive(Debug, Clone)]
pub struct MetricsPacket {
    pub invocations: u64,
    pub successes: u64,
    pub failures: u64,
    pub cold_starts: u64,
    pub fuel_used_total: u64,
    pub memory_peak: u64,
    pub memory_min: u64,
    pub memory_mean: f64,
    pub duration_min: Duration,
    pub duration_max: Duration,
    pub duration_mean: Duration,
    pub window_start: Instant,
    pub window_end: Instant,
}

#[derive(Debug, Clone)]
pub struct MetricsRecord {
    pub invocations: u64,
    pub successes: u64,
    pub failures: u64,
    pub cold_starts: u64,
    pub fuel_used_total: u64,
    pub memory_peak: u64,
    pub memory_min: u64,
    pub memory_mean: f64,
    pub duration_min: Duration,
    pub duration_max: Duration,
    pub duration_mean: Duration,
}

impl MetricsRecord {
    pub fn from_packet(metrics: &MetricsPacket) -> Self {
        MetricsRecord {
            invocations: metrics.invocations,
            successes: metrics.successes,
            failures: metrics.failures,
            cold_starts: metrics.cold_starts,
            fuel_used_total: metrics.fuel_used_total,
            memory_peak: metrics.memory_peak,
            memory_min: metrics.memory_min,
            memory_mean: metrics.memory_mean,
            duration_min: metrics.duration_min,
            duration_max: metrics.duration_max,
            duration_mean: metrics.duration_mean,
        }
    }
}

impl FromRow<'_, MySqlRow> for MetricsRecord {
    fn from_row(row: &MySqlRow) -> sqlx::Result<Self> {
        let invocations: u64 = row.try_get("invocations")?;
        let successes: u64 = row.try_get("successes")?;
        let failures: u64 = row.try_get("failures")?;
        let cold_starts: u64 = row.try_get("cold_starts")?;
        let fuel_used_total: u64 = row.try_get("fuel_used_total")?;
        let memory_peak: u64 = row.try_get("memory_peak")?;
        let memory_min: u64 = row.try_get("memory_min")?;
        let memory_mean: f64 = row.try_get("memory_mean")?;
        let duration_min: u64 = row.try_get("duration_min")?;
        let duration_max: u64 = row.try_get("duration_max")?;
        let duration_mean: u64 = row.try_get("duration_mean")?;

        Ok(MetricsRecord {
            invocations,
            successes,
            failures,
            cold_starts,
            fuel_used_total,
            memory_peak,
            memory_min,
            memory_mean,
            duration_min: Duration::from_millis(duration_min),
            duration_max: Duration::from_millis(duration_max),
            duration_mean: Duration::from_millis(duration_mean),
        })
    }
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
        m.memory_mean = store.data().memory_usage.mean;
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
            memory_mean: store.data().memory_usage.mean,
        };
        if cold {
            measurements.cold_starts += 1;
        }
        println!("metrics: {:?}", measurements);
        metrics_map.write().await.insert(path.clone(), measurements);
    }
}