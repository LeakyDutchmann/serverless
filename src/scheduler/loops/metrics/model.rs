use sqlx::{Row, FromRow};
use sqlx::mysql::MySqlRow;
use tokio::time::{Duration, Instant};
use tokio::sync::RwLock;
use std::sync::Arc;
use std::collections::HashMap;
use tokio::sync::mpsc::Sender;
use wasmtime::Store;
use crate::workers::main_loop::wasm_imports::model::CallerTable;

#[derive(Debug, Clone)]
pub struct MetricsPacket {
    pub success: bool,
    pub cold_start: bool,
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

impl MetricsPacket {
    pub fn new(handling_started: Instant, store: &Store<CallerTable>, fuel_init: u64, cold: bool, success: bool) -> Self {
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
        MetricsPacket {
            success: success,
            cold_start: cold,
            fuel_used_total: fuel_used,
            duration_min: duration,
            duration_max: duration,
            duration_mean: duration,
            window_start: handling_started,
            window_end: handling_ended,
            memory_peak: store.data().memory_usage.peak,
            memory_min: store.data().memory_usage.min,
            memory_mean: store.data().memory_usage.mean,
        }
    }
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
        let mut m = MetricsRecord {
            invocations: 1,
            successes: 0,
            failures: 0,
            cold_starts: 0,
            fuel_used_total: metrics.fuel_used_total,
            memory_peak: metrics.memory_peak,
            memory_min: metrics.memory_min,
            memory_mean: metrics.memory_mean,
            duration_min: metrics.duration_min,
            duration_max: metrics.duration_max,
            duration_mean: metrics.duration_mean,
        };
        if metrics.cold_start {
            m.cold_starts += 1;
        }
        if metrics.success {
            m.successes += 1;
        } else {
            m.failures += 1;
        }
        m
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

pub async fn get_metrics(handling_started: Instant, store: &Store<CallerTable>, path: String, fuel_init: u64, cold: bool) {
    
}