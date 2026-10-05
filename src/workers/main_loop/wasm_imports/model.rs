use wasmtime::Linker;
use tokio::time::{Duration, Instant};
use std::collections::HashMap;
use wasi::http::types::{Fields, FutureTrailers, OutgoingRequest, OutgoingBody, IncomingResponse, OutputStream, InputStream, IncomingBody, FutureIncomingResponse};

use super::logging::provide_logging;
use super::random::provide_random;
use super::http::model::provide_http;


pub struct MemoryUsage {
    pub peak: u64,
    pub min: u64,
    pub mean: f64,
    pub updates: u64
}

impl MemoryUsage {
    pub fn zero() -> Self {
        Self { peak: 0, min: u64::MAX, mean: 0.0, updates: 0 }
    }
    pub fn update(&mut self, usage: u64) {
        let new_count = self.updates + 1;
        self.mean = (self.mean * (self.updates as f64) + usage as f64) / new_count as f64;
        self.updates = new_count;
        self.peak = self.peak.max(usage);
        self.min = self.min.min(usage);
    }
}


pub struct StreamHandle {
    pub handle: Option<u32>,
    pub released: bool,
}

impl StreamHandle {
    pub fn empty() -> Self {
        Self { handle: None, released: false }
    }    
}

pub struct ParentHandle {
    pub handle: u32,
}

pub struct BodyHandle {
    pub handle: Option<u32>,
}

impl BodyHandle {
    pub fn empty() -> Self {
        Self { handle: None }
    }
}

pub struct TrailersHandle {
    pub handle: Option<u32>,
}

impl TrailersHandle {
    pub fn empty() -> Self {
        Self { handle: None }
    }
    pub fn from(handle: u32) -> Self {
        Self { handle: Some(handle) }
    }
}

pub struct CallerTable {
    pub next_handle: u32,
    pub memory_usage: MemoryUsage,
    pub future_incoming_responses: HashMap<u32, FutureIncomingResponse>,
    pub outgoing_requests: HashMap<u32, (OutgoingRequest, BodyHandle)>,
    pub incoming_responses: HashMap<u32, IncomingResponse>,
    pub output_streams: HashMap<u32, (OutputStream, ParentHandle)>,
    pub input_streams: HashMap<u32, (InputStream, ParentHandle)>,
    pub outgoing_body: HashMap<u32, (OutgoingBody, StreamHandle, TrailersHandle)>,
    pub incoming_body: HashMap<u32, (IncomingBody, StreamHandle)>,
    pub outgoing_trailers: HashMap<u32, Fields>,
    pub incoming_trailers: HashMap<u32, Fields>,
    pub future_trailers: HashMap<u32, FutureTrailers>,
}

impl CallerTable {
    pub fn new() -> Self {
        Self {
            next_handle: 1,
            future_incoming_responses: HashMap::new(),
            memory_usage: MemoryUsage::zero(),
            outgoing_requests: HashMap::new(),
            incoming_responses: HashMap::new(),
            output_streams: HashMap::new(),
            input_streams: HashMap::new(),
            outgoing_body: HashMap::new(),
            incoming_body: HashMap::new(),
            future_trailers: HashMap::new(),
            incoming_trailers: HashMap::new(),
            outgoing_trailers: HashMap::new(),
        }
    }
}


pub fn provide_imports(mut linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    match provide_logging(&mut linker) {
        Ok(_) => {},
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to provide logging: {}", e));
        }
    }
    match provide_random(&mut linker) {
        Ok(_) => {},
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to provide random: {}", e));
        }
    }
    match provide_http(&mut linker) {
        Ok(_) => {},
        Err(e) => {
            return Err(anyhow::anyhow!("Failed to provide http: {}", e));
        }
    }
    Ok(())
}