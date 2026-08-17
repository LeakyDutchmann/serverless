use wasmtime::MemoryType;

pub fn validate_memory_exported(memory_type: MemoryType, name: &str) ->  anyhow::Result<()> {
    if memory_type.is_shared() {
        return Err(anyhow::anyhow!("Shared memory is not allowed"));
    }
    if memory_type.minimum() <= 0 {
        return Err(anyhow::anyhow!("Memory minimum must be greater than 0"));
    }
    if memory_type.maximum().is_none() {
        return Err(anyhow::anyhow!("Memory maximum must be defined"));
    }
    if memory_type.maximum().unwrap() < memory_type.minimum() {
        return Err(anyhow::anyhow!("Maximum should be bigger than minimum"));
    }
    if name == "memory" {
        return Ok(())
    }
    Err(anyhow::anyhow!("Failed to validate memory type"))
}