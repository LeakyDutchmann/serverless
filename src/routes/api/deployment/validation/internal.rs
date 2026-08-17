use wasmparser::{Parser, Payload};
use wasmparser::{TypeRef, DataKind, Operator};


pub fn check_internal_exports(bytes: &[u8]) -> anyhow::Result<()> {
    let mut max_memory_size: Option<u64> = None;
    for payload in Parser::new(0).parse_all(bytes) {
        match payload? {
            Payload::MemorySection(memories) => { 
                if memories.count() > 1 {
                    return Err(anyhow::anyhow!("Module contains forbiden amount of defined memories: {}", memories.count()));
                }
                for mem in memories {
                    let mem = mem?;
                    if mem.shared {
                        return Err(anyhow::anyhow!("Module contains shared memory"));
                    }
                    if mem.maximum.is_none() {
                        return Err(anyhow::anyhow!("Module contains memory with no maximum"));
                    }
                    if mem.initial > mem.maximum.unwrap() {
                        return Err(anyhow::anyhow!("Module contains memory with maximum less than initial"));
                    }
                    max_memory_size = Some(mem.maximum.unwrap() * 65536) ;
                }
            }
            Payload::TableSection(tables) => {  
                if tables.count() > 0 {
                    return Err(anyhow::anyhow!("Module contains forbiden amount of tables: {}", tables.count()));
                }
            }
            Payload::ImportSection(imports) => {
                for import in imports.into_imports() {
                    let import = import?;
                    match import.ty {
                        TypeRef::Table(_) => {
                            return Err(anyhow::anyhow!("Module contains forbiden import table: {:?}", import.ty));
                        }
                        TypeRef::Memory(_) => {
                            return Err(anyhow::anyhow!("Module contains forbiden import memory: {:?}", import.ty));
                        }
                        TypeRef::Global(_) => {
                            return Err(anyhow::anyhow!("Module contains forbiden import global: {:?}", import.ty));
                        }
                        _ => {}
                    }
                }
            }
            Payload::ElementSection(_) => {
                return Err(anyhow::anyhow!("Module contains element section"));
            }
            Payload::DataSection(data) => {
                if max_memory_size.is_none() {
                    return Err(anyhow::anyhow!("DataSection encountered before MemorySection"));
                }
                for d in data {
                    let d = d?;
                    match d.kind {
                        DataKind::Active{memory_index, offset_expr} => {
                            if memory_index != 0 {
                                return Err(anyhow::anyhow!("Module contains active data with non-zero memory index: {}", memory_index));
                            }
                            let mut offset: Option<u32> = None;
                            let operators = offset_expr.get_operators_reader(); 
                            for operator in operators {
                                let operator = operator?;
                                match operator {
                                    Operator::I32Const{value} => {
                                        if value < 0 {
                                            return Err(anyhow::anyhow!("Negative offset: {}", value));
                                        }
                                        offset = Some(value as u32);
                                    }
                                    Operator::End => {
                                        break;
                                    }
                                    _ => {
                                        return Err(anyhow::anyhow!("Unsupported operator: {:?}", operator));
                                    }
                                }
                            }
                            let data_len = d.data.len() as u32;
                            if offset.is_none() {
                                return Err(anyhow::anyhow!("No offset found"));
                            }
                            if offset.unwrap() as u64 + data_len as u64 > max_memory_size.unwrap() {
                                return Err(anyhow::anyhow!("Offset + data length exceeds max memory size"));
                            }
                        }
                        _ => {}
                    }
                }
            }
            _ => {}
        }
    }
    Ok(())
}