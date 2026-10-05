use wasi::http::types::{OutgoingRequest, Scheme,  OutgoingBody, Fields, Method};
use wasi::http::outgoing_handler;
use wasmtime::{Linker, Caller};
use crate::workers::main_loop::wasm_imports::model::TrailersHandle;

use super::super::model::{CallerTable, ParentHandle, StreamHandle, BodyHandle};

pub fn register(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    register_new_request(linker)?;
    register_set_method(linker)?;
    register_set_scheme(linker)?;
    register_append_header(linker)?;
    register_set_authority(linker)?;
    register_body(linker)?;
    register_body_stream(linker)?;
    register_body_finish(linker)?;
    register_drops(linker)?;
    register_handle(linker)?;
    register_append_trailers(linker)?;
    Ok(())     
}

fn register_handle(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "handle", |mut caller: Caller<'_, CallerTable>, handle: i32| -> i32 {
        let handle = handle as u32;
        
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        let data = caller.data_mut();
        data.memory_usage.update(m_usage);
        
        if let Some((request, body_handle)) = data.outgoing_requests.remove(&handle) {
            if body_handle.handle.is_some() {
                if let Some((body, stream_handle, trailers_handle)) = data.outgoing_body.remove(&body_handle.handle.unwrap()) {
                    if !stream_handle.released {
                        if stream_handle.handle.is_some() {
                            data.output_streams.remove(&stream_handle.handle.unwrap());
                        } else {
                            println!("body claims to have stream but it does not");
                            return 0;
                        }
                    }
                    
                    let trailers = if trailers_handle.handle.is_some() {
                        if let Some(trailers) = data.outgoing_trailers.remove(&trailers_handle.handle.unwrap()) {
                            Some(trailers)
                        } else {
                            None
                        }
                    } else {
                        None
                    };
                    
                    match OutgoingBody::finish(body, trailers) {
                        Ok(_) => {}
                        Err(e) => {
                            println!("Failed to finish outgoing body: {:?}", e);
                            return 0;
                        }
                    }
                }
            }
            let future = outgoing_handler::handle(request, None);
            match future {
                Ok(f) => {
                    let new_handle = data.next_handle;
                    data.next_handle += 1;
                    data.future_incoming_responses.insert(new_handle, f);
                    return new_handle as i32;
                },
                Err(e) => {
                    eprintln!("failed to handle request: {:?}", e);
                    return 0;
                },
            }
        }
        0
    })?;
    Ok(())
}

fn register_new_request(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "new-outgoing-request", |mut caller: Caller<'_, CallerTable>| -> i32 {
        let request = OutgoingRequest::new(Fields::new());
        
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        let data = caller.data_mut();
        data.memory_usage.update(m_usage);
        
        let handle = data.next_handle;
        data.next_handle += 1;
        data.outgoing_requests.insert(handle, (request, BodyHandle::empty()));
        
        handle as i32
    })?;
    Ok(())
}

fn register_set_method(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "outgoing-request.set-method", |mut caller: Caller<'_, CallerTable>, handle: i32, method: i32| -> (i32, i32) {
        let handle = handle as u32;
        
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        let data = caller.data_mut();
        data.memory_usage.update(m_usage);
        
        if let Some((req, _)) = data.outgoing_requests.get_mut(&handle) {
            let method = match method {
                0 => Method::Get,
                1 => Method::Post,
                _ => {
                    return (1, 0);
                },
            };
            match req.set_method(&method) {
                Ok(_) => (0, 0),
                Err(_) => (1, 0),
            }
        } else {
            (1, 0)
        }
    })?;
    Ok(())
}

fn register_set_scheme(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "outgoing-request.set-scheme", |mut caller: Caller<'_, CallerTable>, handle: i32, scheme: i32| -> (i32, i32) {
        let handle = handle as u32;
        
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        let data = caller.data_mut();
        data.memory_usage.update(m_usage);

        if let Some((req, _)) = data.outgoing_requests.get_mut(&handle) {
            let scheme = match scheme {
                0 => Scheme::Http,
                1 => Scheme::Https,
                _ => {
                    return (1, 0);
                }
            };
            match req.set_scheme(Some(&scheme)) {
                Ok(_) => (0, 0),
                Err(_) => (1, 0),
            }
        } else {
            (1, 0)
        }
    })?;
    Ok(())
}

//here constant buffer size - bad!
fn register_append_header(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "outgoing-request.append-header", |mut caller: Caller<'_, CallerTable>, handle: i32, name_ptr: i32, name_len: i32, val_ptr: i32, val_len: i32| -> (i32, i32) {
        let handle = handle as u32;
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        let mut buffer = [0u8; 4096];
        memory.read(&caller, name_ptr as usize, &mut buffer).unwrap();
        let name = String::from_utf8_lossy(&buffer[..name_len as usize]);
        let data = caller.data_mut();
        data.memory_usage.update(m_usage);
        if let Some((req, _)) = data.outgoing_requests.get_mut(&handle) {
            match req.headers().append(&name, &buffer[val_ptr as usize..val_len as usize]) {
                Ok(_) => (0, 0),
                Err(e) => {
                    println!("Failed to append header: {}", e);
                    (1, 0)
                },
            }
        } else {
            return (1, 0)
        }
    })?;
    Ok(())
}

fn register_set_authority(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "outgoing-request.set-authority", |mut caller: Caller<'_, CallerTable>, handle: i32, authority_ptr: i32, authority_len: i32| -> (i32, i32) {
        let handle = handle as u32;
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller);        
        let mut buffer = [0u8; 4096];
        memory.read(&caller, authority_ptr as usize, &mut buffer).unwrap();
        let authority = String::from_utf8_lossy(&buffer[..authority_len as usize]);

        //Here you have to provide authority checks. Is this goddamn website allowed at all?
        let data = caller.data_mut();
        data.memory_usage.update(m_usage as u64);
        if let Some((req, _)) = data.outgoing_requests.get_mut(&handle) {
            match req.set_authority(Some(&authority)) {
                Ok(_) => (0, 0),
                Err(e) => {
                    eprintln!("failed to set_authority: {:?}", e);
                    (1, 0)
                }
            }
        } else {
            return (1, 0)
        }
    })?;
    Ok(())
}

fn register_body(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "outgoing-request.body", |mut caller: Caller<'_, CallerTable>, handle: i32| -> i32 {
        let handle = handle as u32;
        
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        let data = caller.data_mut();
        data.memory_usage.update(m_usage);
        
        if let Some((req, body_handle)) = data.outgoing_requests.get_mut(&handle) {
            let new_handle = data.next_handle;
            data.next_handle += 1;
            if let Ok(outgoing_body) = req.body() {
                body_handle.handle = Some(new_handle as u32);
                let t_handle = data.next_handle;
                data.next_handle += 1;
                data.outgoing_body.insert(new_handle, (outgoing_body, StreamHandle::empty(), TrailersHandle::from(t_handle)));
                data.outgoing_trailers.insert(t_handle, Fields::new());
                return new_handle as i32
            } else {
                return 0
            }
        } else {
            return 0
        }
    })?;
    Ok(())
}

fn register_body_stream(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "outgoing-body.stream", |mut caller: Caller<'_, CallerTable>, body_handle: i32| -> i32 {
        let handle = body_handle as u32;
        
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        let data = caller.data_mut();
        data.memory_usage.update(m_usage);
        
        if let Some((body, stream_handle, _)) = data.outgoing_body.get_mut(&handle) {
            if stream_handle.handle.is_some() || stream_handle.released {
                return 0
            }
            if let Ok(stream) = body.write() {
                let new_handle = data.next_handle;
                stream_handle.handle.replace(new_handle);
                data.next_handle += 1;
                data.output_streams.insert(new_handle, (stream, ParentHandle { handle }));
                return new_handle as i32
            } else {
                return 0
            }
        } else {
            return 0
        }
    })?;
    Ok(())
}

fn register_append_trailers(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "outgoing-body.append-trailer", |mut caller: Caller<'_, CallerTable>, handle: i32, name_ptr: i32, name_len: i32, val_ptr: i32, val_len: i32| -> (i32, i32) {
        let handle = handle as u32;
        let mut buffer = vec![0u8; name_len as usize];

        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        match memory.read(&caller, name_ptr as usize, &mut buffer) {
            Ok(_) => {}
            Err(_) => return (1, 0),
        }
        let name = String::from_utf8_lossy(&buffer).to_string();
        buffer = vec![0u8; val_len as usize];
        match memory.read(&caller, val_ptr as usize, &mut buffer) {
            Ok(_) => {}
            Err(_) => return (1, 0),
        }
        let data = caller.data_mut();
        data.memory_usage.update(m_usage);
        let val = &buffer.to_vec();
        
        if let Some((_, _, trailers_handle)) = data.outgoing_body.get_mut(&handle) {
            if let Some(handle) = trailers_handle.handle {
                if let Some(trailers) = data.outgoing_trailers.get_mut(&handle) {
                    match trailers.append(&name, val) {
                        Ok(_) => return (0, 0),
                        Err(_) => return (1, 0),
                    }
                } else {
                    return (1, 0);
                }
            } else {
                return (1, 0);
            }
        } else {
            return (1, 0);
        }
        
    })?;
    Ok(())
}

fn register_body_finish(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "outgoing-body.finish", |mut caller: Caller<'_, CallerTable>, handle: i32| -> (i32, i32) {
        let handle = handle as u32;
        
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        let data = caller.data_mut();
        data.memory_usage.update(m_usage);
        
        if let Some((body, stream_handle, trailers_handle)) = data.outgoing_body.remove(&handle) {
            if stream_handle.handle.is_some() {
                if let Some(_) = data.output_streams.remove(&stream_handle.handle.unwrap()) {
                    
                } else {
                    return (1, 0)
                }
            }
            let mut trailers = None;
            if trailers_handle.handle.is_some() {
                if let Some(fields) = data.outgoing_trailers.remove(&trailers_handle.handle.unwrap()) {
                    trailers = Some(fields)
                }
            }
            match OutgoingBody::finish(body, trailers) {
                Ok(_) =>  {
                    
                },
                Err(e) => {
                    println!("Failed to finish body: {}", e);
                    return (1, 0)
                },
            };
            (0, 0)
        } else {
            return (1, 0)
        }
    })?;
    Ok(())
}

fn register_drops(linker: &mut Linker<CallerTable>) -> Result<(), anyhow::Error> {
    linker.func_wrap("wasi:http/types", "drop-outgoing-request", |mut caller: Caller<'_, CallerTable>, handle: i32| -> i32 {
        let handle = handle as u32;
        
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        let data = caller.data_mut();
        data.memory_usage.update(m_usage);
        
        if let Some(_) = data.outgoing_requests.remove(&handle) {
            1
        } else {
            0
        }
    })?;
    linker.func_wrap("wasi:http/types", "drop-outgoing-body", |mut caller: Caller<'_, CallerTable>, handle: i32| -> i32 {
        let handle = handle as u32;
        
        let memory = caller.get_export("memory").unwrap().into_memory().unwrap();
        let m_usage = memory.data_size(&caller) as u64;
        let data = caller.data_mut();
        data.memory_usage.update(m_usage);
        
        if let Some((_, stream_handle, _)) = data.outgoing_body.remove(&handle) {
            if stream_handle.handle.is_some() {
                if let Some(_) = data.output_streams.remove(&stream_handle.handle.unwrap()) {
                    
                } else {
                    return 0
                }
            }
            1
        } else {
            0
        }
    })?;
    Ok(())
}
