use crate::http::utils::{get_body_idx, get_body_len};
use super::models::{ChunkParserResult, ChunkReadingError, ChunkParserError, HttpParseError};

use tokio::net::TcpStream;
use tokio::io::AsyncReadExt;

pub async fn get_wasm_chunked<'a>(buffer: &'a [u8], string_buffer: &'a String, stream: &mut TcpStream) -> Result<Vec<u8>, ChunkReadingError> {
    let mut wasm_vec: Vec<u8> = Vec::new();
    let body_idx = match get_body_idx(&string_buffer) {
        Some(idx) => idx,
        None => {
            eprintln!("failed to find body index");
            return Err(ChunkReadingError::ParseError(HttpParseError::InvalidHeader));
        }
    };
    let mut master_buffer: Vec<u8> = Vec::new();
    master_buffer.extend_from_slice(&buffer[body_idx..]);
    match read_chunks_from_buffer(&mut master_buffer, &mut wasm_vec).await {
        ChunkParserResult::Done => {return Ok(wasm_vec)}
        ChunkParserResult::NeedMoreData => {}
        ChunkParserResult::Error(e) => {
            eprintln!("error ocured while parsing chunk of data: {}", e);
        }
    }
    let mut new_buffer = [0u8; 4096];
    loop {
        let n = match stream.read(&mut new_buffer).await {
            Ok(n) => n,
            Err(e) => {
                return Err(ChunkReadingError::IoError(e))
            },
        };
        if n == 0 {
            return Err(ChunkReadingError::UnexpectedEOF);
        }
        master_buffer.extend_from_slice(&new_buffer[..n]);
        match read_chunks_from_buffer(&mut master_buffer, &mut wasm_vec).await {
            ChunkParserResult::Done => {return Ok(wasm_vec)}
            ChunkParserResult::NeedMoreData => {continue}
            ChunkParserResult::Error(e) => {
                return Err(ChunkReadingError::ParserError(e))
            }
        }
    }
}

pub async fn read_chunks_from_buffer(buffer: &mut Vec<u8>, wasm_vec: &mut Vec<u8>) -> ChunkParserResult {
    loop {
        if let Some(pos) = buffer.windows(2).position(|w| w == b"\r\n") {
            let line = buffer[..pos].to_vec();
            buffer.drain(..pos + 2);
            let size_str = match std::str::from_utf8(&line) {
                Ok(s) => s,
                Err(_) => return ChunkParserResult::Error(ChunkParserError::InvalidUtf8 { line }),
            };
            let hex = size_str.split(';').next().unwrap();
            if let Ok(chunk_size) = usize::from_str_radix(&hex, 16) {
                if chunk_size == 0 {
                    loop {
                        if let Some(pos) = buffer.windows(2).position(|w| w == b"\r\n") {
                            let line = buffer[..pos].to_vec();
                            buffer.drain(..pos + 2);
                            if line.is_empty() {
                                break;
                            }
                            
                        } else {
                            return ChunkParserResult::NeedMoreData;
                        }  
                    }
                    break;
                }
                if buffer.len() >= chunk_size {
                    let chunk = buffer[..chunk_size].to_vec();
                    wasm_vec.extend(chunk);
                    buffer.drain(..chunk_size);

                    if buffer.len() < 2 {
                        return ChunkParserResult::NeedMoreData;
                    }
                    if &buffer[..2] != b"\r\n" {
                        return ChunkParserResult::Error(ChunkParserError::MissingCRLF{
                            after_chunk: buffer[..2].to_vec()
                        });
                    }
                    buffer.drain(..2);
                } else {
                    return ChunkParserResult::NeedMoreData;
                }
            } else {
                return ChunkParserResult::Error(ChunkParserError::InvalidChunkSize{
                    line: hex.to_string()
                });
            }
        } else {
            return ChunkParserResult::NeedMoreData;
        }   
    }
    ChunkParserResult::Done
}

//this should not return string, asshole!!!
pub async fn get_wasm_code<'a>(buffer: &'a [u8], string_buffer: &'a String, stream: &mut TcpStream) -> Result<Vec<u8>, ChunkReadingError> {
    let len = match get_body_len(string_buffer) {
        Some(len) => len,
        None => return Err(ChunkReadingError::ParseError(HttpParseError::InvalidHeader)),
    };
    let body_idx = string_buffer.find("\r\n\r\n");
    let mut wasm: Vec<u8> = Vec::new();
    let mut int_buffer = [0u8; 4096];
    if let Some(idx) = body_idx {
        if buffer[idx + 4..].len() == len {
            Ok(buffer[idx + 4..].to_vec())
        } else {
            wasm.extend_from_slice(&buffer[idx + 4..]);
            while wasm.len() < len {
                let n = match stream.read(&mut int_buffer).await {
                    Ok(n) => n,
                    Err(e) => {
                        return Err(ChunkReadingError::IoError(e))
                    },
                };
                if n == 0 {
                    return Err(ChunkReadingError::UnexpectedEOF);
                }
                wasm.extend_from_slice(&int_buffer[..n]);
            }
            wasm.truncate(len);
            Ok(wasm)
        }
    } else {
        Err(ChunkReadingError::ParseError(HttpParseError::MissingBodySeparator))
    }
}