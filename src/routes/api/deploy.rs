use tokio::net::TcpStream;
use tokio::io::AsyncReadExt;
use std::fmt::Display;
use std::fmt::Formatter;
use std::error::Error;

pub async fn deploy(stream: TcpStream, buffer: &[u8], path: &str) {
    let function_name = get_function_name(path);
    let string_buffer = String::from_utf8_lossy(buffer).to_string();
    let wasm = if string_buffer.contains("Transfer-Encoding: chunked") {
        get_wasm_chunked(&buffer, &string_buffer, stream).await
    } else {
        get_wasm_code(&buffer, &string_buffer)
    };
    match wasm {
        Ok(wasm) => {
            //save to db
            // return success response to a client
        }
        Err(e) => {
            //return err response to a client
            eprintln!("Error: {}", e);
            
        }
    }
}

pub fn get_function_name(path: &str) -> String {
    let new = path.split('/').last().unwrap_or("").to_string();
    println!("path: {path}");
    new
}

pub fn split_in_lines(string: String) -> Vec<String> {
    string.lines().map(|l| l.to_string()).collect::<Vec<String>>()
}

pub enum ChunkReadingError {
    UnexpectedEOF,
    ParserError(ChunkParserError),
    IoError(std::io::Error),
    ParseError(HttpParseError),
}

pub enum HttpParseError {
    MissingBodySeparator,
}

impl Display for HttpParseError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            HttpParseError::MissingBodySeparator => {
                write!(f, "Missing body separator")
            }
        }
    }
}

impl Display for ChunkReadingError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkReadingError::UnexpectedEOF => {
                write!(f, "Connection was closer earlier before reader could finish reading chunk, EOF error")
            }
            ChunkReadingError::ParserError(e) => {
                write!(f, "Parser error: {}", e)
            }
            ChunkReadingError::IoError(e) => {
                write!(f, "IO error: {}", e)
            }
            ChunkReadingError::ParseError(e) => {
                write!(f, "Parse error: {}", e)
            }
        }
    }
}

pub async fn get_wasm_chunked<'a>(buffer: &'a [u8], string_buffer: &'a String, mut stream: TcpStream) -> Result<Vec<u8>, ChunkReadingError> {
    let mut wasm_vec: Vec<u8> = Vec::new();
    let body_idx = get_body_idx(&string_buffer).unwrap();
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

pub fn get_body_idx(string_buffer: &str) -> Option<usize> {
    let raw = string_buffer.find("\r\n\r\n")?;
    Some(raw + 4)
}


pub enum ChunkParserResult {
    Done,
    NeedMoreData,
    Error(ChunkParserError)
}

#[derive(Debug)]
pub enum ChunkParserError {
    InvalidUtf8{line: Vec<u8>},
    InvalidChunkSize{line: String},
    MissingCRLF{after_chunk: Vec<u8>},
    TrailingGarbage { bytes: Vec<u8> },
}

impl Display for ChunkParserError {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        match self {
            ChunkParserError::InvalidUtf8 { line } => {
                write!(f, "Invalid Utf8 characters: {}", String::from_utf8_lossy(&line))
            }
            ChunkParserError::InvalidChunkSize{line} => {
                write!(f, "Size: {}", line)
            }
            ChunkParserError::MissingCRLF{after_chunk} => {
                write!(f, "The line: {} is placed instead of proper CRLF", String::from_utf8_lossy(&after_chunk))
            }
            ChunkParserError::TrailingGarbage { bytes } => {
                write!(f, "Trailing garbage: {:?}", bytes)
            }
        }
    }
}

pub async fn read_chunks_from_buffer(buffer: &mut Vec<u8>, wasm_vec: &mut Vec<u8>) -> ChunkParserResult {
    if buffer.is_empty() || !buffer[0].is_ascii_digit() {
        return ChunkParserResult::Error(
            ChunkParserError::TrailingGarbage { bytes: buffer[..buffer.len().min(8)].to_vec() }
        )
    }
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
pub fn get_wasm_code<'a>(buffer: &'a [u8], string_buffer: &'a String) -> Result<Vec<u8>, ChunkReadingError> {
    let body_idx = string_buffer.find("\r\n\r\n");
    if let Some(idx) = body_idx {
        Ok(buffer[idx + 4..].to_vec())
    } else {
        Err(ChunkReadingError::ParseError(HttpParseError::MissingBodySeparator))
    }
}