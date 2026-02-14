use crate::constants::*;
use crate::error::{HeapError, Result};
use crate::page::Page;
use crate::relation::Relation;
use crate::types::*;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct ToastChunk {
    pub chunk_id: u32,
    pub chunk_seq: u32,
    pub data: Vec<u8>,
}

#[derive(Debug, Clone)]
pub struct ToastPointer {
    pub oid: u32,
    pub chunk_id: u32,
    pub size: u32,
    pub compressed: bool,
}

impl ToastPointer {
    pub fn new(oid: u32, chunk_id: u32, size: u32, compressed: bool) -> Self {
        Self {
            oid,
            chunk_id,
            size,
            compressed,
        }
    }

    pub fn size() -> usize {
        4 + 4 + 4 + 1
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; Self::size()];
        buf[0..4].copy_from_slice(&self.oid.to_le_bytes());
        buf[4..8].copy_from_slice(&self.chunk_id.to_le_bytes());
        buf[8..12].copy_from_slice(&self.size.to_le_bytes());
        buf[12] = if self.compressed { 1 } else { 0 };
        buf
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::size() {
            return Err(HeapError::InvalidTuple(
                "TOAST pointer buffer too small".to_string(),
            ));
        }
        let oid = u32::from_le_bytes([buf[0], buf[1], buf[2], buf[3]]);
        let chunk_id = u32::from_le_bytes([buf[4], buf[5], buf[6], buf[7]]);
        let size = u32::from_le_bytes([buf[8], buf[9], buf[10], buf[11]]);
        let compressed = buf[12] != 0;
        Ok(Self {
            oid,
            chunk_id,
            size,
            compressed,
        })
    }
}

pub struct ToastTable {
    pub relation: Relation,
    pub source_oid: u32,
    chunks: RwLock<HashMap<u32, Vec<ToastChunk>>>,
}

impl ToastTable {
    pub fn create(path: PathBuf, source_oid: u32) -> Result<(Self, u32)> {
        let toast_path = path.join("toast");
        let (relation, rel_node) = Relation::create(toast_path, 3)?;

        let toast = Self {
            relation,
            source_oid,
            chunks: RwLock::new(HashMap::new()),
        };

        Ok((toast, rel_node))
    }

    pub fn open(path: PathBuf) -> Result<Self> {
        let toast_path = path.join("toast");
        let relation = Relation::open(toast_path)?;

        Ok(Self {
            relation,
            source_oid: 0,
            chunks: RwLock::new(HashMap::new()),
        })
    }

    pub fn compress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut compressed = Vec::new();
        let mut remaining = data;

        while !remaining.is_empty() {
            let chunk_size = std::cmp::min(remaining.len(), TOAST_MAX_CHUNK_SIZE);
            let chunk = &remaining[..chunk_size];

            if chunk.len() < 32 {
                compressed.extend_from_slice(chunk);
            } else {
                let mut encoded = Vec::new();
                for (i, &byte) in chunk.iter().enumerate() {
                    if i > 0 && i % 2 == 0 {
                        encoded.push(0xFF);
                    }
                    encoded.push(byte);
                }
                compressed.extend_from_slice(&encoded);
            }

            remaining = &remaining[chunk_size..];
        }

        Ok(compressed)
    }

    pub fn decompress(&self, data: &[u8]) -> Result<Vec<u8>> {
        let mut decompressed = Vec::new();
        let mut i = 0;

        while i < data.len() {
            if i + 1 < data.len() && data[i] == 0xFF {
                i += 1;
                continue;
            }
            decompressed.push(data[i]);
            i += 1;
        }

        Ok(decompressed)
    }

    pub fn store(&self, xid: TransactionId, cid: CommandId, data: &[u8]) -> Result<ToastPointer> {
        if data.len() <= TOAST_TUPLE_THRESHOLD {
            return Err(HeapError::InvalidOperation(
                "Data too small for TOAST".to_string(),
            ));
        }

        let compressed = self.compress(data)?;
        let chunk_id = uuid::Uuid::new_v4().as_u128() as u32;

        let mut chunks = self.chunks.write().unwrap();
        let mut all_chunks = Vec::new();

        let mut offset = 0;
        let mut seq = 0u32;

        while offset < compressed.len() {
            let chunk_size = std::cmp::min(compressed.len() - offset, TOAST_MAX_CHUNK_SIZE);
            let toast_chunk = ToastChunk {
                chunk_id,
                chunk_seq: seq,
                data: compressed[offset..offset + chunk_size].to_vec(),
            };

            all_chunks.push(toast_chunk);

            offset += chunk_size;
            seq += 1;
        }

        chunks.insert(chunk_id, all_chunks);

        Ok(ToastPointer::new(
            self.source_oid,
            chunk_id,
            data.len() as u32,
            true,
        ))
    }

    pub fn fetch(&self, pointer: &ToastPointer) -> Result<Vec<u8>> {
        let chunks = self.chunks.read().unwrap();

        if let Some(stored_chunks) = chunks.get(&pointer.chunk_id) {
            let mut data = Vec::new();

            for chunk in stored_chunks {
                data.extend_from_slice(&chunk.data);
            }

            if pointer.compressed {
                return self.decompress(&data);
            }

            return Ok(data);
        }

        Err(HeapError::InvalidTuple("TOAST chunk not found".to_string()))
    }

    pub fn remove(&self, chunk_id: u32) -> Result<()> {
        let mut chunks = self.chunks.write().unwrap();
        chunks.remove(&chunk_id);
        Ok(())
    }
}
