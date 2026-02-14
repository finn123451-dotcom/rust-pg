use crate::constants::*;
use crate::error::{HeapError, Result};
use std::fs::{File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct XLogRecord {
    pub lsn: u64,
    pub txid: u32,
    pub record_type: XLogRecordType,
    pub block_id: u32,
    pub data: Vec<u8>,
    pub prev_lsn: u64,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum XLogRecordType {
    HeapInsert,
    HeapUpdate,
    HeapDelete,
    HeapVacuum,
    TransactionCommit,
    TransactionAbort,
    Checkpoint,
}

impl XLogRecord {
    pub fn new(txid: u32, record_type: XLogRecordType, block_id: u32, data: Vec<u8>) -> Self {
        Self {
            lsn: 0,
            txid,
            record_type,
            block_id,
            data,
            prev_lsn: 0,
        }
    }

    pub fn size(&self) -> usize {
        8 + 8 + 1 + 4 + 4 + self.data.len()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; self.size()];
        let mut offset = 0;

        buf[offset..offset + 8].copy_from_slice(&self.prev_lsn.to_le_bytes());
        offset += 8;

        buf[offset..offset + 8].copy_from_slice(&self.lsn.to_le_bytes());
        offset += 8;

        buf[offset] = match self.record_type {
            XLogRecordType::HeapInsert => 1,
            XLogRecordType::HeapUpdate => 2,
            XLogRecordType::HeapDelete => 3,
            XLogRecordType::HeapVacuum => 4,
            XLogRecordType::TransactionCommit => 5,
            XLogRecordType::TransactionAbort => 6,
            XLogRecordType::Checkpoint => 7,
        };
        offset += 1;

        buf[offset..offset + 4].copy_from_slice(&self.txid.to_le_bytes());
        offset += 4;

        buf[offset..offset + 4].copy_from_slice(&self.block_id.to_le_bytes());
        offset += 4;

        buf[offset..].copy_from_slice(&self.data);

        buf
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        if buf.len() < 17 {
            return Err(HeapError::CorruptedData("WAL record too small".to_string()));
        }

        let mut offset = 0;
        let prev_lsn = u64::from_le_bytes([
            buf[0], buf[1], buf[2], buf[3], buf[4], buf[5], buf[6], buf[7],
        ]);
        offset += 8;

        let lsn = u64::from_le_bytes([
            buf[8], buf[9], buf[10], buf[11], buf[12], buf[13], buf[14], buf[15],
        ]);
        offset += 8;

        let record_type = match buf[offset] {
            1 => XLogRecordType::HeapInsert,
            2 => XLogRecordType::HeapUpdate,
            3 => XLogRecordType::HeapDelete,
            4 => XLogRecordType::HeapVacuum,
            5 => XLogRecordType::TransactionCommit,
            6 => XLogRecordType::TransactionAbort,
            7 => XLogRecordType::Checkpoint,
            _ => {
                return Err(HeapError::CorruptedData(
                    "Invalid WAL record type".to_string(),
                ))
            }
        };
        offset += 1;

        let txid = u32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        offset += 4;

        let block_id = u32::from_le_bytes([
            buf[offset],
            buf[offset + 1],
            buf[offset + 2],
            buf[offset + 3],
        ]);
        offset += 4;

        let data = buf[offset..].to_vec();

        Ok(Self {
            lsn,
            txid,
            record_type,
            block_id,
            data,
            prev_lsn,
        })
    }
}

pub struct WAL {
    dir: PathBuf,
    current_lsn: RwLock<u64>,
    current_txn_lsn: RwLock<u64>,
}

impl WAL {
    pub fn new(dir: PathBuf) -> Result<Self> {
        if !dir.exists() {
            std::fs::create_dir_all(&dir)?;
        }

        let wal_dir = dir.join("wal");
        if !wal_dir.exists() {
            std::fs::create_dir_all(&wal_dir)?;
        }

        Ok(Self {
            dir: wal_dir,
            current_lsn: RwLock::new(0),
            current_txn_lsn: RwLock::new(0),
        })
    }

    pub fn append(&self, record: &XLogRecord) -> Result<u64> {
        let mut lsn = self.current_lsn.write().unwrap();
        let new_lsn = *lsn + record.size() as u64;
        *lsn = new_lsn;

        let segment_size: u64 = 16 * 1024 * 1024;
        let segment_num = new_lsn / segment_size;
        let segment_offset = new_lsn % segment_size;

        let segment_file = self.dir.join(format!("{:08X}.wal", segment_num));

        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .append(true)
            .open(&segment_file)?;

        file.seek(SeekFrom::Start(segment_offset))?;

        let mut serialized = record.serialize();
        serialized.resize(record.size(), 0);

        file.write_all(&serialized)?;
        file.sync_all()?;

        Ok(new_lsn)
    }

    pub fn flush(&self, txid: u32) -> Result<u64> {
        let mut txn_lsn = self.current_txn_lsn.write().unwrap();
        let lsn = *txn_lsn;
        *txn_lsn = 0;
        Ok(lsn)
    }

    pub fn get_lsn(&self) -> u64 {
        *self.current_lsn.read().unwrap()
    }

    pub fn recover(&self) -> Result<Vec<XLogRecord>> {
        let mut records = Vec::new();

        let entries = std::fs::read_dir(&self.dir)?;
        let mut segment_files: Vec<_> = entries
            .filter_map(|e| e.ok())
            .filter(|e| e.path().extension().map_or(false, |ext| ext == "wal"))
            .collect();

        segment_files.sort_by_key(|e| e.path());

        for entry in segment_files {
            let path = entry.path();
            let mut file = File::open(&path)?;
            let mut buf = vec![0u8; 16 * 1024 * 1024];

            loop {
                match file.read(&mut buf) {
                    Ok(0) => break,
                    Ok(_) => {
                        let mut offset = 0;
                        while offset < buf.len() && buf[offset..].len() >= 17 {
                            if let Ok(record) = XLogRecord::deserialize(&buf[offset..]) {
                                let size = record.size();
                                if size > 0 {
                                    records.push(record);
                                    offset += size;
                                } else {
                                    break;
                                }
                            } else {
                                break;
                            }
                        }
                    }
                    Err(_) => break,
                }
            }
        }

        Ok(records)
    }
}

pub type WALRef = Arc<WAL>;
