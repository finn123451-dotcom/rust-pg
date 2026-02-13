use crate::constants::*;
use crate::error::{HeapError, Result};
use crate::types::*;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;

#[derive(Debug, Clone)]
pub struct HeapTupleHeaderData {
    pub t_xmin: u32,
    pub t_xmax: u32,
    pub t_cid: u32,
    pub t_ctid: ItemPointerData,
    pub t_infomask2: u16,
    pub t_infomask: u16,
    pub t_hoff: u8,
}

impl HeapTupleHeaderData {
    pub fn new(natts: u16) -> Self {
        let t_hoff = Self::compute_hoff(natts, false, false);
        Self {
            t_xmin: INVALID_TRANSACTION_ID,
            t_xmax: INVALID_TRANSACTION_ID,
            t_cid: 0,
            t_ctid: ItemPointerData::invalid(),
            t_infomask2: natts,
            t_infomask: 0,
            t_hoff,
        }
    }

    pub fn compute_hoff(natts: u16, has_null: bool, has_varlena: bool) -> u8 {
        let mut off: usize = HEAP_FIXED_HEADER_SIZE;
        if has_null {
            off += (natts as usize + 7) / 8;
        }
        off = (off + 7) & !7;
        off as u8
    }

    pub fn natts(&self) -> u16 {
        self.t_infomask2 & HEAP_NATTS_MASK
    }

    pub fn has_null(&self) -> bool {
        (self.t_infomask & HEAP_HASNULL) != 0
    }

    pub fn has_varlena(&self) -> bool {
        (self.t_infomask & HEAP_HASVARLENA) != 0
    }

    pub fn xmin_committed(&self) -> bool {
        (self.t_infomask & HEAP_XMIN_COMMITTED) != 0
    }

    pub fn set_xmin_committed(&mut self, committed: bool) {
        if committed {
            self.t_infomask |= HEAP_XMIN_COMMITTED;
        } else {
            self.t_infomask &= !HEAP_XMIN_COMMITTED;
        }
    }

    pub fn xmin_invalid(&self) -> bool {
        (self.t_infomask & HEAP_XMIN_INVALID) != 0
    }

    pub fn set_xmin_invalid(&mut self, invalid: bool) {
        if invalid {
            self.t_infomask |= HEAP_XMIN_INVALID;
        } else {
            self.t_infomask &= !HEAP_XMIN_INVALID;
        }
    }

    pub fn xmax_committed(&self) -> bool {
        (self.t_infomask & HEAP_XMAX_COMMITTED) != 0
    }

    pub fn set_xmax_committed(&mut self, committed: bool) {
        if committed {
            self.t_infomask |= HEAP_XMAX_COMMITTED;
        } else {
            self.t_infomask &= !HEAP_XMAX_COMMITTED;
        }
    }

    pub fn xmax_invalid(&self) -> bool {
        (self.t_infomask & HEAP_XMAX_INVALID) != 0
    }

    pub fn set_xmax_invalid(&mut self, invalid: bool) {
        if invalid {
            self.t_infomask |= HEAP_XMAX_INVALID;
        } else {
            self.t_infomask &= !HEAP_XMAX_INVALID;
        }
    }

    pub fn xmax_is_locked_only(&self) -> bool {
        (self.t_infomask & HEAP_XMAX_IS_LOCKED_ONLY) != 0
    }

    pub fn set_xmax_is_locked_only(&mut self, locked: bool) {
        if locked {
            self.t_infomask |= HEAP_XMAX_IS_LOCKED_ONLY;
        } else {
            self.t_infomask &= !HEAP_XMAX_IS_LOCKED_ONLY;
        }
    }

    pub fn keys_updated(&self) -> bool {
        (self.t_infomask2 & HEAP_KEYS_UPDATED) != 0
    }

    pub fn set_keys_updated(&mut self, updated: bool) {
        if updated {
            self.t_infomask2 |= HEAP_KEYS_UPDATED;
        } else {
            self.t_infomask2 &= !HEAP_KEYS_UPDATED;
        }
    }

    pub fn size() -> usize {
        HEAP_FIXED_HEADER_SIZE
    }

    pub fn serialize(&self, buf: &mut [u8]) -> Result<()> {
        let mut cursor = std::io::Cursor::new(buf);
        cursor.write_u32::<LittleEndian>(self.t_xmin)?;
        cursor.write_u32::<LittleEndian>(self.t_xmax)?;
        cursor.write_u32::<LittleEndian>(self.t_cid)?;
        cursor.write_u32::<LittleEndian>(self.t_ctid.block_number)?;
        cursor.write_u16::<LittleEndian>(self.t_ctid.offset_number)?;
        cursor.write_u16::<LittleEndian>(self.t_infomask2)?;
        cursor.write_u16::<LittleEndian>(self.t_infomask)?;
        cursor.write_u8(self.t_hoff)?;
        Ok(())
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        if buf.len() < HEAP_FIXED_HEADER_SIZE {
            return Err(HeapError::InvalidTuple("buffer too small".to_string()));
        }
        let mut cursor = std::io::Cursor::new(buf);
        let t_xmin = cursor.read_u32::<LittleEndian>()?;
        let t_xmax = cursor.read_u32::<LittleEndian>()?;
        let t_cid = cursor.read_u32::<LittleEndian>()?;
        let block_number = cursor.read_u32::<LittleEndian>()?;
        let offset_number = cursor.read_u16::<LittleEndian>()?;
        let t_infomask2 = cursor.read_u16::<LittleEndian>()?;
        let t_infomask = cursor.read_u16::<LittleEndian>()?;
        let t_hoff = cursor.read_u8()?;
        Ok(Self {
            t_xmin,
            t_xmax,
            t_cid,
            t_ctid: ItemPointerData {
                block_number,
                offset_number,
            },
            t_infomask2,
            t_infomask,
            t_hoff,
        })
    }
}

impl Default for HeapTupleHeaderData {
    fn default() -> Self {
        Self::new(0)
    }
}

#[derive(Debug, Clone)]
pub struct HeapTuple {
    pub header: HeapTupleHeaderData,
    pub null_bitmap: Option<Vec<u8>>,
    pub data: Vec<u8>,
}

impl HeapTuple {
    pub fn new(natts: u16) -> Self {
        let mut header = HeapTupleHeaderData::new(natts);
        let has_null = false;

        let null_bitmap = if has_null && natts > 0 {
            Some(vec![0u8; (natts as usize + 7) / 8])
        } else {
            None
        };

        let data = Vec::new();

        Self {
            header,
            null_bitmap,
            data,
        }
    }

    pub fn with_data(natts: u16, data: Vec<u8>, has_null: bool) -> Self {
        let mut header = HeapTupleHeaderData::new(natts);

        if has_null {
            header.t_infomask |= HEAP_HASNULL;
        }

        let null_bitmap = if has_null && natts > 0 {
            Some(vec![0u8; (natts as usize + 7) / 8])
        } else {
            None
        };

        Self {
            header,
            null_bitmap,
            data,
        }
    }

    pub fn is_null(&self, attnum: u16) -> bool {
        if let Some(ref bitmap) = self.null_bitmap {
            let byte_idx = (attnum - 1) as usize / 8;
            let bit_idx = (attnum - 1) as usize % 8;
            byte_idx < bitmap.len() && (bitmap[byte_idx] & (1 << bit_idx)) != 0
        } else {
            false
        }
    }

    pub fn set_null(&mut self, attnum: u16) {
        if let Some(ref mut bitmap) = self.null_bitmap {
            let byte_idx = (attnum - 1) as usize / 8;
            let bit_idx = (attnum - 1) as usize % 8;
            if byte_idx < bitmap.len() {
                bitmap[byte_idx] |= 1 << bit_idx;
            }
        }
    }

    pub fn size(&self) -> usize {
        let mut size = HEAP_FIXED_HEADER_SIZE;

        if self.header.has_null() {
            if let Some(ref bitmap) = self.null_bitmap {
                size += bitmap.len();
            }
        }

        size = (size + 7) & !7;
        size += self.data.len();

        size
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; self.size()];

        self.header.serialize(&mut buf).unwrap();

        let mut offset = HEAP_FIXED_HEADER_SIZE;

        if let Some(ref bitmap) = self.null_bitmap {
            buf[offset..offset + bitmap.len()].copy_from_slice(bitmap);
            offset += bitmap.len();
        }

        offset = (offset + 7) & !7;

        buf[offset..offset + self.data.len()].copy_from_slice(&self.data);

        buf
    }

    pub fn deserialize(buf: &[u8], natts: u16) -> Result<Self> {
        if buf.len() < HEAP_FIXED_HEADER_SIZE {
            return Err(HeapError::InvalidTuple("buffer too small".to_string()));
        }

        let header = HeapTupleHeaderData::deserialize(buf)?;

        let has_null = header.has_null();
        let null_bitmap_size = if has_null {
            (natts as usize + 7) / 8
        } else {
            0
        };

        let mut offset = HEAP_FIXED_HEADER_SIZE;

        let null_bitmap = if has_null && buf.len() >= offset + null_bitmap_size {
            Some(buf[offset..offset + null_bitmap_size].to_vec())
        } else {
            None
        };

        offset += null_bitmap_size;
        offset = (offset + 7) & !7;

        let data = if offset < buf.len() {
            buf[offset..].to_vec()
        } else {
            Vec::new()
        };

        Ok(Self {
            header,
            null_bitmap,
            data,
        })
    }

    pub fn get_value(&self, attnum: u16) -> Option<&[u8]> {
        if self.is_null(attnum) {
            return None;
        }

        None
    }

    pub fn xmin(&self) -> TransactionId {
        TransactionId(self.header.t_xmin)
    }

    pub fn set_xmin(&mut self, xid: TransactionId) {
        self.header.t_xmin = xid.0;
    }

    pub fn xmax(&self) -> TransactionId {
        TransactionId(self.header.t_xmax)
    }

    pub fn set_xmax(&mut self, xid: TransactionId) {
        self.header.t_xmax = xid.0;
    }

    pub fn cid(&self) -> CommandId {
        CommandId(self.header.t_cid)
    }

    pub fn set_cid(&mut self, cid: CommandId) {
        self.header.t_cid = cid.0;
    }

    pub fn ctid(&self) -> ItemPointerData {
        self.header.t_ctid
    }

    pub fn set_ctid(&mut self, ctid: ItemPointerData) {
        self.header.t_ctid = ctid;
    }
}

impl Default for HeapTuple {
    fn default() -> Self {
        Self::new(0)
    }
}

pub fn heap_tuple_get_struct(heap_tuple: &HeapTuple, natts: u16) -> Result<HeapTupleHeaderData> {
    Ok(heap_tuple.header.clone())
}
