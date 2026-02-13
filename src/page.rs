use crate::constants::*;
use crate::error::{HeapError, Result};
use crate::types::*;
use byteorder::{LittleEndian, ReadBytesExt, WriteBytesExt};
use std::fmt;

#[derive(Debug, Clone)]
pub struct PageHeaderData {
    pub pd_lsn: u64,
    pub pd_checksum: u16,
    pub pd_flags: u16,
    pub pd_lower: u16,
    pub pd_upper: u16,
    pub pd_special: u16,
    pub pd_pagesize_version: u16,
    pub pd_prune_xid: u32,
}

impl PageHeaderData {
    pub fn new(size: usize) -> Self {
        let pagesize_version = ((size as u16) << 4) | HEAP_PAGE_VERSION;
        Self {
            pd_lsn: 0,
            pd_checksum: 0,
            pd_flags: 0,
            pd_lower: 24,
            pd_upper: size as u16,
            pd_special: size as u16,
            pd_pagesize_version: pagesize_version,
            pd_prune_xid: 0,
        }
    }

    pub fn size() -> usize {
        24
    }

    pub fn has_free_lines(&self) -> bool {
        (self.pd_flags & PD_HAS_FREE_LINES) != 0
    }

    pub fn set_has_free_lines(&mut self, has: bool) {
        if has {
            self.pd_flags |= PD_HAS_FREE_LINES;
        } else {
            self.pd_flags &= !PD_HAS_FREE_LINES;
        }
    }

    pub fn is_page_full(&self) -> bool {
        (self.pd_flags & PD_PAGE_FULL) != 0
    }

    pub fn set_page_full(&mut self, full: bool) {
        if full {
            self.pd_flags |= PD_PAGE_FULL;
        } else {
            self.pd_flags &= !PD_PAGE_FULL;
        }
    }

    pub fn all_visible(&self) -> bool {
        (self.pd_flags & PD_ALL_VISIBLE) != 0
    }

    pub fn set_all_visible(&mut self, visible: bool) {
        if visible {
            self.pd_flags |= PD_ALL_VISIBLE;
        } else {
            self.pd_flags &= !PD_ALL_VISIBLE;
        }
    }

    pub fn free_space(&self, page_size: usize) -> usize {
        (self.pd_upper - self.pd_lower) as usize
    }

    pub fn serialize(&self, buf: &mut [u8]) -> Result<()> {
        let mut cursor = std::io::Cursor::new(buf);
        cursor.write_u64::<LittleEndian>(self.pd_lsn)?;
        cursor.write_u16::<LittleEndian>(self.pd_checksum)?;
        cursor.write_u16::<LittleEndian>(self.pd_flags)?;
        cursor.write_u16::<LittleEndian>(self.pd_lower)?;
        cursor.write_u16::<LittleEndian>(self.pd_upper)?;
        cursor.write_u16::<LittleEndian>(self.pd_special)?;
        cursor.write_u16::<LittleEndian>(self.pd_pagesize_version)?;
        cursor.write_u32::<LittleEndian>(self.pd_prune_xid)?;
        Ok(())
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        if buf.len() < Self::size() {
            return Err(HeapError::InvalidPage("buffer too small".to_string()));
        }
        let mut cursor = std::io::Cursor::new(buf);
        let pd_lsn = cursor.read_u64::<LittleEndian>()?;
        let pd_checksum = cursor.read_u16::<LittleEndian>()?;
        let pd_flags = cursor.read_u16::<LittleEndian>()?;
        let pd_lower = cursor.read_u16::<LittleEndian>()?;
        let pd_upper = cursor.read_u16::<LittleEndian>()?;
        let pd_special = cursor.read_u16::<LittleEndian>()?;
        let pd_pagesize_version = cursor.read_u16::<LittleEndian>()?;
        let pd_prune_xid = cursor.read_u32::<LittleEndian>()?;
        Ok(Self {
            pd_lsn,
            pd_checksum,
            pd_flags,
            pd_lower,
            pd_upper,
            pd_special,
            pd_pagesize_version,
            pd_prune_xid,
        })
    }
}

impl Default for PageHeaderData {
    fn default() -> Self {
        Self::new(BLCKSZ)
    }
}

#[derive(Debug, Clone, Copy)]
pub struct ItemIdData {
    pub bits: u32,
}

impl ItemIdData {
    pub fn new() -> Self {
        Self { bits: 0 }
    }

    pub fn set(&mut self, off: u16, len: u16, flags: u8) {
        self.bits = ((off as u32) & 0x7FFF)
            | (((len as u32) & 0x7FFF) << 15)
            | (((flags as u32) & 0x3) << 30);
    }

    pub fn offset(&self) -> u16 {
        (self.bits & 0x7FFF) as u16
    }

    pub fn length(&self) -> u16 {
        ((self.bits >> 15) & 0x7FFF) as u16
    }

    pub fn flags(&self) -> u8 {
        (self.bits >> 30) as u8
    }

    pub fn is_used(&self) -> bool {
        self.flags() == LP_USED
    }

    pub fn is_dead(&self) -> bool {
        self.flags() == LP_DEAD
    }

    pub fn is_normal(&self) -> bool {
        self.flags() == LP_NORMAL
    }
}

impl Default for ItemIdData {
    fn default() -> Self {
        Self::new()
    }
}

#[derive(Clone)]
pub struct Page {
    pub header: PageHeaderData,
    pub item_id_data: Vec<ItemIdData>,
    pub data: Vec<u8>,
    pub page_size: usize,
}

impl Page {
    pub fn new(page_size: usize) -> Self {
        Self {
            header: PageHeaderData::new(page_size),
            item_id_data: Vec::new(),
            data: vec![0u8; page_size],
            page_size,
        }
    }

    pub fn from_raw(data: Vec<u8>) -> Result<Self> {
        let page_size = data.len();
        if page_size < BLCKSZ {
            return Err(HeapError::InvalidPage("page size too small".to_string()));
        }

        let header = PageHeaderData::deserialize(&data[..24])?;
        let lower = header.pd_lower as usize;
        let upper = header.pd_upper as usize;

        if lower < 24 || upper > page_size || lower > upper {
            return Err(HeapError::InvalidPage("invalid page layout".to_string()));
        }

        let item_id_count = (lower - 24) / 4;
        let mut item_id_data = Vec::with_capacity(item_id_count);

        for i in 0..item_id_count {
            let offset = 24 + i * 4;
            let bits = u32::from_le_bytes([
                data[offset],
                data[offset + 1],
                data[offset + 2],
                data[offset + 3],
            ]);
            item_id_data.push(ItemIdData { bits });
        }

        Ok(Self {
            header,
            item_id_data,
            data,
            page_size,
        })
    }

    pub fn get_item(&self, offset: u16) -> Option<&[u8]> {
        let offset_idx = (offset - 1) as usize;
        if offset_idx >= self.item_id_data.len() {
            return None;
        }
        let item_id = &self.item_id_data[offset_idx];
        if !item_id.is_used() {
            return None;
        }
        let off = item_id.offset() as usize;
        let len = item_id.length() as usize;
        if off + len > self.page_size {
            return None;
        }
        Some(&self.data[off..off + len])
    }

    pub fn get_item_mut(&mut self, offset: u16) -> Option<&mut [u8]> {
        let offset_idx = (offset - 1) as usize;
        if offset_idx >= self.item_id_data.len() {
            return None;
        }
        let item_id = &self.item_id_data[offset_idx];
        if !item_id.is_used() {
            return None;
        }
        let off = item_id.offset() as usize;
        let len = item_id.length() as usize;
        if off + len > self.page_size {
            return None;
        }
        Some(&mut self.data[off..off + len])
    }

    pub fn add_item(&mut self, data: &[u8]) -> Result<u16> {
        let item_len = data.len() as u16;
        if self.header.free_space(self.page_size) < item_len as usize + 4 {
            return Err(HeapError::NoFreeSpace);
        }

        let new_offset = self.header.pd_upper - item_len;
        self.header.pd_upper = new_offset;

        let new_item_id_idx = self.item_id_data.len() as u16;
        let new_item_id_offset = self.header.pd_lower as usize;
        self.header.pd_lower = (new_item_id_offset + 4) as u16;

        self.data[new_offset as usize..new_offset as usize + item_len as usize]
            .copy_from_slice(data);

        let mut item_id = ItemIdData::new();
        item_id.set(new_offset, item_len, LP_USED);
        self.item_id_data.push(item_id);

        self.header.set_has_free_lines(false);
        self.header
            .set_page_full(self.header.free_space(self.page_size) < 32);

        Ok(new_item_id_idx + 1)
    }

    pub fn remove_item(&mut self, offset: u16) -> Result<()> {
        let offset_idx = (offset - 1) as usize;
        if offset_idx >= self.item_id_data.len() {
            return Err(HeapError::InvalidTuple("invalid offset".to_string()));
        }

        let item_id = &mut self.item_id_data[offset_idx];
        let off = item_id.offset();
        let len = item_id.length();

        item_id.bits = (LP_DEAD as u32) << 30;

        let new_upper = off + len;
        if new_upper > self.header.pd_upper {
            self.header.pd_upper = new_upper;
        }

        self.header.set_has_free_lines(true);

        Ok(())
    }

    pub fn item_count(&self) -> usize {
        self.item_id_data.len()
    }

    pub fn free_space(&self) -> usize {
        self.header.free_space(self.page_size)
    }

    pub fn has_free_lines(&self) -> bool {
        self.header.has_free_lines()
    }

    pub fn dead_items(&self) -> Vec<u16> {
        self.item_id_data
            .iter()
            .enumerate()
            .filter(|(_, item)| item.is_dead())
            .map(|(i, _)| (i + 1) as u16)
            .collect()
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = vec![0u8; self.page_size];

        self.header.serialize(&mut buf).unwrap();

        for (i, item_id) in self.item_id_data.iter().enumerate() {
            let offset = 24 + i * 4;
            buf[offset..offset + 4].copy_from_slice(&item_id.bits.to_le_bytes());
        }

        let data_start = self.header.pd_upper as usize;
        let data_end = self.page_size;
        if data_start < data_end {
            buf[data_start..data_end].copy_from_slice(&self.data[data_start..data_end]);
        }

        buf
    }

    pub fn is_valid(&self) -> bool {
        self.header.pd_lower >= 24
            && self.header.pd_upper <= self.page_size as u16
            && self.header.pd_lower <= self.header.pd_upper
            && self.header.pd_special == self.page_size as u16
    }
}

impl Default for Page {
    fn default() -> Self {
        Self::new(BLCKSZ)
    }
}

impl fmt::Debug for Page {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Page")
            .field("header", &self.header)
            .field("item_count", &self.item_id_data.len())
            .field("free_space", &self.free_space())
            .finish()
    }
}
