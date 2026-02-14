use crate::constants::*;
use crate::error::Result;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct VisibilityMap {
    pages: RwLock<HashMap<u32, Vec<u8>>>,
    page_size: usize,
}

impl VisibilityMap {
    pub fn new() -> Self {
        Self {
            pages: RwLock::new(HashMap::new()),
            page_size: BLCKSZ,
        }
    }

    pub fn set_all_visible(&self, block_num: u32, all_visible: bool) -> Result<()> {
        let mut pages = self.pages.write().unwrap();

        let vm_page_num = block_num / 8192;
        let bit_pos = block_num % 8192;

        let entry = pages
            .entry(vm_page_num)
            .or_insert_with(|| vec![0u8; self.page_size]);

        if all_visible {
            entry[(bit_pos / 8) as usize] |= 1 << (bit_pos % 8);
        } else {
            entry[(bit_pos / 8) as usize] &= !(1 << (bit_pos % 8));
        }

        Ok(())
    }

    pub fn is_all_visible(&self, block_num: u32) -> bool {
        let pages = self.pages.read().unwrap();

        let vm_page_num = block_num / 8192;
        let bit_pos = block_num % 8192;

        if let Some(entry) = pages.get(&vm_page_num) {
            (entry[(bit_pos / 8) as usize] & (1 << (bit_pos % 8))) != 0
        } else {
            false
        }
    }

    pub fn set_page_dirty(&self, block_num: u32) -> Result<()> {
        self.set_all_visible(block_num, false)
    }

    pub fn get_visible_blocks(&self, page_count: u32) -> Vec<u32> {
        let pages = self.pages.read().unwrap();
        let mut visible = Vec::new();

        for block_num in 0..page_count {
            let vm_page_num = block_num / 8192;
            let bit_pos = block_num % 8192;

            if let Some(entry) = pages.get(&vm_page_num) {
                if (entry[(bit_pos / 8) as usize] & (1 << (bit_pos % 8))) != 0 {
                    visible.push(block_num);
                }
            }
        }

        visible
    }
}

impl Default for VisibilityMap {
    fn default() -> Self {
        Self::new()
    }
}

pub type VisibilityMapRef = Arc<VisibilityMap>;
