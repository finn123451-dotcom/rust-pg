use crate::constants::*;
use crate::error::Result;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct FreeSpaceMap {
    page_free_space: RwLock<HashMap<u32, u16>>,
    page_size: usize,
}

impl FreeSpaceMap {
    pub fn new(page_size: usize) -> Self {
        Self {
            page_free_space: RwLock::new(HashMap::new()),
            page_size,
        }
    }

    pub fn update(&self, block_num: u32, free_space: u16) -> Result<()> {
        let mut fsm = self.page_free_space.write().unwrap();

        let vm_page_num = block_num / 256;

        if free_space < 32 {
            fsm.insert(block_num, 0);
        } else {
            fsm.insert(block_num, free_space);
        }

        Ok(())
    }

    pub fn get_free_space(&self, block_num: u32) -> u16 {
        let fsm = self.page_free_space.read().unwrap();
        *fsm.get(&block_num).unwrap_or(&0)
    }

    pub fn find_page_with_space(&self, required_space: u16) -> Option<u32> {
        let fsm = self.page_free_space.read().unwrap();

        let mut candidates: Vec<(u32, u16)> = fsm
            .iter()
            .filter(|(_, &space)| space >= required_space)
            .map(|(&block, &space)| (block, space))
            .collect();

        candidates.sort_by(|a, b| b.1.cmp(&a.1));

        candidates.first().map(|(block, _)| *block)
    }

    pub fn get_all_free_space(&self) -> HashMap<u32, u16> {
        let fsm = self.page_free_space.read().unwrap();
        fsm.clone()
    }
}

pub type FreeSpaceMapRef = Arc<FreeSpaceMap>;
