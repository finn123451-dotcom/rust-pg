use crate::constants::*;
use crate::error::{HeapError, Result};
use crate::page::Page;
use crate::storage::{Storage, StorageRef};
use crate::types::*;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub struct Relation {
    pub rel_node: u32,
    pub db_node: u32,
    pub spc_node: u32,
    pub natts: u16,
    pub storage: StorageRef,
}

impl Relation {
    pub fn create(path: PathBuf, natts: u16) -> Result<(Self, u32)> {
        let rel_node = uuid::Uuid::new_v4().as_u128() as u32;
        let db_node = 0u32;
        let spc_node = 0u32;

        let storage = Arc::new(Storage::new(path)?);

        let page = Page::new(BLCKSZ);
        storage.write_page(0, &page)?;

        let rel = Self {
            rel_node,
            db_node,
            spc_node,
            natts,
            storage,
        };

        Ok((rel, rel_node))
    }

    pub fn open(path: PathBuf) -> Result<Self> {
        let storage = Arc::new(Storage::open(path)?);

        if storage.page_count() == 0 {
            let page = Page::new(BLCKSZ);
            storage.write_page(0, &page)?;
        }

        Ok(Self {
            rel_node: 0,
            db_node: 0,
            spc_node: 0,
            natts: 0,
            storage,
        })
    }

    pub fn read_page(&self, block_num: u32) -> Result<Page> {
        self.storage.read_page(block_num)
    }

    pub fn write_page(&self, block_num: u32, page: &Page) -> Result<()> {
        self.storage.write_page(block_num, page)
    }

    pub fn allocate_page(&self) -> Result<u32> {
        self.storage.allocate_page()
    }

    pub fn page_count(&self) -> u32 {
        self.storage.page_count()
    }

    pub fn close(&self) -> Result<()> {
        self.storage.close()
    }

    pub fn drop(&self) -> Result<()> {
        self.storage.drop_all()
    }
}
