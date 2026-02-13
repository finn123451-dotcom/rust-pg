use crate::constants::*;
use crate::error::{HeapError, Result};
use crate::page::Page;
use std::collections::HashMap;
use std::fs::{self, File, OpenOptions};
use std::io::{Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

pub struct Storage {
    dir: PathBuf,
    pages: RwLock<HashMap<u32, Vec<u8>>>,
    max_block: RwLock<u32>,
}

impl Storage {
    pub fn new(dir: PathBuf) -> Result<Self> {
        if !dir.exists() {
            fs::create_dir_all(&dir)?;
        }

        Ok(Self {
            dir,
            pages: RwLock::new(HashMap::new()),
            max_block: RwLock::new(0),
        })
    }

    pub fn open(dir: PathBuf) -> Result<Self> {
        if !dir.exists() {
            fs::create_dir_all(&dir)?;
        }

        let mut max_block = 0u32;
        let mut pages = HashMap::new();

        let entries = fs::read_dir(&dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "dat") {
                let file_name = path.file_stem().and_then(|s| s.to_str()).unwrap_or("");
                if let Ok(block_num) = u32::from_str_radix(file_name, 10) {
                    let mut file = File::open(&path)?;
                    let mut data = vec![0u8; BLCKSZ];
                    file.read_exact(&mut data)?;
                    pages.insert(block_num, data);
                    if block_num > max_block {
                        max_block = block_num;
                    }
                }
            }
        }

        Ok(Self {
            dir,
            pages: RwLock::new(pages),
            max_block: RwLock::new(max_block),
        })
    }

    pub fn read_page(&self, block_num: u32) -> Result<Page> {
        let pages = self.pages.read().unwrap();

        if let Some(data) = pages.get(&block_num) {
            return Page::from_raw(data.clone());
        }

        let file_path = self.dir.join(format!("{}.dat", block_num));
        if !file_path.exists() {
            return Err(HeapError::PageNotFound(block_num));
        }

        let mut file = File::open(&file_path)?;
        let mut data = vec![0u8; BLCKSZ];
        file.read_exact(&mut data)?;

        let mut pages = self.pages.write().unwrap();
        pages.insert(block_num, data.clone());

        Page::from_raw(data)
    }

    pub fn write_page(&self, block_num: u32, page: &Page) -> Result<()> {
        let data = page.serialize();

        let file_path = self.dir.join(format!("{}.dat", block_num));
        let mut file = OpenOptions::new()
            .create(true)
            .write(true)
            .truncate(true)
            .open(&file_path)?;

        file.write_all(&data)?;
        file.sync_all()?;

        let mut pages = self.pages.write().unwrap();
        pages.insert(block_num, data);

        let mut max_block = self.max_block.write().unwrap();
        if block_num > *max_block {
            *max_block = block_num;
        }

        Ok(())
    }

    pub fn allocate_page(&self) -> Result<u32> {
        let max_block = *self.max_block.read().unwrap();
        let new_block = max_block + 1;

        let page = Page::new(BLCKSZ);
        self.write_page(new_block, &page)?;

        let mut max = self.max_block.write().unwrap();
        if new_block > *max {
            *max = new_block;
        }

        Ok(new_block)
    }

    pub fn page_count(&self) -> u32 {
        let max = *self.max_block.read().unwrap();
        if max == 0 && !self.dir.join("0.dat").exists() {
            0
        } else {
            max + 1
        }
    }

    pub fn flush(&self) -> Result<()> {
        let pages = self.pages.read().unwrap();

        for (block_num, data) in pages.iter() {
            let file_path = self.dir.join(format!("{}.dat", block_num));
            let mut file = OpenOptions::new()
                .create(true)
                .write(true)
                .truncate(true)
                .open(&file_path)?;
            file.write_all(data)?;
        }

        Ok(())
    }

    pub fn close(&self) -> Result<()> {
        self.flush()?;
        Ok(())
    }

    pub fn drop_all(&self) -> Result<()> {
        let entries = fs::read_dir(&self.dir)?;
        for entry in entries {
            let entry = entry?;
            let path = entry.path();
            if path.extension().map_or(false, |ext| ext == "dat") {
                fs::remove_file(path)?;
            }
        }

        let mut pages = self.pages.write().unwrap();
        pages.clear();

        let mut max_block = self.max_block.write().unwrap();
        *max_block = 0;

        Ok(())
    }
}

pub type StorageRef = Arc<Storage>;
