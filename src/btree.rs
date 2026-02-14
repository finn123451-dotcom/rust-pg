use crate::constants::*;
use crate::error::{HeapError, Result};
use crate::page::Page;
use crate::relation::Relation;
use crate::types::*;
use std::cmp::Ordering;
use std::collections::HashMap;
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

#[derive(Debug, Clone)]
pub struct BTreeKey {
    pub key: Vec<u8>,
    pub ctid: ItemPointerData,
}

impl BTreeKey {
    pub fn new(key: Vec<u8>, ctid: ItemPointerData) -> Self {
        Self { key, ctid }
    }
}

impl PartialEq for BTreeKey {
    fn eq(&self, other: &Self) -> bool {
        self.key == other.key
    }
}

impl Eq for BTreeKey {}

impl PartialOrd for BTreeKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for BTreeKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key.cmp(&other.key)
    }
}

#[derive(Debug, Clone)]
pub struct BTreePage {
    pub is_leaf: bool,
    pub is_root: bool,
    pub left_sibling: u32,
    pub right_sibling: u32,
    pub keys: Vec<BTreeKey>,
    pub children: Vec<u32>,
}

impl BTreePage {
    pub fn new(is_leaf: bool) -> Self {
        Self {
            is_leaf,
            is_root: false,
            left_sibling: 0,
            right_sibling: 0,
            keys: Vec::new(),
            children: Vec::new(),
        }
    }

    pub fn size(&self) -> usize {
        let key_size = self.keys.iter().map(|k| k.key.len() + 8).sum::<usize>();
        let child_size = self.children.len() * 4;
        1 + 1 + 4 + 4 + key_size + child_size
    }

    pub fn serialize(&self) -> Vec<u8> {
        let mut buf = Vec::new();

        buf.push(if self.is_leaf { 1 } else { 0 });
        buf.push(if self.is_root { 1 } else { 0 });
        buf.extend_from_slice(&self.left_sibling.to_le_bytes());
        buf.extend_from_slice(&self.right_sibling.to_le_bytes());
        buf.extend_from_slice(&(self.keys.len() as u16).to_le_bytes());

        for key in &self.keys {
            buf.extend_from_slice(&(key.key.len() as u16).to_le_bytes());
            buf.extend_from_slice(&key.key);
            buf.extend_from_slice(&key.ctid.block_number.to_le_bytes());
            buf.extend_from_slice(&key.ctid.offset_number.to_le_bytes());
        }

        if !self.is_leaf {
            buf.extend_from_slice(&(self.children.len() as u16).to_le_bytes());
            for &child in &self.children {
                buf.extend_from_slice(&child.to_le_bytes());
            }
        }

        buf
    }

    pub fn deserialize(buf: &[u8]) -> Result<Self> {
        if buf.len() < 10 {
            return Err(HeapError::CorruptedData("BTree page too small".to_string()));
        }

        let is_leaf = buf[0] != 0;
        let is_root = buf[1] != 0;
        let left_sibling = u32::from_le_bytes([buf[2], buf[3], buf[4], buf[5]]);
        let right_sibling = u32::from_le_bytes([buf[6], buf[7], buf[8], buf[9]]);

        let mut offset = 10;
        let key_count = if buf.len() >= offset + 2 {
            u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize
        } else {
            0
        };
        offset += 2;

        let mut keys = Vec::new();
        for _ in 0..key_count {
            if buf.len() < offset + 2 {
                break;
            }
            let key_len = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
            offset += 2;

            if buf.len() < offset + key_len + 8 {
                break;
            }
            let key = buf[offset..offset + key_len].to_vec();
            offset += key_len;

            let block_num = u32::from_le_bytes([
                buf[offset],
                buf[offset + 1],
                buf[offset + 2],
                buf[offset + 3],
            ]);
            let offset_num = u16::from_le_bytes([buf[offset + 4], buf[offset + 5]]);
            offset += 6;

            keys.push(BTreeKey::new(
                key,
                ItemPointerData {
                    block_number: block_num,
                    offset_number: offset_num,
                },
            ));
        }

        let mut children = Vec::new();
        if !is_leaf && buf.len() >= offset + 2 {
            let child_count = u16::from_le_bytes([buf[offset], buf[offset + 1]]) as usize;
            offset += 2;

            for _ in 0..child_count {
                if buf.len() < offset + 4 {
                    break;
                }
                let child = u32::from_le_bytes([
                    buf[offset],
                    buf[offset + 1],
                    buf[offset + 2],
                    buf[offset + 3],
                ]);
                children.push(child);
                offset += 4;
            }
        }

        Ok(Self {
            is_leaf,
            is_root,
            left_sibling,
            right_sibling,
            keys,
            children,
        })
    }
}

pub struct BTreeIndex {
    pub relation: Relation,
    pub root_block: RwLock<Option<u32>>,
    pub key_count: RwLock<usize>,
}

impl BTreeIndex {
    pub fn create(path: PathBuf) -> Result<(Self, u32)> {
        let (relation, rel_node) = Relation::create(path, 0)?;

        let index = Self {
            relation,
            root_block: RwLock::new(None),
            key_count: RwLock::new(0),
        };

        let mut root_page = BTreePage::new(true);
        root_page.is_root = true;
        index.relation.write_page(0, &Page::new(BLCKSZ))?;

        *index.root_block.write().unwrap() = Some(0);

        Ok((index, rel_node))
    }

    pub fn open(path: PathBuf) -> Result<Self> {
        let relation = Relation::open(path)?;

        Ok(Self {
            relation,
            root_block: RwLock::new(Some(0)),
            key_count: RwLock::new(0),
        })
    }

    pub fn insert(&self, key: Vec<u8>, ctid: ItemPointerData) -> Result<()> {
        let root = *self.root_block.read().unwrap();

        if root.is_none() {
            return Err(HeapError::InvalidOperation(
                "Index not initialized".to_string(),
            ));
        }

        let mut page = self.relation.read_page(root.unwrap())?;
        let btree_page = BTreePage::new(true);

        let mut keys = btree_page.keys.clone();
        keys.push(BTreeKey::new(key, ctid));
        keys.sort();

        let mut count = self.key_count.write().unwrap();
        *count += 1;

        Ok(())
    }

    pub fn search(&self, key: &[u8]) -> Result<Vec<ItemPointerData>> {
        let root = *self.root_block.read().unwrap();

        if root.is_none() {
            return Ok(Vec::new());
        }

        self.search_page(root.unwrap(), key)
    }

    fn search_page(&self, block_num: u32, key: &[u8]) -> Result<Vec<ItemPointerData>> {
        let page_data = self.relation.read_page(block_num)?;
        let items = page_data.get_item(1);

        if items.is_none() {
            return Ok(Vec::new());
        }

        let btree_page = BTreePage::deserialize(items.unwrap())?;
        let mut results = Vec::new();

        for tree_key in &btree_page.keys {
            if tree_key.key == key {
                results.push(tree_key.ctid);
            }
        }

        if !btree_page.is_leaf {
            for &child in &btree_page.children {
                let mut child_results = self.search_page(child, key)?;
                results.extend(child_results);
            }
        }

        Ok(results)
    }

    pub fn delete(&self, key: &[u8]) -> Result<bool> {
        let root = *self.root_block.read().unwrap();

        if root.is_none() {
            return Ok(false);
        }

        let mut count = self.key_count.write().unwrap();
        if *count > 0 {
            *count -= 1;
            return Ok(true);
        }

        Ok(false)
    }

    pub fn scan(&self) -> Result<Vec<(Vec<u8>, ItemPointerData)>> {
        let root = *self.root_block.read().unwrap();

        if root.is_none() {
            return Ok(Vec::new());
        }

        self.scan_page(root.unwrap())
    }

    fn scan_page(&self, block_num: u32) -> Result<Vec<(Vec<u8>, ItemPointerData)>> {
        let page_data = self.relation.read_page(block_num)?;
        let items = page_data.get_item(1);

        if items.is_none() {
            return Ok(Vec::new());
        }

        let btree_page = BTreePage::deserialize(items.unwrap())?;
        let mut results = Vec::new();

        for tree_key in &btree_page.keys {
            results.push((tree_key.key.clone(), tree_key.ctid));
        }

        if !btree_page.is_leaf {
            for &child in &btree_page.children {
                let mut child_results = self.scan_page(child)?;
                results.extend(child_results);
            }
        }

        Ok(results)
    }
}
