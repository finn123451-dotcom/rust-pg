use crate::constants::*;
use crate::error::{HeapError, Result};
use crate::heap_tuple::{HeapTuple, HeapTupleHeaderData};
use crate::page::Page;
use crate::relation::Relation;
use crate::transaction::{Transaction, TransactionManager};
use crate::types::*;
use crate::visibility::Visibility;
use std::path::PathBuf;
use std::sync::Arc;

pub struct HeapRelation {
    pub relation: Relation,
    pub natts: u16,
    pub tx_manager: Arc<TransactionManager>,
}

impl HeapRelation {
    pub fn create(path: PathBuf, natts: u16) -> Result<(Self, u32)> {
        let tx_manager = Arc::new(TransactionManager::new());

        let (relation, rel_node) = Relation::create(path, natts)?;

        let heap = Self {
            relation,
            natts,
            tx_manager,
        };

        Ok((heap, rel_node))
    }

    pub fn open(path: PathBuf, natts: u16) -> Result<Self> {
        let tx_manager = Arc::new(TransactionManager::new());

        let relation = Relation::open(path)?;

        Ok(Self {
            relation,
            natts,
            tx_manager,
        })
    }

    pub fn insert(
        &self,
        xid: TransactionId,
        cid: CommandId,
        data: &[u8],
    ) -> Result<ItemPointerData> {
        let tuple_size = HeapTupleHeaderData::size() + data.len();

        let page_count = self.relation.page_count();
        let mut block_num = 0u32;
        let mut found_page = false;

        for bn in 0..page_count {
            let page = self.relation.read_page(bn)?;
            if page.free_space() >= tuple_size + 4 {
                block_num = bn;
                found_page = true;
                break;
            }
        }

        if !found_page {
            if page_count > 0 {
                let last_page = self.relation.read_page(page_count - 1)?;
                if last_page.free_space() >= tuple_size + 4 {
                    block_num = page_count - 1;
                    found_page = true;
                }
            }
        }

        if !found_page {
            block_num = self.relation.allocate_page()?;
        }

        let mut page = self.relation.read_page(block_num)?;

        let mut heap_tuple = HeapTuple::with_data(self.natts, data.to_vec(), false);
        heap_tuple.header.t_xmin = xid.0;
        heap_tuple.header.t_xmax = 0;
        heap_tuple.header.t_cid = cid.0;
        heap_tuple.header.t_ctid = ItemPointerData {
            block_number: block_num,
            offset_number: 0,
        };

        let serialized = heap_tuple.serialize();

        let offset = page.add_item(&serialized)?;

        heap_tuple.header.t_ctid = ItemPointerData {
            block_number: block_num,
            offset_number: offset,
        };

        let serialized = heap_tuple.serialize();
        if let Some(tuple_data) = page.get_item_mut(offset) {
            tuple_data[..serialized.len()].copy_from_slice(&serialized);
        }

        self.relation.write_page(block_num, &page)?;

        Ok(ItemPointerData {
            block_number: block_num,
            offset_number: offset,
        })
    }

    pub fn update(
        &self,
        xid: TransactionId,
        cid: CommandId,
        old_ctid: ItemPointerData,
        new_data: &[u8],
    ) -> Result<Option<ItemPointerData>> {
        let mut old_page = self.relation.read_page(old_ctid.block_number)?;

        let old_tuple_data = old_page
            .get_item(old_ctid.offset_number)
            .ok_or_else(|| HeapError::InvalidTuple("failed to get old item".to_string()))?;

        let mut old_tuple = HeapTuple::deserialize(old_tuple_data, self.natts)?;

        if !old_tuple.xmax().is_invalid() {
            return Ok(None);
        }

        old_tuple.header.t_xmax = xid.0;
        old_tuple.header.t_cid = cid.0;

        let new_ctid = self.insert(xid, cid, new_data)?;

        old_tuple.header.t_ctid = new_ctid;

        let serialized = old_tuple.serialize();
        let tuple_data = old_page
            .get_item_mut(old_ctid.offset_number)
            .ok_or_else(|| {
                HeapError::InvalidTuple("failed to get old item for update".to_string())
            })?;
        tuple_data[..serialized.len()].copy_from_slice(&serialized);

        self.relation.write_page(old_ctid.block_number, &old_page)?;

        Ok(Some(new_ctid))
    }

    pub fn delete(
        &self,
        xid: TransactionId,
        cid: CommandId,
        ctid: ItemPointerData,
    ) -> Result<bool> {
        let mut page = self.relation.read_page(ctid.block_number)?;

        let tuple_data = page
            .get_item(ctid.offset_number)
            .ok_or_else(|| HeapError::InvalidTuple("failed to get item for delete".to_string()))?;

        let mut heap_tuple = HeapTuple::deserialize(tuple_data, self.natts)?;

        if !heap_tuple.xmax().is_invalid() {
            return Ok(false);
        }

        heap_tuple.header.t_xmax = xid.0;
        heap_tuple.header.t_cid = cid.0;

        let serialized = heap_tuple.serialize();
        let tuple_data = page
            .get_item_mut(ctid.offset_number)
            .ok_or_else(|| HeapError::InvalidTuple("failed to get item for delete".to_string()))?;
        tuple_data[..serialized.len()].copy_from_slice(&serialized);

        self.relation.write_page(ctid.block_number, &page)?;

        Ok(true)
    }

    pub fn get(&self, ctid: ItemPointerData) -> Result<Option<HeapTuple>> {
        let page = self.relation.read_page(ctid.block_number)?;

        let tuple_data = page.get_item(ctid.offset_number);

        match tuple_data {
            Some(data) => {
                let heap_tuple = HeapTuple::deserialize(data, self.natts)?;
                Ok(Some(heap_tuple))
            }
            None => Ok(None),
        }
    }

    pub fn scan(
        &self,
        snapshot: &Snapshot,
        cur_xid: TransactionId,
    ) -> Result<Vec<(ItemPointerData, HeapTuple)>> {
        let mut results = Vec::new();

        let page_count = self.relation.page_count();

        for block_num in 0..page_count {
            let page = self.relation.read_page(block_num)?;

            for offset_idx in 0..page.item_count() {
                let offset = (offset_idx + 1) as u16;

                let tuple_data = match page.get_item(offset) {
                    Some(data) => data,
                    None => continue,
                };

                let heap_tuple = match HeapTuple::deserialize(tuple_data, self.natts) {
                    Ok(t) => t,
                    Err(_) => continue,
                };

                let visible = match snapshot.mode {
                    VisibilityMode::Any => Visibility::heap_tuple_satisfies_any(&heap_tuple),
                    VisibilityMode::Self_ => {
                        Visibility::heap_tuple_satisfies_self(&heap_tuple, cur_xid, snapshot.curcid)
                    }
                    VisibilityMode::Stable => {
                        Visibility::heap_tuple_satisfies_stable(&heap_tuple, snapshot)
                    }
                    VisibilityMode::MVCC => {
                        Visibility::heap_tuple_satisfies_mvcc(&heap_tuple, snapshot, cur_xid)
                    }
                };

                if visible {
                    results.push((
                        ItemPointerData {
                            block_number: block_num,
                            offset_number: offset,
                        },
                        heap_tuple,
                    ));
                }
            }
        }

        Ok(results)
    }

    pub fn vacuum(&self) -> Result<u32> {
        let mut removed_count = 0u32;
        let page_count = self.relation.page_count();

        for block_num in 0..page_count {
            let mut page = self.relation.read_page(block_num)?;

            for offset_idx in 0..page.item_count() {
                let offset = (offset_idx + 1) as u16;

                if let Some(tuple_data) = page.get_item(offset) {
                    if let Ok(heap_tuple) = HeapTuple::deserialize(tuple_data, self.natts) {
                        let xmax = heap_tuple.xmax();
                        if !xmax.is_invalid() {
                            page.remove_item(offset)?;
                            removed_count += 1;
                        }
                    }
                }
            }

            if removed_count > 0 {
                self.relation.write_page(block_num, &page)?;
            }
        }

        Ok(removed_count)
    }

    pub fn close(&self) -> Result<()> {
        self.relation.close()
    }

    pub fn drop(&self) -> Result<()> {
        self.relation.drop()
    }
}

pub struct HeapEngine {
    pub heap: HeapRelation,
    pub current_tx: Option<Transaction>,
}

impl HeapEngine {
    pub fn create(path: PathBuf, natts: u16) -> Result<(Self, u32)> {
        let (heap, rel_node) = HeapRelation::create(path, natts)?;

        Ok((
            Self {
                heap,
                current_tx: None,
            },
            rel_node,
        ))
    }

    pub fn open(path: PathBuf, natts: u16) -> Result<Self> {
        let heap = HeapRelation::open(path, natts)?;

        Ok(Self {
            heap,
            current_tx: None,
        })
    }

    pub fn begin(&mut self) -> Transaction {
        let tx = Transaction::new(self.heap.tx_manager.clone());
        self.current_tx = Some(tx.clone());
        tx
    }

    pub fn commit(&mut self) {
        if let Some(tx) = self.current_tx.take() {
            tx.commit();
        }
    }

    pub fn abort(&mut self) {
        if let Some(tx) = self.current_tx.take() {
            tx.abort();
        }
    }

    pub fn insert(&self, data: &[u8]) -> Result<ItemPointerData> {
        let tx = self
            .current_tx
            .as_ref()
            .ok_or_else(|| HeapError::InvalidTransaction("no active transaction".to_string()))?;

        self.heap.insert(tx.xid(), tx.get_cid(), data)
    }

    pub fn update(
        &self,
        ctid: ItemPointerData,
        new_data: &[u8],
    ) -> Result<Option<ItemPointerData>> {
        let tx = self
            .current_tx
            .as_ref()
            .ok_or_else(|| HeapError::InvalidTransaction("no active transaction".to_string()))?;

        self.heap.update(tx.xid(), tx.get_cid(), ctid, new_data)
    }

    pub fn delete(&self, ctid: ItemPointerData) -> Result<bool> {
        let tx = self
            .current_tx
            .as_ref()
            .ok_or_else(|| HeapError::InvalidTransaction("no active transaction".to_string()))?;

        self.heap.delete(tx.xid(), tx.get_cid(), ctid)
    }

    pub fn get(&self, ctid: ItemPointerData) -> Result<Option<HeapTuple>> {
        self.heap.get(ctid)
    }

    pub fn scan(&self) -> Result<Vec<(ItemPointerData, HeapTuple)>> {
        let tx_manager = self.heap.tx_manager.clone();
        let current_cid = if let Some(ref tx) = self.current_tx {
            tx.get_cid()
        } else {
            CommandId::invalid()
        };

        let snapshot = tx_manager.get_snapshot(current_cid);
        let cur_xid = if let Some(ref tx) = self.current_tx {
            tx.xid()
        } else {
            TransactionId::first_normal()
        };

        self.heap.scan(&snapshot, cur_xid)
    }

    pub fn vacuum(&self) -> Result<u32> {
        self.heap.vacuum()
    }

    pub fn close(&self) -> Result<()> {
        self.heap.close()
    }

    pub fn drop(&self) -> Result<()> {
        self.heap.drop()
    }
}
