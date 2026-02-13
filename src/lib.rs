pub mod constants;
pub mod error;
pub mod heap;
pub mod heap_tuple;
pub mod page;
pub mod relation;
pub mod storage;
pub mod transaction;
pub mod types;
pub mod visibility;

pub use error::HeapError;
pub use heap::*;
pub use heap_tuple::*;
pub use page::*;
pub use relation::*;
pub use storage::*;
pub use transaction::*;
pub use types::*;
pub use visibility::*;

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use tempfile::TempDir;

    use super::constants::*;
    use super::error::Result;
    use super::heap::{HeapEngine, HeapRelation};
    use super::heap_tuple::{HeapTuple, HeapTupleHeaderData};
    use super::page::{ItemIdData, Page};
    use super::relation::Relation;
    use super::storage::Storage;
    use super::transaction::{Transaction, TransactionManager};
    use super::types::*;
    use super::visibility::Visibility;

    #[test]
    fn test_page_creation() {
        let page = Page::new(8192);
        assert_eq!(page.page_size, 8192);
        assert_eq!(page.header.pd_lower, 24);
        assert_eq!(page.header.pd_upper, 8192);
        assert_eq!(page.item_count(), 0);
    }

    #[test]
    fn test_page_add_item() {
        let mut page = Page::new(8192);
        let data = b"test_data";
        let offset = page.add_item(data).unwrap();
        assert_eq!(offset, 1);
        assert_eq!(page.item_count(), 1);

        let retrieved = page.get_item(1).unwrap();
        assert_eq!(retrieved, data);
    }

    #[test]
    fn test_page_multiple_items() {
        let mut page = Page::new(8192);
        let data1 = b"item1";
        let data2 = b"item2";
        let data3 = b"item3";

        let offset1 = page.add_item(data1).unwrap();
        let offset2 = page.add_item(data2).unwrap();
        let offset3 = page.add_item(data3).unwrap();

        assert_eq!(offset1, 1);
        assert_eq!(offset2, 2);
        assert_eq!(offset3, 3);

        assert_eq!(page.get_item(1).unwrap(), data1);
        assert_eq!(page.get_item(2).unwrap(), data2);
        assert_eq!(page.get_item(3).unwrap(), data3);
    }

    #[test]
    fn test_page_remove_item() {
        let mut page = Page::new(8192);
        let data = b"test_data";
        page.add_item(data).unwrap();

        assert_eq!(page.item_count(), 1);

        page.remove_item(1).unwrap();

        let items = page.dead_items();
        assert_eq!(items.len(), 1);
    }

    #[test]
    fn test_page_free_space() {
        let mut page = Page::new(8192);
        let initial_free = page.free_space();

        let data = b"test_data";
        page.add_item(data).unwrap();

        let new_free = page.free_space();
        assert!(new_free < initial_free);
    }

    #[test]
    fn test_page_serialize() {
        let mut page = Page::new(8192);
        let data = b"test_data";
        page.add_item(data).unwrap();

        let serialized = page.serialize();
        assert_eq!(serialized.len(), 8192);

        let restored = Page::from_raw(serialized).unwrap();
        assert_eq!(restored.item_count(), 1);
    }

    #[test]
    fn test_heap_tuple_header() {
        let header = HeapTupleHeaderData::new(4);
        assert_eq!(header.natts(), 4);
        assert!(!header.has_null());
        assert!(!header.has_varlena());
    }

    #[test]
    fn test_heap_tuple_serialization() {
        let mut heap_tuple = HeapTuple::with_data(2, b"test_data".to_vec(), false);
        heap_tuple.header.t_xmin = 100;
        heap_tuple.header.t_xmax = 0;

        let serialized = heap_tuple.serialize();
        assert!(serialized.len() > 0);

        let restored = HeapTuple::deserialize(&serialized, 2).unwrap();
        assert_eq!(restored.header.t_xmin, 100);
    }

    #[test]
    fn test_heap_tuple_null_bitmap() {
        let mut heap_tuple = HeapTuple::with_data(4, b"data".to_vec(), true);
        heap_tuple.set_null(1);
        heap_tuple.set_null(3);

        assert!(heap_tuple.is_null(1));
        assert!(!heap_tuple.is_null(2));
        assert!(heap_tuple.is_null(3));
        assert!(!heap_tuple.is_null(4));
    }

    #[test]
    fn test_transaction_manager() {
        let manager = TransactionManager::new();

        let xid1 = manager.begin();
        assert!(xid1.is_valid());

        let cid1 = manager.get_cid();
        assert!(cid1.0 > 0);

        manager.commit(xid1);
        assert!(manager.is_committed(xid1));
    }

    #[test]
    fn test_transaction() {
        let manager = Arc::new(TransactionManager::new());

        let xid = manager.begin();
        assert!(xid.is_valid());

        manager.commit(xid);

        assert!(manager.is_committed(xid));
    }

    #[test]
    fn test_snapshot() {
        let manager = Arc::new(TransactionManager::new());

        manager.begin();

        let snapshot = manager.get_snapshot(CommandId(1));
        assert!(snapshot.xmin.is_valid());
    }

    #[test]
    fn test_visibility_mvcc() {
        let mut heap_tuple = HeapTuple::with_data(1, b"test".to_vec(), false);
        heap_tuple.header.t_xmin = 5;

        let snapshot = Snapshot::new(10, 100, vec![], 5, VisibilityMode::MVCC);

        let result =
            Visibility::heap_tuple_satisfies_mvcc(&heap_tuple, &snapshot, TransactionId(5));
        assert!(result);
    }

    #[test]
    fn test_visibility_self() {
        let mut heap_tuple = HeapTuple::with_data(1, b"test".to_vec(), false);
        heap_tuple.header.t_xmin = 100;

        let result =
            Visibility::heap_tuple_satisfies_self(&heap_tuple, TransactionId(100), CommandId(1));
        assert!(result);
    }

    #[test]
    fn test_storage() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let storage = Storage::new(path.clone()).unwrap();

        let page = Page::new(8192);
        storage.write_page(0, &page).unwrap();

        let read_page = storage.read_page(0).unwrap();
        assert_eq!(read_page.item_count(), 0);
    }

    #[test]
    fn test_storage_multiple_pages() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let storage = Storage::new(path.clone()).unwrap();

        let mut page1 = Page::new(8192);
        page1.add_item(b"data1").unwrap();
        storage.write_page(0, &page1).unwrap();

        let mut page2 = Page::new(8192);
        page2.add_item(b"data2").unwrap();
        storage.write_page(1, &page2).unwrap();

        let read_page1 = storage.read_page(0).unwrap();
        let read_page2 = storage.read_page(1).unwrap();

        assert_eq!(read_page1.item_count(), 1);
        assert_eq!(read_page2.item_count(), 1);
    }

    #[test]
    fn test_heap_insert() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let (heap, _) = HeapRelation::create(path, 2).unwrap();

        let ctid = heap
            .insert(TransactionId(100), CommandId(1), b"test_data")
            .unwrap();
        assert!(ctid.is_valid());

        let retrieved = heap.get(ctid).unwrap();
        assert!(retrieved.is_some());
    }

    #[test]
    fn test_heap_update() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let (heap, _) = HeapRelation::create(path, 2).unwrap();

        let ctid = heap
            .insert(TransactionId(100), CommandId(1), b"original")
            .unwrap();

        let new_ctid = heap
            .update(TransactionId(101), CommandId(2), ctid, b"updated")
            .unwrap();
        assert!(new_ctid.is_some());

        let old_tuple = heap.get(ctid).unwrap().unwrap();
        assert!(!old_tuple.xmax().is_invalid());
    }

    #[test]
    fn test_heap_delete() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let (heap, _) = HeapRelation::create(path, 2).unwrap();

        let ctid = heap
            .insert(TransactionId(100), CommandId(1), b"test")
            .unwrap();

        let deleted = heap.delete(TransactionId(101), CommandId(2), ctid).unwrap();
        assert!(deleted);

        let tuple = heap.get(ctid).unwrap().unwrap();
        assert!(!tuple.xmax().is_invalid());
    }

    #[test]
    fn test_heap_scan() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let (heap, _) = HeapRelation::create(path, 2).unwrap();

        heap.insert(TransactionId(100), CommandId(1), b"data1")
            .unwrap();
        heap.insert(TransactionId(101), CommandId(1), b"data2")
            .unwrap();
        heap.insert(TransactionId(102), CommandId(1), b"data3")
            .unwrap();

        let snapshot = Snapshot::new(1, 200, vec![], 10, VisibilityMode::MVCC);
        let results = heap.scan(&snapshot, TransactionId(150)).unwrap();

        assert_eq!(results.len(), 3);
    }

    #[test]
    fn test_heap_vacuum() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let (heap, _) = HeapRelation::create(path, 2).unwrap();

        let ctid1 = heap
            .insert(TransactionId(100), CommandId(1), b"data1")
            .unwrap();
        let ctid2 = heap
            .insert(TransactionId(101), CommandId(1), b"data2")
            .unwrap();

        heap.delete(TransactionId(102), CommandId(2), ctid1)
            .unwrap();

        let removed = heap.vacuum().unwrap();
        assert!(removed > 0);
    }

    #[test]
    fn test_heap_engine_full_workflow() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let (mut engine, rel_id) = HeapEngine::create(path, 2).unwrap();
        assert!(rel_id > 0);

        engine.begin();

        let ctid1 = engine.insert(b"row1").unwrap();
        let ctid2 = engine.insert(b"row2").unwrap();
        let ctid3 = engine.insert(b"row3").unwrap();

        let results = engine.scan().unwrap();
        assert_eq!(results.len(), 3);

        let updated = engine.update(ctid1, b"row1_updated").unwrap();
        assert!(updated.is_some());

        let deleted = engine.delete(ctid2).unwrap();
        assert!(deleted);

        engine.commit();

        let results = engine.scan().unwrap();
        assert_eq!(results.len(), 1);

        engine.close().unwrap();
    }

    #[test]
    fn test_mvcc_isolation() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let tx_manager = Arc::new(TransactionManager::new());

        let heap = HeapRelation {
            relation: Relation::open(path.clone()).unwrap(),
            natts: 2,
            tx_manager: tx_manager.clone(),
        };

        let ctid = heap
            .insert(TransactionId(100), CommandId(1), b"data")
            .unwrap();

        let snapshot = Snapshot::new(1, 200, vec![], 10, VisibilityMode::MVCC);
        let results = heap.scan(&snapshot, TransactionId(150)).unwrap();

        assert_eq!(results.len(), 1);
    }

    #[test]
    fn test_concurrent_transactions() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let (mut engine1, _) = HeapEngine::create(path.clone(), 2).unwrap();
        let (mut engine2, _) = HeapEngine::create(path, 2).unwrap();

        engine1.begin();
        engine1.insert(b"tx1_data").unwrap();

        engine2.begin();
        let results = engine2.scan().unwrap();
        assert_eq!(results.len(), 0);

        engine1.commit();

        engine2.commit();
    }

    #[test]
    fn test_page_header_flags() {
        let mut page = Page::new(8192);

        assert!(!page.header.has_free_lines());
        page.header.set_has_free_lines(true);
        assert!(page.header.has_free_lines());

        assert!(!page.header.is_page_full());
        page.header.set_page_full(true);
        assert!(page.header.is_page_full());

        assert!(!page.header.all_visible());
        page.header.set_all_visible(true);
        assert!(page.header.all_visible());
    }

    #[test]
    fn test_item_id_data() {
        let mut item_id = ItemIdData::new();

        item_id.set(100, 50, LP_USED);

        assert_eq!(item_id.offset(), 100);
        assert_eq!(item_id.length(), 50);
        assert_eq!(item_id.flags(), LP_USED);
        assert!(item_id.is_used());
    }

    #[test]
    fn test_heap_tuple_ctid() {
        let mut heap_tuple = HeapTuple::new(2);

        let ctid = ItemPointerData {
            block_number: 5,
            offset_number: 10,
        };

        heap_tuple.set_ctid(ctid);

        let retrieved_ctid = heap_tuple.ctid();
        assert_eq!(retrieved_ctid.block_number, 5);
        assert_eq!(retrieved_ctid.offset_number, 10);
    }

    #[test]
    fn test_visibility_any() {
        let mut heap_tuple = HeapTuple::new(1);
        heap_tuple.header.t_xmax = 0;

        assert!(Visibility::heap_tuple_satisfies_any(&heap_tuple));

        heap_tuple.header.t_xmax = 100;
        heap_tuple.header.set_xmax_committed(true);

        assert!(!Visibility::heap_tuple_satisfies_any(&heap_tuple));
    }

    #[test]
    fn test_hint_bits() {
        let mut heap_tuple = HeapTuple::new(1);

        Visibility::set_hint_bits(
            &mut heap_tuple,
            HeapTupleStatus::Comitted,
            HeapTupleStatus::Aborted,
        );

        assert!(heap_tuple.header.xmin_committed());
        assert!(heap_tuple.header.xmax_invalid());
    }

    #[test]
    fn test_large_data_insert() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let (heap, _) = HeapRelation::create(path, 1).unwrap();

        let large_data = vec![0u8; 1000];
        let ctid = heap
            .insert(TransactionId(100), CommandId(1), &large_data)
            .unwrap();

        let retrieved = heap.get(ctid).unwrap().unwrap();
        assert_eq!(retrieved.data.len(), 1000);
    }

    #[test]
    fn test_multiple_pages_insert() {
        let temp_dir = TempDir::new().unwrap();
        let path = temp_dir.path().to_path_buf();

        let (heap, _) = HeapRelation::create(path, 1).unwrap();

        for i in 0..100 {
            let data = format!("data_{}", i);
            heap.insert(TransactionId(100 + i), CommandId(1), data.as_bytes())
                .unwrap();
        }

        let page_count = heap.relation.page_count();
        assert!(page_count >= 1);
    }
}
