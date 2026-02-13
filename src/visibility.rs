use crate::constants::*;
use crate::heap_tuple::HeapTuple;
use crate::types::*;

pub struct Visibility;

impl Visibility {
    pub fn heap_tuple_satisfies_mvcc(
        heap_tuple: &HeapTuple,
        snapshot: &Snapshot,
        cur_xid: TransactionId,
    ) -> bool {
        let xmin = heap_tuple.xmin();
        let xmax = heap_tuple.xmax();

        if !Self::heap_tuple_satisfiesvisibility(heap_tuple, snapshot, cur_xid) {
            return false;
        }

        if !Self::heap_txns_satisfies_update(heap_tuple, snapshot, cur_xid) {
            return false;
        }

        true
    }

    fn heap_tuple_satisfiesvisibility(
        heap_tuple: &HeapTuple,
        snapshot: &Snapshot,
        cur_xid: TransactionId,
    ) -> bool {
        let xmin = heap_tuple.xmin();

        if xmin.0 == cur_xid.0 {
            return true;
        }

        if xmin.0 >= snapshot.xmin.0 && xmin.0 < snapshot.xmax.0 {
            if heap_tuple.header.xmin_committed() {
                return true;
            }
            if heap_tuple.header.xmin_invalid() {
                return false;
            }
            if snapshot.xip.contains(&xmin) {
                return false;
            }
            return true;
        }

        if xmin.0 < snapshot.xmin.0 {
            if heap_tuple.header.xmin_committed() {
                return true;
            }
            if heap_tuple.header.xmin_invalid() {
                return false;
            }
            return true;
        }

        false
    }

    fn heap_txns_satisfies_update(
        heap_tuple: &HeapTuple,
        snapshot: &Snapshot,
        cur_xid: TransactionId,
    ) -> bool {
        let xmax = heap_tuple.xmax();

        if xmax.is_invalid() {
            return true;
        }

        if xmax.0 == cur_xid.0 {
            if heap_tuple.header.xmax_is_locked_only() {
                return true;
            }
            return false;
        }

        if xmax.0 >= snapshot.xmax.0 {
            return true;
        }

        if xmax.0 < snapshot.xmin.0 {
            if heap_tuple.header.xmax_committed() {
                return false;
            }
            if heap_tuple.header.xmax_invalid() {
                return true;
            }
            return false;
        }

        if heap_tuple.header.xmax_committed() {
            return false;
        }

        if heap_tuple.header.xmax_invalid() {
            return true;
        }

        if heap_tuple.header.xmax_is_locked_only() {
            return true;
        }

        false
    }

    pub fn heap_tuple_satisfies_any(heap_tuple: &HeapTuple) -> bool {
        let xmax = heap_tuple.xmax();

        if !xmax.is_invalid() {
            if heap_tuple.header.xmax_committed() {
                return false;
            }
            if heap_tuple.header.xmax_invalid() {
                return true;
            }
            if heap_tuple.header.xmax_is_locked_only() {
                return true;
            }
        }

        true
    }

    pub fn heap_tuple_satisfies_self(
        heap_tuple: &HeapTuple,
        cur_xid: TransactionId,
        cur_cid: CommandId,
    ) -> bool {
        let xmin = heap_tuple.xmin();
        let xmax = heap_tuple.xmax();

        if xmin.0 == cur_xid.0 {
            let tuple_cid = heap_tuple.cid();
            if tuple_cid.0 > cur_cid.0 {
                return false;
            }
        }

        if xmax.0 == cur_xid.0 {
            if heap_tuple.header.xmax_is_locked_only() {
                return true;
            }
            return false;
        }

        if xmax.is_invalid() {
            return true;
        }

        if heap_tuple.header.xmax_committed() {
            return false;
        }

        true
    }

    pub fn heap_tuple_satisfies_stable(heap_tuple: &HeapTuple, snapshot: &Snapshot) -> bool {
        let xmin = heap_tuple.xmin();
        let xmax = heap_tuple.xmax();

        if xmin.0 >= snapshot.xmin.0 && xmin.0 < snapshot.xmax.0 {
            if heap_tuple.header.xmin_committed() {
                return true;
            }
            if heap_tuple.header.xmin_invalid() {
                return false;
            }
            return false;
        }

        if xmin.0 < snapshot.xmin.0 {
            if heap_tuple.header.xmin_committed() {
                return true;
            }
            if heap_tuple.header.xmin_invalid() {
                return false;
            }
            return false;
        }

        if xmax.is_invalid() {
            return true;
        }

        if xmax.0 >= snapshot.xmin.0 && xmax.0 < snapshot.xmax.0 {
            if heap_tuple.header.xmax_committed() {
                return false;
            }
            if heap_tuple.header.xmax_invalid() {
                return true;
            }
            return false;
        }

        if xmax.0 < snapshot.xmin.0 {
            if heap_tuple.header.xmax_committed() {
                return false;
            }
            if heap_tuple.header.xmax_invalid() {
                return true;
            }
            return false;
        }

        true
    }

    pub fn set_hint_bits(
        heap_tuple: &mut HeapTuple,
        xmin_status: HeapTupleStatus,
        xmax_status: HeapTupleStatus,
    ) {
        match xmin_status {
            HeapTupleStatus::Comitted => {
                heap_tuple.header.set_xmin_committed(true);
                heap_tuple.header.set_xmin_invalid(false);
            }
            HeapTupleStatus::Aborted => {
                heap_tuple.header.set_xmin_committed(false);
                heap_tuple.header.set_xmin_invalid(true);
            }
            _ => {}
        }

        match xmax_status {
            HeapTupleStatus::Comitted => {
                heap_tuple.header.set_xmax_committed(true);
                heap_tuple.header.set_xmax_invalid(false);
            }
            HeapTupleStatus::Aborted => {
                heap_tuple.header.set_xmax_committed(false);
                heap_tuple.header.set_xmax_invalid(true);
            }
            _ => {}
        }
    }

    pub fn get_temporal(heap_tuple: &HeapTuple, snapshot: &Snapshot) -> HeapTupleStatus {
        let xmin = heap_tuple.xmin();
        let xmax = heap_tuple.xmax();

        if xmin.is_invalid() {
            return HeapTupleStatus::Dead;
        }

        if !xmax.is_invalid() {
            return HeapTupleStatus::Modified;
        }

        if xmin.0 >= snapshot.xmin.0 && xmin.0 < snapshot.xmax.0 {
            return HeapTupleStatus::InProgress;
        }

        if xmin.0 < snapshot.xmin.0 {
            if heap_tuple.header.xmin_committed() {
                return HeapTupleStatus::Live;
            }
            if heap_tuple.header.xmin_invalid() {
                return HeapTupleStatus::Dead;
            }
        }

        HeapTupleStatus::Unknown
    }
}
