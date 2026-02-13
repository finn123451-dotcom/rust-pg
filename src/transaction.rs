use crate::constants::*;
use crate::types::*;
use std::collections::HashMap;
use std::sync::{Arc, RwLock};

pub struct TransactionManager {
    current_xid: RwLock<TransactionId>,
    next_cid: RwLock<CommandId>,
    committed: RwLock<HashMap<TransactionId, bool>>,
    in_progress: RwLock<Vec<TransactionId>>,
}

impl TransactionManager {
    pub fn new() -> Self {
        Self {
            current_xid: RwLock::new(TransactionId(FIRST_NORMAL_TRANSACTION_ID)),
            next_cid: RwLock::new(CommandId(1)),
            committed: RwLock::new(HashMap::new()),
            in_progress: RwLock::new(Vec::new()),
        }
    }

    pub fn begin(&self) -> TransactionId {
        let mut xid = self.current_xid.write().unwrap();
        let new_xid = xid.0;
        *xid = TransactionId(new_xid + 1);

        let mut in_progress = self.in_progress.write().unwrap();
        in_progress.push(TransactionId(new_xid));

        TransactionId(new_xid)
    }

    pub fn commit(&self, xid: TransactionId) {
        {
            let mut committed = self.committed.write().unwrap();
            committed.insert(xid, true);
        }

        {
            let mut in_progress = self.in_progress.write().unwrap();
            in_progress.retain(|&x| x != xid);
        }
    }

    pub fn abort(&self, xid: TransactionId) {
        {
            let mut committed = self.committed.write().unwrap();
            committed.insert(xid, false);
        }

        {
            let mut in_progress = self.in_progress.write().unwrap();
            in_progress.retain(|&x| x != xid);
        }
    }

    pub fn get_cid(&self) -> CommandId {
        let mut cid = self.next_cid.write().unwrap();
        let new_cid = *cid;
        *cid = CommandId(cid.0 + 1);
        new_cid
    }

    pub fn is_committed(&self, xid: TransactionId) -> bool {
        if xid.is_invalid() {
            return false;
        }

        if xid.0 == BOOTSTRAP_TRANSACTION_ID {
            return true;
        }

        let committed = self.committed.read().unwrap();
        committed.get(&xid).copied().unwrap_or(false)
    }

    pub fn is_in_progress(&self, xid: TransactionId) -> bool {
        let in_progress = self.in_progress.read().unwrap();
        in_progress.contains(&xid)
    }

    pub fn current_xid(&self) -> TransactionId {
        *self.current_xid.read().unwrap()
    }

    pub fn get_snapshot(&self, current_cid: CommandId) -> Snapshot {
        let xid = *self.current_xid.read().unwrap();
        let in_progress = self.in_progress.read().unwrap();

        let xmin = if in_progress.is_empty() {
            TransactionId(xid.0 - 1)
        } else {
            in_progress
                .iter()
                .min()
                .copied()
                .unwrap_or(TransactionId(xid.0 - 1))
        };

        let xip: Vec<TransactionId> = in_progress.clone();

        Snapshot::new(
            xmin.0,
            xid.0,
            xip.iter().map(|x| x.0).collect(),
            current_cid.0,
            VisibilityMode::MVCC,
        )
    }
}

impl Default for TransactionManager {
    fn default() -> Self {
        Self::new()
    }
}

pub struct Transaction {
    pub xid: TransactionId,
    pub cid: CommandId,
    pub manager: Arc<TransactionManager>,
}

impl Clone for Transaction {
    fn clone(&self) -> Self {
        Self {
            xid: self.xid,
            cid: self.cid,
            manager: self.manager.clone(),
        }
    }
}

impl Transaction {
    pub fn new(manager: Arc<TransactionManager>) -> Self {
        let xid = manager.begin();
        let cid = manager.get_cid();
        Self { xid, cid, manager }
    }

    pub fn commit(mut self) {
        self.manager.commit(self.xid);
    }

    pub fn abort(mut self) {
        self.manager.abort(self.xid);
    }

    pub fn get_cid(&self) -> CommandId {
        self.cid
    }

    pub fn xid(&self) -> TransactionId {
        self.xid
    }
}

impl Drop for Transaction {
    fn drop(&mut self) {
        if !std::thread::panicking() {
            self.manager.abort(self.xid);
        }
    }
}
