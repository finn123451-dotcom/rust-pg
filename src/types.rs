use std::fmt;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Ord, PartialOrd)]
pub struct TransactionId(pub u32);

impl TransactionId {
    pub fn invalid() -> Self {
        Self(0)
    }

    pub fn bootstrap() -> Self {
        Self(1)
    }

    pub fn first_normal() -> Self {
        Self(2)
    }

    pub fn is_valid(&self) -> bool {
        self.0 != 0
    }

    pub fn is_invalid(&self) -> bool {
        self.0 == 0
    }

    pub fn is_in_progress(&self, xmin: TransactionId, xmax: TransactionId) -> bool {
        self.0 >= xmin.0 && self.0 < xmax.0
    }

    pub fn is_committed(&self) -> bool {
        false
    }
}

impl Default for TransactionId {
    fn default() -> Self {
        Self::invalid()
    }
}

impl fmt::Display for TransactionId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}", self.0)
    }
}

impl From<u32> for TransactionId {
    fn from(x: u32) -> Self {
        Self(x)
    }
}

impl From<TransactionId> for u32 {
    fn from(x: TransactionId) -> Self {
        x.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct CommandId(pub u32);

impl CommandId {
    pub fn invalid() -> Self {
        Self(0)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct ItemPointerData {
    pub block_number: u32,
    pub offset_number: u16,
}

impl ItemPointerData {
    pub fn invalid() -> Self {
        Self {
            block_number: 0,
            offset_number: 0,
        }
    }

    pub fn is_valid(&self) -> bool {
        self.block_number != 0 || self.offset_number != 0
    }

    pub fn is_invalid(&self) -> bool {
        !self.is_valid()
    }
}

impl fmt::Display for ItemPointerData {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "({}, {})", self.block_number, self.offset_number)
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct BlockId {
    pub db_node: u32,
    pub rel_node: u32,
    pub block_num: u32,
}

impl BlockId {
    pub fn new(db_node: u32, rel_node: u32, block_num: u32) -> Self {
        Self {
            db_node,
            rel_node,
            block_num,
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct Oid(pub u32);

impl Oid {
    pub fn invalid() -> Self {
        Self(0)
    }
}

pub type RelationId = Oid;
pub type TablespaceId = Oid;
pub type DatabaseId = Oid;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum VisibilityMode {
    MVCC,
    Self_,
    Any,
    Stable,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Snapshot {
    pub xmin: TransactionId,
    pub xmax: TransactionId,
    pub xip: Vec<TransactionId>,
    pub curcid: CommandId,
    pub mode: VisibilityMode,
}

impl Snapshot {
    pub fn new(xmin: u32, xmax: u32, xip: Vec<u32>, curcid: u32, mode: VisibilityMode) -> Self {
        Self {
            xmin: TransactionId(xmin),
            xmax: TransactionId(xmax),
            xip: xip.into_iter().map(TransactionId).collect(),
            curcid: CommandId(curcid),
            mode,
        }
    }

    pub fn invalid() -> Self {
        Self {
            xmin: TransactionId::invalid(),
            xmax: TransactionId::invalid(),
            xip: Vec::new(),
            curcid: CommandId::invalid(),
            mode: VisibilityMode::MVCC,
        }
    }

    pub fn contains(&self, xid: TransactionId) -> bool {
        xid.0 >= self.xmin.0 && xid.0 < self.xmax.0
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HeapTupleStatus {
    Live,
    Dead,
    Comitted,
    Aborted,
    InProgress,
    Modified,
    Bad,
    Unknown,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum LockMode {
    NoLock,
    ForUpdate,
    ForNoKeyUpdate,
    ForShare,
    ForKeyShare,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum UpdateResult {
    Updated,
    Deleted,
    SelfModified,
    NoResult,
}
