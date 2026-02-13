pub const BLCKSZ: usize = 8192;
pub const MAXIMUM_ALIGNSIZE: usize = 8;

pub const HEAP_PAGE_MAGIC: u16 = 0x0D0A;
pub const HEAP_PAGE_VERSION: u16 = 4;

pub const HEAP_FIXED_HEADER_SIZE: usize = 24;
pub const HEAP_MINIMUM_HEADER_SIZE: usize = 23;

pub const TOAST_TUPLE_THRESHOLD: usize = 2048;
pub const TOAST_TUPLE_TARGET: usize = 1992;
pub const TOAST_MAX_CHUNK_SIZE: usize = 1992;

pub const INVALID_TRANSACTION_ID: u32 = 0;
pub const BOOTSTRAP_TRANSACTION_ID: u32 = 1;
pub const FIRST_NORMAL_TRANSACTION_ID: u32 = 2;
pub const MAX_TRANSACTION_ID: u32 = 0xFFFFFFFF;

pub const INVALID_COMMAND_ID: u32 = 0;
pub const MAX_COMMAND_ID: u32 = 0xFFFFFFFF;

pub const INVALID_OFFSET_NUMBER: u16 = 0;
pub const FIRST_VALID_OFFSET_NUMBER: u16 = 1;

pub const PD_ALL_VISIBLE: u16 = 0x0004;
pub const PD_PAGE_FULL: u16 = 0x0002;
pub const PD_HAS_FREE_LINES: u16 = 0x0001;

pub const HEAP_HASNULL: u16 = 0x0001;
pub const HEAP_HASVARLENA: u16 = 0x0002;
pub const HEAP_HASOID: u16 = 0x0004;
pub const HEAP_XMIN_COMMITTED: u16 = 0x0100;
pub const HEAP_XMIN_INVALID: u16 = 0x0200;
pub const HEAP_XMAX_COMMITTED: u16 = 0x0400;
pub const HEAP_XMAX_INVALID: u16 = 0x0800;
pub const HEAP_XMAX_IS_LOCKED_ONLY: u16 = 0x1000;
pub const HEAP_XMAX_KEYS_UPDATED: u16 = 0x2000;
pub const HEAP_TUPLE_COMPRESSED: u16 = 0x2000;
pub const HEAP_TUPLE_FROZEN: u16 = 0x4000;
pub const HEAP_KEYS_UPDATED: u16 = 0x1000;

pub const HEAP_NATTS_MASK: u16 = 0x0FFF;

pub const LP_USED: u8 = 1;
pub const LP_DEAD: u8 = 2;
pub const LP_NORMAL: u8 = 0;

pub const VARLENA_COMPRESSED: u32 = 0x40000000;
pub const VARLENA_EXTERNAL: u32 = 0x80000000;
pub const VARLENA_BIT_MASK: u32 = 0x3FFFFFFF;

pub const SNAPSHOT_MVCC: i32 = 0;
pub const SNAPSHOT_SELF: i32 = 1;
pub const SNAPSHOT_ANY: i32 = 2;
pub const SNAPSHOT_STABLE: i32 = 3;
