use scroll::{Pread, Pwrite};

pub const MAGIC: u16 = 0xF30A;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Pread, Pwrite)]
pub struct ExtentHeader {
    pub magic: u16,
    pub entry_count: u16,
    pub max_entry_count: u16,
    pub depth: u16,
    pub generation: u32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Pread, Pwrite)]
pub struct ExtentInternalNode {
    pub block: u32,
    pub leaf_lo: u32,
    pub leaf_hi: u16,
    pub unused: u16,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Pread, Pwrite)]
pub struct ExtentLeaf {
    pub block: u32,
    pub len: u16,
    pub start_hi: u16,
    pub start_lo: u32,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Pread, Pwrite)]
pub struct ExtentTail {
    pub checksum: u32,
}
