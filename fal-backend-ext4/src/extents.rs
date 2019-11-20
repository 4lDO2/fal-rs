use crate::{calculate_crc32c, inode::Blocks, read_block, Filesystem};

use scroll::{Pread, Pwrite};

use std::cmp::Ordering;

/// The magic number identifying extent trees.
pub const MAGIC: u16 = 0xF30A;

/// The size of an extent tree item, equal to both sizeof(ExtentInternalNode) and sizeof(ExtentLeaf).
pub const ITEM_SIZE: usize = 12;

/// The size of the tree header.
pub const HEADER_SIZE: usize = 12;

pub const TAIL_SIZE: usize = 4;

/// The header of an extent tree, stored at the beginning of the inode.blocks field, or its own
/// extent tree block.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Pread, Pwrite)]
pub struct ExtentHeader {
    pub magic: u16,
    pub entry_count: u16,
    pub max_entry_count: u16,
    pub depth: u16,
    pub generation: u32,
}

/// An internal node, pointing to a sub-node.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Pread, Pwrite)]
pub struct ExtentInternalNode {
    pub block: u32,
    pub leaf_lo: u32,
    pub leaf_hi: u16,
    pub unused: u16,
}

impl ExtentInternalNode {
    pub fn logical_block(&self) -> u32 {
        self.block
    }
    pub fn physical_leaf_block(&self) -> u64 {
        u64::from(self.leaf_lo) | u64::from(self.leaf_hi) << 32
    }
}

impl Ord for ExtentInternalNode {
    fn cmp(&self, other: &Self) -> Ordering {
        Ord::cmp(&self.block, &other.block)
    }
}
impl PartialOrd for ExtentInternalNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

/// A leaf, mapping a virtual block address within a file, to a range of physical bloks.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Pread, Pwrite)]
pub struct ExtentLeaf {
    pub block: u32,
    pub raw_len: u16,
    pub start_hi: u16,
    pub start_lo: u32,
}

impl Ord for ExtentLeaf {
    fn cmp(&self, other: &Self) -> Ordering {
        Ord::cmp(&self.block, &other.block)
    }
}
impl PartialOrd for ExtentLeaf {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(Ord::cmp(self, other))
    }
}

impl ExtentLeaf {
    /// The length of the extent, in blocks.
    pub fn len(&self) -> u16 {
        if self.raw_len > 32768 {
            self.raw_len - 32768
        } else {
            self.raw_len
        }
    }
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
    pub fn is_initialized(&self) -> bool {
        self.raw_len <= 32768
    }
    /// The logical block of the extent within the file, which really is just `(offset / block_size)`.
    pub fn logical_block(&self) -> u32 {
        self.block
    }
    /// The block address of the first block that this extent points to.
    pub fn physical_start_block(&self) -> u64 {
        u64::from(self.start_lo) | u64::from(self.start_hi) << 32
    }
}

/// The last 4 bytes of an extent tree, which stores the CRC-32C checksum.
#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, Pread, Pwrite)]
pub struct ExtentTail {
    pub checksum: u32,
}

/// The items within a tree, being either internal nodes or leaves.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub enum ExtentTreeBody {
    // TODO: SmallVec could theoretically be used here, reducing cache misses. The extent trees
    // will probably still be stored in a vector. However, this will likely not matter as the I/O
    // speed is the only possible bottleneck. I'll need to profile this soon.
    Internal(Vec<ExtentInternalNode>),
    Leaf(Vec<ExtentLeaf>),
}

/// An extent tree, used for resolving virtual block addresses derived from file offsets, to
/// physical blocks containing the actual data.
#[derive(Clone, Debug, Eq, Hash, PartialEq)]
pub struct ExtentTree {
    pub header: ExtentHeader,
    pub body: ExtentTreeBody,
    pub tail: Option<ExtentTail>,
    pub checksum_seed: u32,
}

impl ExtentTree {
    pub fn calculate_crc32c(seed: u32, bytes: &[u8]) -> u32 {
        calculate_crc32c(seed, &bytes[..bytes.len() - TAIL_SIZE])
    }
    /// Initialize an extent tree from the blocks field of an inode.
    pub fn from_inode_blocks_field(seed: u32, blocks: &Blocks) -> Result<Self, scroll::Error> {
        Self::init(seed, &blocks.inner, false)
    }
    pub fn from_block(seed: u32, bytes: &[u8]) -> Result<Self, scroll::Error> {
        Self::init(seed, bytes, true)
    }

    fn init(seed: u32, bytes: &[u8], use_tail: bool) -> Result<Self, scroll::Error> {
        let header: ExtentHeader = bytes.pread_with(0, scroll::LE)?;

        if header.entry_count > header.max_entry_count {
            return Err(scroll::Error::BadInput { size: 2, msg: "Entry count too high" });
        }
        if header.max_entry_count as usize * ITEM_SIZE > 60 {
            return Err(scroll::Error::BadInput { size: 2,
                msg: "Max entry count makes it overflow the available space of the extent tree",
            });
        }

        let body = if header.depth > 0 {
            ExtentTreeBody::Internal(
                (0..header.entry_count as usize)
                    .map(|i| {
                        bytes
                            .pread_with(HEADER_SIZE + i * ITEM_SIZE, scroll::LE)
                            .unwrap()
                    })
                    .collect(),
            )
        } else {
            ExtentTreeBody::Leaf(
                (0..header.entry_count as usize)
                    .map(|i| {
                        bytes
                            .pread_with(HEADER_SIZE + i * ITEM_SIZE, scroll::LE)
                            .unwrap()
                    })
                    .collect(),
            )
        };

        let tail: ExtentTail = bytes
            .pread_with(bytes.len() - TAIL_SIZE, scroll::LE)
            .unwrap();

        if use_tail && tail.checksum != Self::calculate_crc32c(seed, bytes) {
            panic!("extent tail checksum mismatch");
        }

        Ok(Self { header, body, tail: if use_tail { Some(tail) } else { None }, checksum_seed: seed })
    }

    /// Check that all the items in the current node are sorted. This is a fundamental requirement
    /// for B+ trees, and helps performance.
    pub fn is_sorted(&self) -> bool {
        match self.body {
            ExtentTreeBody::Internal(ref items) => {
                let mut old = None;
                for item in items {
                    if !old.map(|old| old < item).unwrap_or(true) {
                        return false;
                    }
                    old = Some(item);
                }
            }
            ExtentTreeBody::Leaf(ref items) => {
                let mut old = None;
                for item in items {
                    if !old.map(|old| old < item).unwrap_or(true) {
                        return false;
                    }
                    old = Some(item);
                }
            }
        }
        true
    }

    fn resolve_local_internal(&self, logical_block: u32) -> Result<usize, usize> {
        let items = self
            .internal_node_items()
            .expect("Calling resolve_local_internal on a leaf node");
        items.binary_search_by_key(&logical_block, |item| item.block)
    }
    fn resolve_local_leaf(&self, logical_block: u32) -> Result<usize, usize> {
        let items = self
            .leaves()
            .expect("Calling resolve_local_leaf on an internal node");
        items.binary_search_by_key(&logical_block, |item| item.block)
    }
    fn leaves(&self) -> Option<&[ExtentLeaf]> {
        match self.body {
            ExtentTreeBody::Leaf(ref items) => Some(items),
            _ => None,
        }
    }
    fn internal_node_items(&self) -> Option<&[ExtentInternalNode]> {
        match self.body {
            ExtentTreeBody::Internal(ref items) => Some(items),
            _ => None,
        }
    }
    pub fn is_leaf(&self) -> bool {
        self.header.depth == 0
    }
    pub fn is_internal(&self) -> bool {
        self.header.depth > 0
    }
    /// Get the extent that contains the logical_block. The extent may have a start offset smaller
    /// than the logical_block, but the logical block has to be inside the bounds indicated by the
    /// start block and length of the extent.
    pub fn resolve<D: fal::Device>(
        &self,
        filesystem: &Filesystem<D>,
        logical_block: u32,
    ) -> Option<ExtentLeaf> {
        if self.is_leaf() {
            // Find the closest item, i.e. the item which would have been placed before the logical
            // block in case it was inserted in sorted order.
            let closest_idx = match self.resolve_local_leaf(logical_block) {
                Ok(equal_idx) => equal_idx,
                Err(lower_idx) => std::cmp::min(lower_idx, self.leaves().unwrap().len() - 1),
            };
            let closest_item = self.leaves().unwrap()[closest_idx];

            if logical_block < closest_item.logical_block() + u32::from(closest_item.len()) {
                // There was an extent where the logical block fitted inside its bounds.
                Some(closest_item)
            } else {
                // There was an extent with an offset lower than the logical block, but the logical
                // block didn't fit because of the length of the extent.
                None
            }
        } else {
            // Find the closest extent key, which points to the subtree containing a possibly closer extent key.
            let closest_lower_or_eq_idx = match self.resolve_local_internal(logical_block) {
                Ok(idx) => idx,
                Err(closest_lower_idx) => closest_lower_idx,
            };
            let item = self.internal_node_items().unwrap()[closest_lower_or_eq_idx];

            // Load the subtree and search it.
            let child_node_block = read_block(filesystem, item.physical_leaf_block()).unwrap();
            let child_node = Self::from_block(self.checksum_seed, &child_node_block).unwrap();
            child_node.resolve(filesystem, logical_block)
        }
    }
}
