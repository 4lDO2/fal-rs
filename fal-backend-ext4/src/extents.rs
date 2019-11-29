use std::{
    cmp::Ordering,
    convert::TryInto,
    io,
};

use crate::{block_group, calculate_crc32c, inode::Blocks, read_block, Filesystem, write_block};

use quick_error::quick_error;
use scroll::{Pread, Pwrite};

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
    pub fn new(rel_baddr: u32, leaf: u64) -> Self {
        Self {
            block: rel_baddr,
            leaf_lo: leaf as u32,
            leaf_hi: (leaf >> 32) as u16,
            unused: 0,
        }
    }
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
    pub fn new(rel_baddr: u32, len: u16, phys_baddr: u64, initialized: bool) -> Self {
        assert!(len <= 32768);

        Self {
            block: rel_baddr,
            start_lo: phys_baddr as u32,
            start_hi: (phys_baddr >> 32) as u16,
            raw_len: if initialized {
                len
            } else {
                len + 32768
            },
        }
    }
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
    checksum_seed: u32,
    size: usize,
}

impl ExtentTree {
    pub fn calculate_crc32c(seed: u32, bytes: &[u8]) -> u32 {
        calculate_crc32c(seed, &bytes[..bytes.len() - TAIL_SIZE])
    }
    /// Initialize an extent tree from the blocks field of an inode.
    pub fn from_inode_blocks_field(seed: u32, blocks: &Blocks) -> Result<Self, scroll::Error> {
        Self::parse(seed, &blocks.inner, false)
    }
    pub fn from_block(seed: u32, bytes: &[u8]) -> Result<Self, scroll::Error> {
        Self::parse(seed, bytes, true)
    }
    pub fn to_inode_blocks_field(this: &Self, blocks: &mut Blocks) -> Result<(), scroll::Error> {
        Self::serialize(this, &mut blocks.inner, false)
    }
    pub fn to_block(this: &Self, bytes: &mut [u8]) -> Result<(), scroll::Error> {
        Self::serialize(this, bytes, true)
    }

    fn parse(seed: u32, bytes: &[u8], use_tail: bool) -> Result<Self, scroll::Error> {
        let header: ExtentHeader = bytes.pread_with(0, scroll::LE)?;

        if header.entry_count > header.max_entry_count {
            return Err(scroll::Error::BadInput { size: 2, msg: "Entry count too high" });
        }

        let calculated_max_entry_size = (bytes.len() - HEADER_SIZE - if use_tail {
            TAIL_SIZE
        } else {
            0
        }) / ITEM_SIZE;

        if header.max_entry_count as usize != calculated_max_entry_size {
            return Err(scroll::Error::BadInput { size: 2, msg: "Invalid max entry count" });
        }

        let body = if header.depth > 0 {
            ExtentTreeBody::Internal(
                (0..header.entry_count as usize)
                    .map(|i| {
                        bytes.pread_with(HEADER_SIZE + i * ITEM_SIZE, scroll::LE)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        } else {
            ExtentTreeBody::Leaf(
                (0..header.entry_count as usize)
                    .map(|i| {
                        bytes.pread_with(HEADER_SIZE + i * ITEM_SIZE, scroll::LE)
                    })
                    .collect::<Result<Vec<_>, _>>()?,
            )
        };

        let tail: ExtentTail = bytes.pread_with(bytes.len() - TAIL_SIZE, scroll::LE)?;

        if use_tail && tail.checksum != Self::calculate_crc32c(seed, bytes) {
            return Err(scroll::Error::BadInput { size: 4, msg: "extent block checksum mismatch" });
        }

        Ok(Self { header, body, tail: if use_tail { Some(tail) } else { None }, checksum_seed: seed, size: bytes.len() })
    }
    fn serialize(this: &Self, bytes: &mut [u8], use_tail: bool) -> Result<(), scroll::Error> {
        let mut offset = 0;
        bytes.gwrite_with(&this.header, &mut offset, scroll::LE)?;

        match this.body {
            ExtentTreeBody::Internal(ref items) => for item in items {
                bytes.gwrite_with(item, &mut offset, scroll::LE)?;
            }
            ExtentTreeBody::Leaf(ref leaves) => for leaf in leaves {
                bytes.gwrite_with(leaf, &mut offset, scroll::LE)?;
            }
        }

        if use_tail {
            assert!(this.tail.is_some());
            let new_tail = ExtentTail {
                checksum: Self::calculate_crc32c(this.checksum_seed, bytes),
            };
            bytes.pwrite_with(&new_tail, bytes.len() - TAIL_SIZE, scroll::LE)?;
        }
        Ok(())
    }
    fn calculate_max_entry_count(size: usize) -> usize {
        (size - HEADER_SIZE - TAIL_SIZE) / ITEM_SIZE
    }
    pub fn new_subtree(parent: &Self, size: usize) -> Self {
        let depth = parent.header.depth.checked_sub(1).expect("Creating a subtree from a leaf");

        Self {
            header: ExtentHeader {
                magic: MAGIC,
                depth,
                entry_count: 0,
                generation: 0,
                max_entry_count: Self::calculate_max_entry_count(size).try_into().expect("what extent tree block size (aka block size) are you using?"),
            },
            body: if depth == 0 {
                ExtentTreeBody::Leaf(vec! [])
            } else {
                ExtentTreeBody::Internal(vec! [])
            },
            checksum_seed: parent.checksum_seed,
            size,
            // The checksum only really matters when parsing, since it's updated dynamically.
            tail: Some(ExtentTail { checksum: 0 }),
        }
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
    fn leaves_mut(&mut self) -> Option<&mut Vec<ExtentLeaf>> {
        match self.body {
            ExtentTreeBody::Leaf(ref mut items) => Some(items),
            _ => None,
        }
    }
    fn internal_node_items(&self) -> Option<&[ExtentInternalNode]> {
        match self.body {
            ExtentTreeBody::Internal(ref items) => Some(items),
            _ => None,
        }
    }
    fn internal_node_items_mut(&mut self) -> Option<&mut Vec<ExtentInternalNode>> {
        match self.body {
            ExtentTreeBody::Internal(ref mut items) => Some(items),
            _ => None,
        }
    }
    fn local_items_capacity(&self) -> usize {
        self.header.max_entry_count as usize
    }
    fn local_items_len(&self) -> usize {
        self.header.entry_count as usize
    }
    fn node_is_full(&self) -> bool {
        assert!(self.local_items_len() <= self.local_items_capacity());
        self.local_items_len() == self.local_items_capacity()
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

quick_error! {
    #[derive(Debug)]
    enum AllocateExtentLeafError {
        NodeIsFull {}
        AlreadyExists {}
        AllocateBlockError(err: block_group::AllocateBlockError) {
            from()
            cause(err)
        }
        SerializationError(err: scroll::Error) {
            from()
            cause(err)
        }
        DiskIoError(err: io::Error) {
            from()
            cause(err)
        }
    }
}

fn allocate_extent_leaf<D: fal::DeviceMut>(filesystem: &Filesystem<D>, root: &mut ExtentTree, rel_baddr: u32, len: u16) -> Result<(), AllocateExtentLeafError> {
    if root.is_leaf() {
        if root.node_is_full() {
            return Err(AllocateExtentLeafError::NodeIsFull);
        }
        let allocated_range = block_group::allocate_blocks(filesystem, len.into())?;

        assert_eq!(allocated_range.end - allocated_range.start, u64::from(len));

        match root.resolve_local_leaf(rel_baddr) {
            Ok(_) => return Err(AllocateExtentLeafError::AlreadyExists),

            Err(new_index) => {
                // TODO: Check whether the item at this index (which is either before or after the
                // current extent) overlaps the extent we're currently allocating.

                // Unlike the usual case where Err leads to return Err, in this case Err(idx) means
                // that the key (an extent with rel_baddr as start) wasn't found, which is good
                // since we are allocating it.
                let new_leaf = ExtentLeaf::new(rel_baddr, len, allocated_range.start, true);
                root.leaves_mut().unwrap().insert(new_index, new_leaf);
            }
        }
    } else {
        if root.node_is_full() {
            return Err(AllocateExtentLeafError::NodeIsFull);
        }
        let allocated_block = block_group::allocate_block(filesystem)?;

        let mut block_bytes = vec! [0u8; filesystem.superblock.block_size() as usize];

        let mut subtree = ExtentTree::new_subtree(&root, filesystem.superblock.block_size() as usize);

        // Create a new subtree containing the single to-be-allocated extent.
        allocate_extent_leaf(filesystem, &mut subtree, rel_baddr, len)?;
        ExtentTree::serialize(&subtree, &mut block_bytes, true)?;
        write_block(filesystem, allocated_block, &block_bytes)?;

        // Update the root to include the new entry.
        match root.resolve_local_internal(rel_baddr) {
            Ok(_) => return Err(AllocateExtentLeafError::AlreadyExists),

            Err(new_index) => {
                // The same thing as with the leaf insertion happens here; a new internal subtree
                // pointer entry is simply inserted in a sorted order.

                let new_entry = ExtentInternalNode::new(rel_baddr, allocated_block);
                root.internal_node_items_mut().unwrap().insert(new_index, new_entry);
            }
        }
    }
    Ok(())
}
pub fn allocate_extent<D: fal::DeviceMut>(filesystem: &Filesystem<D>, root: &mut ExtentTree, rel_baddr: u32, len: u16) {
    allocate_extent_leaf(filesystem, root, rel_baddr, len).unwrap();
}
