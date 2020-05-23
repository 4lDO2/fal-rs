use std::{cmp::Ordering, convert::TryInto, error::Error as _, io, mem, ops::Range};

use crate::{
    allocate_block_bytes, block_group, calculate_crc32c, disk::BlockKind, inode::Blocks, Filesystem,
};

use scroll::{Pread, Pwrite};
use thiserror::Error;

mod pairs;
pub use pairs::Pairs;

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

const MAX_EXTENT_LEN: u16 = 32768;

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
            raw_len: if initialized { len } else { len + 32768 },
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
    pub fn rel_range(&self) -> Range<u32> {
        self.block..self.block + u32::from(self.len())
    }
    pub fn overlaps(&self, range: Range<u32>) -> bool {
        let self_range = self.rel_range();
        range.contains(&self_range.start)
            || range.contains(&(self_range.end - 1))
            || self_range.contains(&range.start)
            || self_range.contains(&(range.end - 1))
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

impl ExtentTreeBody {
    pub fn len(&self) -> usize {
        match self {
            Self::Internal(ref int) => int.len(),
            Self::Leaf(ref leaf) => leaf.len(),
        }
    }
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
    pub fn to_inode_blocks_field(
        this: &mut Self,
        blocks: &mut Blocks,
    ) -> Result<(), scroll::Error> {
        Self::serialize(this, &mut blocks.inner, false)
    }
    pub fn to_block(this: &mut Self, bytes: &mut [u8]) -> Result<(), scroll::Error> {
        Self::serialize(this, bytes, true)
    }

    fn parse(seed: u32, bytes: &[u8], use_tail: bool) -> Result<Self, scroll::Error> {
        let header: ExtentHeader = bytes.pread_with(0, scroll::LE)?;

        if header.entry_count > header.max_entry_count {
            return Err(scroll::Error::BadInput {
                size: 2,
                msg: "Entry count too high",
            });
        }

        let calculated_max_entry_size =
            (bytes.len() - HEADER_SIZE - if use_tail { TAIL_SIZE } else { 0 }) / ITEM_SIZE;

        if header.max_entry_count as usize != calculated_max_entry_size {
            return Err(scroll::Error::BadInput {
                size: 2,
                msg: "Invalid max entry count",
            });
        }

        let body = if header.depth > 0 {
            ExtentTreeBody::Internal(
                (0..header.entry_count as usize)
                    .map(|i| bytes.pread_with(HEADER_SIZE + i * ITEM_SIZE, scroll::LE))
                    .collect::<Result<Vec<_>, _>>()?,
            )
        } else {
            ExtentTreeBody::Leaf(
                (0..header.entry_count as usize)
                    .map(|i| bytes.pread_with(HEADER_SIZE + i * ITEM_SIZE, scroll::LE))
                    .collect::<Result<Vec<_>, _>>()?,
            )
        };

        let tail: ExtentTail = bytes.pread_with(bytes.len() - TAIL_SIZE, scroll::LE)?;

        if use_tail && tail.checksum != Self::calculate_crc32c(seed, bytes) {
            return Err(scroll::Error::BadInput {
                size: 4,
                msg: "extent block checksum mismatch",
            });
        }

        Ok(Self {
            header,
            body,
            tail: if use_tail { Some(tail) } else { None },
            checksum_seed: seed,
            size: bytes.len(),
        })
    }
    fn serialize(this: &mut Self, bytes: &mut [u8], use_tail: bool) -> Result<(), scroll::Error> {
        this.header.entry_count = this.body.len().try_into().expect("Too many entries");

        let mut offset = 0;
        bytes.gwrite_with(&this.header, &mut offset, scroll::LE)?;

        match this.body {
            ExtentTreeBody::Internal(ref items) => {
                for item in items {
                    bytes.gwrite_with(item, &mut offset, scroll::LE)?;
                }
            }
            ExtentTreeBody::Leaf(ref leaves) => {
                for leaf in leaves {
                    bytes.gwrite_with(leaf, &mut offset, scroll::LE)?;
                }
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
    fn calculate_max_entry_count(size: usize, use_tail: bool) -> usize {
        (size - HEADER_SIZE - if use_tail { TAIL_SIZE } else { 0 }) / ITEM_SIZE
    }
    pub fn new_subtree(parent: &Self, size: usize) -> Self {
        let depth = parent
            .header
            .depth
            .checked_sub(1)
            .expect("Creating a subtree from a leaf");

        Self {
            header: ExtentHeader {
                magic: MAGIC,
                depth,
                entry_count: 0,
                generation: 0,
                max_entry_count: Self::calculate_max_entry_count(size, true)
                    .try_into()
                    .expect("what extent tree block size (aka block size) are you using?"),
            },
            body: if depth == 0 {
                ExtentTreeBody::Leaf(vec![])
            } else {
                ExtentTreeBody::Internal(vec![])
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
    pub(crate) fn leaves(&self) -> Option<&[ExtentLeaf]> {
        match self.body {
            ExtentTreeBody::Leaf(ref items) => Some(items),
            _ => None,
        }
    }
    pub(crate) fn leaves_mut(&mut self) -> Option<&mut Vec<ExtentLeaf>> {
        match self.body {
            ExtentTreeBody::Leaf(ref mut items) => Some(items),
            _ => None,
        }
    }
    pub(crate) fn internal_node_items(&self) -> Option<&[ExtentInternalNode]> {
        match self.body {
            ExtentTreeBody::Internal(ref items) => Some(items),
            _ => None,
        }
    }
    pub(crate) fn internal_node_items_mut(&mut self) -> Option<&mut Vec<ExtentInternalNode>> {
        match self.body {
            ExtentTreeBody::Internal(ref mut items) => Some(items),
            _ => None,
        }
    }
    pub(crate) fn local_items_capacity(&self) -> usize {
        self.header.max_entry_count as usize
    }
    pub(crate) fn local_items_len(&self) -> usize {
        self.header.entry_count as usize
    }
    pub(crate) fn dyn_local_items_len(&mut self) -> usize {
        self.header.entry_count = self.body.len().try_into().expect("Too many node items");
        self.local_items_len()
    }
    pub(crate) fn node_is_full(&self) -> bool {
        assert!(self.local_items_len() <= self.local_items_capacity());
        self.local_items_len() >= self.local_items_capacity()
    }
    pub(crate) fn node_will_be_full(&self) -> bool {
        self.local_items_len() + 1 >= self.local_items_capacity()
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
    pub fn resolve<D: fal::DeviceRo>(
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
                Err(closest_lower_idx) => {
                    if closest_lower_idx < self.body.len() {
                        closest_lower_idx
                    } else {
                        self.body.len() - 1
                    }
                }
            };
            let mut item = self.internal_node_items().unwrap()[closest_lower_or_eq_idx];

            if logical_block < item.block {
                item = self.internal_node_items()?[closest_lower_or_eq_idx - 1];
            }

            // Load the subtree and search it.
            let mut child_node_block = allocate_block_bytes(&filesystem.superblock);
            filesystem
                .disk
                .read_block(
                    filesystem,
                    BlockKind::Metadata,
                    item.physical_leaf_block(),
                    &mut child_node_block,
                )
                .unwrap();
            let child_node = Self::from_block(self.checksum_seed, &child_node_block).unwrap();
            child_node.resolve(filesystem, logical_block)
        }
    }
    fn first_rel_baddr(&self) -> Option<u32> {
        match self.body {
            ExtentTreeBody::Internal(ref int) => int.first().map(|node| node.block),
            ExtentTreeBody::Leaf(ref leaf) => leaf.first().map(|leaf| leaf.block),
        }
    }
}

mod alloc_extent_leaf_err {
    use super::*;

    #[derive(Debug, Error)]
    pub enum AllocateExtentLeafError {
        #[error("node is full")]
        NodeIsFull,

        #[error("node already exists")]
        AlreadyExists,

        #[error("failed to allocate block: {0}")]
        AllocateBlockError(#[from] block_group::AllocateBlockError),

        #[error("parse or serialization error: {0}")]
        SerializationError(#[from] scroll::Error),

        #[error("disk i/o error: {0}")]
        DiskIoError(#[from] fal::DeviceError),

        #[error("the base relative block address {:?}, overlapped an existing extent when allocating {:?}", r1, r2)]
        Overlaps { r1: Range<u32>, r2: Range<u32> },
    }
}
pub use alloc_extent_leaf_err::AllocateExtentLeafError;

fn allocate_extent_leaf<D: fal::Device>(
    filesystem: &Filesystem<D>,
    root: &mut ExtentTree,
    rel_baddr: u32,
    len: u16,
) -> Result<(), AllocateExtentLeafError> {
    if root.is_leaf() {
        if root.node_is_full() {
            return Err(AllocateExtentLeafError::NodeIsFull);
        }
        let allocated_range = block_group::allocate_blocks(filesystem, len.into())?;

        assert_eq!(allocated_range.end - allocated_range.start, u64::from(len));

        match root.resolve_local_leaf(rel_baddr) {
            Ok(_) => {
                return Err(AllocateExtentLeafError::AlreadyExists);
            }

            Err(new_index) => {
                let leaves = root.leaves().unwrap();
                let previous_extent = leaves.get(if new_index == 0 { 1 } else { new_index } - 1);
                let range = rel_baddr..rel_baddr + u32::from(len);

                if let Some(previous_extent) = previous_extent {
                    if new_index > 0 && previous_extent.overlaps(range.clone()) {
                        return Err(AllocateExtentLeafError::Overlaps {
                            r1: previous_extent.rel_range(),
                            r2: range,
                        });
                    }
                }

                // Unlike the usual case where Err leads to return Err, in this case Err(idx) means
                // that the key (an extent with rel_baddr as start) wasn't found, which is good
                // since we are allocating it.
                let new_leaf = ExtentLeaf::new(rel_baddr, len, allocated_range.start, true);
                root.leaves_mut().unwrap().insert(new_index, new_leaf);
            }
        }
    } else {
        let mut block_bytes = allocate_block_bytes(&filesystem.superblock);

        fn insert_new_subtree<E: fal::Device>(
            filesystem: &Filesystem<E>,
            root: &mut ExtentTree,
            rel_baddr: u32,
            len: u16,
            block_bytes: &mut [u8],
        ) -> Result<(), AllocateExtentLeafError> {
            if root.node_is_full() {
                return Err(AllocateExtentLeafError::NodeIsFull);
            }
            let allocated_block = block_group::allocate_block(filesystem)?;

            let mut subtree =
                ExtentTree::new_subtree(&root, filesystem.superblock.block_size() as usize);

            // Create a new subtree containing the single to-be-allocated extent.
            allocate_extent_leaf(filesystem, &mut subtree, rel_baddr, len)?;

            ExtentTree::serialize(&mut subtree, block_bytes, true)?;
            filesystem.disk.write_block(
                filesystem,
                BlockKind::Metadata,
                allocated_block,
                &block_bytes,
            )?;

            // Update the root to include the new entry.
            match root.resolve_local_internal(rel_baddr) {
                Ok(_) => {
                    return Err(AllocateExtentLeafError::AlreadyExists);
                }

                Err(new_index) => {
                    // The same thing as with the leaf insertion happens here; a new internal subtree
                    // pointer entry is simply inserted in a sorted order.

                    let new_entry = ExtentInternalNode::new(rel_baddr, allocated_block);
                    root.internal_node_items_mut()
                        .unwrap()
                        .insert(new_index, new_entry);
                    Ok(())
                }
            }
        };
        // Try to climb up (closer to the leaves), and allocate there.
        let mut idx = match root.resolve_local_internal(rel_baddr) {
            // If rel_baddr already exists in the internal nodes, that doesn't necessarily mean
            // that it's populated with leaves.
            Ok(i) => i,

            // If a binary search fails, then this index will be where to place the item to keep
            // the array sorted. There will be a check whether the index is not the previous node
            // (which possibly contains the leaves where rel_baddr can be inserted), which holds if
            // the index is zero and the item count is zero, or if the index is outside the range
            // of the nodes. If so, then a new subtree will simply be allocated.
            Err(i) => i,
        };
        if idx >= root.dyn_local_items_len() {
            idx = root.dyn_local_items_len() - 1;
        }
        if idx == 0 && root.dyn_local_items_len() == 0 {
            // This node is empty, which means that a new subtree shall be allocated.
            return insert_new_subtree(filesystem, root, rel_baddr, len, &mut block_bytes);
        }
        let block = root.internal_node_items().unwrap()[idx].physical_leaf_block();
        filesystem
            .disk
            .read_block(filesystem, BlockKind::Metadata, block, &mut block_bytes)?;

        let mut subtree = ExtentTree::from_block(root.checksum_seed, &block_bytes)?;

        match allocate_extent_leaf(filesystem, &mut subtree, rel_baddr, len) {
            Ok(()) => {
                ExtentTree::serialize(&mut subtree, &mut block_bytes, true)?;

                filesystem.disk.write_block(
                    filesystem,
                    BlockKind::Metadata,
                    block,
                    &block_bytes,
                )?;
                return Ok(());
            }
            Err(AllocateExtentLeafError::NodeIsFull) => {
                // If the current subtree was full, then a new subtree has to be inserted.
                return insert_new_subtree(filesystem, root, rel_baddr, len, &mut block_bytes);
            }
            Err(other) => return Err(other),
        }
    }
    Ok(())
}

mod alloc_extents_blk_error {
    use super::*;

    #[derive(Debug, Error)]
    pub enum AllocateExtentBlocksError {
        #[error("the allocation of a new extent leaf or the extension of an existing leaf failed: {}", err)]
        AllocLeafError {
            #[from]
            err: AllocateExtentLeafError,
        },

        #[error("additional blocks couldn't be allocated since the tree couldn't add more levels of depth: {}", err)]
        IncreaseLevelError {
            #[from]
            err: super::IncreaseLevelError,
        },
    }
}
pub use alloc_extents_blk_error::AllocateExtentBlocksError;

mod increase_level_error {
    use super::*;

    #[derive(Debug, Error)]
    pub enum IncreaseLevelError {
        #[error("the depth reached 5, the maximum")]
        DepthLimit,

        #[error("parse or serialization error: {0}")]
        ParseError(#[from] scroll::Error),

        #[error("block allocation error for subtree: {0}")]
        AllocateBlockError(#[from] block_group::AllocateBlockError),

        #[error("disk i/o error: {0}")]
        DiskIoErr(#[from] fal::DeviceError),
    }
}
pub use increase_level_error::IncreaseLevelError;

pub fn increase_level<D: fal::Device>(
    filesystem: &Filesystem<D>,
    root: &mut ExtentTree,
) -> Result<(), IncreaseLevelError> {
    let mut old_root = mem::replace(
        root,
        ExtentTree {
            header: ExtentHeader {
                magic: MAGIC,
                depth: if root.header.depth == 5 {
                    /*for leaf in root.pairs(filesystem) {
                        dbg!(leaf.unwrap());
                    }
                    panic!();*/
                    return Err(IncreaseLevelError::DepthLimit);
                } else {
                    root.header.depth + 1
                },
                entry_count: 1,
                max_entry_count: root.header.max_entry_count,
                generation: 0,
            },
            body: ExtentTreeBody::Internal(vec![]),
            checksum_seed: root.checksum_seed,
            size: root.size, // typically 60 bytes
            tail: None,
        },
    );
    old_root.size = filesystem.superblock.block_size() as usize;
    old_root.header.max_entry_count = ((old_root.size - HEADER_SIZE - TAIL_SIZE) / ITEM_SIZE)
        .try_into()
        .expect("too many entries to fit in a u16");
    old_root.tail = Some(ExtentTail { checksum: 0 });

    let mut block_bytes = allocate_block_bytes(&filesystem.superblock);
    ExtentTree::serialize(&mut old_root, &mut block_bytes, true)?;

    let baddr = block_group::allocate_block(filesystem)?;
    filesystem
        .disk
        .write_block(filesystem, BlockKind::Metadata, baddr, &block_bytes)?;

    let subtree_pointer = ExtentInternalNode::new(
        old_root
            .first_rel_baddr()
            .expect("increasing the level of an empty tree"),
        baddr,
    );
    root.internal_node_items_mut()
        .unwrap()
        .push(subtree_pointer);

    Ok(())
}

pub fn allocate_extent_blocks<D: fal::Device>(
    filesystem: &Filesystem<D>,
    root: &mut ExtentTree,
    rel_baddr: u32,
    block_count: u32,
) -> Result<(), AllocateExtentBlocksError> {
    let blocks_per_extent = u32::from(MAX_EXTENT_LEN);

    // BIG TODO: If AllocExtentLeafError::NodeIsFull is returned, a new tree (in the inode blocks
    // field) shall be created, and the existing tree shall be turned into a block-based subtree.

    let mut current_rel_block = 0;

    while current_rel_block + blocks_per_extent < block_count {
        match allocate_extent_leaf(
            filesystem,
            root,
            rel_baddr + current_rel_block,
            blocks_per_extent as u16,
        ) {
            Ok(()) => (),
            Err(AllocateExtentLeafError::NodeIsFull) => {
                increase_level(filesystem, root)?;
                allocate_extent_leaf(
                    filesystem,
                    root,
                    rel_baddr + current_rel_block,
                    blocks_per_extent as u16,
                )?;
            }
            Err(other) => return Err(other.into()),
        }
        current_rel_block += blocks_per_extent;
    }
    match allocate_extent_leaf(
        filesystem,
        root,
        rel_baddr + current_rel_block,
        (block_count - current_rel_block * blocks_per_extent) as u16,
    ) {
        Ok(()) => (),
        Err(AllocateExtentLeafError::NodeIsFull) => {
            increase_level(filesystem, root)?;
            allocate_extent_leaf(
                filesystem,
                root,
                rel_baddr + current_rel_block,
                (block_count - current_rel_block * blocks_per_extent) as u16,
            )?;
        }
        Err(other) => return Err(other.into()),
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    #[test]
    fn overlaps() {
        use super::ExtentLeaf;

        let a = ExtentLeaf::new(0, 1000, 0, true); // from 0-1000
        assert!(a.overlaps(32..96));
        assert!(a.overlaps(0..1));
        assert!(a.overlaps(999..1001));
        assert!(!a.overlaps(1001..2000));

        let b = ExtentLeaf::new(2000, 1000, 13370, true);
        assert!(b.overlaps(1500..2500));
        assert!(!b.overlaps(1000..1999));
        assert!(!b.overlaps(3000..7000));
    }
}
