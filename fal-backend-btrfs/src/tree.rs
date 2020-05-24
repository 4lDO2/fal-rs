use core::{
    borrow::Borrow,
    cmp::Ordering,
    fmt, mem,
};

use std::borrow::Cow;

use zerocopy::{AsBytes, ByteSlice, FromBytes, LayoutVerified, Unaligned};

use crate::{
    chunk_map::ChunkMap,
    filesystem,
    items::{
        BlockGroupItem, ChunkItem, CsumItem, DevExtent, DevItem, DevStatsItem, DirItem, ExtentItem,
        FileExtentItem, InodeItem, InodeRef, RootItem, RootRef, UuidItem,
    },
    superblock::{ChecksumType, Superblock},
    Checksum, DiskKey, DiskKeyType, InvalidChecksum,

    u64_le, u32_le,
};

// TODO: Paths are small and smallvec / arrayvec should be used.
type Path<'a> = Vec<(Cow<'a, Tree>, usize)>;

#[derive(Clone, Copy, Debug, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct Header {
    pub checksum: [u8; 32],
    pub fsid: [u8; 16],

    pub logical_addr: u64_le,
    pub flags: u64_le,

    pub chunk_tree_uuid: [u8; 16],
    pub generation: u64_le,
    pub owner: u64_le,
    pub item_count: u32_le,
    pub level: u8,
}

impl Header {
    pub fn parse<'a>(checksum_ty: ChecksumType, bytes: &'a [u8]) -> Result<LayoutVerified<&'a [u8], Self>, InvalidChecksum> {
        let this = LayoutVerified::new_unaligned(bytes).unwrap();

        let stored_checksum = Checksum::parse(checksum_ty, &bytes[..32]).ok_or(InvalidChecksum::UnsupportedChecksum(checksum_ty))?;
        let checksum = Checksum::calculate(checksum_ty, &bytes[32..]).ok_or(InvalidChecksum::UnsupportedChecksum(checksum_ty))?;

        if stored_checksum != checksum {
            return Err(InvalidChecksum::Mismatch);
        }
        Ok(this)
    }
}

pub struct Node {
    pub header: Header,
    pub key_ptrs: [KeyPtr],
}
impl fmt::Debug for Node {
     fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
         f.debug_struct("Node")
             .field("header", &{self.header})
             .field("key_ptrs", &&self.key_ptrs)
             .finish()
     }
}

#[derive(Clone, Copy, Debug, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct KeyPtr {
    pub key: DiskKey,
    pub block_ptr: u64_le,
    pub generation: u64_le,
}

impl Node {
    pub fn parse<'a>(header: &'a Header, bytes: &'a [u8]) -> Option<&'a Self> {
        let item_count = header.item_count.get();

        unsafe {
            if bytes.len() < mem::size_of::<Header>() {
                return None;
            }

            if mem::size_of::<Header>() + item_count as usize * mem::size_of::<KeyPtr>() >= bytes.len() {
                return None;
            }

            let begin = bytes.as_ptr() as *const KeyPtr;
            let len = item_count as usize;

            Some(&*(std::slice::from_raw_parts(begin, len) as *const [KeyPtr] as *const Self))
        }
    }
}

pub struct Leaf {
    pub header: Header,
    pub data: [u8],
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct Item {
    pub key: DiskKey,
    pub offset: u32_le,
    pub size: u32_le,
}

#[derive(Debug)]
pub enum Value<'a, B: ByteSlice = &'a [u8]> {
    BlockGroupItem(LayoutVerified<B, BlockGroupItem>),
    Chunk(&'a ChunkItem),
    Device(LayoutVerified<B, DevItem>),
    DevExtent(LayoutVerified<B, DevExtent>),
    DirIndex(LayoutVerified<B, DirItem>),
    DirItem(LayoutVerified<B, DirItem>),
    ExtentCsum(&'a CsumItem),
    ExtentData(&'a FileExtentItem),
    ExtentItem(LayoutVerified<B, ExtentItem>),
    InodeItem(LayoutVerified<B, InodeItem>),
    InodeRef(&'a InodeRef),
    MetadataItem(LayoutVerified<B, ExtentItem>),
    PersistentItem(LayoutVerified<B, DevStatsItem>), // NOTE: Currently the only persistent item is the dev stats item.
    Root(LayoutVerified<B, RootItem>),
    RootRef(LayoutVerified<B, RootRef>),
    RootBackref(LayoutVerified<B, RootRef>),
    UuidSubvol(&'a UuidItem),
    UuidReceivedSubvol(&'a UuidItem),
    XattrItem(LayoutVerified<B, DirItem>),
    Unknown,
}

impl<'a, B: ByteSlice> Value<'a, B> {
    pub fn as_root_item(&self) -> Option<LayoutVerified<B, RootItem>> {
        match self {
            &Self::Root(item) => Some(item),
            _ => None,
        }
    }
    pub fn as_chunk_item(&self) -> Option<&'a ChunkItem> {
        match self {
            &Self::Chunk(item) => Some(item),
            _ => None,
        }
    }
    pub fn is_chunk_item(&self) -> bool {
        match self {
            &Self::Chunk(_) => true,
            _ => false,
        }
    }
}

impl Leaf {
    /*pub fn parse(checksum_type: ChecksumType, header: Header, bytes: &[u8]) -> Self {
        let pairs = (0..header.item_count as usize)
            .map(|i| Item::parse(&bytes[i * 25..(i + 1) * 25]))
            .map(|item: Item| {
                let value = {
                    let value_bytes =
                        &bytes[item.offset as usize..item.offset as usize + item.size as usize];
                    match item.key.ty {
                        DiskKeyType::BlockGroupItem => {
                            Value::BlockGroupItem(BlockGroupItem::parse(value_bytes))
                        }
                        DiskKeyType::ChunkItem => Value::Chunk(ChunkItem::parse(value_bytes)),
                        DiskKeyType::DevExtent => Value::DevExtent(DevExtent::parse(value_bytes)),
                        DiskKeyType::DevItem => Value::Device(DevItem::parse(value_bytes)),
                        DiskKeyType::DirIndex => Value::DirIndex(DirItem::parse(value_bytes)),
                        DiskKeyType::DirItem => Value::DirItem(DirItem::parse(value_bytes)),
                        DiskKeyType::ExtentCsum => {
                            Value::ExtentCsum(CsumItem::parse(checksum_type, value_bytes))
                        }
                        DiskKeyType::ExtentData => {
                            Value::ExtentData(FileExtentItem::parse(value_bytes))
                        }
                        DiskKeyType::ExtentItem => {
                            Value::ExtentItem(ExtentItem::parse(value_bytes))
                        }
                        DiskKeyType::InodeItem => Value::InodeItem(InodeItem::parse(value_bytes)),
                        DiskKeyType::InodeRef => Value::InodeRef(InodeRef::parse(value_bytes)),
                        DiskKeyType::MetadataItem => {
                            Value::MetadataItem(ExtentItem::parse(value_bytes))
                        }
                        DiskKeyType::PersistentItem => {
                            Value::PersistentItem(DevStatsItem::parse(value_bytes))
                        }
                        DiskKeyType::RootBackref => Value::RootBackref(RootRef::parse(value_bytes)),
                        DiskKeyType::RootItem => Value::Root(RootItem::parse(value_bytes)),
                        DiskKeyType::RootRef => Value::RootRef(RootRef::parse(value_bytes)),
                        DiskKeyType::UuidSubvol => Value::UuidSubvol(UuidItem::parse(value_bytes)),
                        DiskKeyType::UuidReceivedSubvol => {
                            Value::UuidReceivedSubvol(UuidItem::parse(value_bytes))
                        }
                        DiskKeyType::XattrItem => Value::XattrItem(DirItem::parse(value_bytes)),
                        DiskKeyType::Unknown => Value::Unknown,
                        other => unimplemented!("{:?}", other),
                    }
                };
                (item, value)
            })
            .collect::<Vec<_>>();

        Self { header, pairs }
    }
    */
    pub fn parse<'a>(checksum_ty: ChecksumType, header: &'a Header, bytes: &'a [u8]) -> Option<&'a Self> {
        unsafe {
            if bytes.len() < mem::size_of::<Header>() {
                return None;
            }

            let begin = bytes.as_ptr() as *const KeyPtr;
            let len = bytes.len() - mem::size_of::<Header>();

            Some(&*(std::slice::from_raw_parts(begin, len) as *const [KeyPtr] as *const Self))
        }
    }
    pub fn iter<'a>(&'a self) -> impl Iterator<Item = (LayoutVerified<&'a [u8], Item>, Value<'a>)> + 'a {
        (0..self.header.item_count.get() as usize)
            .map_while(|i| LayoutVerified::new_unaligned(&self.data[i * mem::size_of::<Item>()..(i + 1) * mem::size_of::<Item>()]))
            .map_while(|item: LayoutVerified<&'a [u8], Item>| -> Option<_> {
                let value_bytes = &self.data[item.offset.get() as usize..item.offset.get() as usize + item.size.get() as usize];

                let ty = match item.key.ty() {
                    Some(ty) => ty,
                    None => return Some(Value::Unknown), // TODO: Warn
                };

                let value: Value::<'a> = match ty {
                    DiskKeyType::BlockGroupItem => Value::BlockGroupItem(LayoutVerified::new_unaligned(value_bytes)?),
                    DiskKeyType::ChunkItem => Value::Chunk(ChunkItem::parse(value_bytes)?),
                    DiskKeyType::DevExtent => Value::DevExtent(LayoutVerified::new_unaligned(value_bytes)?),
                    DiskKeyType::DevItem => Value::Device(LayoutVerified::new_unaligned(value_bytes)?),
                    DiskKeyType::DirIndex => Value::DirIndex(LayoutVerified::new_unaligned(value_bytes)?),
                    DiskKeyType::DirItem => Value::DirItem(LayoutVerified::new_unaligned(value_bytes)?),
                    DiskKeyType::ExtentCsum => Value::ExtentCsum(CsumItem::parse(value_bytes)),
                    DiskKeyType::ExtentData => Value::ExtentData(FileExtentItem::parse(value_bytes)?),
                    DiskKeyType::ExtentItem => Value::ExtentItem(LayoutVerified::new_unaligned(value_bytes)?),
                    DiskKeyType::InodeItem => Value::InodeItem(LayoutVerified::new_unaligned(value_bytes)?),
                    DiskKeyType::InodeRef => Value::InodeRef(InodeRef::parse(value_bytes)?),
                    DiskKeyType::MetadataItem => Value::MetadataItem(LayoutVerified::new_unaligned(value_bytes)?),
                    DiskKeyType::PersistentItem => Value::PersistentItem(LayoutVerified::new_unaligned(value_bytes)?),
                    DiskKeyType::RootBackref => Value::RootBackref(LayoutVerified::new_unaligned(value_bytes)?),
                    DiskKeyType::RootItem => Value::Root(LayoutVerified::new_unaligned(value_bytes)?),
                    DiskKeyType::RootRef => Value::RootRef(LayoutVerified::new_unaligned(value_bytes)?),
                    DiskKeyType::UuidSubvol => Value::UuidSubvol(UuidItem::parse(value_bytes)),
                    DiskKeyType::UuidReceivedSubvol => Value::UuidReceivedSubvol(UuidItem::parse(value_bytes)),
                    DiskKeyType::XattrItem => Value::XattrItem(LayoutVerified::new_unaligned(value_bytes)?),
                };
                Some((item, value))
            })
    }
}
impl fmt::Debug for Leaf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Pairs<'a>(&'a Leaf);

        impl<'a> fmt::Debug for Pairs<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_list()
                    .entries(self.0.iter())
                    .finish()
            }
        }

        f.debug_struct("Leaf")
            .field("header", &{self.header})
            .field("pairs", &Pairs(self))
            .finish()
    }
}

#[derive(Clone, Copy, Debug)]
pub enum Tree<'a> {
    Internal(&'a Node),
    Leaf(&'a Leaf),
}

fn always_equal<'b>(_: &'b DiskKey, _: &'b DiskKey) -> Ordering {
    Ordering::Equal
}

impl<'a> Tree<'a> {
    pub fn as_leaf(&self) -> Option<&Leaf> {
        match self {
            Self::Leaf(l) => Some(l),
            Self::Internal(_) => None,
        }
    }
    pub fn as_internal(&self) -> Option<&Node> {
        match self {
            Self::Internal(n) => Some(n),
            Self::Leaf(_) => None,
        }
    }
    pub fn header(&self) -> &Header {
        match self {
            Self::Internal(internal) => &internal.header,
            Self::Leaf(leaf) => &leaf.header,
        }
    }
    pub fn parse(checksum_type: ChecksumType, bytes: &'a [u8]) -> Result<&'a Self, InvalidChecksum> {
        let header = Header::parse(checksum_type, &bytes)?;

        Ok(match header.level {
            0 => Self::Leaf(Leaf::parse(checksum_type, header, &bytes[mem::size_of::<Header>()..])),
            _ => Self::Internal(Node::parse(header, &bytes[mem::size_of::<Header>()..])),
        })
    }
    pub fn load<D: fal::DeviceRo>(
        device: &mut D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        addr: u64,
    ) -> owning_ref::BoxRef<[u8]> {
        let block = filesystem::read_node(device, superblock, chunk_map, addr);
        Self::parse(superblock.checksum_ty(), &block)
    }
    fn get_generic<D: fal::Device>(
        &self,
        device: &mut D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        key: &DiskKey,
        compare: fn(k1: &DiskKey, k2: &DiskKey) -> Ordering,
    ) -> Option<((DiskKey, Value), Path)> {
        let mut path = Vec::with_capacity(self.header().level as usize);

        path.push((Cow::Borrowed(self), 0));

        let item_index = loop {
            match path.last().map(|(tree, _)| tree).unwrap().borrow() {
                Self::Leaf(leaf) => {
                    assert_eq!(leaf.header.level, 0);

                    // Leaf nodes are guaranteed to contain the key and the value, if they exist.
                    break leaf
                        .pairs
                        .iter()
                        .position(|(item, _)| compare(&item.key, key) == Ordering::Equal);
                }
                Self::Internal(internal) => {
                    assert!(internal.header.level > 0);

                    // Find the closest key ptr. If the key that we are searching for is larger than the
                    // closest key ptr, we search that tree, and so on.
                    let (i, key_ptr) = match internal
                        .key_ptrs
                        .iter()
                        .enumerate()
                        .filter(|(_, key_ptr)| compare(&key_ptr.key, key) != Ordering::Greater)
                        .max_by(|(_, key_ptr1), (_, key_ptr2)| {
                            compare(&key_ptr1.key, &key_ptr2.key)
                        }) {
                        Some(ptr) => ptr,
                        None => return None,
                    };

                    let subtree = Self::load(device, superblock, chunk_map, key_ptr.block_ptr);
                    path.push((Cow::Owned(subtree), i));
                    continue;
                }
            }
        };
        item_index.map(|i| {
            path.last_mut().unwrap().1 = i;
            let (key, ref value) = path
                .last()
                .map(|(node, _)| node.as_leaf().unwrap())
                .unwrap()
                .pairs[i];
            ((key.key, value.clone()), path)
        })
    }
    pub fn get<D: fal::Device>(
        &self,
        device: &mut D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        key: &DiskKey,
    ) -> Option<Value> {
        self.get_with_path(device, superblock, chunk_map, key)
            .map(|(value, _)| value)
    }
    pub fn get_with_path<D: fal::Device>(
        &self,
        device: &mut D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        key: &DiskKey,
    ) -> Option<(Value, Path)> {
        self.get_generic(device, superblock, chunk_map, key, Ord::cmp)
            .map(|((_, value), path)| (value, path))
    }
    pub fn pairs<'a, D: fal::Device>(
        &'a self,
        device: &'a mut D,
        superblock: &'a Superblock,
        chunk_map: &'a ChunkMap,
    ) -> Pairs<'a, D> {
        Pairs {
            chunk_map,
            device,
            superblock,
            path: vec![(Cow::Borrowed(self), 0)],
            previous_key: None,
            function: Box::new(always_equal),
        }
    }
    /// Find similar pairs, i.e. pairs which key have the same oid and type, but possibly different offsets.
    pub fn similar_pairs<'a, D: fal::Device>(
        &'a self,
        device: &'a mut D,
        superblock: &'a Superblock,
        chunk_map: &'a ChunkMap,
        partial_key: &DiskKey,
    ) -> Option<Pairs<'a, D>> {
        let (_, path) = match self.get_generic(
            device,
            superblock,
            chunk_map,
            partial_key,
            DiskKey::compare_without_offset,
        ) {
            Some(p) => p,
            None => return None,
        };

        Some(Pairs {
            chunk_map,
            device,
            superblock,
            path,
            previous_key: None,
            function: Box::new(DiskKey::compare_without_offset),
        })
    }
}

/// Stack-based tree traversal iterator
pub struct Pairs<'a, D: fal::Device> {
    device: &'a mut D,
    superblock: &'a Superblock,
    chunk_map: &'a ChunkMap,
    path: Vec<(Cow<'a, Tree>, usize)>,

    function: Box<for<'b> fn(k1: &'b DiskKey, k2: &'b DiskKey) -> Ordering>,
    previous_key: Option<DiskKey>,
}

impl<'a, D: fal::Device> Iterator for Pairs<'a, D> {
    type Item = (DiskKey, Value);

    fn next(&mut self) -> Option<Self::Item> {
        // Only used to escape the borrow checker while retaining logic.
        enum Action {
            ClimbUp(KeyPtr),
            ClimbDown,
        };

        let (current_tree, current_index) = match self.path.last_mut() {
            Some(l) => l,
            None => return None,
        };

        let action = match (*current_tree).borrow() {
            Tree::Leaf(leaf) => match leaf.pairs.get(*current_index) {
                Some((item, value)) => {
                    // If there is a pair available, just yield it and continue.
                    if let Some(previous_key) = self.previous_key {
                        let function = &self.function;
                        if function(&previous_key, &item.key) != Ordering::Equal {
                            return None;
                        }
                        self.previous_key = Some(item.key);
                    }
                    *current_index += 1;
                    return Some((item.key, value.clone()));
                }
                None => {
                    // When there are no more elements in the current node, we need to go one node
                    // back, increase the index there and load a new leaf.
                    Action::ClimbDown
                }
            },
            Tree::Internal(node) => {
                match node.key_ptrs.get(*current_index) {
                    Some(&key_ptr) => {
                        // If there is a new undiscovered leaf node, we climb up the tree (closer
                        // to the leaves), and search it.
                        Action::ClimbUp(key_ptr)
                    }
                    None => {
                        // Otherwise, we climb down to the parent node, and continue searching
                        // there.
                        Action::ClimbDown
                    }
                }
            }
        };

        match action {
            Action::ClimbUp(key_ptr) => self.path.push((
                Cow::Owned(Tree::load(
                    self.device,
                    self.superblock,
                    self.chunk_map,
                    key_ptr.block_ptr,
                )),
                0,
            )),
            Action::ClimbDown => {
                self.path.pop();

                if let Some((_, i)) = self.path.last_mut() {
                    *i += 1;
                }
            }
        }

        // Recurse until another pair is found or until the entire tree has been traversed.
        self.next()
    }
}
