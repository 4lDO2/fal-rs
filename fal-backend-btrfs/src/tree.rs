use crate::{
    Checksum, DiskKey, DiskKeyType,
    chunk_map::ChunkMap,
    filesystem,
    items::{BlockGroupItem, ChunkItem, CsumItem, DevExtent, DevItem, DevStatsItem, DirItem, ExtentItem, FileExtentItem, InodeItem, InodeRef, RootItem, RootRef, UuidItem},
    superblock::{ChecksumType, Superblock}
};

use std::{
    borrow::{Borrow, Cow},
    cmp::Ordering,
};

use fal::{read_u8, read_u32, read_u64, read_uuid};

use uuid::Uuid;

// TODO: Paths are small and smallvec / arrayvec should be used.
type Path<'a> = Vec<(Cow<'a, Tree>, usize)>;

#[derive(Clone, Copy, Debug)]
pub struct Header {
    pub checksum: Checksum,
    pub fsid: Uuid,

    pub logical_addr: u64,
    pub flags: u64,

    pub chunk_tree_uuid: Uuid,
    pub generation: u64,
    pub owner: u64,
    pub item_count: u32,
    pub level: u8,
}

impl Header {
    pub const LEN: usize = 101;

    pub fn parse(checksum_type: ChecksumType, bytes: &[u8]) -> Self {
        let checksum_bytes = &bytes[..32];
        let checksum = Checksum::new(checksum_type, checksum_bytes);

        assert_eq!(checksum, Checksum::calculate(checksum_type, &bytes[32..]));

        let fsid = read_uuid(bytes, 32);
        
        let logical_addr = read_u64(bytes, 48);
        let flags = read_u64(bytes, 56);

        let chunk_tree_uuid = read_uuid(bytes, 64);
        let generation = read_u64(bytes, 80);
        let owner = read_u64(bytes, 88);
        let item_count = read_u32(bytes, 96);
        let level = read_u8(bytes, 100);

        Header {
            checksum,
            fsid,

            logical_addr,
            flags,

            chunk_tree_uuid,
            generation,
            owner,
            item_count,
            level,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Node {
    pub header: Header,
    pub key_ptrs: Vec<KeyPtr>,
}

#[derive(Clone, Copy, Debug)]
pub struct KeyPtr {
    pub key: DiskKey,
    pub block_ptr: u64,
    pub generation: u64,
}

impl KeyPtr {
    pub const LEN: usize = 33;
    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            key: DiskKey::parse(&bytes[..17]),
            block_ptr: read_u64(bytes, 17),
            generation: read_u64(bytes, 25),
        }
    }
}

impl Node {
    pub fn parse(header: Header, bytes: &[u8]) -> Self {
        let key_ptrs = (0..header.item_count as usize).map(|i| KeyPtr::parse(&bytes[i * 33 .. (i + 1) * 33])).collect();

        Self {
            header,
            key_ptrs,
        }
    }
}

#[derive(Clone, Debug)]
pub struct Leaf {
    pub header: Header,
    pub pairs: Vec<(Item, Value)>,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct Item {
    pub key: DiskKey,
    pub offset: u32,
    pub size: u32,
}

#[derive(Clone, Debug)]
pub enum Value {
    BlockGroupItem(BlockGroupItem),
    Chunk(ChunkItem),
    Device(DevItem),
    DevExtent(DevExtent),
    DirIndex(DirItem),
    DirItem(DirItem),
    ExtentCsum(CsumItem),
    ExtentData(FileExtentItem),
    ExtentItem(ExtentItem),
    InodeItem(InodeItem),
    InodeRef(InodeRef),
    MetadataItem(ExtentItem),
    PersistentItem(DevStatsItem), // NOTE: Currently the only persistent item is the dev stats item.
    Root(RootItem),
    RootRef(RootRef),
    RootBackref(RootRef),
    UuidSubvol(UuidItem),
    UuidReceivedSubvol(UuidItem),
    XattrItem(DirItem),
    Unknown,
}

impl Value {
    pub fn into_root_item(self) -> Option<RootItem> {
        match self {
            Self::Root(item) => Some(item),
            _ => None,
        }
    }
    pub fn into_chunk_item(self) -> Option<ChunkItem> {
        match self {
            Self::Chunk(item) => Some(item),
            _ => None,
        }
    }
    pub fn is_chunk_item(&self) -> bool {
        match self {
            Self::Chunk(_) => true,
            _ => false,
        }
    }
}

impl Item {
    pub const LEN: usize = 25;

    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            key: DiskKey::parse(&bytes[..17]),
            offset: read_u32(bytes, 17),
            size: read_u32(bytes, 21),
        }
    }
}

impl Leaf {
    pub fn parse(checksum_type: ChecksumType, header: Header, bytes: &[u8]) -> Self {
        let pairs = (0..header.item_count as usize).map(|i| Item::parse(&bytes[i * 25 .. (i + 1) * 25])).map(|item: Item| {
            let value = {
                let value_bytes = &bytes[item.offset as usize .. item.offset as usize + item.size as usize];
                match item.key.ty {
                    DiskKeyType::BlockGroupItem => Value::BlockGroupItem(BlockGroupItem::parse(value_bytes)),
                    DiskKeyType::ChunkItem => Value::Chunk(ChunkItem::parse(value_bytes)),
                    DiskKeyType::DevExtent => Value::DevExtent(DevExtent::parse(value_bytes)),
                    DiskKeyType::DevItem => Value::Device(DevItem::parse(value_bytes)),
                    DiskKeyType::DirIndex => Value::DirIndex(DirItem::parse(value_bytes)),
                    DiskKeyType::DirItem => Value::DirItem(DirItem::parse(value_bytes)),
                    DiskKeyType::ExtentCsum => Value::ExtentCsum(CsumItem::parse(checksum_type, value_bytes)),
                    DiskKeyType::ExtentData => Value::ExtentData(FileExtentItem::parse(value_bytes)),
                    DiskKeyType::ExtentItem => Value::ExtentItem(ExtentItem::parse(value_bytes)),
                    DiskKeyType::InodeItem => Value::InodeItem(InodeItem::parse(value_bytes)),
                    DiskKeyType::InodeRef => Value::InodeRef(InodeRef::parse(value_bytes)),
                    DiskKeyType::MetadataItem => Value::MetadataItem(ExtentItem::parse(value_bytes)),
                    DiskKeyType::PersistentItem => Value::PersistentItem(DevStatsItem::parse(value_bytes)),
                    DiskKeyType::RootBackref => Value::RootBackref(RootRef::parse(value_bytes)),
                    DiskKeyType::RootItem => Value::Root(RootItem::parse(value_bytes)),
                    DiskKeyType::RootRef => Value::RootRef(RootRef::parse(value_bytes)),
                    DiskKeyType::UuidSubvol => Value::UuidSubvol(UuidItem::parse(value_bytes)),
                    DiskKeyType::UuidReceivedSubvol => Value::UuidReceivedSubvol(UuidItem::parse(value_bytes)),
                    DiskKeyType::XattrItem => Value::XattrItem(DirItem::parse(value_bytes)),
                    DiskKeyType::Unknown => Value::Unknown,
                    other => unimplemented!("{:?}", other),
                }
            };
            (item, value)
        }).collect::<Vec<_>>();

        Self {
            header,
            pairs,
        }
    }
}

#[derive(Clone, Debug)]
pub enum Tree {
    Internal(Node),
    Leaf(Leaf),
}

fn always_equal<'b>(_: &'b DiskKey, _: &'b DiskKey) -> Ordering {
    Ordering::Equal
}

impl Tree {
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
    pub fn parse(checksum_type: ChecksumType, bytes: &[u8]) -> Self {
        let header = Header::parse(checksum_type, &bytes);

        match header.level {
            0 => Self::Leaf(Leaf::parse(checksum_type, header, &bytes[Header::LEN..])),
            _ => Self::Internal(Node::parse(header, &bytes[Header::LEN..])),
        }
    }
    pub fn load<D: fal::Device>(device: &mut D, superblock: &Superblock, chunk_map: &ChunkMap, addr: u64) -> Self {
        let block = filesystem::read_node(device, superblock, chunk_map, addr);
        Self::parse(superblock.checksum_type, &block)
    }
    fn get_generic<D: fal::Device>(&self, device: &mut D, superblock: &Superblock, chunk_map: &ChunkMap, key: &DiskKey, compare: fn(k1: &DiskKey, k2: &DiskKey) -> Ordering) -> Option<((DiskKey, Value), Path)> {
        let mut path = Vec::with_capacity(self.header().level as usize);

        path.push((Cow::Borrowed(self), 0));

        let item_index = loop {
            match path.last().map(|(tree, _)| tree).unwrap().borrow() {
                Self::Leaf(leaf) => {
                    assert_eq!(leaf.header.level, 0);

                    // Leaf nodes are guaranteed to contain the key and the value, if they exist.
                    break leaf.pairs.iter().position(|(item, _)| compare(&item.key, key) == Ordering::Equal);
                }
                Self::Internal(internal) => {
                    assert!(internal.header.level > 0);

                    // Find the closest key ptr. If the key that we are searching for is larger than the
                    // closest key ptr, we search that tree, and so on.
                    let (i, key_ptr) = match internal.key_ptrs.iter().enumerate().filter(|(_, key_ptr)| compare(&key_ptr.key, key) != Ordering::Greater).max_by(|(_, key_ptr1), (_, key_ptr2)| compare(&key_ptr1.key, &key_ptr2.key)) {
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
            let (key, ref value) = path.last().map(|(node, _)| node.as_leaf().unwrap()).unwrap().pairs[i];
            ((key.key, value.clone()), path)
        })
    }
    pub fn get<D: fal::Device>(&self, device: &mut D, superblock: &Superblock, chunk_map: &ChunkMap, key: &DiskKey) -> Option<Value> {
        self.get_with_path(device, superblock, chunk_map, key).map(|(value, _)| value)
    }
    pub fn get_with_path<D: fal::Device>(&self, device: &mut D, superblock: &Superblock, chunk_map: &ChunkMap, key: &DiskKey) -> Option<(Value, Path)> {
        self.get_generic(device, superblock, chunk_map, key, Ord::cmp).map(|((_, value), path)| (value, path))
    }
    pub fn pairs<'a, D: fal::Device>(&'a self, device: &'a mut D, superblock: &'a Superblock, chunk_map: &'a ChunkMap) -> Pairs<'a, D> {
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
    pub fn similar_pairs<'a, D: fal::Device>(&'a self, device: &'a mut D, superblock: &'a Superblock, chunk_map: &'a ChunkMap, partial_key: &DiskKey) -> Option<Pairs<'a, D>> {
        let (_, path) = match self.get_generic(device, superblock, chunk_map, partial_key, DiskKey::compare_without_offset) {
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
                    *current_index += 1;
                    if let Some(previous_key) = self.previous_key {
                        let function = &self.function;
                        if function(&previous_key, &item.key) != Ordering::Equal {
                            return None
                        }
                        self.previous_key = Some(item.key);
                    }
                    return Some((item.key, value.clone()))
                }
                None => {
                    // When there are no more elements in the current node, we need to go one node
                    // back, increase the index there and load a new leaf.
                    Action::ClimbDown
                }
            }
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
            Action::ClimbUp(key_ptr) => self.path.push((Cow::Owned(Tree::load(self.device, self.superblock, self.chunk_map, key_ptr.block_ptr)), 0)),
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
