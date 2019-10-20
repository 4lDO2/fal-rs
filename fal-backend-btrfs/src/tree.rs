use crate::{
    Checksum, DiskKey, DiskKeyType,
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

impl Tree {
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
    pub fn load<D: fal::Device>(device: &mut D, superblock: &Superblock, addr: u64) -> Self {
        let block = filesystem::read_node_raw(device, superblock, addr);
        Self::parse(superblock.checksum_type, &block)
    }
    fn get_generic<D: fal::Device>(&self, device: &mut D, superblock: &Superblock, key: &DiskKey, compare: fn(k1: &DiskKey, k2: &DiskKey) -> Ordering) -> Option<Value> {
        match self {
            Self::Leaf(leaf) => {
                assert_eq!(leaf.header.level, 0);

                // Leaf nodes are guaranteed to contain the key and the value, if they exist.
                leaf.pairs.iter().find(|(item, _)| &item.key == key).cloned().map(|(_, value)| value)
            }
            Self::Internal(internal) => {
                assert!(internal.header.level > 0);

                // Find the closest key ptr. If the key that we are searching for is larger than the
                // closest key ptr, we search that tree, and so on.
                let key_ptr = match internal.key_ptrs.iter().filter(|key_ptr| &key_ptr.key <= key).max_by_key(|key_ptr| key_ptr.key) {
                    Some(ptr) => ptr,
                    None => return None,
                };

                let subtree = Self::load(device, superblock, key_ptr.block_ptr);
                subtree.get_generic(device, superblock, key, compare)
            }
        }
    }
    pub fn get<D: fal::Device>(&self, device: &mut D, superblock: &Superblock, key: &DiskKey) -> Option<Value> {
        self.get_generic(device, superblock, key, Ord::cmp)
    }
    pub fn pairs<'a, D: fal::Device>(&'a self, device: &'a mut D, superblock: &'a Superblock) -> Pairs<'a, D> {
        Pairs {
            device,
            superblock,
            path: vec![(Cow::Borrowed(self), 0)],
        }
    }
}

/// Stack-based tree traversal iterator
#[derive(Debug)]
pub struct Pairs<'a, D: fal::Device> {
    device: &'a mut D,
    superblock: &'a Superblock,
    path: Vec<(Cow<'a, Tree>, usize)>,
}

impl<'a, D: fal::Device> Iterator for Pairs<'a, D> {
    type Item = (Item, Value);

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
                    return Some((*item, value.clone()))
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
            Action::ClimbUp(key_ptr) => self.path.push((Cow::Owned(Tree::load(self.device, self.superblock, key_ptr.block_ptr)), 0)),
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
