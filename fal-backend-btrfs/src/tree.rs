use std::borrow::Cow;

use core::{borrow::Borrow, cmp::Ordering, fmt, mem, ops};

use zerocopy::{AsBytes, ByteSlice, FromBytes, LayoutVerified, Unaligned};

use crate::{
    chunk_map::ChunkMap,
    filesystem,
    items::{
        BlockGroupItem, ChunkItem, CsumItem, DevExtent, DevItem, DevStatsItem, DirItem, ExtentItem,
        FileExtentItem, InodeItem, InodeRef, RootItem, RootRef, UuidItem,
    },
    superblock::{ChecksumType, Superblock},
    u32_le, u64_le, Checksum, DiskKey, DiskKeyType, InvalidChecksum, PackedUuid,
};

// TODO: Paths are small and smallvec / arrayvec should be used.

pub enum OwnedOrBorrowedTree<'a> {
    Owned(TreeOwned),
    Borrowed(Tree<'a>),
}

impl<'a> OwnedOrBorrowedTree<'a> {
    pub fn as_ref(&'a self) -> Tree<'a> {
        match self {
            &Self::Owned(ref owned) => owned.as_ref(),
            &Self::Borrowed(borrowed) => borrowed,
        }
    }
}

pub type Path<'a> = Vec<(OwnedOrBorrowedTree<'a>, usize)>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct Header {
    pub checksum: [u8; 32],
    pub fsid: PackedUuid,

    pub logical_addr: u64_le,
    pub flags: u64_le,

    pub chunk_tree_uuid: PackedUuid,
    pub generation: u64_le,
    pub owner: u64_le,
    pub item_count: u32_le,
    pub level: u8,
}

impl Header {
    pub fn parse<'a>(
        checksum_ty: ChecksumType,
        bytes: &'a [u8],
    ) -> Result<LayoutVerified<&'a [u8], Self>, InvalidChecksum> {
        let this = LayoutVerified::new_unaligned(&bytes[..mem::size_of::<Header>()]).unwrap();

        let stored_checksum = Checksum::parse(checksum_ty, &bytes[..32])
            .ok_or(InvalidChecksum::UnsupportedChecksum(checksum_ty))?;
        let checksum = Checksum::calculate(checksum_ty, &bytes[32..])
            .ok_or(InvalidChecksum::UnsupportedChecksum(checksum_ty))?;

        if stored_checksum != checksum {
            return Err(InvalidChecksum::Mismatch);
        }
        Ok(this)
    }
}

#[derive(Eq, Hash, PartialEq)]
pub struct Node {
    pub header: Header,
    pub key_ptrs: [KeyPtr],
}
impl fmt::Debug for Node {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Node")
            .field("header", &{ self.header })
            .field("key_ptrs", &&self.key_ptrs)
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, AsBytes, FromBytes, Unaligned)]
#[repr(packed)]
pub struct KeyPtr {
    pub key: DiskKey,
    pub block_ptr: u64_le,
    pub generation: u64_le,
}

impl Node {
    pub fn parse<'a>(header: &Header, bytes: &'a [u8]) -> Option<&'a Self> {
        let item_count = header.item_count.get();

        unsafe {
            if bytes.len() < mem::size_of::<Header>() {
                return None;
            }

            if mem::size_of::<Header>() + item_count as usize * mem::size_of::<KeyPtr>()
                >= bytes.len()
            {
                return None;
            }

            let begin = bytes.as_ptr() as *const KeyPtr;
            let len = item_count as usize;

            Some(&*(std::slice::from_raw_parts(begin, len) as *const [KeyPtr] as *const Self))
        }
    }
}

#[derive(Eq, Hash, PartialEq)]
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

#[derive(Clone, Debug)]
pub enum Value<'a> {
    BlockGroupItem(Cow<'a, BlockGroupItem>),
    Chunk(Cow<'a, ChunkItem>),
    Device(Cow<'a, DevItem>),
    DevExtent(Cow<'a, DevExtent>),
    DirIndex(Cow<'a, DirItem>),
    DirItem(Cow<'a, DirItem>),
    ExtentCsum(Cow<'a, CsumItem>),
    ExtentData(Cow<'a, FileExtentItem>),
    ExtentItem(Cow<'a, ExtentItem>),
    InodeItem(Cow<'a, InodeItem>),
    InodeRef(Cow<'a, InodeRef>),
    MetadataItem(Cow<'a, ExtentItem>),
    PersistentItem(Cow<'a, DevStatsItem>), // NOTE: Currently the only persistent item is the dev stats item.
    Root(Cow<'a, RootItem>),
    RootRef(Cow<'a, RootRef>),
    RootBackref(Cow<'a, RootRef>),
    UuidSubvol(Cow<'a, UuidItem>),
    UuidReceivedSubvol(Cow<'a, UuidItem>),
    XattrItem(Cow<'a, DirItem>),
    Unknown,
}
impl<'a> Value<'a> {
    pub fn as_root_item(&'a self) -> Option<&'a RootItem> {
        match self {
            &Self::Root(ref item) => Some(item.borrow()),
            _ => None,
        }
    }
    pub fn as_chunk_item(&'a self) -> Option<&'a ChunkItem> {
        match self {
            &Self::Chunk(ref item) => Some(item.borrow()),
            _ => None,
        }
    }
    pub fn is_chunk_item(&self) -> bool {
        match self {
            &Self::Chunk(_) => true,
            _ => false,
        }
    }
    pub fn to_static(self) -> Value<'static> {
        match self {
            Self::BlockGroupItem(i) => Value::BlockGroupItem(Cow::<'static>::Owned(i.into_owned())),
            Self::Chunk(i) => Value::Chunk(Cow::<'static>::Owned(i.into_owned())),
            Self::Device(i) => Value::Device(Cow::<'static>::Owned(i.into_owned())),
            Self::DevExtent(i) => Value::DevExtent(Cow::<'static>::Owned(i.into_owned())),
            Self::DirIndex(i) => Value::DirIndex(Cow::<'static>::Owned(i.into_owned())),
            Self::DirItem(i) => Value::DirIndex(Cow::<'static>::Owned(i.into_owned())),
            Self::ExtentCsum(i) => Value::ExtentCsum(Cow::<'static>::Owned(i.into_owned())),
            Self::ExtentData(i) => Value::ExtentData(Cow::<'static>::Owned(i.into_owned())),
            Self::ExtentItem(i) => Value::ExtentItem(Cow::<'static>::Owned(i.into_owned())),
            Self::InodeItem(i) => Value::InodeItem(Cow::<'static>::Owned(i.into_owned())),
            Self::InodeRef(i) => Value::InodeRef(Cow::<'static>::Owned(i.into_owned())),
            Self::MetadataItem(i) => Value::MetadataItem(Cow::<'static>::Owned(i.into_owned())),
            Self::PersistentItem(i) => Value::PersistentItem(Cow::<'static>::Owned(i.into_owned())),
            Self::Root(i) => Value::Root(Cow::<'static>::Owned(i.into_owned())),
            Self::RootRef(i) => Value::RootRef(Cow::<'static>::Owned(i.into_owned())),
            Self::RootBackref(i) => Value::RootBackref(Cow::<'static>::Owned(i.into_owned())),
            Self::UuidSubvol(i) => Value::UuidSubvol(Cow::<'static>::Owned(i.into_owned())),
            Self::UuidReceivedSubvol(i) => {
                Value::UuidReceivedSubvol(Cow::<'static>::Owned(i.into_owned()))
            }
            Self::XattrItem(i) => Value::XattrItem(Cow::<'static>::Owned(i.into_owned())),
            Self::Unknown => Value::Unknown,
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
    pub fn parse<'a>(bytes: &'a [u8]) -> Option<&'a Self> {
        unsafe {
            if bytes.len() < mem::size_of::<Header>() {
                return None;
            }

            let begin = bytes.as_ptr() as *const KeyPtr;
            let len = bytes.len() - mem::size_of::<Header>();

            Some(&*(std::slice::from_raw_parts(begin, len) as *const [KeyPtr] as *const Self))
        }
    }
    pub fn iter<'a>(
        &'a self,
    ) -> impl Iterator<Item = (LayoutVerified<&'a [u8], Item>, Value<'a>)> + 'a {
        (0..self.header.item_count.get() as usize)
            .map_while(move |i| {
                LayoutVerified::new_unaligned(
                    &self.data[i * mem::size_of::<Item>()..(i + 1) * mem::size_of::<Item>()],
                )
            })
            .map_while(
                move |item: LayoutVerified<&'a [u8], Item>| -> Option<(_, _)> {
                    let value_bytes = &self.data[item.offset.get() as usize
                        ..item.offset.get() as usize + item.size.get() as usize];

                    let ty = match item.key.ty() {
                        Some(ty) => ty,
                        None => return Some((item, Value::Unknown)), // TODO: Warn
                    };

                    let value: Value<'a> = match ty {
                        DiskKeyType::BlockGroupItem => Value::BlockGroupItem(Cow::Borrowed(
                            LayoutVerified::<_, BlockGroupItem>::new_unaligned(value_bytes)?
                                .into_ref(),
                        )),
                        DiskKeyType::ChunkItem => {
                            Value::Chunk(Cow::Borrowed(ChunkItem::parse(value_bytes)?))
                        }
                        DiskKeyType::DevExtent => Value::DevExtent(Cow::Borrowed(
                            LayoutVerified::<_, DevExtent>::new_unaligned(value_bytes)?.into_ref(),
                        )),
                        DiskKeyType::DevItem => Value::Device(Cow::Borrowed(
                            LayoutVerified::<_, DevItem>::new_unaligned(value_bytes)?.into_ref(),
                        )),
                        DiskKeyType::DirIndex => {
                            Value::DirIndex(Cow::Borrowed(DirItem::parse(value_bytes)?))
                        }
                        DiskKeyType::DirItem => {
                            Value::DirItem(Cow::Borrowed(DirItem::parse(value_bytes)?))
                        }
                        DiskKeyType::ExtentCsum => {
                            Value::ExtentCsum(Cow::Borrowed(CsumItem::parse(value_bytes)))
                        }
                        DiskKeyType::ExtentData => {
                            Value::ExtentData(Cow::Borrowed(FileExtentItem::parse(value_bytes)?))
                        }
                        DiskKeyType::ExtentItem => Value::ExtentItem(Cow::Borrowed(
                            LayoutVerified::<_, ExtentItem>::new_unaligned(value_bytes)?.into_ref(),
                        )),
                        DiskKeyType::InodeItem => Value::InodeItem(Cow::Borrowed(
                            LayoutVerified::<_, InodeItem>::new_unaligned(value_bytes)?.into_ref(),
                        )),
                        DiskKeyType::InodeRef => {
                            Value::InodeRef(Cow::Borrowed(InodeRef::parse(value_bytes)?))
                        }
                        DiskKeyType::MetadataItem => Value::MetadataItem(Cow::Borrowed(
                            LayoutVerified::<_, ExtentItem>::new_unaligned(value_bytes)?.into_ref(),
                        )),
                        DiskKeyType::PersistentItem => Value::PersistentItem(Cow::Borrowed(
                            LayoutVerified::<_, DevStatsItem>::new_unaligned(value_bytes)?
                                .into_ref(),
                        )),
                        DiskKeyType::RootBackref => Value::RootBackref(Cow::Borrowed(
                            LayoutVerified::<_, RootRef>::new_unaligned(value_bytes)?.into_ref(),
                        )),
                        DiskKeyType::RootItem => Value::Root(Cow::Borrowed(
                            LayoutVerified::<_, RootItem>::new_unaligned(value_bytes)?.into_ref(),
                        )),
                        DiskKeyType::RootRef => Value::RootRef(Cow::Borrowed(
                            LayoutVerified::<_, RootRef>::new_unaligned(value_bytes)?.into_ref(),
                        )),
                        DiskKeyType::UuidSubvol => {
                            Value::UuidSubvol(Cow::Borrowed(UuidItem::parse(value_bytes)))
                        }
                        DiskKeyType::UuidReceivedSubvol => {
                            Value::UuidReceivedSubvol(Cow::Borrowed(UuidItem::parse(value_bytes)))
                        }
                        DiskKeyType::XattrItem => {
                            Value::XattrItem(Cow::Borrowed(DirItem::parse(value_bytes)?))
                        }
                        DiskKeyType::Unknown => panic!(),
                        _ => Value::Unknown,
                    };
                    Some((item, value))
                },
            )
    }
}
impl fmt::Debug for Leaf {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct Pairs<'a>(&'a Leaf);

        impl<'a> fmt::Debug for Pairs<'a> {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                f.debug_list().entries(self.0.iter()).finish()
            }
        }

        f.debug_struct("Leaf")
            .field("header", &{ self.header })
            .field("pairs", &Pairs(self))
            .finish()
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub enum Tree<'a> {
    Internal(&'a Node),
    Leaf(&'a Leaf),
}
#[derive(Clone)]
pub struct TreeOwned {
    block: Box<[u8]>,
}
impl TreeOwned {
    pub fn as_ref<'a>(&'a self) -> Tree<'a> {
        let header = LayoutVerified::<&'a [u8], Header>::new_unaligned(
            &self.block[..mem::size_of::<Header>()],
        )
        .unwrap();
        Tree::parse_inner(&*header, &self.block)
    }
}
impl fmt::Debug for TreeOwned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.as_ref(), f)
    }
}

fn always_equal<'b>(_: &'b DiskKey, _: &'b DiskKey) -> Ordering {
    Ordering::Equal
}

impl<'a> Tree<'a> {
    pub fn as_leaf(self) -> Option<&'a Leaf> {
        match self {
            Self::Leaf(l) => Some(l),
            Self::Internal(_) => None,
        }
    }
    pub fn as_internal(self) -> Option<&'a Node> {
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
    fn parse_inner(header: &Header, bytes: &'a [u8]) -> Self {
        match header.level {
            0 => Self::Leaf(Leaf::parse(&bytes).unwrap()),
            _ => Self::Internal(Node::parse(header, &bytes).unwrap()),
        }
    }
    pub fn parse(checksum_type: ChecksumType, bytes: &'a [u8]) -> Result<Self, InvalidChecksum> {
        let header = Header::parse(checksum_type, &bytes)?;
        Ok(Self::parse_inner(&*header, bytes))
    }

    pub fn load<D: fal::DeviceRo>(
        device: &mut D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        addr: u64,
    ) -> Result<TreeOwned, InvalidChecksum> {
        let block = filesystem::read_node(device, superblock, chunk_map, addr);
        let _ = Header::parse(superblock.checksum_ty(), &block)?;
        Ok(TreeOwned { block })
    }
    fn get_generic<D: fal::Device>(
        &'a self,
        device: &mut D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        key: &DiskKey,
        compare: fn(k1: &DiskKey, k2: &DiskKey) -> Ordering,
    ) -> Option<((DiskKey, Value<'static>), Path<'a>)> {
        let mut path = Vec::with_capacity(self.header().level.into());
        path.push((OwnedOrBorrowedTree::Borrowed(*self), 0));

        let item_index: Option<usize> = loop {
            let (i, subtree) = match path.last().map(|(tree, _)| tree).unwrap().as_ref() {
                Tree::Leaf(leaf) => {
                    assert_eq!(leaf.header.level, 0);

                    // Leaf nodes are guaranteed to contain the key and the value, if they exist.
                    break leaf
                        .iter()
                        .position(|(item, _)| compare(&item.key, key) == Ordering::Equal);
                }
                Tree::Internal(internal) => {
                    assert!(internal.header.level > 0);

                    // Find the closest key ptr. If the key that we are searching for is larger than the
                    // closest key ptr, we search that tree, and so on.
                    let (i, block_ptr) = match internal
                        .key_ptrs
                        .iter()
                        .enumerate()
                        .filter(|(_, key_ptr)| compare(&key_ptr.key, key) != Ordering::Greater)
                        .max_by(|(_, key_ptr1), (_, key_ptr2)| {
                            compare(&key_ptr1.key, &key_ptr2.key)
                        }) {
                        Some((i, key_ptr)) => (i, key_ptr.block_ptr.get()),
                        None => return None,
                    };

                    (
                        i,
                        Self::load(device, superblock, chunk_map, block_ptr).unwrap(),
                    )
                }
            };
            path.push((OwnedOrBorrowedTree::Owned(subtree), i));
            continue;
        };
        let item_index = match item_index {
            Some(i) => i,
            None => return None,
        };
        path.last_mut().unwrap().1 = item_index;

        let (key, value) = path
            .last()
            .map(|(node, _)| node.as_ref().as_leaf().unwrap())
            .unwrap()
            .iter()
            .nth(item_index)
            .unwrap();
        Some(((key.key, value.to_static()), path))
    }
    pub fn get<D: fal::Device>(
        &'a self,
        device: &mut D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        key: &DiskKey,
    ) -> Option<Value<'a>> {
        self.get_with_path(device, superblock, chunk_map, key)
            .map(|(value, _)| value)
    }
    pub fn get_with_path<D: fal::Device>(
        &'a self,
        device: &mut D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        key: &DiskKey,
    ) -> Option<(Value<'a>, Path)> {
        self.get_generic(device, superblock, chunk_map, key, Ord::cmp)
            .map(|((_, value), path)| (value, path))
    }
    pub fn pairs<D: fal::Device>(
        &'a self,
        device: &'a mut D,
        superblock: &'a Superblock,
        chunk_map: &'a ChunkMap,
    ) -> Pairs<'a, D> {
        let path = vec![(OwnedOrBorrowedTree::Borrowed(*self), 0)];

        Pairs {
            chunk_map,
            device,
            superblock,
            path,
            previous_key: None,
            function: Box::new(always_equal),
        }
    }
    /// Find similar pairs, i.e. pairs which key have the same oid and type, but possibly different offsets.
    pub fn similar_pairs<D: fal::Device>(
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
    path: Path<'a>,

    function: Box<for<'b> fn(k1: &'b DiskKey, k2: &'b DiskKey) -> Ordering>,
    previous_key: Option<DiskKey>,
}

impl<'a, D: fal::Device> Iterator for Pairs<'a, D> {
    type Item = (DiskKey, Value<'static>);

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

        let action = match (*current_tree).as_ref() {
            Tree::Leaf(leaf) => match leaf.iter().nth(*current_index) {
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
                    return Some((item.key, value.to_static()));
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
                OwnedOrBorrowedTree::Owned(
                    Tree::load(
                        self.device,
                        self.superblock,
                        self.chunk_map,
                        key_ptr.block_ptr.get(),
                    )
                    .unwrap(),
                ),
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
