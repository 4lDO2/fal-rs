use core::{borrow::Borrow, cmp::Ordering, fmt, mem};

use zerocopy::{AsBytes, FromBytes, LayoutVerified, Unaligned};

use crate::{
    chunk_map::ChunkMap,
    filesystem,
    items::{
        BlockGroupItem, ChunkItem, CsumItem, DevExtent, DevItem, DevStatsItem, DirItem,
        ExtentItemFull, ExtentItemFullWrapper, FileExtentItem, InodeItem, InodeRef, RootItem,
        RootRef, UuidItem,
    },
    superblock::{ChecksumType, IncompatFlags, Superblock},
    u32_le, u64_le, Checksum, DiskKey, DiskKeyType, InvalidChecksum, PackedUuid,
};

// TODO: Paths are small and smallvec / arrayvec should be used.

/// A representation of data either being borrowed or owned, very similar to std's `Cow`, but being
/// co-variant.
#[derive(Copy, Debug)]
pub enum Cow<'a, O, B: ?Sized + 'a = O> {
    Borrowed(&'a B),
    Owned(O),
}

impl<'a, O, B: ?Sized + 'a> Clone for Cow<'a, O, B>
where
    B: ToOwned<Owned = O>
    O: Borrow<B>
{
    fn clone(&self) -> Self {
        Cow::Owned(self.into_owned())
    }
}

impl<'a, O, B: ?Sized + 'a> Cow<'a, O, B>
where
    B: ToOwned<Owned = O>,
    O: Borrow<B>,
{
    pub fn into_owned(self) -> O {
        match self {
            Self::Owned(o) => o,
            Self::Borrowed(b) => b.to_owned(),
        }
    }
}

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
    pub fn to_static(self) -> OwnedOrBorrowedTree<'static> {
        todo!()
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
    Chunk(Cow<'a, Box<ChunkItem>, ChunkItem>),
    Device(Cow<'a, DevItem>),
    DevExtent(Cow<'a, DevExtent>),
    DirIndex(Cow<'a, Box<ChunkItem>, DirItem>),
    DirItem(Cow<'a, Box<DirItem>, DirItem>),
    ExtentCsum(Cow<'a, Box<CsumItem>, CsumItem>),
    ExtentData(Cow<'a, Box<FileExtentItem>, FileExtentItem>),
    ExtentItem(ExtentItemFullWrapper<'a>),
    InodeItem(Cow<'a, InodeItem>),
    InodeRef(Cow<'a, Box<InodeRef>, InodeRef>),
    MetadataItem(ExtentItemFullWrapper<'a>),
    PersistentItem(Cow<'a, DevStatsItem>), // NOTE: Currently the only persistent item is the dev stats item.
    Root(Cow<'a, RootItem>),
    RootRef(Cow<'a, Box<RootRef>, RootRef>),
    RootBackref(Cow<'a, Box<RootRef>, RootRef>),
    UuidSubvol(Cow<'a, Box<UuidItem>, UuidItem>),
    UuidReceivedSubvol(Cow<'a, Box<UuidItem>, UuidItem>),
    XattrItem(Cow<'a, Box<DirItem>, DirItem>),
    Unknown,
}
impl<'a> Value<'a> {
    pub fn as_root_item(&'a self) -> Option<&'a RootItem> {
        match self {
            &Self::Root(ref item) => Some(item.borrow()),
            _ => None,
        }
    }
    pub fn into_inode_item(self) -> Option<InodeItem> {
        match self {
            Self::InodeItem(item) => Some(item.into_owned()),
            _ => None,
        }
    }
    pub fn into_dir_item(self) -> Option<Cow<'a, Box<DirItem>, DirItem>> {
        match self {
            Self::DirItem(item) => Some(item),
            _ => None,
        }
    }
    pub fn into_dir_index(self) -> Option<Cow<'a, Box<DirItem>, DirItem>> {
        match self {
            Self::DirIndex(item) => Some(item),
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
    /// Convert self to a Value which is guaranteed not to contain any borrowed data, cloning into
    /// a Box if necessary.
    pub fn to_static(self) -> Value<'static> {
        match self {
            Self::BlockGroupItem(i) => Value::BlockGroupItem(Cow::<'static>::Owned(i.into_owned())),
            Self::Chunk(i) => Value::Chunk(Cow::<'static>::Owned(i.into_owned())),
            Self::Device(i) => Value::Device(Cow::<'static>::Owned(i.into_owned())),
            Self::DevExtent(i) => Value::DevExtent(Cow::<'static>::Owned(i.into_owned())),
            Self::DirIndex(i) => Value::DirIndex(Cow::<'static>::Owned(i.into_owned())),
            Self::DirItem(i) => Value::DirItem(Cow::<'static>::Owned(i.into_owned())),
            Self::ExtentCsum(i) => Value::ExtentCsum(Cow::<'static>::Owned(i.into_owned())),
            Self::ExtentData(i) => Value::ExtentData(Cow::<'static>::Owned(i.into_owned())),
            Self::ExtentItem(i) => Value::ExtentItem(i.to_static()),
            Self::InodeItem(i) => Value::InodeItem(Cow::<'static>::Owned(i.into_owned())),
            Self::InodeRef(i) => Value::InodeRef(Cow::<'static>::Owned(i.into_owned())),
            Self::MetadataItem(i) => Value::MetadataItem(i.to_static()),
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
    /// Returns Some(self) if the value is borrowed, otherwise None.
    pub fn as_borrowed(self) -> Option<Self> {
        Some(match self {
            Self::BlockGroupItem(Cow::Borrowed(i)) => Self::BlockGroupItem(Cow::Borrowed(i)),
            Self::Chunk(Cow::Borrowed(i)) => Self::Chunk(Cow::Borrowed(i)),
            Self::Device(Cow::Borrowed(i)) => Self::Device(Cow::Borrowed(i)),
            Self::DevExtent(Cow::Borrowed(i)) => Self::DevExtent(Cow::Borrowed(i)),
            Self::DirIndex(Cow::Borrowed(i)) => Self::DirIndex(Cow::Borrowed(i)),
            Self::DirItem(Cow::Borrowed(i)) => Self::DirItem(Cow::Borrowed(i)),
            Self::ExtentCsum(Cow::Borrowed(i)) => Self::ExtentCsum(Cow::Borrowed(i)),
            Self::ExtentData(Cow::Borrowed(i)) => Self::ExtentData(Cow::Borrowed(i)),
            Self::ExtentItem(i) => Self::ExtentItem(i.as_borrowed()?),
            Self::InodeItem(Cow::Borrowed(i)) => Self::InodeItem(Cow::Borrowed(i)),
            Self::InodeRef(Cow::Borrowed(i)) => Self::InodeRef(Cow::Borrowed(i)),
            Self::MetadataItem(i) => Self::MetadataItem(i.as_borrowed()?),
            Self::PersistentItem(Cow::Borrowed(i)) => Self::PersistentItem(Cow::Borrowed(i)),
            Self::Root(Cow::Borrowed(i)) => Self::Root(Cow::Borrowed(i)),
            Self::RootRef(Cow::Borrowed(i)) => Self::RootRef(Cow::Borrowed(i)),
            Self::RootBackref(Cow::Borrowed(i)) => Self::RootBackref(Cow::Borrowed(i)),
            Self::UuidSubvol(Cow::Borrowed(i)) => Self::UuidSubvol(Cow::Borrowed(i)),
            Self::UuidReceivedSubvol(Cow::Borrowed(i)) => {
                Self::UuidReceivedSubvol(Cow::Borrowed(i))
            }
            Self::XattrItem(Cow::Borrowed(i)) => Self::XattrItem(Cow::Borrowed(i)),
            Self::Unknown => Value::Unknown,
            _ => return None,
        })
    }
    /// Returns a borrowed value for self ('b), or None if the value was already borrowed.
    pub fn owned_as_borrowed<'b>(&'b self) -> Option<Value<'b>> {
        Some(match self {
            &Self::BlockGroupItem(Cow::Owned(ref i)) => Value::BlockGroupItem(Cow::<'b>::Borrowed(i)),
            &Self::Chunk(Cow::Owned(ref i)) => Value::Chunk(Cow::<'b>::Borrowed(i)),
            &Self::Device(Cow::Owned(ref i)) => Value::Device(Cow::<'b>::Borrowed(i)),
            &Self::DevExtent(Cow::Owned(ref i)) => Value::DevExtent(Cow::<'b>::Borrowed(i)),
            &Self::DirIndex(Cow::Owned(ref i)) => Value::DirIndex(Cow::<'b>::Borrowed(i)),
            &Self::DirItem(Cow::Owned(ref i)) => Value::DirItem(Cow::<'b>::Borrowed(i)),
            &Self::ExtentCsum(Cow::Owned(ref i)) => Value::ExtentCsum(Cow::<'b>::Borrowed(i)),
            &Self::ExtentData(Cow::Owned(ref i)) => Value::ExtentData(Cow::<'b>::Borrowed(i)),
            &Self::ExtentItem(ref i) => Value::ExtentItem(i.owned_as_borrowed()?),
            &Self::InodeItem(Cow::Owned(ref i)) => Value::InodeItem(Cow::<'b>::Borrowed(i)),
            &Self::InodeRef(Cow::Owned(ref i)) => Value::InodeRef(Cow::<'b>::Borrowed(i)),
            &Self::MetadataItem(ref i) => Value::MetadataItem(i.owned_as_borrowed()?),
            &Self::PersistentItem(Cow::Owned(ref i)) => Value::PersistentItem(Cow::<'b>::Borrowed(i)),
            &Self::Root(Cow::Owned(ref i)) => Value::Root(Cow::<'b>::Borrowed(i)),
            &Self::RootRef(Cow::Owned(ref i)) => Value::RootRef(Cow::<'b>::Borrowed(i)),
            &Self::RootBackref(Cow::Owned(ref i)) => Value::RootBackref(Cow::<'b>::Borrowed(i)),
            &Self::UuidSubvol(Cow::Owned(ref i)) => Value::UuidSubvol(Cow::<'b>::Borrowed(i)),
            &Self::UuidReceivedSubvol(Cow::Owned(ref i)) => {
                Value::UuidReceivedSubvol(Cow::<'b>::Borrowed(i))
            }
            &Self::XattrItem(Cow::Owned(ref i)) => Value::XattrItem(Cow::<'b>::Borrowed(i)),
            &Self::Unknown => Value::Unknown,
            _ => return None,
        })
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
        incompat_flags: IncompatFlags,
    ) -> impl Iterator<Item = (LayoutVerified<&'a [u8], Item>, Value<'a>)> + 'a {
        (0..self.header.item_count.get() as usize)
            .map_while(move |i| {
                LayoutVerified::new_unaligned(
                    &self.data[i * mem::size_of::<Item>()..(i + 1) * mem::size_of::<Item>()],
                )
            })
            .map_while(
                move |item: LayoutVerified<&'a [u8], Item>| -> Option<(_, _)> {
                    // Sometimes, parsing may fail. For now we print out dbg statements where there
                    // so that nothing is silently discarded.
                    macro_rules! dbg_none ( ( $value:expr ) => {{ if $value.is_none() { dbg!(); $value } else { $value } }} );

                    let value_bytes = &self.data[item.offset.get() as usize
                        ..item.offset.get() as usize + item.size.get() as usize];

                    let ty = match item.key.ty() {
                        Some(ty) => ty,
                        None => return Some((item, Value::Unknown)), // TODO: Warn
                    };

                    let value: Value<'a> = match ty {
                        DiskKeyType::BlockGroupItem => Value::BlockGroupItem(Cow::Borrowed(
                            dbg_none!(LayoutVerified::<_, BlockGroupItem>::new_unaligned(value_bytes))?
                                .into_ref(),
                        )),
                        DiskKeyType::ChunkItem => {
                            Value::Chunk(Cow::Borrowed(dbg_none!(ChunkItem::parse(value_bytes))?))
                        }
                        DiskKeyType::DevExtent => Value::DevExtent(Cow::Borrowed(
                            dbg_none!(LayoutVerified::<_, DevExtent>::new_unaligned(value_bytes))?.into_ref(),
                        )),
                        DiskKeyType::DevItem => Value::Device(Cow::Borrowed(
                            dbg_none!(LayoutVerified::<_, DevItem>::new_unaligned(value_bytes))?.into_ref(),
                        )),
                        DiskKeyType::DirIndex => {
                            Value::DirIndex(Cow::Borrowed(dbg_none!(DirItem::parse(value_bytes))?))
                        }
                        DiskKeyType::DirItem => {
                            Value::DirItem(Cow::Borrowed(dbg_none!(DirItem::parse(value_bytes))?))
                        }
                        DiskKeyType::ExtentCsum => {
                            Value::ExtentCsum(Cow::Borrowed(CsumItem::parse(value_bytes)))
                        }
                        DiskKeyType::ExtentData => {
                            Value::ExtentData(Cow::Borrowed(dbg_none!(FileExtentItem::parse(value_bytes))?))
                        }
                        DiskKeyType::ExtentItem => Value::ExtentItem(ExtentItemFullWrapper(
                            Cow::Borrowed(dbg_none!(ExtentItemFull::parse(value_bytes))?),
                            incompat_flags,
                        )),
                        DiskKeyType::InodeItem => Value::InodeItem(Cow::Borrowed(
                            dbg_none!(LayoutVerified::<_, InodeItem>::new_unaligned(value_bytes))?.into_ref(),
                        )),
                        DiskKeyType::InodeRef => {
                            Value::InodeRef(Cow::Borrowed(dbg_none!(InodeRef::parse(value_bytes))?))
                        }
                        DiskKeyType::MetadataItem => Value::MetadataItem(ExtentItemFullWrapper(
                            Cow::Borrowed(dbg_none!(ExtentItemFull::parse(value_bytes))?),
                            incompat_flags,
                        )),
                        DiskKeyType::PersistentItem => Value::PersistentItem(Cow::Borrowed(
                            dbg_none!(LayoutVerified::<_, DevStatsItem>::new_unaligned(value_bytes))?
                                .into_ref(),
                        )),
                        DiskKeyType::RootBackref => Value::RootBackref(Cow::Borrowed(
                            dbg_none!(RootRef::parse(value_bytes))?,
                        )),
                        DiskKeyType::RootItem => Value::Root(Cow::Borrowed(
                            dbg_none!(LayoutVerified::<_, RootItem>::new_unaligned(value_bytes))?.into_ref(),
                        )),
                        DiskKeyType::RootRef => Value::RootRef(Cow::Borrowed(
                            dbg_none!(RootRef::parse(value_bytes))?,
                        )),
                        DiskKeyType::UuidSubvol => {
                            Value::UuidSubvol(Cow::Borrowed(UuidItem::parse(value_bytes)))
                        }
                        DiskKeyType::UuidReceivedSubvol => {
                            Value::UuidReceivedSubvol(Cow::Borrowed(UuidItem::parse(value_bytes)))
                        }
                        DiskKeyType::XattrItem => {
                            Value::XattrItem(Cow::Borrowed(dbg_none!(DirItem::parse(value_bytes))?))
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
                // FIXME
                f.debug_list()
                    .entries(self.0.iter(IncompatFlags::empty()))
                    .finish()
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
    pub fn is_leaf(self) -> bool {
        matches!(self, Self::Leaf(_))
    }
    pub fn is_internal(self) -> bool {
        matches!(self, Self::Internal(_))
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
        device: &D,
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
        device: &D,
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
                        .iter(superblock.incompat_flags().unwrap())
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
            .iter(superblock.incompat_flags().unwrap())
            .nth(item_index)
            .unwrap();
        Some(((key.key, value.to_static()), path))
    }
    pub fn get<D: fal::Device>(
        &'a self,
        device: &D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        key: &DiskKey,
    ) -> Option<Value<'a>> {
        self.get_with_path(device, superblock, chunk_map, key)
            .map(|(value, _)| value)
    }
    pub fn get_with_path<D: fal::Device>(
        &'a self,
        device: &D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        key: &DiskKey,
    ) -> Option<(Value<'a>, Path)> {
        self.get_generic(device, superblock, chunk_map, key, Ord::cmp)
            .map(|((_, value), path)| (value, path))
    }
    pub fn pairs(
        &'a self,
    ) -> PairsIterState<'a> {
        let path = vec![(OwnedOrBorrowedTree::Borrowed(*self), 0)];

        PairsIterState {
            path,
            previous_key: None,
            function: always_equal,
        }
    }
    /// Find similar pairs, i.e. pairs which key have the same oid and type, but possibly different offsets.
    pub fn similar_pairs<'b, D: fal::Device>(
        &'a self,
        device: &D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        partial_key: &DiskKey,
    ) -> Option<PairsIterState<'a>> {
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

        Some(PairsIterState {
            path,
            previous_key: None,
            function: DiskKey::compare_without_offset,
        })
    }
}

/// Stack-based tree traversal iterator.
pub struct Pairs<'a, 'b, D> {
    device: &'b D,
    superblock: &'b Superblock,
    chunk_map: &'b ChunkMap,
    state: &'a mut PairsIterState<'a>,
}

/// The state of the Pairs iterator.
pub struct PairsIterState<'a> {
    path: Path<'a>,
    function: for<'b> fn(k1: &'b DiskKey, k2: &'b DiskKey) -> Ordering,
    previous_key: Option<DiskKey>,
}

enum Either<A, B> { A(A), B(B) }

pub enum Step<'a, 'b> {
    Value(Option<(DiskKey, Either<Value<'a>, Value<'b>>)>),
    Step,
}

impl<'a> PairsIterState<'a> {
    /// Converts this state into a 'static state by making sure that the path is owned rather than
    /// borrowed.
    pub fn to_static(self) -> PairsIterState<'static> {
        PairsIterState {
            path: self.path.into_iter().map(|(tree, index)| (tree.to_static(), index)).collect(),
            function: self.function,
            previous_key: self.previous_key,
        }
    }
    pub fn step<'b, D: fal::DeviceRo>(&'b mut self, device: &D, superblock: &Superblock, chunk_map: &ChunkMap, count: usize) -> Step<'a, 'b> {
        let (current_index, is_leaf) = match self.path.last() {
            Some(&(ref l, current_index)) => (current_index, l.as_ref().is_leaf()),
            None => return Step::Value(None),
        };

        if is_leaf {
            if self.path.last().unwrap().0.as_ref().as_leaf().unwrap().iter(superblock.incompat_flags().unwrap()).nth(current_index).is_some() {
                let &(ref current_tree, current_index) = match self.path.last() {
                    Some(l) => l,
                    None => return Step::Value(None),
                };

                let (item, _) = current_tree.as_ref().as_leaf().unwrap().iter(superblock.incompat_flags().unwrap()).nth(current_index).unwrap();
                // If there is a pair available, just yield it and continue.
                if let Some(previous_key) = self.previous_key {
                    let function = &self.function;
                    if function(&previous_key, &item.key) != Ordering::Equal {
                        return Step::Value(None);
                    }
                    self.previous_key = Some(item.key);
                }
                let key = item.key;
                // TODO: Support stepping backwards
                self.path.last_mut().unwrap().1 += count;

                let &(ref current_tree, current_index) = match self.path.last() {
                    Some(l) => l,
                    None => return Step::Value(None),
                };

                // FIXME: Fix increment update nth
                return Step::Value(Some((key, match current_tree {
                    &OwnedOrBorrowedTree::Owned(ref owned) => if let Tree::Leaf(ref owned) = owned.as_ref() {
                        Either::B(owned.iter(superblock.incompat_flags().unwrap()).nth(current_index).unwrap().1)
                    } else {
                        unreachable!()
                    }
                    &OwnedOrBorrowedTree::Borrowed(Tree::Leaf(ref borrowed)) => {
                        Either::A(borrowed.iter(superblock.incompat_flags().unwrap()).nth(current_index).unwrap().1)
                    }
                    _ => unreachable!(),
                })));
            } else {
                // When there are no more elements in the current node, we need to go one node
                // back, increase the index there and load a new leaf.
                self.path.pop();

                if let Some((_, i)) = self.path.last_mut() {
                    *i += 1;
                }
                return Step::Step;
            }
        } else {
            if let Some(block_ptr) = self.path.last().unwrap().0.as_ref().as_internal().unwrap().key_ptrs.get(current_index).map(|key_ptr| key_ptr.block_ptr.get()) {
                // If there is a new undiscovered leaf node, we climb up the tree (closer
                // to the leaves), and search it.
                self.path.push((
                    OwnedOrBorrowedTree::Owned(
                        Tree::load(
                            device,
                            superblock,
                            chunk_map,
                            block_ptr,
                        )
                        .unwrap(),
                    ),
                    0,
                ));
            } else {
                // Otherwise, we climb down to the parent node, and continue searching
                // there.
                self.path.pop();

                if let Some((_, i)) = self.path.last_mut() {
                    *i += 1;
                }
            }
            return Step::Step;
        }
    }
    pub fn next_owned<'b, D: fal::DeviceRo>(&'b mut self, device: &D, superblock: &Superblock, chunk_map: &ChunkMap) -> Option<(DiskKey, Value<'static>)> {
        //loop {
            let step: Step<'a, 'b> = self.step(device, superblock, chunk_map, 1);

            match step {
                Step::Value(Some((key, value))) => match value {
                    Either::<Value<'a>, Value<'b>>::A(a) => return Some((key, Value::<'static>::to_static(a))),
                    Either::<Value<'a>, Value<'b>>::B(b) => return Some((key, Value::<'static>::to_static(b))),
                }
                Step::Value(None) => return None,
                Step::Step => panic!(),
            }
        //}
    }
    pub fn iter<'b, D>(&'a mut self, device: &'b D, superblock: &'b Superblock, chunk_map: &'b ChunkMap) -> Pairs<'a, 'b, D> {
        Pairs::assemble(self, device, superblock, chunk_map)
    }
    // TODO: previous
}

impl<'a, 'b, D> Pairs<'a, 'b, D> {
    /// Disassemble the iterator into its internal state.
    pub fn disassemble(self) -> &'a PairsIterState<'a> {
        self.state
    }
    pub fn assemble(state: &'a mut PairsIterState<'a>, device: &'b D, superblock: &'b Superblock, chunk_map: &'b ChunkMap) -> Self {
        Self {
            state,
            device,
            superblock,
            chunk_map,
        }
    }
}

impl<'a, 'b, D: fal::DeviceRo> Iterator for Pairs<'a, 'b, D> {
    type Item = (DiskKey, Value<'static>);

    fn next(&mut self) -> Option<Self::Item> {
        // XXX: If the reference to state wasn't mutable, we would have been able to yield
        // Value<'a>, but apparently just because the Pairs iterator owns the mutable reference,
        // the borrow checker won't even allow the reference to be created in the first place, even
        // though it's never returned and converted into 'static as soon as it's possible.
        self.state.next_owned(self.device, self.superblock, self.chunk_map)
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn cow_being_covariant() {
        use super::Cow as MyCow;

        fn useless_fn<'a, O, B>(p: MyCow<'static, O, B>) -> MyCow<'a, O, B> {
            p
        }

        use std::borrow::Cow as StdCow;

        fn useless_std_fn<'a, O>(p: StdCow<'static, O>) -> StdCow<'a, O> {
            p
        }
    }
}
