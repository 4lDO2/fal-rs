use core::{borrow::Borrow, cmp::Ordering, fmt, mem, pin::Pin, slice};
use alloc::sync::Arc;
use alloc::borrow::Cow;

use zerocopy::{AsBytes, FromBytes, LayoutVerified, Unaligned};

use crate::{
    chunk_map::ChunkMap,
    filesystem,
    items::{
        BlockGroupItem, ChunkItem, CsumItem, DevExtent, DevItem, DevStatsItem, DirItem,
        ExtentItemFull, ExtentItemFullWrapperCow, ExtentItemFullWrapperRef, FileExtentItem, InodeItem, InodeRef, RootItem,
        RootRef, UuidItem,
    },
    superblock::{ChecksumType, IncompatFlags, Superblock},
    u32_le, u64_le, Checksum, DiskKey, DiskKeyType, InvalidChecksum, PackedUuid,
};

// TODO: Paths are small and smallvec / arrayvec should be used.

#[derive(Debug)]
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
        match self {
            Self::Owned(value) => OwnedOrBorrowedTree::Owned(value),
            Self::Borrowed(reference) => OwnedOrBorrowedTree::Owned(reference.to_tree_owned()),
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
    pub fn parse_without_checksum_verif<'a>(bytes: &'a [u8]) -> Option<LayoutVerified<&'a [u8], Self>> {
        LayoutVerified::new_unaligned(&bytes[..mem::size_of::<Header>()])
    }
    pub fn parse<'a>(
        checksum_ty: ChecksumType,
        bytes: &'a [u8],
    ) -> Result<LayoutVerified<&'a [u8], Self>, InvalidChecksum> {
        let this = Self::parse_without_checksum_verif(bytes).unwrap();

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
#[repr(packed)]
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

            Some(&*(slice::from_raw_parts(begin, len) as *const [KeyPtr] as *const Self))
        }
    }
    pub fn struct_size(&self) -> usize {
        mem::size_of::<Header>() + self.key_ptrs.len() * mem::size_of::<KeyPtr>()
    }
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const Self as *const u8, self.struct_size()) }
    }
}

#[derive(Eq, Hash, PartialEq)]
#[repr(packed)]
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
pub enum ValueCow<'a> {
    BlockGroupItem(Cow<'a, BlockGroupItem>),
    Chunk(Cow<'a, ChunkItem>),
    Device(Cow<'a, DevItem>),
    DevExtent(Cow<'a, DevExtent>),
    DirIndex(Cow<'a, DirItem>),
    DirItem(Cow<'a, DirItem>),
    ExtentCsum(Cow<'a, CsumItem>),
    ExtentData(Cow<'a, FileExtentItem>),
    ExtentItem(ExtentItemFullWrapperCow<'a>),
    InodeItem(Cow<'a, InodeItem>),
    InodeRef(Cow<'a, InodeRef>),
    MetadataItem(ExtentItemFullWrapperCow<'a>),
    PersistentItem(Cow<'a, DevStatsItem>), // NOTE: Currently the only persistent item is the dev stats item.
    Root(Cow<'a, RootItem>),
    RootRef(Cow<'a, RootRef>),
    RootBackref(Cow<'a, RootRef>),
    UuidSubvol(Cow<'a, UuidItem>),
    UuidReceivedSubvol(Cow<'a, UuidItem>),
    XattrItem(Cow<'a, DirItem>),
    Unknown,
}
#[derive(Clone, Copy, Debug)]
pub enum ValueRef<'a> {
    BlockGroupItem(&'a BlockGroupItem),
    Chunk(&'a ChunkItem),
    Device(&'a DevItem),
    DevExtent(&'a DevExtent),
    DirIndex(&'a DirItem),
    DirItem(&'a DirItem),
    ExtentCsum(&'a CsumItem),
    ExtentData(&'a FileExtentItem),
    ExtentItem(ExtentItemFullWrapperRef<'a>),
    InodeItem(&'a InodeItem),
    InodeRef(&'a InodeRef),
    MetadataItem(ExtentItemFullWrapperRef<'a>),
    PersistentItem(&'a DevStatsItem), // NOTE: Currently the only persistent item is the dev stats item.
    Root(&'a RootItem),
    RootRef(&'a RootRef),
    RootBackref(&'a RootRef),
    UuidSubvol(&'a UuidItem),
    UuidReceivedSubvol(&'a UuidItem),
    XattrItem(&'a DirItem),
    Unknown,
}

impl<'a> ValueRef<'a> {
    fn parse_inner(incompat_flags: IncompatFlags, item: &Item, data: &'a [u8]) -> Option<Self> {
        // Sometimes, parsing may fail. For now we print out dbg statements where there
        // so that nothing is silently discarded.
        macro_rules! dbg_none ( ( $value:expr ) => {{ if $value.is_none() { dbg!(); $value } else { $value } }} );

        let value_bytes = &data[item.offset.get() as usize
            ..item.offset.get() as usize + item.size.get() as usize];

        let ty = match item.key.ty() {
            Some(ty) => ty,
            None => panic!("unknown type: {}", item.key.ty), //return Some(ValueRef::Unknown), // TODO: Warn
        };

        let value: ValueRef<'a> = match ty {
            DiskKeyType::BlockGroupItem => ValueRef::BlockGroupItem(
                dbg_none!(LayoutVerified::<_, BlockGroupItem>::new_unaligned(value_bytes))?
                    .into_ref(),
            ),
            DiskKeyType::ChunkItem => {
                ValueRef::Chunk(dbg_none!(ChunkItem::parse(value_bytes))?)
            }
            DiskKeyType::DevExtent => ValueRef::DevExtent(
                dbg_none!(LayoutVerified::<_, DevExtent>::new_unaligned(value_bytes))?.into_ref(),
            ),
            DiskKeyType::DevItem => ValueRef::Device(
                dbg_none!(LayoutVerified::<_, DevItem>::new_unaligned(value_bytes))?.into_ref(),
            ),
            DiskKeyType::DirIndex => {
                ValueRef::DirIndex(dbg_none!(DirItem::parse(value_bytes))?)
            }
            DiskKeyType::DirItem => {
                ValueRef::DirItem(dbg_none!(DirItem::parse(value_bytes))?)
            }
            DiskKeyType::ExtentCsum => {
                ValueRef::ExtentCsum(CsumItem::parse(value_bytes))
            }
            DiskKeyType::ExtentData => {
                ValueRef::ExtentData(dbg_none!(FileExtentItem::parse(value_bytes))?)
            }
            DiskKeyType::ExtentItem => ValueRef::ExtentItem(ExtentItemFullWrapperRef(
                dbg_none!(ExtentItemFull::parse(value_bytes))?,
                incompat_flags,
            )),
            DiskKeyType::InodeItem => ValueRef::InodeItem(
                dbg_none!(LayoutVerified::<_, InodeItem>::new_unaligned(value_bytes))?.into_ref(),
            ),
            DiskKeyType::InodeRef => {
                ValueRef::InodeRef(dbg_none!(InodeRef::parse(value_bytes))?)
            }
            DiskKeyType::MetadataItem => ValueRef::MetadataItem(ExtentItemFullWrapperRef(
                dbg_none!(ExtentItemFull::parse(value_bytes))?,
                incompat_flags,
            )),
            DiskKeyType::PersistentItem => ValueRef::PersistentItem(
                dbg_none!(LayoutVerified::<_, DevStatsItem>::new_unaligned(value_bytes))?
                    .into_ref(),
            ),
            DiskKeyType::RootBackref => ValueRef::RootBackref(
                dbg_none!(RootRef::parse(value_bytes))?,
            ),
            DiskKeyType::RootItem => ValueRef::Root(
                dbg_none!(LayoutVerified::<_, RootItem>::new_unaligned(value_bytes))?.into_ref(),
            ),
            DiskKeyType::RootRef => ValueRef::RootRef(
                dbg_none!(RootRef::parse(value_bytes))?,
            ),
            DiskKeyType::UuidSubvol => {
                ValueRef::UuidSubvol(UuidItem::parse(value_bytes))
            }
            DiskKeyType::UuidReceivedSubvol => {
                ValueRef::UuidReceivedSubvol(UuidItem::parse(value_bytes))
            }
            DiskKeyType::XattrItem => {
                ValueRef::XattrItem(dbg_none!(DirItem::parse(value_bytes))?)
            }
            DiskKeyType::Unknown => ValueRef::Unknown, // It seems like an item with the oid TREE_LOG has no real type. TODO: Right?

            other => {
                eprintln!("Unimplemented key type: {:?}", other);
                ValueRef::Unknown
            }
        };
        Some(value)
    }
    pub fn as_root_item(self) -> Option<&'a RootItem> {
        match self {
            Self::Root(item) => Some(item),
            _ => None,
        }
    }
    pub fn as_inode_item(self) -> Option<&'a InodeItem> {
        match self {
            Self::InodeItem(item) => Some(item),
            _ => None,
        }
    }
    pub fn as_dir_item(self) -> Option<&'a DirItem> {
        match self {
            Self::DirItem(item) => Some(item),
            _ => None,
        }
    }
    pub fn as_dir_index(self) -> Option<&'a DirItem> {
        match self {
            Self::DirIndex(item) => Some(item),
            _ => None,
        }
    }
    pub fn is_dir_index(self) -> bool {
        self.as_dir_index().is_some()
    }
    pub fn as_xattr_item(self) -> Option<&'a DirItem> {
        match self {
            Self::XattrItem(item) => Some(item),
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
}

impl<'a> ValueCow<'a> {
    /// Convert self to a Value which is guaranteed not to contain any borrowed data, cloning into
    /// a Box if necessary.
    pub fn to_static(self) -> ValueCow<'static> {
        match self {
            Self::BlockGroupItem(i) => ValueCow::BlockGroupItem(Cow::<'static>::Owned(i.into_owned())),
            Self::Chunk(i) => ValueCow::Chunk(Cow::<'static>::Owned(i.into_owned())),
            Self::Device(i) => ValueCow::Device(Cow::<'static>::Owned(i.into_owned())),
            Self::DevExtent(i) => ValueCow::DevExtent(Cow::<'static>::Owned(i.into_owned())),
            Self::DirIndex(i) => ValueCow::DirIndex(Cow::<'static>::Owned(i.into_owned())),
            Self::DirItem(i) => ValueCow::DirItem(Cow::<'static>::Owned(i.into_owned())),
            Self::ExtentCsum(i) => ValueCow::ExtentCsum(Cow::<'static>::Owned(i.into_owned())),
            Self::ExtentData(i) => ValueCow::ExtentData(Cow::<'static>::Owned(i.into_owned())),
            Self::ExtentItem(i) => ValueCow::ExtentItem(i.to_static()),
            Self::InodeItem(i) => ValueCow::InodeItem(Cow::<'static>::Owned(i.into_owned())),
            Self::InodeRef(i) => ValueCow::InodeRef(Cow::<'static>::Owned(i.into_owned())),
            Self::MetadataItem(i) => ValueCow::MetadataItem(i.to_static()),
            Self::PersistentItem(i) => ValueCow::PersistentItem(Cow::<'static>::Owned(i.into_owned())),
            Self::Root(i) => ValueCow::Root(Cow::<'static>::Owned(i.into_owned())),
            Self::RootRef(i) => ValueCow::RootRef(Cow::<'static>::Owned(i.into_owned())),
            Self::RootBackref(i) => ValueCow::RootBackref(Cow::<'static>::Owned(i.into_owned())),
            Self::UuidSubvol(i) => ValueCow::UuidSubvol(Cow::<'static>::Owned(i.into_owned())),
            Self::UuidReceivedSubvol(i) => {
                ValueCow::UuidReceivedSubvol(Cow::<'static>::Owned(i.into_owned()))
            }
            Self::XattrItem(i) => ValueCow::XattrItem(Cow::<'static>::Owned(i.into_owned())),
            Self::Unknown => ValueCow::Unknown,
        }
    }
    // Converts self into a ValueRef if self is borrowed, so that the lifetime of the return value
    // is completely untied to self.
    pub fn as_borrowed(&self) -> Option<ValueRef<'a>> {
        Some(match self {
            Self::BlockGroupItem(Cow::Borrowed(i)) => ValueRef::BlockGroupItem(i),
            Self::Chunk(Cow::Borrowed(i)) => ValueRef::Chunk(i),
            Self::Device(Cow::Borrowed(i)) => ValueRef::Device(i),
            Self::DevExtent(Cow::Borrowed(i)) => ValueRef::DevExtent(i),
            Self::DirIndex(Cow::Borrowed(i)) => ValueRef::DirIndex(i),
            Self::DirItem(Cow::Borrowed(i)) => ValueRef::DirItem(i),
            Self::ExtentCsum(Cow::Borrowed(i)) => ValueRef::ExtentCsum(i),
            Self::ExtentData(Cow::Borrowed(i)) => ValueRef::ExtentData(i),
            Self::ExtentItem(i) => ValueRef::ExtentItem(i.as_borrowed()?),
            Self::InodeItem(Cow::Borrowed(i)) => ValueRef::InodeItem(i),
            Self::InodeRef(Cow::Borrowed(i)) => ValueRef::InodeRef(i),
            Self::MetadataItem(i) => ValueRef::MetadataItem(i.as_borrowed()?),
            Self::PersistentItem(Cow::Borrowed(i)) => ValueRef::PersistentItem(i),
            Self::Root(Cow::Borrowed(i)) => ValueRef::Root(i),
            Self::RootRef(Cow::Borrowed(i)) => ValueRef::RootRef(i),
            Self::RootBackref(Cow::Borrowed(i)) => ValueRef::RootBackref(i),
            Self::UuidSubvol(Cow::Borrowed(i)) => ValueRef::UuidSubvol(i),
            Self::UuidReceivedSubvol(Cow::Borrowed(i)) => ValueRef::UuidReceivedSubvol(i),
            Self::XattrItem(Cow::Borrowed(i)) => ValueRef::XattrItem(i),
            Self::Unknown => ValueRef::Unknown,
            _ => return None,
        })
    }
    /// Returns a borrowed value ref for self ('b), or None if the value was already borrowed.
    pub fn owned_as_borrowed<'b>(&'b self) -> Option<ValueRef<'b>> {
        Some(match self {
            &Self::BlockGroupItem(Cow::Owned(ref i)) => ValueRef::BlockGroupItem(i),
            &Self::Chunk(Cow::Owned(ref i)) => ValueRef::Chunk(i),
            &Self::Device(Cow::Owned(ref i)) => ValueRef::Device(i),
            &Self::DevExtent(Cow::Owned(ref i)) => ValueRef::DevExtent(i),
            &Self::DirIndex(Cow::Owned(ref i)) => ValueRef::DirIndex(i),
            &Self::DirItem(Cow::Owned(ref i)) => ValueRef::DirItem(i),
            &Self::ExtentCsum(Cow::Owned(ref i)) => ValueRef::ExtentCsum(i),
            &Self::ExtentData(Cow::Owned(ref i)) => ValueRef::ExtentData(i),
            &Self::ExtentItem(ref i) => ValueRef::ExtentItem(i.owned_as_borrowed()?),
            &Self::InodeItem(Cow::Owned(ref i)) => ValueRef::InodeItem(i),
            &Self::InodeRef(Cow::Owned(ref i)) => ValueRef::InodeRef(i),
            &Self::MetadataItem(ref i) => ValueRef::MetadataItem(i.owned_as_borrowed()?),
            &Self::PersistentItem(Cow::Owned(ref i)) => ValueRef::PersistentItem(i),
            &Self::Root(Cow::Owned(ref i)) => ValueRef::Root(i),
            &Self::RootRef(Cow::Owned(ref i)) => ValueRef::RootRef(i),
            &Self::RootBackref(Cow::Owned(ref i)) => ValueRef::RootBackref(i),
            &Self::UuidSubvol(Cow::Owned(ref i)) => ValueRef::UuidSubvol(i),
            &Self::UuidReceivedSubvol(Cow::Owned(ref i)) => ValueRef::UuidReceivedSubvol(i),
            &Self::XattrItem(Cow::Owned(ref i)) => ValueRef::XattrItem(i),
            &Self::Unknown => ValueRef::Unknown,
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

            let begin = bytes.as_ptr();
            let len = bytes.len() - mem::size_of::<Header>();

            Some(&*(std::slice::from_raw_parts(begin, len) as *const [u8] as *const Self))
        }
    }
    pub fn iter<'a>(
        &'a self,
        incompat_flags: IncompatFlags,
    ) -> impl Iterator<Item = (LayoutVerified<&'a [u8], Item>, ValueRef<'a>)> + 'a {
        self.data.chunks(mem::size_of::<Item>())
            .take(self.header.item_count.get() as usize)
            .map_while(LayoutVerified::new_unaligned)
            .map_while(
                move |item: LayoutVerified<&'a [u8], Item>| -> Option<(_, _)> { match ValueRef::parse_inner(incompat_flags, &*item, &self.data) {
                    Some(v) => Some((item, v)),
                    None => {
                        eprintln!("Item {:?} failed to parse", item);
                        None
                    }
                }})
    }
    pub fn struct_size(&self) -> usize {
        mem::size_of::<Header>() + self.data.len()
    }
    pub fn as_bytes(&self) -> &[u8] {
        unsafe { slice::from_raw_parts(self as *const Self as *const u8, self.struct_size()) }
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
    pub block: Arc<[u8]>,
}
impl TreeOwned {
    fn as_ref_inner<'a>(block: &'a [u8]) -> Tree<'a> {
        let header = LayoutVerified::<&'a [u8], Header>::new_unaligned(
            &block[..mem::size_of::<Header>()],
        )
        .unwrap();
        Tree::parse_inner(&*header, &block)
    }
    pub fn as_ref<'a>(&'a self) -> Tree<'a> {
        Self::as_ref_inner(&self.block)
    }
}
impl fmt::Debug for TreeOwned {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.as_ref(), f)
    }
}

pub fn always_equal(_: &DiskKey, _: &DiskKey) -> Ordering {
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
        let block = Arc::from(filesystem::read_node(device, superblock, chunk_map, addr));
        let header = Header::parse(superblock.checksum_ty(), &block)?; // validate checksum
        if header.logical_addr.get() != addr {
            panic!("Mismatch: {} != {}", header.logical_addr.get(), addr);
        }
        Ok(TreeOwned { block })
    }
    fn get_generic<'b, D: fal::Device>(
        path: &'b mut Path<'a>,
        device: &D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        key: &DiskKey,
        compare: fn(k1: &DiskKey, k2: &DiskKey) -> Ordering,
    ) -> Option<(DiskKey, ValueRef<'b>)> {
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
                    assert_ne!(internal.header.level, 0);

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
        Some((key.key, value))
    }
    pub fn get_with_path<'b, D: fal::Device>(
        path: &'b mut Path<'a>,
        device: &D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        key: &DiskKey,
    ) -> Option<ValueRef<'b>> {
        Self::get_generic(path, device, superblock, chunk_map, key, Ord::cmp)
            .map(|(_, value)| value)
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
    pub fn similar_pairs<D: fal::Device>(
        self,
        device: &D,
        superblock: &Superblock,
        chunk_map: &ChunkMap,
        partial_key: &DiskKey,
    ) -> Option<PairsIterState<'a>> {
        let mut path = vec!((OwnedOrBorrowedTree::Borrowed(self), 0));
        match Self::get_generic(
            &mut path,
            device,
            superblock,
            chunk_map,
            partial_key,
            DiskKey::compare_without_offset,
        ) {
            Some(_) => (),
            None => return None,
        };

        Some(PairsIterState {
            path,
            previous_key: None,
            function: DiskKey::compare_without_offset,
        })
    }
    fn to_tree_owned(&self) -> TreeOwned {
        match self {
            &Self::Internal(node) => {
                // TODO: Maybe we should really make sure that the size be always equal to the node
                // size.
                let mut bytes = vec! [0u8; node.struct_size()].into_boxed_slice();
                bytes.copy_from_slice(node.as_bytes());
                TreeOwned { block: Arc::from(bytes) }
            }
            &Self::Leaf(node) => {
                let mut bytes = vec! [0u8; node.struct_size()].into_boxed_slice();
                bytes.copy_from_slice(node.as_bytes());
                TreeOwned { block: Arc::from(bytes) }
            }
        }
    }
}

/// Stack-based tree traversal iterator. Since this iterator is heavily lifetime-tied, and `!Sync`,
/// it's _not_ supposed to be stored anywhere, but just being temporarily constructed from
/// `PairsIterState`.
pub struct Pairs<'ctx, 'a, 'b, D> {
    device: &'ctx D,
    superblock: &'ctx Superblock,
    chunk_map: &'ctx ChunkMap,
    state: &'a mut PairsIterState<'b>,
}

/// The state of the Pairs iterator.
pub struct PairsIterState<'a> {
    pub(crate) path: Path<'a>,
    pub(crate) function: for<'k1, 'k2> fn(k1: &'k1 DiskKey, k2: &'k2 DiskKey) -> Ordering,
    pub(crate) previous_key: Option<DiskKey>,
}
impl<'a> fmt::Debug for PairsIterState<'a> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        struct FnDebug(for<'k1, 'k2> fn(k2: &'k1 DiskKey, k2: &'k2 DiskKey) -> Ordering);

        impl fmt::Debug for FnDebug {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "(compare function at {:p})", self.0 as *const u8)
            }
        }

        f.debug_struct("PairsIterState")
            .field("path", &self.path)
            .field("function", &FnDebug(self.function))
            .field("previous_key", &self.previous_key)
            .finish()
    }
}


#[derive(Debug)]
pub enum Either<A, B> { A(A), B(B) }

impl<'a, 'b: 'a> Either<ValueRef<'a>, ValueRef<'b>> {
    pub fn as_ref(self) -> ValueRef<'a> {
        match self {
            Self::A(value) | Self::B(value) => value,
        }
    }
}
impl<'a> Either<ValueRef<'a>, OwningValueRef> {
    pub fn as_ref<'b: 'a>(&'b self) -> ValueRef<'b> {
        match self {
            &Self::A(value) => value,
            &Self::B(ref value) => value.as_ref(),
        }
    }
}

pub struct OwningValueRef {
    tree_arc: TreeOwned,
    item: Item,
    incompat_flags: IncompatFlags,
}
impl OwningValueRef {
    pub fn as_ref<'a>(&'a self) -> ValueRef<'a> {
        ValueRef::parse_inner(self.incompat_flags, &self.item, &self.tree_arc.as_ref().as_leaf().unwrap().data).unwrap()
    }
}
impl fmt::Debug for OwningValueRef {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Debug::fmt(&self.as_ref(), f)
    }
}

pub enum Step<'a> {
    Value(Option<(DiskKey, Either<ValueRef<'a>, OwningValueRef>)>),
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
    pub fn step<D: fal::DeviceRo>(&mut self, device: &D, superblock: &Superblock, chunk_map: &ChunkMap, count: usize) -> Step<'a> {
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

                let &(ref current_tree, _) = match self.path.last() {
                    Some(l) => l,
                    None => return Step::Value(None),
                };

                // FIXME: Fix increment update nth
                return Step::Value(Some((key, match current_tree {
                    &OwnedOrBorrowedTree::Owned(ref owned) => if let Tree::Leaf(ref owned_leaf) = owned.as_ref() {
                        let (item, _) = owned_leaf.iter(superblock.incompat_flags().unwrap()).nth(current_index).unwrap();
                        Either::B(OwningValueRef {
                            incompat_flags: superblock.incompat_flags().unwrap(),
                            tree_arc: owned.clone(), // by ref
                            item: *item,
                        })
                    } else {
                        unreachable!()
                    },
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
    pub fn next_owned<D: fal::DeviceRo>(&mut self, device: &D, superblock: &Superblock, chunk_map: &ChunkMap) -> Option<(DiskKey, Either<ValueRef<'a>, OwningValueRef>)> {
        loop {
            match self.step(device, superblock, chunk_map, 1) {
                Step::Value(v) => return v,
                Step::Step => continue,
            }
        }
    }
    pub fn iter<'ctx, 'b, D>(&'b mut self, device: &'ctx D, superblock: &'ctx Superblock, chunk_map: &'ctx ChunkMap) -> Pairs<'ctx, 'b, 'a, D> {
        Pairs::assemble(self, device, superblock, chunk_map)
    }
    // TODO: previous
}

impl<'ctx, 'a, 'b, D> Pairs<'ctx, 'a, 'b, D> {
    /// Disassemble the iterator into its internal state.
    pub fn disassemble(self) -> &'a PairsIterState<'a> {
        self.state
    }
    pub fn assemble(state: &'a mut PairsIterState<'b>, device: &'ctx D, superblock: &'ctx Superblock, chunk_map: &'ctx ChunkMap) -> Self {
        Self {
            state,
            device,
            superblock,
            chunk_map,
        }
    }
}

impl<'ctx, 'a, 'b, D: fal::DeviceRo> Iterator for Pairs<'ctx, 'a, 'b, D> {
    type Item = (DiskKey, Either<ValueRef<'a>, OwningValueRef>);

    fn next(&mut self) -> Option<Self::Item> {
        // Note that we can't yield references to data stored in the Path of the state, since that
        // would imply someone could collect all the references from this iterator into a vec, and
        // then continue using those after the state has been dropped. Thus, the state will have to
        // keep track of the nodes which are still referenced, and moved elsewhere before the
        // references are dropped.
        //
        // In practice: every tree block gets a refcount. However in
        // almost all situations, the tree block will be freed immediately after the next node is
        // reached, since most of the times this iterator is used, the items will be passed by
        // value (unless they are DSTs).
        //
        // The only alternative would be to always yield owned values. This would be completely
        // fine for all the values that are fixed in size, but for DSTs, there would be one
        // allocation per value, which is a lot more allocations than simply keeping the blocks in
        // memory until they aren't used anymore.
        self.state.next_owned(self.device, self.superblock, self.chunk_map)
    }
}

#[cfg(test)]
mod tests {
    use crate::tree::Leaf;

    const LEAF_BYTES: &'static [u8] = include_bytes!("../assets/leaf.bin");

    fn check_leaf(leaf: &Leaf) {
        use crate::{Checksum, DiskKey, DiskKeyType, oid, PackedUuid};
        use crate::superblock::IncompatFlags;
        use crate::tree::Header;

        use zerocopy::{U32, U64};
        // From the superblock from the filesystem in which the trees were stored.
        let incompat_flags = IncompatFlags::MIXED_BACKREF | IncompatFlags::BIG_METADATA | IncompatFlags::EXTENDED_IREF | IncompatFlags::SKINNY_METADATA;

        let fsid = PackedUuid { bytes: *uuid::Uuid::parse_str("9d9f9ba4-1298-4896-850f-706de1794fb6").unwrap().as_bytes() };

        assert_eq!(leaf.header, Header {
            checksum: Checksum::Xxhash(0x5D37_71D5_9674_161B).bytes(),
            fsid,
            logical_addr: U64::new(22036480),
            flags: U64::new(0x100000000000001), // TODO: Understand these flags
            chunk_tree_uuid: PackedUuid { bytes: *uuid::Uuid::parse_str("c90581bd-4c1f-44cd-a545-53820a5967a5").unwrap().as_bytes() },
            generation: U64::new(5),
            owner: U64::new(3),
            item_count: U32::new(4),
            level: 0,
        });
        assert_eq!(leaf.iter(incompat_flags).count(), 4);
    }
    #[test]
    fn leaf_parsing() {
        let leaf = Leaf::parse(&LEAF_BYTES).unwrap();
        check_leaf(leaf);
    }
    #[test]
    fn tree_owned() {
        use crate::superblock::ChecksumType;
        use crate::tree::Tree;

        let borrowed = Tree::parse(ChecksumType::Xxhash, LEAF_BYTES).unwrap();
        let owned = borrowed.to_tree_owned();

        assert_eq!(borrowed, owned.as_ref());
    }

    #[test]
    fn node_parsing() {
        const NODE_BYTES: &'static [u8] = include_bytes!("../assets/node.bin");
    }
}
