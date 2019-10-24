use bitflags::bitflags;

use crate::{
    omap::{Omap, OmapKey, OmapValue},
    read_block, read_obj_phys,
    superblock::NxSuperblock,
    BlockAddr, ObjPhys, ObjectIdentifier, ObjectType,
};

use fal::parsing::{read_u16, read_u32, read_u64};

use std::{borrow::Cow, cmp::Ordering};

pub type Compare = fn(_: &BTreeKey, _: &BTreeKey) -> Ordering;
pub type Path<'a> = Vec<(Cow<'a, BTreeNode>, usize)>;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct NodeLocation {
    pub start: u16,
    pub len: u16,
}
impl NodeLocation {
    pub fn start(&self) -> u16 {
        self.start
    }
    pub fn end(&self) -> u16 {
        self.start + self.len
    }
}

pub fn read_nloc(bytes: &[u8], off: &mut usize) -> NodeLocation {
    NodeLocation {
        start: read_u16(bytes, off),
        len: read_u16(bytes, off),
    }
}

bitflags! {
    pub struct BTreeFlags: u32 {
        const U64_KEYS = 0x1;
        const SEQUENTIAL_INSERT = 0x2;
        const ALLOW_GHOSTS = 0x4;
        const EPHEMERAL = 0x8;
        const PHYSICAL = 0x10;
        const NONPERSISTENT = 0x20;
        const KEY_VAL_NONALIGNED = 0x40;
    }
}

bitflags! {
    pub struct BTreeNodeFlags: u16 {
        const ROOT = 0x1;
        const LEAF = 0x2;
        const FIXED_KV_SIZE = 0x4;
        const CHECK_KOFF_INVAL = 0x8000;
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BTreeInfoFixed {
    pub flags: BTreeFlags,
    pub node_size: u32,
    pub key_size: u32,
    pub val_size: u32,
}

impl BTreeInfoFixed {
    pub const LEN: usize = 16;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;
        Self {
            flags: BTreeFlags::from_bits(read_u32(bytes, &mut offset)).unwrap(),
            node_size: read_u32(bytes, &mut offset),
            key_size: read_u32(bytes, &mut offset),
            val_size: read_u32(bytes, &mut offset),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct BTreeInfo {
    pub fixed: BTreeInfoFixed,
    pub longest_key: u32,
    pub longest_val: u32,
    pub key_count: u64,
    pub node_count: u64,
}

impl BTreeInfo {
    pub const LEN: usize = 40;
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = BTreeInfoFixed::LEN;

        Self {
            fixed: BTreeInfoFixed::parse(&bytes[..offset]),
            longest_key: read_u32(bytes, &mut offset),
            longest_val: read_u32(bytes, &mut offset),
            key_count: read_u64(bytes, &mut offset),
            node_count: read_u64(bytes, &mut offset),
        }
    }
}

#[derive(Clone, Debug)]
pub struct KvLocation {
    pub key: NodeLocation,
    pub value: NodeLocation,
}

impl KvLocation {
    pub const LEN: usize = 8;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;
        Self {
            key: read_nloc(bytes, &mut offset),
            value: read_nloc(bytes, &mut offset),
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct KvOffset {
    pub key: u16,
    pub value: u16,
}

impl KvOffset {
    pub const LEN: usize = 4;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;
        Self {
            key: read_u16(bytes, &mut offset),
            value: read_u16(bytes, &mut offset),
        }
    }
}

fn off2loc(offset: KvOffset, key_size: u16, val_size: u16) -> KvLocation {
    KvLocation {
        key: NodeLocation {
            start: offset.key,
            len: key_size,
        },
        value: NodeLocation {
            start: offset.value,
            len: val_size,
        },
    }
}

#[derive(Clone, Debug)]
pub enum BTreeToc {
    Locations(Vec<KvLocation>),
    Offsets(Vec<KvOffset>),
}
impl BTreeToc {
    fn get(&self, index: usize, key_size: u16, val_size: u16) -> Option<KvLocation> {
        match self {
            Self::Locations(locations) => locations.get(index).map(Clone::clone),
            Self::Offsets(offsets) => offsets
                .get(index)
                .map(|offset| off2loc(*offset, key_size, val_size)),
        }
    }
}

#[derive(Clone, Debug)]
pub struct BTreeNode {
    pub header: ObjPhys,
    pub flags: BTreeNodeFlags,
    pub level: u16,
    pub key_count: u32,
    pub table_space: NodeLocation,
    pub free_space: NodeLocation,
    pub key_free_list: NodeLocation,
    pub val_free_list: NodeLocation,
    pub keyval_area: Vec<u8>,
    pub info: Option<BTreeInfo>,
    pub toc: BTreeToc,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq, PartialOrd, Ord)]
pub enum BTreeKey {
    OmapKey(OmapKey),
}

impl BTreeKey {
    pub fn into_omap_key(self) -> Option<OmapKey> {
        match self {
            Self::OmapKey(key) => Some(key),
        }
    }
    pub fn as_omap_key(&self) -> Option<&OmapKey> {
        match self {
            Self::OmapKey(ref key) => Some(key),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum BTreeValue {
    OmapValue(OmapValue),
}

impl BTreeValue {
    pub fn into_omap_value(self) -> Option<OmapValue> {
        match self {
            Self::OmapValue(val) => Some(val),
        }
    }
    pub fn as_omap_value(&self) -> Option<&OmapValue> {
        match self {
            Self::OmapValue(ref val) => Some(val),
        }
    }
}

impl BTreeNode {
    pub fn load<D: fal::Device>(
        device: &mut D,
        superblock: &NxSuperblock,
        addr: BlockAddr,
    ) -> Self {
        Self::parse(&read_block(superblock, device, addr))
    }
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let header = read_obj_phys(bytes, &mut offset);

        let flags = BTreeNodeFlags::from_bits(read_u16(bytes, &mut offset)).unwrap();
        let level = read_u16(bytes, &mut offset);

        let key_count = read_u32(bytes, &mut offset);
        let table_space = read_nloc(bytes, &mut offset);
        let free_space = read_nloc(bytes, &mut offset);
        let key_free_list = read_nloc(bytes, &mut offset);
        let val_free_list = read_nloc(bytes, &mut offset);

        assert_eq!(offset, 56);

        let toc = if flags.contains(BTreeNodeFlags::FIXED_KV_SIZE) {
            BTreeToc::Offsets(
                ((offset as u16 + table_space.start) / KvOffset::LEN as u16
                    ..(offset as u16 + table_space.end()) / KvOffset::LEN as u16)
                    .map(|i| {
                        KvOffset::parse(
                            &bytes[i as usize * KvOffset::LEN..(i as usize + 1) * KvOffset::LEN],
                        )
                    })
                    .take(key_count as usize)
                    .collect(),
            )
        } else {
            BTreeToc::Locations(
                ((offset as u16 + table_space.start) as usize / KvLocation::LEN
                    ..(offset as u16 + table_space.end()) as usize / KvLocation::LEN)
                    .map(|i| {
                        KvLocation::parse(&bytes[i * KvLocation::LEN..(i + 1) * KvLocation::LEN])
                    })
                    .take(key_count as usize)
                    .collect(),
            )
        };
        let keyval_area = bytes[offset + table_space.end() as usize
            ..(bytes.len()
                - if header.object_type.ty == ObjectType::Btree {
                    BTreeInfo::LEN
                } else {
                    0
                })]
            .to_owned();

        let info = if header.object_type.ty == ObjectType::Btree {
            Some(BTreeInfo::parse(&bytes[bytes.len() - BTreeInfo::LEN..]))
        } else {
            None
        };

        Self {
            header,
            flags,
            key_count,
            keyval_area,
            free_space,
            info,
            key_free_list,
            level,
            val_free_list,
            table_space,
            toc,
        }
    }
    fn get_generic<D: fal::Device>(
        &self,
        device: &mut D,
        superblock: &NxSuperblock,
        omap: Option<&Omap>,
        key: &BTreeKey,
        key_size: u16,
        val_size: u16,
        compare: Compare,
    ) -> Option<((BTreeKey, BTreeValue), Path)> {
        let mut path = Vec::with_capacity(self.level as usize);
        path.push((Cow::Borrowed(self), 0));

        let leaf_index = loop {
            if self.is_leaf() {
                match self
                    .current_node_keys(key_size, val_size)
                    .position(|k| compare(&k, key) == Ordering::Equal)
                {
                    Some(idx) => break idx,
                    None => return None,
                };
            } else {
                // TODO: Are the subtree oids virtual or physical?
                let (index, _) =
                    match self.current_node_closest_key(key, key_size, val_size, compare) {
                        Some(t) => t,
                        None => return None,
                    };

                let subtree = Self::load(
                    device,
                    superblock,
                    self.internal_key_ptr(index, key_size, val_size).unwrap().0 as i64,
                );
                assert_eq!(subtree.level, path.last().unwrap().0.level - 1);

                if omap.is_none() && subtree.header.object_type.is_virtual() {
                    panic!("Loaded a virtual subtree without access to an omap");
                }
                path.push((Cow::Owned(subtree), index));
                continue;
            }
        };
        path.last_mut().unwrap().1 = leaf_index;
        Some((
            (
                *key,
                self.leaf_value(leaf_index, key_size, val_size).unwrap(),
            ),
            path,
        ))
    }
    pub fn is_root(&self) -> bool {
        self.flags.contains(BTreeNodeFlags::ROOT)
    }
    pub fn is_leaf(&self) -> bool {
        self.flags.contains(BTreeNodeFlags::LEAF)
    }
    fn current_node_key(&self, index: usize, key_size: u16, val_size: u16) -> Option<BTreeKey> {
        let toc_entry = match self.toc.get(index, key_size, val_size) {
            Some(entry) => entry,
            None => return None,
        };
        let key_bytes =
            &self.keyval_area[toc_entry.key.start as usize..toc_entry.key.end() as usize];
        match self.header.object_subtype {
            ObjectType::ObjectMap => Some(BTreeKey::OmapKey(OmapKey::parse(key_bytes))),
            _ => unimplemented!(),
        }
    }
    fn current_node_closest_key(
        &self,
        key: &BTreeKey,
        key_size: u16,
        val_size: u16,
        compare: Compare,
    ) -> Option<(usize, BTreeKey)> {
        self.current_node_keys(key_size, val_size)
            .enumerate()
            .filter(|(_, current_key)| compare(&current_key, key) != Ordering::Greater)
            .max_by(|(_, k1), (_, k2)| compare(&k1, &k2))
    }
    fn leaf_value(&self, index: usize, key_size: u16, val_size: u16) -> Option<BTreeValue> {
        assert!(self.is_leaf());

        let toc_entry = match self.toc.get(index, key_size, val_size) {
            Some(entry) => entry,
            None => return None,
        };

        let start = self.keyval_area.len() - toc_entry.value.start as usize;

        let value_bytes = &self.keyval_area[start..start + toc_entry.value.len as usize];

        match self.header.object_subtype {
            ObjectType::ObjectMap => Some(BTreeValue::OmapValue(OmapValue::parse(&value_bytes))),
            _ => unimplemented!(),
        }
    }
    fn internal_key_ptr(
        &self,
        index: usize,
        key_size: u16,
        val_size: u16,
    ) -> Option<ObjectIdentifier> {
        assert!(!self.is_leaf());

        let toc_entry = match self.toc.get(index, key_size, val_size) {
            Some(entry) => entry,
            None => return None,
        };

        let start = self.keyval_area.len() - toc_entry.value.start as usize;
        assert_eq!(toc_entry.value.len, 8);

        let value_bytes = &self.keyval_area[start..start + toc_entry.value.len as usize];
        Some(ObjectIdentifier::from(fal::read_u64(value_bytes, 0)))
    }
    fn current_node_keys<'a>(
        &'a self,
        key_size: u16,
        val_size: u16,
    ) -> impl Iterator<Item = BTreeKey> + 'a {
        (0..self.key_count as usize)
            .map(move |i| self.current_node_key(i, key_size, val_size).unwrap())
    }
}

#[derive(Debug)]
pub struct BTree {
    pub root: BTreeNode,
}

impl BTree {
    pub fn load<D: fal::Device>(
        device: &mut D,
        superblock: &NxSuperblock,
        addr: BlockAddr,
    ) -> Self {
        let root = BTreeNode::load(device, superblock, addr);

        assert!(root.is_root());

        Self { root }
    }
    pub fn info(&self) -> &BTreeInfo {
        self.root.info.as_ref().unwrap()
    }
    pub fn get_generic<D: fal::Device>(
        &self,
        device: &mut D,
        superblock: &NxSuperblock,
        omap: Option<&Omap>,
        key: &BTreeKey,
        compare: Compare,
    ) -> Option<((BTreeKey, BTreeValue), Path)> {
        let fixed = &self.root.info.unwrap().fixed;
        self.root.get_generic(
            device,
            superblock,
            omap,
            key,
            fixed.key_size as u16,
            fixed.val_size as u16,
            compare,
        )
    }
    pub fn get<D: fal::Device>(
        &self,
        device: &mut D,
        superblock: &NxSuperblock,
        omap: Option<&Omap>,
        key: &BTreeKey,
    ) -> Option<BTreeValue> {
        self.get_generic(device, superblock, omap, key, Ord::cmp)
            .map(|((_, value), _)| value)
    }
    pub fn pairs<'a, D: fal::Device>(
        &'a self,
        device: &'a mut D,
        superblock: &'a NxSuperblock,
        omap: Option<&'a Omap>,
    ) -> Pairs<'a, D> {
        if omap.is_none() && self.root.header.object_type.is_virtual() {
            panic!("Iterating over the pairs of a virtual object (tree) without an omap.");
        }

        Pairs {
            device,
            superblock,
            omap,
            path: vec![(Cow::Borrowed(&self.root), 0)],

            compare: Ord::cmp,
            previous_key: None,
        }
    }
    pub fn similar_pairs<'a, D: fal::Device>(
        &'a self,
        device: &'a mut D,
        superblock: &'a NxSuperblock,
        omap: Option<&'a Omap>,
        key: &BTreeKey,
        compare: Compare,
    ) -> Option<Pairs<'a, D>> {
        let path = match self
            .get_generic(device, superblock, omap, key, compare)
            .map(|(_, path)| path)
        {
            Some(p) => p,
            None => return None,
        };

        if omap.is_none() && self.root.header.object_type.is_virtual() {
            panic!("Iterating over the pairs of a virtual object (tree) without an omap.");
        }

        Some(Pairs {
            device,
            superblock,
            omap,
            path,

            compare,
            previous_key: None,
        })
    }
    pub fn keys<'a, D: fal::Device>(
        &'a self,
        device: &'a mut D,
        superblock: &'a NxSuperblock,
        omap: Option<&'a Omap>,
    ) -> impl Iterator<Item = BTreeKey> + 'a {
        self.pairs(device, superblock, omap).map(|(k, _)| k)
    }
    pub fn values<'a, D: fal::Device>(
        &'a self,
        device: &'a mut D,
        superblock: &'a NxSuperblock,
        omap: Option<&'a Omap>,
    ) -> impl Iterator<Item = BTreeValue> + 'a {
        self.pairs(device, superblock, omap).map(|(_, v)| v)
    }
}

/// Stack-based tree traversal iterator
pub struct Pairs<'a, D: fal::Device> {
    device: &'a mut D,
    superblock: &'a NxSuperblock,
    path: Path<'a>,
    omap: Option<&'a Omap>,

    compare: Compare,
    previous_key: Option<BTreeKey>,
}

// Based on the btrfs Pairs code with some modifications.
impl<'a, D: fal::Device> Iterator for Pairs<'a, D> {
    type Item = (BTreeKey, BTreeValue);

    fn next(&mut self) -> Option<Self::Item> {
        enum Action {
            ClimbDown,
            ClimbUp(ObjectIdentifier),
        }

        let info = self.path[0].0.info.unwrap().fixed;

        let (current_tree, current_index) = match self.path.last_mut() {
            Some(l) => l,
            None => return None,
        };

        let action = if current_tree.is_leaf() {
            // Leaf
            match current_tree.current_node_key(
                *current_index,
                info.key_size as u16,
                info.val_size as u16,
            ) {
                Some(key) => {
                    // If there is a pair available, just yield it and continue.
                    if let Some(previous_key) = self.previous_key {
                        let compare = &self.compare;
                        if compare(&previous_key, &key) != Ordering::Equal {
                            return None;
                        }
                    }
                    self.previous_key = Some(key);
                    let value = match current_tree.leaf_value(
                        *current_index,
                        info.key_size as u16,
                        info.val_size as u16,
                    ) {
                        Some(v) => v,
                        None => return None,
                    };
                    *current_index += 1;
                    return Some((key, value.clone()));
                }
                None => {
                    // When there are no more elements in the current node, we need to go one node
                    // back, increase the index there and load a new leaf.
                    Action::ClimbDown
                }
            }
        } else {
            // Internal
            match current_tree.internal_key_ptr(
                *current_index,
                info.key_size as u16,
                info.val_size as u16,
            ) {
                Some(ptr) => {
                    // If there is a new undiscovered leaf node, we climb up the tree (closer
                    // to the leaves), and search it.
                    Action::ClimbUp(ptr)
                }
                None => {
                    // Otherwise, we climb down to the parent node, and continue searching
                    // there.
                    Action::ClimbDown
                }
            }
        };

        match action {
            Action::ClimbUp(ptr) => self.path.push((
                Cow::Owned({
                    // If the subtree ptrs are virtual, then make sure nothing bad happens without
                    // noticing.

                    let baddr = match self.omap {
                        Some(omap) => {
                            omap.get_partial_latest(
                                self.device,
                                self.superblock,
                                OmapKey::partial(ptr),
                            )
                            .unwrap()
                            .1
                            .paddr
                        }
                        None => ptr.0 as i64,
                    };

                    let subtree = BTreeNode::load(self.device, self.superblock, baddr);

                    assert_eq!(subtree.level, self.path.last().unwrap().0.level - 1);

                    if self.omap.is_none() && subtree.header.object_type.is_virtual() {
                        panic!("Loaded a virtual subtree without access to an omap");
                    }
                    subtree
                }),
                0,
            )),

            Action::ClimbDown => {
                self.path.pop();

                if let Some((_, i)) = self.path.last_mut() {
                    *i += 1;
                } else {
                    return None;
                }
            }
        }

        // Recurse until another pair is found or until the entire tree has been traversed.
        self.next()
    }
}
