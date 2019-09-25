use crate::superblock::{ObjectType, ObjPhys};
use fal::{read_u16, read_u32, read_u64};
use std::{
    convert::{TryFrom, TryInto},
    ops::{BitAnd, BitOr, Range},
};

pub type NodeLocation = Range<u16>;

pub fn read_nloc(bytes: &[u8], off: usize) -> NodeLocation {
    read_u16(bytes, off)..read_u16(bytes, off + 2)
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct BTreeFlags(pub u32);

impl BTreeFlags {
    pub fn u64_keys() -> Self {
        Self(0x1)
    }
    pub fn sequential_insert() -> Self {
        Self(0x2)
    }
    pub fn allow_ghosts() -> Self {
        Self(0x4)
    }
    pub fn ephemeral() -> Self {
        Self(0x8)
    }
    pub fn physical() -> Self {
        Self(0x10)
    }
    pub fn nonpersistent() -> Self {
        Self(0x20)
    }
    pub fn key_val_nonaligned() -> Self {
        Self(0x40)
    }
    pub fn none() -> Self {
        Self(0)
    }
    pub fn all() -> Self {
        Self::u64_keys()
            | Self::sequential_insert()
            | Self::allow_ghosts()
            | Self::ephemeral()
            | Self::physical()
            | Self::nonpersistent()
            | Self::key_val_nonaligned()
    }
}

#[derive(Debug)]
pub struct UnknownMask<T>(pub T);

impl TryFrom<u32> for BTreeFlags {
    type Error = UnknownMask<u32>;
    fn try_from(raw: u32) -> Result<Self, Self::Error> {
        let complement = raw & !(Self::all().0);
        if complement != 0 {
            return Err(UnknownMask(complement));
        }
        Ok(Self(raw))
    }
}

impl BitAnd for BTreeFlags {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}
impl BitOr for BTreeFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::fmt::Debug for BTreeFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut components = vec![];

        if *self & Self::u64_keys() != Self::none() {
            components.push("UINT64_KEYS");
        }
        if *self & Self::sequential_insert() != Self::none() {
            components.push("SEQUENTIAL_INSERT");
        }
        if *self & Self::allow_ghosts() != Self::none() {
            components.push("ALLOW_GHOSTS");
        }
        if *self & Self::ephemeral() != Self::none() {
            components.push("EPHEMERAL");
        }
        if *self & Self::physical() != Self::none() {
            components.push("PHYSICAL");
        }
        if *self & Self::nonpersistent() != Self::none() {
            components.push("NONPERSISTENT");
        }
        if *self & Self::key_val_nonaligned() != Self::none() {
            components.push("KV_NONALIGNED");
        }

        write!(f, "{}", components.join(" | "))
    }
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct BTreeNodeFlags(pub u16);

impl BTreeNodeFlags {
    pub fn root() -> Self {
        Self(0x1)
    }
    pub fn leaf() -> Self {
        Self(0x2)
    }
    pub fn fixed_kv_size() -> Self {
        Self(0x4)
    }
    pub fn check_koff_inval() -> Self {
        Self(0x8000)
    }
    pub fn none() -> Self {
        Self(0)
    }
    pub fn all() -> Self {
        Self::root() | Self::leaf() | Self::fixed_kv_size() | Self::check_koff_inval()
    }
}

impl TryFrom<u16> for BTreeNodeFlags {
    type Error = UnknownMask<u16>;
    fn try_from(raw: u16) -> Result<Self, Self::Error> {
        let outside = raw & !(Self::all().0);
        if outside != 0 {
            return Err(UnknownMask(outside));
        }
        Ok(Self(raw))
    }
}

impl BitAnd for BTreeNodeFlags {
    type Output = Self;
    fn bitand(self, rhs: Self) -> Self {
        Self(self.0 & rhs.0)
    }
}
impl BitOr for BTreeNodeFlags {
    type Output = Self;
    fn bitor(self, rhs: Self) -> Self {
        Self(self.0 | rhs.0)
    }
}

impl std::fmt::Debug for BTreeNodeFlags {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        let mut components = vec![];

        if *self & Self::root() != Self::none() {
            components.push("ROOT");
        }
        if *self & Self::leaf() != Self::none() {
            components.push("LEAF");
        }
        if *self & Self::fixed_kv_size() != Self::none() {
            components.push("FIXED_KV_SIZE");
        }
        if *self & Self::check_koff_inval() != Self::none() {
            components.push("CHECK_KOFF_INVAL");
        }

        write!(f, "{}", components.join(" | "))
    }
}

#[derive(Debug)]
pub struct BTreeInfoFixed {
    flags: BTreeFlags,
    node_size: u32,
    key_size: u32,
    val_size: u32,
}

impl BTreeInfoFixed {
    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            flags: read_u32(bytes, 0).try_into().unwrap(),
            node_size: read_u32(bytes, 4),
            key_size: read_u32(bytes, 8),
            val_size: read_u32(bytes, 12),
        }
    }
}

#[derive(Debug)]
pub struct BTreeInfo {
    fixed: BTreeInfoFixed,
    longest_key: u32,
    longest_val: u32,
    key_count: u64,
    node_count: u64,
}

impl BTreeInfo {
    pub const LEN: usize = 40;
    pub fn parse(bytes: &[u8]) -> Self {
        Self {
            fixed: BTreeInfoFixed::parse(&bytes[..16]),
            longest_key: read_u32(bytes, 16),
            longest_val: read_u32(bytes, 20),
            key_count: read_u64(bytes, 24),
            node_count: read_u64(bytes, 32),
        }
    }
}

#[derive(Debug)]
pub struct BTreeNode {
    header: ObjPhys,
    flags: BTreeNodeFlags,
    level: u16,
    key_count: u32,
    table_space: NodeLocation,
    free_space: NodeLocation,
    key_free_list: NodeLocation,
    val_free_list: NodeLocation,
    data: Vec<u64>,
    info: Option<BTreeInfo>,
}

impl BTreeNode {
    pub fn parse(bytes: &[u8]) -> Self {
        let header = ObjPhys::parse(&bytes[..32]);

        Self {
            header,
            flags: read_u16(bytes, 32).try_into().unwrap(),
            level: read_u16(bytes, 34),
            key_count: read_u32(bytes, 36),
            table_space: read_nloc(bytes, 40),
            free_space: read_nloc(bytes, 44),
            key_free_list: read_nloc(bytes, 48),
            val_free_list: read_nloc(bytes, 52),
            data: (56 / 8..(bytes.len() - if header.object_type.ty == ObjectType::Btree { BTreeInfo::LEN } else { 0 }) / 8).map(|i| read_u64(bytes, i * 8)).collect(),
            info: if header.object_type.ty == ObjectType::Btree {
                Some(BTreeInfo::parse(&bytes[bytes.len() - BTreeInfo::LEN..]))
            } else {
                None
            }
        }
    }
}
