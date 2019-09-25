use std::{
    convert::{TryFrom, TryInto},
    ops::{BitAnd, BitOr, Range},
};
use fal::{read_u16, read_u32, read_u64};
use crate::superblock::ObjPhys;

pub type NodeLocation = Range<u16>;

pub fn read_nloc(bytes: &[u8], off: usize) -> NodeLocation {
    read_u16(bytes, off)..read_u16(bytes, off + 2)
}

#[derive(Clone, Copy, Eq, PartialEq)]
pub struct BTreeNodeFlags(pub u16);

impl BTreeNodeFlags {
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
        Self::u64_keys() | Self::sequential_insert() | Self::allow_ghosts() | Self::ephemeral() | Self::physical() | Self::nonpersistent() | Self::key_val_nonaligned()
    }
}

#[derive(Debug)]
pub struct BTreeNodeFlagsUnknown(pub u16);

impl TryFrom<u16> for BTreeNodeFlags {
    type Error = BTreeNodeFlagsUnknown;
    fn try_from(raw: u16) -> Result<Self, Self::Error> {
        let outside = raw & !(Self::all().0);
        if outside != 0 {
            return Err(BTreeNodeFlagsUnknown(outside))
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
            data: vec! [], // FIXME
        }
    }
}
