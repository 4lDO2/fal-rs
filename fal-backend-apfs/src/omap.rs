use crate::{
    btree::{BTree, BTreeKey},
    checkpoint, read_block, read_obj_phys,
    superblock::NxSuperblock,
    BlockAddr, ObjPhys, ObjectIdentifier, ObjectTypeAndFlags, TransactionIdentifier,
};
use fal::parsing::{read_u32, read_u64};

use std::{cmp::Ordering, collections::HashMap};

use bitflags::bitflags;

#[derive(Debug)]
pub struct OmapPhys {
    pub header: ObjPhys,
    pub flags: u32,
    pub snapshot_count: u32,
    pub tree_type: ObjectTypeAndFlags,
    pub snapshot_tree_type: ObjectTypeAndFlags,
    pub tree_oid: ObjectIdentifier,
    pub snapshot_tree_oid: ObjectIdentifier,
    pub most_recent_snapshot: TransactionIdentifier,
    pub pending_revert_min: TransactionIdentifier,
    pub pending_revert_max: TransactionIdentifier,
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct OmapKey {
    pub oid: ObjectIdentifier,
    pub xid: TransactionIdentifier,
}

impl OmapKey {
    pub const LEN: usize = 16;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;
        Self {
            oid: read_u64(bytes, &mut offset).into(),
            xid: read_u64(bytes, &mut offset),
        }
    }
    pub fn partial(oid: ObjectIdentifier) -> Self {
        Self { oid, xid: 0 }
    }
    pub fn compare(k1: &BTreeKey, k2: &BTreeKey) -> Ordering {
        Ord::cmp(k1, k2)
    }
    // Skip the xids. Useful when you want to find the key with the greatest xid.
    pub fn compare_partial(k1: &BTreeKey, k2: &BTreeKey) -> Ordering {
        Ord::cmp(
            &k1.as_omap_key().unwrap().oid,
            &k2.as_omap_key().unwrap().oid,
        )
    }
}

impl PartialOrd for OmapKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for OmapKey {
    fn cmp(&self, other: &Self) -> Ordering {
        self.oid.cmp(&other.oid).then(self.xid.cmp(&other.xid))
    }
}

bitflags! {
    pub struct OmapValueFlags: u32 {
        const DELETED = 0x1;
        const SAVED = 0x2;
        const ENCRYPTED = 0x4;
        const NOHEADER = 0x8;
        const CRYPTO_GENERATION = 0x10;
    }
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct OmapValue {
    pub flags: OmapValueFlags,
    pub size: u32,
    pub paddr: BlockAddr,
}

impl OmapValue {
    pub const LEN: usize = 16;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;
        Self {
            flags: OmapValueFlags::from_bits(read_u32(bytes, &mut offset)).unwrap(),
            size: read_u32(bytes, &mut offset),
            paddr: read_u64(bytes, &mut offset) as i64,
        }
    }
}

#[derive(Clone, Copy, Debug)]
pub struct OmapSnapshot {
    pub flags: u32,
    pub padding: u32,
    pub oid: ObjectIdentifier,
}

impl OmapPhys {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        Self {
            header: read_obj_phys(bytes, &mut offset),
            flags: read_u32(bytes, &mut offset),
            snapshot_count: read_u32(bytes, &mut offset),
            tree_type: ObjectTypeAndFlags::from_raw(read_u32(bytes, &mut offset)),
            snapshot_tree_type: ObjectTypeAndFlags::from_raw(read_u32(bytes, &mut offset)),
            tree_oid: read_u64(bytes, &mut offset).into(),
            snapshot_tree_oid: read_u64(bytes, &mut offset).into(),
            most_recent_snapshot: read_u64(bytes, &mut offset),
            pending_revert_min: read_u64(bytes, &mut offset),
            pending_revert_max: read_u64(bytes, &mut offset),
        }
    }
}

#[derive(Debug)]
pub struct Omap {
    pub omap: OmapPhys,
    pub tree: BTree,
}

impl Omap {
    pub fn load<D: fal::DeviceRo>(device: &D, superblock: &NxSuperblock, baddr: BlockAddr) -> Self {
        let block = read_block(superblock, device, baddr);

        let omap = OmapPhys::parse(&block);

        if omap.tree_type.is_virtual() {
            unimplemented!("Omaps with virtual addresses aren't implemented");
        }

        let tree = BTree::load(device, superblock, omap.tree_oid.0 as i64);

        Self { omap, tree }
    }
    pub fn get<D: fal::DeviceRo>(
        &self,
        device: &D,
        superblock: &NxSuperblock,
        key: OmapKey,
    ) -> Option<OmapValue> {
        // Make sure that we aren't using a partial key accidentally.
        debug_assert_ne!(key.xid, 0);
        self.tree
            .get(
                device,
                superblock,
                Resolver::Physical,
                &BTreeKey::OmapKey(key),
            )
            .map(|v| v.into_omap_value().unwrap())
    }
    pub fn get_partial<'a, D: fal::DeviceRo>(
        &'a self,
        device: &'a D,
        superblock: &'a NxSuperblock,
        key: OmapKey,
    ) -> Option<impl Iterator<Item = (OmapKey, OmapValue)> + 'a> {
        self.tree
            .similar_pairs(
                device,
                superblock,
                Resolver::Virtual(self),
                &BTreeKey::OmapKey(key),
                OmapKey::compare_partial,
            )
            .map(|iter| {
                iter.map(|(k, v)| (k.into_omap_key().unwrap(), v.into_omap_value().unwrap()))
            })
    }
    pub fn get_partial_latest<'a, D: fal::DeviceRo>(
        &'a self,
        device: &'a D,
        superblock: &'a NxSuperblock,
        key: OmapKey,
    ) -> Option<(OmapKey, OmapValue)> {
        match self.get_partial(device, superblock, key).map(|iter| {
            iter.filter(|(k, _)| {
                OmapKey::compare_partial(&BTreeKey::OmapKey(*k), &BTreeKey::OmapKey(key))
                    != Ordering::Greater
            })
            .max_by_key(|(k, _)| k.xid)
        }) {
            Some(Some(t)) => Some(t),
            _ => None,
        }
    }
}

pub type EphemeralMap = HashMap<ObjectIdentifier, checkpoint::GenericObject>;

#[derive(Clone, Copy, Debug)]
pub enum Resolver<'a, 'b> {
    Physical,
    Virtual(&'a Omap),
    Ephemeral(&'b EphemeralMap),
}

impl Resolver<'_, '_> {
    pub fn is_physical(self) -> bool {
        if let Self::Physical = self {
            true
        } else {
            false
        }
    }
    pub fn is_virtual(self) -> bool {
        if let Self::Virtual(_) = self {
            true
        } else {
            false
        }
    }
    pub fn is_ephemeral(self) -> bool {
        if let Self::Ephemeral(_) = self {
            true
        } else {
            false
        }
    }
}
