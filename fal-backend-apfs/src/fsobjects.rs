use crate::{
    btree::{BTreeKey, BTreeValue},
    crypto::ProtectionClass,
    ObjectIdentifier, ObjectTypeFlags,
};

use std::cmp::Ordering;

use fal::parsing::{read_u16, read_u32, read_u64};

type ArrayString = arrayvec::ArrayString<[u8; 128]>;
use enum_primitive::*;

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct JKey {
    pub oid: ObjectIdentifier,
    pub ty: JObjType,
}

enum_from_primitive! {
    #[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
    pub enum JObjType {
        Any = 0,
        SnapshotMetadata = 1,
        Extent = 2,
        Inode = 3,
        Xattr = 4,
        SiblingLink = 5,
        DataStreamId = 6,
        CryptoState = 7,
        FileExtent = 8,
        DirRecord = 9,
        DirStats = 10,
        SnapshotName = 11,
        SiblingMap = 12,
    }
}

impl JKey {
    const OBJECT_ID_MASK: u64 = 0x0FFF_FFFF_FFFF_FFFF;
    const OBJECT_TYPE_MASK: u64 = 0xF000_0000_0000_0000;
    const OBJECT_TYPE_SHIFT: u64 = 60;

    const LEN: usize = 8;

    pub fn parse(bytes: &[u8]) -> Self {
        let object_id_and_type = fal::read_u64(bytes, 0);

        let oid = ObjectIdentifier::from(object_id_and_type & Self::OBJECT_ID_MASK);
        let ty = JObjType::from_u8(((object_id_and_type & Self::OBJECT_TYPE_MASK) >> Self::OBJECT_TYPE_SHIFT) as u8).unwrap();

        Self {
            oid,
            ty,
        }
    }
}

impl Ord for JKey {
    fn cmp(&self, with: &Self) -> Ordering {
        Ord::cmp(&self.oid, &with.oid)
            .then(Ord::cmp(&self.ty, &with.ty))
    }
}
impl PartialOrd for JKey {
    fn partial_cmp(&self, with: &Self) -> Option<Ordering> {
        Some(self.cmp(with))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JInodeKey {
    pub header: JKey,
}

#[derive(Clone, Debug)]
pub struct JInodeVal {
    pub parent_id: u64,
    pub private_id: u64,

    pub create_time: u64,
    pub modification_time: u64,
    pub change_time: u64,
    pub access_time: u64,

    pub internal_flags: u64,

    pub children_or_hardlink_count: i32,

    pub default_protection_class: ProtectionClass,
    pub write_generation_counter: u32,
    pub bsd_flags: u32,

    pub uid: u32,
    pub gid: u32,
    pub mode: u16,

    pub pad1: u16,
    pub pad2: u16,

    // TODO: Extended fields
}

impl JInodeVal {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        Self {
            parent_id: read_u64(bytes, &mut offset),
            private_id: read_u64(bytes, &mut offset),

            create_time: read_u64(bytes, &mut offset),
            modification_time: read_u64(bytes, &mut offset),
            change_time: read_u64(bytes, &mut offset),
            access_time: read_u64(bytes, &mut offset),

            internal_flags: read_u64(bytes, &mut offset),
            children_or_hardlink_count: read_u32(bytes, &mut offset) as i32,
            default_protection_class: ProtectionClass::from_u32(read_u32(bytes, &mut offset)).unwrap(),
            write_generation_counter: read_u32(bytes, &mut offset),
            bsd_flags: read_u32(bytes, &mut offset),

            uid: read_u32(bytes, &mut offset),
            gid: read_u32(bytes, &mut offset),
            mode: read_u16(bytes, &mut offset),

            pad1: read_u16(bytes, &mut offset),
            pad2: read_u16(bytes, &mut offset),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct JDrecKey {
    pub header: JKey,
    pub name: ArrayString,
}

impl JDrecKey {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;
        let header = read_jkey(bytes, &mut offset);
        let name_len = read_u16(bytes, &mut offset) as usize;
        dbg!(offset, name_len);
        let name = String::from_utf8(bytes[offset..offset + (name_len - 1)].to_owned()).unwrap();

        Self {
            header,
            name: ArrayString::from(&name).unwrap(), // FIXME
        }
    }
}

impl Ord for JDrecKey {
    fn cmp(&self, with: &Self) -> Ordering {
        Ord::cmp(&self.header, &with.header)
            .then(Ord::cmp(&self.name, &with.name))
    }
}
impl PartialOrd for JDrecKey {
    fn partial_cmp(&self, with: &Self) -> Option<Ordering> {
        Some(self.cmp(with))
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct JDrecHashedKey {
    pub header: JKey,
    pub hash: u32,
    pub name: ArrayString,
}

impl Ord for JDrecHashedKey {
    fn cmp(&self, with: &Self) -> Ordering {
        Ord::cmp(&self.header, &with.header)
            .then(Ord::cmp(&self.name, &with.name))
    }
}
impl PartialOrd for JDrecHashedKey {
    fn partial_cmp(&self, with: &Self) -> Option<Ordering> {
        Some(self.cmp(with))
    }
}

impl JDrecHashedKey {
    const LEN_MASK: u32 = 0x0000_03FF;
    const HASH_MASK: u32 = 0xFFFF_F400;
    const HASH_SHIFT: u32 = 10;

    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;
        let header = read_jkey(bytes, &mut offset);
        let name_len_and_hash = read_u32(bytes, &mut offset);

        let name_len = (name_len_and_hash & Self::LEN_MASK) as usize;
        let hash = (name_len_and_hash & Self::HASH_MASK) >> Self::HASH_SHIFT;

        let name = String::from_utf8(bytes[offset..offset + (name_len - 1)].to_owned()).unwrap();

        Self {
            header,
            name: ArrayString::from(&name).unwrap(), // FIXME
            hash,
        }
    }
}

#[derive(Clone, Debug)]
pub struct JDrecVal {
    pub file_id: u64,
    pub date_added: u64,
    pub flags: u16,

    // TODO: Extented fields
}

impl JDrecVal {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        Self {
            file_id: read_u64(bytes, &mut offset),
            date_added: read_u64(bytes, &mut offset),
            flags: read_u16(bytes, &mut offset),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, Ord, PartialEq, PartialOrd)]
pub struct JDirStatsKey {
    pub header: JKey,
}

#[derive(Debug)]
pub struct JDirStatsVal {
    pub children_count: u64,
    pub total_size: u64,
    pub chained_key: u64,
    pub gen_count: u64,
}

impl JDirStatsVal {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        Self {
            children_count: read_u64(bytes, &mut offset),
            total_size: read_u64(bytes, &mut offset),
            chained_key: read_u64(bytes, &mut offset),
            gen_count: read_u64(bytes, &mut offset),
        }
    }
}

#[derive(Clone, Copy, Debug, Eq, Hash, PartialEq)]
pub struct JXattrKey {
    pub header: JKey,
    pub name: ArrayString,
}

impl JXattrKey {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        let header = read_jkey(bytes, &mut offset);
        let name_length = read_u16(bytes, &mut offset) as usize;
        let name = String::from_utf8(bytes[offset .. offset + (name_length - 1)].to_owned()).unwrap();

        Self {
            header,
            name: ArrayString::from(&name).unwrap(), // FIXME
        }
    }
}

impl Ord for JXattrKey {
    fn cmp(&self, with: &Self) -> Ordering {
        Ord::cmp(&self.header, &with.header)
            .then(Ord::cmp(&self.name, &with.name))
    }
}
impl PartialOrd for JXattrKey {
    fn partial_cmp(&self, with: &Self) -> Option<Ordering> {
        Some(self.cmp(with))
    }
}

#[derive(Debug)]
pub struct JXattrVal {
    pub flags: u16,
    pub len: u16,
    // TODO
}

impl JXattrVal {
    pub fn parse(bytes: &[u8]) -> Self {
        let mut offset = 0;

        Self {
            flags: read_u16(bytes, &mut offset),
            len: read_u16(bytes, &mut offset),
        }
    }
}

pub fn read_jkey(bytes: &[u8], offset: &mut usize) -> JKey {
    let jkey = JKey::parse(&bytes[..JKey::LEN]);
    *offset += JKey::LEN;
    jkey
}

impl BTreeKey {
    pub fn parse_jkey(bytes: &[u8]) -> Self {
        let base = JKey::parse(bytes);

        match base.ty {
            JObjType::Any => Self::AnyKey(base),
            JObjType::Inode => Self::InodeKey(JInodeKey { header: base }),
            JObjType::DirRecord => Self::DrecHashedKey(JDrecHashedKey::parse(bytes)),
            other => unimplemented!("{:?}", other),
        }
    }
}
impl BTreeValue {
    pub fn parse_from_jkey(jkey: &JKey, bytes: &[u8]) -> Self {
        match jkey.ty {
            JObjType::Inode => Self::Inode(JInodeVal::parse(bytes)),
            JObjType::DirRecord => Self::DirRecord(JDrecVal::parse(bytes)),
            other => unimplemented!("value for key {:?}", other),
        }
    }
}
