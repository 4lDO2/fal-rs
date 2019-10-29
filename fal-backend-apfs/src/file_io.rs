use crate::{
    btree::{BTreeKey, Path},
    fsobjects::{
        ChildrenOrHardlinkCount, InodeType, JDrecType, JFileExtentKey, JFileExtentVal, JInodeKey,
        JInodeVal,
    },
};
use fal::time::Timespec;

#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Inode {
    pub key: JInodeKey,
    pub value: JInodeVal,
    pub block_size: u32,
}

impl Inode {
    pub const INVALID_INODE_ADDR: u64 = 0;
    pub const ROOT_DIR_PARENT_INODE_ADDR: u64 = 1;
    pub const ROOT_DIR_INODE_ADDR: u64 = 2;
    pub const PRIVATE_DIR_INODE_ADDR: u64 = 2;
    pub const SNAPSHOT_DIR_INODE_ADDR: u64 = 2;
    pub const MIN_USER_INODE_ADDR: u64 = 16;
}

impl fal::Inode for Inode {
    type InodeAddr = u64;

    fn generation_number(&self) -> Option<u64> {
        Some(self.value.write_generation_counter.into())
    }
    fn addr(&self) -> u64 {
        self.key.header.oid.into()
    }
    fn attrs(&self) -> fal::Attributes<u64> {
        fal::Attributes {
            access_time: nanosecs_to_timespec(self.value.access_time),
            creation_time: nanosecs_to_timespec(self.value.create_time),
            modification_time: nanosecs_to_timespec(self.value.modification_time),
            change_time: nanosecs_to_timespec(self.value.change_time),

            block_count: self.value.block_count(self.block_size),
            size: self.value.size(),

            filetype: self.value.ty.into(),
            flags: self.value.bsd_flags,
            group_id: self.value.gid,
            user_id: self.value.uid,
            hardlink_count: match self.value.children_or_hardlink_count {
                ChildrenOrHardlinkCount::ChildrenCount(_) => 1,
                ChildrenOrHardlinkCount::HardlinkCount(c) => c as u64,
            },
            inode: self.key.header.oid.into(),
            permissions: self.value.permissions,
            rdev: self.value.rdev(),
        }
    }
    fn set_perm(&mut self, permissions: u16) {
        self.value.permissions = permissions;
    }
    fn set_uid(&mut self, uid: u32) {
        self.value.uid = uid;
    }
    fn set_gid(&mut self, gid: u32) {
        self.value.gid = gid;
    }
}
impl From<InodeType> for fal::FileType {
    fn from(ty: InodeType) -> Self {
        match ty {
            InodeType::File => fal::FileType::RegularFile,
            InodeType::Dir => fal::FileType::Directory,
            InodeType::BlockDev => fal::FileType::BlockDevice,
            InodeType::CharDev => fal::FileType::CharacterDevice,
            InodeType::Fifo => fal::FileType::NamedPipe,
            InodeType::Socket => fal::FileType::Socket,
            InodeType::Symlink => fal::FileType::Symlink,
            InodeType::Whiteout => unimplemented!(),
        }
    }
}
impl From<JDrecType> for fal::FileType {
    fn from(ty: JDrecType) -> Self {
        match ty {
            JDrecType::Unknown => unimplemented!(),
            JDrecType::File => fal::FileType::RegularFile,
            JDrecType::Dir => fal::FileType::Directory,
            JDrecType::BlockDev => fal::FileType::BlockDevice,
            JDrecType::CharDev => fal::FileType::CharacterDevice,
            JDrecType::Fifo => fal::FileType::NamedPipe,
            JDrecType::Socket => fal::FileType::Socket,
            JDrecType::Symlink => fal::FileType::Symlink,
            JDrecType::Whiteout => unimplemented!(),
        }
    }
}
fn nanosecs_to_timespec(ns: u64) -> Timespec {
    let secs: u64 = ns / 1_000_000_000;
    let nsec: u32 = (ns % 1_000_000_000) as u32;
    Timespec::new(secs as i64, nsec as i32)
}

#[derive(Debug)]
pub struct FileHandle {
    pub(crate) fh: u64,
    pub(crate) offset: u64,
    pub(crate) inode: Inode,
    pub(crate) extra: Option<Extra>,
    pub(crate) current_extent: Option<(JFileExtentKey, JFileExtentVal)>,
}

#[derive(Debug)]
pub struct Extra {
    pub(crate) path: Path<'static>,
    pub(crate) previous_key: Option<BTreeKey>,
}
