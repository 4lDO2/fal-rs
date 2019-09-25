pub extern crate libc;
pub extern crate time;

use std::{
    convert::TryFrom,
    ffi::{OsStr, OsString},
    io::{self, prelude::*},
    mem,
};
use time::Timespec;
use uuid::Uuid;

pub fn read_uuid(block: &[u8], offset: usize) -> Uuid {
    let mut bytes = [0u8; 16];
    bytes.copy_from_slice(&block[offset..offset + 16]);
    uuid::builder::Builder::from_bytes(bytes).build()
}
pub fn read_u64(block: &[u8], offset: usize) -> u64 {
    let mut bytes = [0u8; mem::size_of::<u64>()];
    bytes.copy_from_slice(&block[offset..offset + mem::size_of::<u64>()]);
    u64::from_le_bytes(bytes)
}
pub fn read_u32(block: &[u8], offset: usize) -> u32 {
    let mut bytes = [0u8; mem::size_of::<u32>()];
    bytes.copy_from_slice(&block[offset..offset + mem::size_of::<u32>()]);
    u32::from_le_bytes(bytes)
}
pub fn read_u16(block: &[u8], offset: usize) -> u16 {
    let mut bytes = [0u8; mem::size_of::<u16>()];
    bytes.copy_from_slice(&block[offset..offset + mem::size_of::<u16>()]);
    u16::from_le_bytes(bytes)
}
pub fn read_u8(block: &[u8], offset: usize) -> u8 {
    let mut bytes = [0u8; mem::size_of::<u8>()];
    bytes.copy_from_slice(&block[offset..offset + mem::size_of::<u8>()]);
    u8::from_le_bytes(bytes)
}
pub fn write_uuid(block: &mut [u8], offset: usize, uuid: &Uuid) {
    let bytes = uuid.as_bytes();
    block[offset..offset + bytes.len()].copy_from_slice(bytes);
}
pub fn write_u32(block: &mut [u8], offset: usize, number: u32) {
    let bytes = number.to_le_bytes();
    block[offset..offset + bytes.len()].copy_from_slice(&bytes)
}
pub fn write_u64(block: &mut [u8], offset: usize, number: u64) {
    let bytes = number.to_le_bytes();
    block[offset..offset + bytes.len()].copy_from_slice(&bytes)
}
pub fn write_u16(block: &mut [u8], offset: usize, number: u16) {
    let bytes = number.to_le_bytes();
    block[offset..offset + bytes.len()].copy_from_slice(&bytes)
}
pub fn write_u8(block: &mut [u8], offset: usize, number: u8) {
    let bytes = number.to_le_bytes();
    block[offset..offset + bytes.len()].copy_from_slice(&bytes)
}

/// An readonly device, such as the file /dev/sda. Typically implemented by the frontend.
pub trait Device: Read + Seek {
    // TODO: Add support for querying bad sectors etc.
    // TODO: Remove the std::io traits, TODO: right?.
}

impl Device for std::fs::File {}
impl Device for &mut std::fs::File {}
impl DeviceMut for std::fs::File {}
impl DeviceMut for &mut std::fs::File {}

/// A read-write device.
pub trait DeviceMut: Device + Write {}

/// An abstract inode structure.
pub trait Inode: Clone {
    type InodeAddr: Into<u64> + Eq + std::fmt::Debug;

    fn generation_number(&self) -> Option<u64>;
    fn addr(&self) -> Self::InodeAddr;
    fn attrs(&self) -> Attributes<Self::InodeAddr>;

    fn set_perm(&mut self, permissions: u16);
    fn set_uid(&mut self, uid: u32);
    fn set_gid(&mut self, gid: u32);
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum FileType {
    RegularFile,
    Directory,
    BlockDevice,
    Symlink,
    NamedPipe,
    Socket,
    CharacterDevice,
}

pub struct Attributes<InodeAddr: Into<u64>> {
    pub filetype: FileType,
    pub size: u64,
    pub block_count: u64,
    pub hardlink_count: u64,
    pub permissions: u16,
    pub user_id: u32,
    pub group_id: u32,
    pub rdev: u32,
    pub inode: InodeAddr,

    pub creation_time: Timespec,
    pub modification_time: Timespec,
    pub change_time: Timespec,
    pub access_time: Timespec,

    /// macOS only
    pub flags: u32,
}

pub struct FsAttributes {
    pub block_size: u32, // TODO: Fundamental FS block size (struct statvfs.f_frsize)?

    pub total_blocks: u32,
    pub free_blocks: u32,
    pub available_blocks: u32,

    pub inode_count: u64,
    pub free_inodes: u64,

    pub max_fname_len: u32,
}

pub struct DirectoryEntry<InodeAddr: Into<u64>> {
    pub name: OsString,
    pub filetype: FileType,
    pub inode: InodeAddr,
    pub offset: u64,
}

pub trait FileHandle {
    type InodeStruct: Inode;

    fn fh(&self) -> u64;

    fn offset(&self) -> u64;
    fn set_offset(&mut self, offset: u64);

    fn inode(&self) -> &Self::InodeStruct;
}

#[derive(Debug)]
pub enum Error {
    BadFd,
    NoEntity,
    Overflow,
    Invalid,
    IsDirectory,
    NotDirectory,
    AccessDenied,
    Other(i32),
    Io(io::Error),
}
impl Error {
    pub fn errno(&self) -> i32 {
        match self {
            Self::BadFd => libc::EBADF,
            Self::NoEntity => libc::ENOENT,
            Self::Other(n) => *n,
            Self::Overflow => libc::EOVERFLOW,
            Self::Invalid => libc::EINVAL,
            Self::IsDirectory => libc::EISDIR,
            Self::NotDirectory => libc::ENOTDIR,
            Self::AccessDenied => libc::EACCES,
            Self::Io(_) => libc::EIO,
        }
    }
}

impl std::error::Error for Error {}

impl std::fmt::Display for Error {
    fn fmt(&self, formatter: &mut std::fmt::Formatter) -> std::fmt::Result {
        match self {
            Error::NoEntity => writeln!(formatter, "no such file or directory"),
            Error::BadFd => writeln!(formatter, "bad file descriptor"),
            Error::Overflow => writeln!(formatter, "overflow"),
            Error::Invalid => writeln!(formatter, "invalid argument"),
            Error::Io(err) => writeln!(formatter, "i/o error: `{}`", err),
            Error::IsDirectory => writeln!(formatter, "is directory"),
            Error::NotDirectory => writeln!(formatter, "not directory"),
            Error::AccessDenied => writeln!(formatter, "access denied"),
            Error::Other(n) => writeln!(formatter, "other ({})", n),
        }
    }
}

impl From<io::Error> for Error {
    fn from(error: io::Error) -> Self {
        Self::Io(error)
    }
}

pub type Result<T> = std::result::Result<T, Error>;

// TODO: Add some kind of Result type.
/// An abstract filesystem. Typically implemented by the backend.
pub trait Filesystem<D: Device>
where
    Self: Sized,
{
    /// An inode address. u32 on ext2.
    type InodeAddr: From<u32> + Into<u64> + Copy + TryFrom<u64> + Eq + std::fmt::Debug;

    /// An inode structure, capable of retrieving inode information.
    type InodeStruct: Inode;

    type FileHandle: FileHandle;

    /// The root inode address (for example 2 on ext2/3/4).
    fn root_inode(&self) -> Self::InodeAddr;

    // TODO: Support mounting multiple devices as one filesystem, for filesystems that support it.
    /// Mount the filesystem from a device. The path paramter is only used to change the "last
    /// mount path" for filesystems that supports it".
    fn mount(device: D, path: &OsStr) -> Self;

    /// Unmount the filesystem.
    fn unmount(self);

    /// Load an inode structure from the filesystem. For example, on ext2, the address 2 would load the root directory.
    fn load_inode(&mut self, address: Self::InodeAddr) -> Result<Self::InodeStruct>;

    /// Open a file from an inode address.
    fn open_file(&mut self, inode: Self::InodeAddr) -> Result<u64>;

    /// Read bytes from a file.
    fn read(&mut self, fh: u64, offset: u64, buffer: &mut [u8]) -> Result<usize>;

    /// Close an opened file or directory. The filesystem implementation will ensure that unread bytes get
    /// flushed before closing.
    fn close(&mut self, file: u64) -> Result<()>;

    /// Open a directory from an inode address.
    fn open_directory(&mut self, address: Self::InodeAddr) -> Result<u64>;

    /// Get an entry from a directory.
    fn read_directory(
        &mut self,
        directory: u64,
        offset: i64,
    ) -> Result<Option<DirectoryEntry<Self::InodeAddr>>>;

    /// Get a directory entry from a directory inode and a name.
    fn lookup_direntry(
        &mut self,
        parent: Self::InodeAddr,
        name: &OsStr,
    ) -> Result<DirectoryEntry<Self::InodeAddr>>;

    /// Read a symlink.
    fn readlink(&mut self, inode: Self::InodeAddr) -> Result<Box<[u8]>>;

    /// Retrieve a reference to data about an open file handle.
    fn fh(&self, fh: u64) -> &Self::FileHandle;

    /// Retrieve a mutable reference to file handle data.
    fn fh_mut(&mut self, fh: u64) -> &mut Self::FileHandle;

    /// Get the statvfs of the filesystem.
    fn filesystem_attrs(&self) -> FsAttributes;
}

pub trait FilesystemMut<D: DeviceMut>: Filesystem<D>
where
    Self: Sized,
{
    fn unmount(self) {}

    /// Write the inode metadata to disk.
    fn store_inode(&mut self, inode: &Self::InodeStruct) -> Result<()>;
}

#[derive(Debug)]
pub struct Permissions {
    pub read: bool,
    pub write: bool,
    pub execute: bool,
}

/// Parse a single octal digit of permissions into three booleans.
pub fn mask_permissions(mask: u8) -> Permissions {
    Permissions {
        read: mask & 0o4 != 0,
        write: mask & 0o2 != 0,
        execute: mask & 0o1 != 0,
    }
}
/// Check which permissions a user of a certain group has on a file with the specified attributes.
pub fn check_permissions<A: Into<u64>>(uid: u32, gid: u32, attrs: &Attributes<A>) -> Permissions {
    if attrs.user_id == uid {
        let user_mask = (attrs.permissions & 0o700) >> 6;
        mask_permissions(user_mask as u8)
    } else if attrs.group_id == gid {
        let group_mask = (attrs.permissions & 0o070) >> 3;
        mask_permissions(group_mask as u8)
    } else {
        let others_mask = attrs.permissions & 0o007;
        mask_permissions(others_mask as u8)
    }
}
