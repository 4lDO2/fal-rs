#![cfg_attr(not(feature = "std"), no_std)]

pub extern crate libc;

extern crate alloc;

use alloc::{boxed::Box, vec::Vec};

use core::{
    convert::TryFrom,
    mem,
    ops::{Add, Div, Mul, Rem},
};
pub use uuid::Uuid;

#[derive(Clone, Copy, Debug, Default, Eq, Hash, PartialEq)]
pub struct Timespec {
    pub sec: i64,
    pub nsec: i32,
}

impl Timespec {
    pub const fn new(sec: i64, nsec: i32) -> Self {
        Self { sec, nsec }
    }
}

pub fn read_uuid(block: &[u8], offset: usize) -> Uuid {
    uuid::Builder::from_slice(&block[offset..offset + 16])
        .unwrap()
        .build()
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

/// More ergonomic parsing functions.
pub mod parsing {
    use uuid::Uuid;

    pub fn read_u8(block: &[u8], offset: &mut usize) -> u8 {
        let ret = super::read_u8(block, *offset);
        *offset += 1;
        ret
    }
    pub fn read_u16(block: &[u8], offset: &mut usize) -> u16 {
        let ret = super::read_u16(block, *offset);
        *offset += 2;
        ret
    }
    pub fn read_u32(block: &[u8], offset: &mut usize) -> u32 {
        let ret = super::read_u32(block, *offset);
        *offset += 4;
        ret
    }
    pub fn read_u64(block: &[u8], offset: &mut usize) -> u64 {
        let ret = super::read_u64(block, *offset);
        *offset += 8;
        ret
    }
    pub fn read_uuid(block: &[u8], offset: &mut usize) -> Uuid {
        let ret = super::read_uuid(block, *offset);
        *offset += 16;
        ret
    }
    pub fn write_u8(block: &mut [u8], offset: &mut usize, number: u8) {
        super::write_u8(block, *offset, number);
        *offset += 1;
    }
    pub fn write_u16(block: &mut [u8], offset: &mut usize, number: u16) {
        super::write_u16(block, *offset, number);
        *offset += 2;
    }
    pub fn write_u32(block: &mut [u8], offset: &mut usize, number: u32) {
        super::write_u32(block, *offset, number);
        *offset += 4;
    }
    pub fn write_u64(block: &mut [u8], offset: &mut usize, number: u64) {
        super::write_u64(block, *offset, number);
        *offset += 8;
    }
    pub fn write_uuid(block: &mut [u8], offset: &mut usize, uuid: &Uuid) {
        super::write_uuid(block, *offset, uuid);
        *offset += 16;
    }
    pub fn skip(offset: &mut usize, amount: usize) -> &mut usize {
        *offset += amount;
        offset
    }
    pub fn assert_eq(offset: &mut usize, eq: usize) -> &mut usize {
        assert_eq!(*offset, eq);
        offset
    }
}

pub struct DeviceError {
    inner: Box<dyn IoError>,
}
impl DeviceError {
    pub fn inner(&self) -> &dyn IoError {
        &*self.inner
    }
}
impl core::fmt::Debug for DeviceError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Debug::fmt(&*self.inner, f)
    }
}
impl core::fmt::Display for DeviceError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        core::fmt::Display::fmt(&*self.inner, f)
    }
}
impl std::error::Error for DeviceError {}

impl<E: IoError> From<E> for DeviceError {
    fn from(err: E) -> Self {
        Self {
            inner: Box::new(err),
        }
    }
}

/// A trait which allows device I/O errors to get some kind of abstraction.
pub trait IoError: core::fmt::Debug + core::fmt::Display + std::error::Error + 'static {
    /// Returns whether the operations that failed because of this error should retry. This
    /// corresponds to something like EINTR, not EAGAIN or EWOULDBLOCK.
    fn should_retry(&self) -> bool;
}

#[derive(Debug)]
pub struct WriteZeroError;

impl core::fmt::Display for WriteZeroError {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "wrote zero bytes")
    }
}
impl std::error::Error for WriteZeroError {}

#[derive(Debug)]
pub struct UnexpectedEof;

impl core::fmt::Display for UnexpectedEof {
    fn fmt(&self, f: &mut core::fmt::Formatter<'_>) -> core::fmt::Result {
        write!(f, "unexpected EOF")
    }
}
impl std::error::Error for UnexpectedEof {}

impl IoError for UnexpectedEof {
    fn should_retry(&self) -> bool {
        false
    }
}
impl IoError for WriteZeroError {
    fn should_retry(&self) -> bool {
        false
    }
}

/// Information about a physical disk.
#[derive(Clone, Copy, Debug)]
pub struct DiskInfo {
    pub block_size: u32,
    pub block_count: u64,
    pub sector_size: Option<u64>,
    pub is_rotational: Option<bool>,
}

/// A readonly block device, such as the file /dev/sda. Typically implemented by the frontend.
/// This trait doesn't require a seeking method, since all reads are supposed to be atomic (as in
/// that the seek and read call cannot be divided).
///
/// This trait only uses immutable references to self, so it's up to the implementer to use
/// locking, atomic I/O if possible, or single-threaded interior mutability.
pub trait DeviceRo: std::fmt::Debug {
    // TODO: Add support for querying bad sectors etc (or perhaps that's done via simply checking
    // if the block can actually be written to successfully, when its data was invalid (e.g. a
    // checksum)).
    // TODO: Concurrent I/O.
    // TODO: Trimming.
    // TODO: Async/await.

    /// Read blocks from the device at a specific offset. All blocks have to be read.
    fn read_blocks(&self, block: u64, buf: &mut [u8]) -> Result<(), DeviceError>;

    /// Retrieve miscellaneous information about the disk, such as block size, preferred alignment,
    /// number of blocks, etc.
    fn disk_info(&self) -> Result<DiskInfo, DeviceError>;

    /// Seek to an offset. This method isn't required, but only there for performance
    /// optimizations, mainly for HDDs. By default, this is a no-op.
    #[allow(unused_variables)]
    fn seek(&self, block: u64) -> Result<(), DeviceError> {
        Ok(())
    }
}
/// A read-write device.
pub trait Device: DeviceRo {
    /// Write blocks to the device at a specific offset. Unlike std's Write trait, this will error
    /// if all blocks weren't written.
    #[must_use]
    fn write_blocks(&self, block: u64, buf: &[u8]) -> Result<(), DeviceError>;

    // TODO: Flushing

    /// Discard a block (TRIM), to inform hardware that the block doesn't have to be stored
    /// anymore. Only available on some command sets and hardware, and OSes. By default this is a
    /// no-op.
    #[allow(unused_variables)]
    fn discard(&self, start_block: u64, count: u64) -> Result<(), DeviceError> {
        Ok(())
    }
}

#[cfg(feature = "std")]
mod file_device {
    use super::*;
    use std::io::{self, prelude::*};
    use std::sync::Mutex;

    pub struct BasicDevice<D> {
        // TODO: Use a concurrent tree to instead lock based on ranges.
        device: Mutex<D>,
    }
    impl<D> BasicDevice<D> {
        const BLOCK_SIZE: u32 = 512;

        pub fn new(inner: D) -> Self {
            Self {
                device: Mutex::new(inner),
            }
        }
        pub fn into_inner(self) -> D {
            self.device.into_inner().unwrap()
        }
    }
    #[derive(Debug)]
    struct NewOffsetFromSeekMismatch {
        requested: u64,
        got: u64,
    }

    impl std::fmt::Display for NewOffsetFromSeekMismatch {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(
                f,
                "the resulting offset {} after a seek call didn't match the requested {}",
                self.requested, self.got
            )
        }
    }
    impl std::error::Error for NewOffsetFromSeekMismatch {}

    impl IoError for io::Error {
        fn should_retry(&self) -> bool {
            self.kind() == io::ErrorKind::Interrupted
        }
    }
    impl From<WriteZeroError> for io::Error {
        fn from(_err: WriteZeroError) -> Self {
            Self::from(io::ErrorKind::WriteZero)
        }
    }
    impl From<UnexpectedEof> for io::Error {
        fn from(_err: UnexpectedEof) -> Self {
            Self::from(io::ErrorKind::UnexpectedEof)
        }
    }

    impl<D> std::fmt::Debug for BasicDevice<D> {
        fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
            write!(f, "(device)")
        }
    }

    impl<D: Read + Seek> DeviceRo for BasicDevice<D> {
        fn read_blocks(&self, block: u64, buf: &mut [u8]) -> Result<(), DeviceError> {
            let mut guard = self.device.lock().unwrap();

            let offset = block * u64::from(Self::BLOCK_SIZE);
            let _ = guard.seek(io::SeekFrom::Start(offset))?;

            guard.read_exact(buf)?;
            // TODO: Check len
            Ok(())
        }

        fn disk_info(&self) -> Result<DiskInfo, DeviceError> {
            let size = self.device.lock().unwrap().seek(io::SeekFrom::End(0))?;

            Ok(DiskInfo {
                block_size: Self::BLOCK_SIZE,
                block_count: size / u64::from(Self::BLOCK_SIZE),
                is_rotational: None,
                sector_size: None,
            })
        }
    }
    impl<D: Read + Seek + Write> Device for BasicDevice<D> {
        fn write_blocks(&self, block: u64, buffer: &[u8]) -> Result<(), DeviceError> {
            let mut guard = self.device.lock().unwrap();

            let offset = block * u64::from(Self::BLOCK_SIZE);
            let _ = guard.seek(io::SeekFrom::Start(offset))?;

            guard.write_all(buffer)?;
            Ok(())
        }
    }
}

#[cfg(feature = "std")]
pub use file_device::*;

/// An abstract inode structure.
pub trait Inode: Clone {
    type InodeAddr: Into<u64> + Eq + core::fmt::Debug;

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

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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

    pub flags: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub struct FsAttributes {
    pub block_size: u32, // TODO: Fundamental FS block size (struct statvfs.f_frsize)?

    pub total_blocks: u64,
    pub free_blocks: u64,
    pub available_blocks: u64,

    pub inode_count: u64,
    pub free_inodes: u64,

    pub max_fname_len: u32,
}

#[derive(Debug)]
pub struct DirectoryEntry<InodeAddr: Into<u64>> {
    pub name: Vec<u8>,
    pub filetype: FileType,
    pub inode: InodeAddr,
    pub offset: u64,
}

#[derive(Debug)]
pub enum Error {
    BadFd,
    NoEntity,
    Overflow,
    Invalid,
    IsDirectory,
    NoData,
    FileTooBig,
    NotDirectory,
    AccessDenied,
    ReadonlyFs,
    Other(i32),
    Io,
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
            Self::ReadonlyFs => libc::EROFS,
            Self::FileTooBig => libc::EFBIG,
            Self::NoData => libc::ENODATA,
            Self::Io => libc::EIO,
        }
    }
}

#[cfg(feature = "std")]
impl std::error::Error for Error {}

impl core::fmt::Display for Error {
    fn fmt(&self, formatter: &mut core::fmt::Formatter) -> core::fmt::Result {
        match self {
            Error::NoEntity => write!(formatter, "no such file or directory"),
            Error::BadFd => write!(formatter, "bad file descriptor"),
            Error::Overflow => write!(formatter, "overflow"),
            Error::Invalid => write!(formatter, "invalid argument"),
            Error::Io => write!(formatter, "i/o error"),
            Error::IsDirectory => write!(formatter, "is directory"),
            Error::NotDirectory => write!(formatter, "not directory"),
            Error::AccessDenied => write!(formatter, "access denied"),
            Error::ReadonlyFs => write!(formatter, "read-only filesystem"),
            Error::FileTooBig => write!(formatter, "file too big"),
            Error::NoData => write!(formatter, "no data"),
            Error::Other(n) => write!(formatter, "other ({})", n),
        }
    }
}

pub type Result<T, E = Error> = core::result::Result<T, E>;

/// Describes how access times are handled within a filesystem implementation. These do not
/// directly correspond to the options documented in mount(8), but they are related.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub enum AccessTime {
    /// Update the access time of inodes when they are read.
    Atime,

    /// Don't update the access times of inodes.
    Noatime,

    /// Update the access times of inodes when they are changed or modified.
    Relatime,
    // TODO: strictatime, lazyatime?
}

#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct Options {
    /// Files are writable
    pub write: bool,

    /// Files are executable
    pub execute: bool,

    /// The underlying disk won't be touched, and metadata such as the last mount time won't be
    /// altered. This flag is automatically enabled if the user was not allowed to open the disk in
    /// write mode.
    pub immutable: bool,

    /// How file access times are written.
    pub f_atime: AccessTime,

    /// How directory access times are written.
    pub d_atime: AccessTime,
}

impl Default for Options {
    fn default() -> Self {
        Self {
            write: true,
            execute: true,
            f_atime: AccessTime::Relatime,
            d_atime: AccessTime::Relatime,
            immutable: false,
        }
    }
}

/// An abstract filesystem. Typically implemented by the backend.
pub trait Filesystem<D: Device>
where
    Self: Sized,
{
    /// An inode address. u32 on ext2.
    type InodeAddr: From<u32> + Into<u64> + Copy + TryFrom<u64> + Eq + core::fmt::Debug;

    /// An inode structure, capable of retrieving inode information.
    type InodeStruct: Inode + Clone;

    /// Any type that can represent filesystem-specific options.
    type Options: Default;

    /// The root inode address (for example 2 on ext2/3/4).
    fn root_inode(&self) -> Self::InodeAddr;

    // TODO: Support mounting multiple devices as one filesystem, for filesystems that support it.
    /// Mount the filesystem from a device. The path paramter is only used to change the "last
    /// mount path" for filesystems that support it.
    fn mount(
        device: D,
        general_options: Options,
        fs_specific_options: Self::Options,
        path: &[u8],
    ) -> Self;

    /// Unmount the filesystem.
    fn unmount(self);

    /// Load an inode structure from the filesystem. For example, on ext2, the address 2 would load the root directory.
    fn load_inode(&mut self, address: Self::InodeAddr) -> Result<Self::InodeStruct>;

    /// Open a file from an inode address.
    fn open_file(&mut self, inode: Self::InodeAddr) -> Result<u64>;

    /// Read bytes from a file.
    fn read(&mut self, fh: u64, offset: u64, buffer: &mut [u8]) -> Result<usize>;

    /// Write bytes to a file.
    fn write(&mut self, fh: u64, offset: u64, buffer: &[u8]) -> Result<u64>;

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

    /// Get a directory entry from a directory inode address and a name.
    fn lookup_direntry(
        &mut self,
        parent: Self::InodeAddr,
        name: &[u8],
    ) -> Result<DirectoryEntry<Self::InodeAddr>>;

    /// Read a symbolic link.
    fn readlink(&mut self, inode: Self::InodeAddr) -> Result<Box<[u8]>>;

    /// Get the offset of an open file handle.
    fn fh_offset(&self, fh: u64) -> u64;

    // XXX: Rust's type system doesn't support associated types with lifetimes. If a backend wants
    // to use a concurrent hashmap for storing the file handles, then there won't be a direct
    // reference, but an RAII guard. Basically all RAII guards store their owner as a reference,
    // and to replace the return type with a guard, an associated type with a lifetime is required.
    // On the other hand, inodes aren't typically that slow to clone. However with generic associated
    // types, this problem will likely be easily solved, as Self::InodeStruct can have an
    // untethered lifetime.
    //
    // https://github.com/rust-lang/rust/issues/44265
    //
    /// Retrieve a reference to data about an open file handle.
    fn fh_inode(&self, fh: u64) -> Self::InodeStruct;

    /// Set the current offset of a file handle.
    fn set_fh_offset(&mut self, fh: u64, offset: u64);

    /// Get the statvfs of the filesystem.
    fn filesystem_attrs(&self) -> FsAttributes;

    /// Write the inode metadata to disk.
    fn store_inode(&mut self, inode: &Self::InodeStruct) -> Result<()>;

    /// Remove an entry from a directory. If the hard link count of the inode pointed to by that
    /// entry reaches zero, the inode is deleted and its space is freed. However, if that inode is
    /// still open, it won't be removed until that.
    fn unlink(&mut self, parent: Self::InodeAddr, name: &[u8]) -> Result<()>;

    /// Get a named extended attribute from an inode.
    fn get_xattr(&mut self, inode: &Self::InodeStruct, name: &[u8]) -> Result<Vec<u8>>;

    /// List the extended attribute names (keys) from an inode.
    // TODO: Perhaps this double vector should be a null-separated u8 vector instead.
    fn list_xattrs(&mut self, inode: &Self::InodeStruct) -> Result<Vec<Vec<u8>>>;
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

pub fn align<T>(number: T, alignment: T) -> T
where
    T: Add<Output = T>
        + Copy
        + Div<Output = T>
        + Rem<Output = T>
        + From<u8>
        + Mul<Output = T>
        + PartialEq,
{
    (if number % alignment != T::from(0u8) {
        number / alignment + T::from(1u8)
    } else {
        number / alignment
    }) * alignment
}

pub fn div_round_up<T>(numer: T, denom: T) -> T
where
    T: Add<Output = T> + Copy + Div<Output = T> + Rem<Output = T> + From<u8> + PartialEq,
{
    if numer % denom != T::from(0u8) {
        numer / denom + T::from(1u8)
    } else {
        numer / denom
    }
}
pub fn round_up<T>(number: T, to: T) -> T
where
    T: Add<Output = T>
        + Copy
        + Div<Output = T>
        + Mul<Output = T>
        + Rem<Output = T>
        + From<u8>
        + PartialEq,
{
    div_round_up(number, to) * number
}
