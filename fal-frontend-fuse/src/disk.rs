use std::fs::File;
use std::io::prelude::*;
use std::ops::Range;
use std::thread::ThreadId;
use std::io;

use fal::DeviceRo as _;

use cranelift_bforest::Map;
use crossbeam_queue::ArrayQueue;
use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};
use std::os::unix::fs::{FileExt, FileTypeExt, MetadataExt};

// TODO: io_uring, when async fn works in traits and the io_uring ecosystem gets better.

pub type Block = u64;

pub struct Device {
    file: File,

    last_cached_disk_info: Mutex<Option<fal::DiskInfo>>,

    // TODO: Concurrent struct
    locked_ranges: RwLock<Map<Block, u64>>,
}

#[cfg(target_os = "linux")]
mod linux_ioctls {
    use std::convert::TryFrom;
    use std::io;

    use thiserror::Error;

    // actual types based on block/ioctl.c in the Linux kernel source, and the numbers are defined
    // in <linux/fs.h>.
    nix::ioctl_read!(linux_blk_get_blocksize, 0x12, 112, libc::c_int); // BLKBSZGET
    nix::ioctl_read!(linux_blk_get_sectorsize, 0x12, 104, libc::c_int); // BLKSSZGET
    nix::ioctl_read!(linux_blk_rotational, 0x12, 126, libc::c_ushort); // BLKROTATIONAL

    /// Get the block size of a block device.
    ///
    /// # Safety
    /// `fd` has to point to a valid file descriptor, representing a real block device.
    pub unsafe fn get_blocksize(fd: libc::c_int) -> Result<u32, IoctlError> {
        let mut size: libc::c_int = 0;
        match linux_blk_get_blocksize(fd, &mut size) {
            Ok(_) => (),
            Err(error) => return Err(IoctlError::IoError(error)),
        }
        u32::try_from(size).or(Err(IoctlError::Overflow))
    }
    /// Get the physical sector size of a block device.
    ///
    /// # Safety
    /// `fd` has to point to a valid file descriptor, representing a real block device.
    pub unsafe fn get_sectorsize(fd: libc::c_int) -> Result<u32, IoctlError> {
        let mut size: libc::c_int = 0;
        match linux_blk_get_sectorsize(fd, &mut size) {
            Ok(_) => (),
            Err(error) => return Err(IoctlError::IoError(error)),
        }
        u32::try_from(size).or(Err(IoctlError::Overflow))
    }

    /// Check whether a device is a spinning HDD or not.
    ///
    /// # Safety
    /// `fd` has to point to a valid file descriptor, representing a real block device.
    pub unsafe fn is_rotational(fd: libc::c_int) -> Result<bool, IoctlError> {
        let mut value: libc::c_ushort = 0;
        match linux_blk_rotational(fd, &mut value) {
            Ok(_) => (),
            Err(error) => return Err(IoctlError::IoError(error)),
        }
        Ok(value != 0)
    }

    #[derive(Debug, Error)]
    pub enum IoctlError {
        #[error("disk i/o error: {0}")]
        IoError(#[from] nix::Error),

        #[error("integer conversion error")]
        Overflow,
    }
}
#[cfg(target_os = "linux")]
pub use linux_ioctls::IoctlError;

impl Device {
    pub fn initialize(file: File) -> Result<Self, fal::DeviceError> {
        Ok(Self {
            file,
            last_cached_disk_info: Mutex::new(None),
            locked_ranges: RwLock::new(Map::new()),
        })
    }
    fn block_size(&self) -> Result<u32, fal::DeviceError> {
        Ok(if let &Some(ref info) = &*self.last_cached_disk_info.lock() {
            info.block_size
        } else {
            self.disk_info()?.block_size
        })
    }
    fn acquire_lock(&self, start: u64, count: u64) {
        /*let read_guard = self.locked_ranges.read();

        let end = start + count;

        let (bottom_base, bottom_len) = read_guard.get_or_less::<()>(start);
        let (top_base, top_len) = read_guard.get_or_less::<()>(end);

        let bottom_range = Range { start: bottom_base, end: bottom_base + bottom_len };
        let top_range = Range { start: top_base, end: top_base + top_len };

        if bottom_range.contains(start) || bottom_range.contains(end) || top_range.contains(start) || top_range.contains(end) {
            // Already locked; we'll have to wait for someone else to release the lock.
            todo!()
        }
        let write_guard = read_guard.upgrade();
        if write_guard.insert((start, count)).is_some() {
            todo!()
        }
        */
    }
    fn release_lock(&self, start: u64, count: u64) {
        /*let read_guard = self.locked_ranges.read();

        let end = start + count;

        let (bottom_base, bottom_len) = read_guard.get_or_less::<()>(start);
        let (top_base, top_len) = read_guard.get_or_less::<()>(end);

        let bottom_range = Range { start: bottom_base, end: bottom_base + bottom_len };
        let top_range = Range { start: top_base, end: top_base + top_len };

        assert_eq!(bottom_range, top_range);

        let write_guard = read_guard.upgrade();
        write_guard.remove(bottom_base);

        if bottom_base < start {
            write_guard.insert((bottom_base, start - bottom_base));
        }
        if bottom_base + bottom_len > start + count {
            write_guard.insert((start + count, bottom_base + bottom_len - start + count));
        }*/
        // FIXME: Unpark a single thread that requests this range.
    }
    fn io<F: FnMut(&File, u64) -> io::Result<usize>>(&self, block: u64, count: u64, mut f: F) -> Result<(), fal::DeviceError> {
        let block_size = self.block_size()?;

        // Lock the entire range so that no other threads can read possibly corrupt or out-of-date
        // data.
        self.acquire_lock(block, count);

        let mut blocks_read = 0;

        // Gradually release blocks from the locked range as more reads are finished.
        while blocks_read < count {
            let bytes_read = match f(&self.file, (block + count) * u64::from(block_size)) {
                Ok(b) => b,
                Err(err) => {
                    self.release_lock(block, count);
                    return Err(err.into());
                }
            };
            let current_blocks_read = bytes_read as u64 / u64::from(block_size);
            blocks_read += current_blocks_read;
            self.release_lock(block + blocks_read, block + count);
        }
        Ok(())
    }
}

impl fal::DeviceRo for Device {
    fn read_blocks(&self, block: u64, buf: &mut [u8]) -> Result<(), fal::DeviceError> {
        let bytes_to_read = buf.len() as u64;
        let block_size = self.block_size()?;

        assert_eq!(bytes_to_read % u64::from(block_size), 0);

        let blocks_to_read = bytes_to_read / u64::from(block_size);
        let mut bytes_read = 0;

        self.io(block, blocks_to_read, |file, offset| {
            let result = file.read_at(&mut buf[bytes_read..], offset)?;
            bytes_read += result;
            Ok(result)
        })
    }
    fn disk_info(&self) -> Result<fal::DiskInfo, fal::DeviceError> {
        let metadata = self.file.metadata()?;

        let disk_info = if metadata.file_type().is_block_device() {
            #[cfg(target_os = "linux")]
            fal::DiskInfo {
                block_size: unsafe { linux_ioctls::get_blocksize(self.file.as_raw_fd() as libc::c_int)? },
                block_count: metadata.len(),
                sector_size: unsafe { linux_ioctls::get_sectorsize(self.file.as_raw_fd() as libc::c_int).map(Into::into).ok() },
                is_rotational: unsafe { linux_ioctls::is_rotational(self.file.as_raw_fd() as libc::c_int).ok() },
            }
        } else if metadata.file_type().is_char_device() {
            todo!("IIRC freebsd uses character devices over block devices")
        } else if metadata.file_type().is_file() {
            fal::DiskInfo {
                block_size: metadata.blksize() as u32, // FIXME
                block_count: metadata.blocks(),
                sector_size: None,
                is_rotational: None,
            }
        } else {
            return Err(io::Error::new(io::ErrorKind::InvalidInput, "backing device is neither a regular file, character device, nor a block device").into());
        };
        if let Some(mut guard) = self.last_cached_disk_info.try_lock() {
            *guard = Some(disk_info);
        }
        Ok(disk_info)
    }
    fn seek(&self, block: u64) -> Result<(), fal::DeviceError> {
        (&self.file).seek(io::SeekFrom::Start(block * u64::from(self.block_size()?)))?;
        Ok(())
    }
}
impl fal::Device for Device {
    fn write_blocks(&self, block: u64, mut buf: &[u8]) -> Result<(), fal::DeviceError> {
        let bytes_to_read = buf.len() as u64;
        let block_size = self.block_size()?;

        assert_eq!(bytes_to_read % u64::from(block_size), 0);

        let blocks_to_read = bytes_to_read / u64::from(self.block_size()?);

        self.io(block, blocks_to_read, |file, offset| {
            let result = file.write_at(buf, offset)?;
            buf = &buf[result..];
            Ok(result)
        })
    }
}

#[derive(Debug, Error)]
pub enum DiskError {
    #[error("disk i/o error: {0}")]
    IoError(#[from] io::Error),

    #[error("disk ioctl error: {0}")]
    IoctlError(#[from] IoctlError),
}
impl fal::IoError for DiskError {
    fn should_retry(&self) -> bool {
        false
    }
}
impl fal::IoError for IoctlError {
    fn should_retry(&self) -> bool {
        false
    }
}
impl std::fmt::Debug for Device {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Device")
            .finish()
    }
}
