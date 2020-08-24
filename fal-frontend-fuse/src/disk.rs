use std::fs::File;
use std::io;
use std::io::prelude::*;
use std::ops::Range;
use std::thread::ThreadId;

use fal::DeviceRo as _;

use cranelift_bforest::Map;
use crossbeam_queue::ArrayQueue;
use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use std::os::unix::fs::{FileExt, FileTypeExt, MetadataExt};
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

// TODO: io_uring, when async fn works in traits and the io_uring ecosystem gets better.

pub type Block = u64;

pub struct Device {
    file: File,

    last_cached_disk_info: Mutex<Option<fal::DiskInfo>>,

    // TODO: Concurrent struct
    locked_ranges: RwLock<Map<Block, u64>>,
}

#[cfg(target_os = "linux")]
mod linux_blk {
    use std::convert::TryFrom;
    use std::io;

    use thiserror::Error;

    pub type Result<T = (), E = IoctlError> = ::core::result::Result<T, E>;

    // actual types based on `block/ioctl.c` and `block/blk-zoned.c` in the Linux kernel source,
    // and the numbers are defined in `include/linux/fs.h`.

    mod raw {
        // NOTE: The term "sector" here __always__ refers to 512-byte sectors, i.e. the size
        // divided by 512. For real sector sizes, use `get_physical_blocksize`.

        nix::ioctl_write_ptr!(set_readonly, 0x12, 93, libc::c_int); // BLKROSET
        nix::ioctl_read!(get_readonly, 0x12, 94, libc::c_int); // BLKROGET
        nix::ioctl_none!(reread_partition_table, 0x12, 95); // BLKRRPART
        nix::ioctl_read!(get_sector_count, 0x12, 96, libc::c_ulong); // BLKGETSIZE

        // NOTE: BLKFLSBUF appears not to be called anywhere in the kernel, but is it still used?

        nix::ioctl_write_int!(set_sector_read_ahead, 0x12, 98, libc::c_ulong); // BLKRASET
        nix::ioctl_read!(get_sector_read_ahead, 0x12, 99, libc::c_long); // BLKRAGET
        nix::ioctl_write_int!(set_sector_fs_read_ahead, 0x12, 100, libc::c_ulong);// BLKFRASET
        nix::ioctl_read!(get_sector_fs_read_ahead, 0x12, 101, libc::c_long); // BLKFRAGET

        // NOTE: BLKSECTGET appears not to be implemented whatsoever; it is only ever used in a
        // switch statement in ioctl_compat, where it is a no-op.

        nix::ioctl_read!(get_sector_per_request, 0x12, 103, libc::c_ushort); // BLKSECTGET
        nix::ioctl_read!(get_logical_blocksize, 0x12, 104, libc::c_int); // BLKSSZGET

        nix::ioctl_read!(get_soft_blocksize, 0x12, 112, libc::c_int); // BLKBSZGET
        nix::ioctl_write_ptr!(set_soft_blocksize, 0x12, 113, libc::c_int); // BLKBSZSET
        nix::ioctl_read!(get_size64, 0x12, 114, u64); // BLKGETSIZE64

        // TODO: BLKTRACESETUP
        // TODO: BLKTRACESTART
        // TODO: BLKTRACESTOP
        // TODO: BLKTRACETEARDOWN

        nix::ioctl_write_ptr!(discard_sector_range, 0x12, 119, [u64; 2]); // BLKDISCARD
        nix::ioctl_read!(get_minimum_io_size, 0x12, 120, libc::c_uint); // BLKIOMIN
        nix::ioctl_read!(get_optimal_io_size, 0x12, 121, libc::c_uint); // BLKIOOPT
        nix::ioctl_read!(get_alignment_offset, 0x12, 122, libc::c_int); // BLKALIGNOFF
        nix::ioctl_read!(physical_blocksize, 0x12, 123, libc::c_uint); // BLKPBSZGET

        // NOTE: There is no documentation on what this does, and the source code simply returns a
        // zero.
        nix::ioctl_read!(_discard_zeroes, 0x12, 124, libc::uint); // BLKDISCARDZEROES

        nix::ioctl_write_ptr!(securely_discard_sector_range, 0x12, 125, [u64; 2]); // BLKSECDISCARD
        nix::ioctl_read!(is_rotational, 0x12, 126, libc::c_ushort); // BLKROTATIONAL
        nix::ioctl_write_ptr!(zeroout_sector_range, 0x12, 127, [u64; 2]); // BLKZEROOUT

        // TODO: BLKREPORTZONE
        // TODO: BLKRESETZONE
        // TODO: BLKGETZONESZ
        // TODO: BVKGETNRZONES
        // TODO: BLKOPENZONE
        // TODO: BLKCLOSEZONE
        // TODO: BLKFINISHZONE
    }

    pub unsafe fn set_readonly(fd: libc::c_int, readonly: bool) -> Result<()> {
        let data: libc::c_int = readonly.into();
        raw::set_readonly(fd, &data)
    }
    pub unsafe fn get_readonly(fd: libc::c_int) -> Result<bool> {
        let mut data: libc::c_int = -1; // begin with an initially invalid value
        raw::get_readonly(&mut data)?;
    }
    pub unsafe fn reread_partition_table(fd: libc::c_int) -> Result<()> {
        raw::reread_partition_table(fd)?;
        Ok(())
    }
    pub unsafe fn get_sector_count(fd: libc::c_int) -> Result<u64> {
        let mut count: libc::c_ulong = 0;
        raw::get_sector_count(fd, count)?;
        Ok(count)
    }
    pub unsafe fn set_sector_read_ahead(fd: libc::c_int, read_ahead: u64) -> Result<()> {
        raw::set_sector_read_ahead(fd, read_ahead)?;
        Ok(())
    }
    pub unsafe fn get_sector_read_ahead(fd: libc::c_int) -> Result<u64> {
        let mut read_ahead: libc::c_long = -1;
        raw::get_sector_read_ahead(fd, &mut read_ahead)?;
        Ok(read_ahead.try_into()?)
    }
    pub unsafe fn set_sector_fs_read_ahead(fd: libc::c_int, read_ahead: u64) -> Result<()> {
        raw::set_sector_fs_read_ahead(fd, read_ahead)?;
        Ok(())
    }
    pub unsafe fn get_sector_fs_read_ahead(fd: libc::c_int) -> Result<u64> {
        let mut read_ahead: libc::c_long = -1;
        raw::get_sector_fs_read_ahead(fd, &mut read_ahead)?;
        Ok(read_ahead.try_into()?)
    }
    pub unsafe fn get_sector_per_request(fd: libc::c_int) -> Result<u16> {
        let mut sector_per_req: libc::c_ushort = 0;
        raw::get_sector_per_request(fd, &mut sector_per_req)?;
        Ok(sector_per_req.try_into()?)
    }

    pub unsafe fn get_soft_blocksize(fd: libc::c_int) -> Result<u32> {
        let mut size: libc::c_int = -1;
        raw::get_soft_blocksize(fd, &mut size)?;
        Ok(u32::try_from(size)?)
    }
    pub unsafe fn set_soft_blocksize(fd: libc::c_int, size: u32) -> Result<()> {
        let blocksize: libc::c_int = size.try_into()?;
        raw::set_soft_blocksize(fd, &blocksize)?;
        Ok(())
    }
    pub unsafe fn get_logical_blocksize(fd: libc::c_int) -> Result<u32> {
        let mut size: libc::c_int = -1;
        raw::get_logical_blocksize(fd, &mut size)?;
        Ok(u32::try_from(size)?)
    }
    pub unsafe fn get_physical_blocksize(fd: libc::c_int) -> Result<u32> {
        let mut size: libc::c_int = -1;
        raw::get_physical_blocksize(fd, &mut size)?;
        Ok(u32::try_from(size)?)
    }
    pub unsafe fn get_size_in_bytes(fd: libc::c_int) -> Result<u64> {
        let mut size: u64 = 0;
        raw::get_size64(fd, &mut size)?;
        Ok(size)
    }
    pub unsafe fn get_minimum_io_size(fd: libc::c_int) -> Result<u32> {
        let mut size: libc::c_uint = 0;
        raw::get_minimum_io_size(fd, &mut size)?;
        Ok(size.try_into()?)
    }
    pub unsafe fn get_optimal_io_size(fd: libc::c_int) -> Result<u32> {
        let mut size: libc::c_uint = 0;
        raw::get_optimal_io_size(fd, &mut size)?;
        Ok(size.try_into()?)
    }
    pub unsafe fn get_alignment_offset(fd: libc::c_int) -> Result<u32> {
        let mut size: libc::c_int = 0;
        raw::get_alignment_offset(fd, &mut size)?;
        Ok(size.try_into()?)
    }
    pub unsafe fn is_rotational(fd: libc::c_int) -> Result<bool> {
        let mut value: libc::c_ushort = 2; // pick an initially invalid value
        raw::is_rotational(fd, &mut value)?;
        Ok(bool::try_from(value)?)
    }

    pub unsafe fn discard_sector_range(
        fd: libc::c_int,
        base: u64,
        count: u64,
        securely: bool,
    ) -> Result<(), IoctlError> {
        let array: [u64; 2] = [base, count];
        if securely {
            raw::securely_discard_sector_range(fd, &array)?;
        } else {
            raw::discard_sector_range(fd, &array)?;
        }
        Ok(())
    }
    pub unsafe fn zeroout_sector_range(fd: libc::c_int, base: u64, count: u64) -> Result<(), IoctlError> {
        let array = [u64; 2] = [base, count];
        raw::zeroout_sector_range(&array)?;
        Ok(())
    }

    #[derive(Debug, Error)]
    pub enum IoctlError {
        #[error("disk i/o error: {0}")]
        IoError(#[from] nix::Error),

        #[error("integer conversion error")]
        Overflow(#[from] core::num::TryFromIntError),
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
        Ok(
            if let &Some(ref info) = &*self.last_cached_disk_info.lock() {
                info.block_size
            } else {
                self.disk_info()?.block_size
            },
        )
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
}

impl fal::DeviceRo for Device {
    fn read_blocking(&self, offset: u64, mut buffers: &mut [fal::IoSliceMut]) -> Result<(), fal::DeviceError> {
        let fd = self.file.as_raw_fd();

        let mut bytes_read = 0;

        loop {
            if buffers.is_empty() {
                return Ok(());
            }

            let iovecs = fal::IoSliceMut::as_raw_iovecs(buffers);

            // TODO: More platforms that support this.
            #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "netbsd", target_os = "openbsd", target_os = "dragonflybsd"))]
            let result = {
                // TODO: Maybe add read_at_vectored and write_at_vectored to std?
                let raw_result = unsafe { libc::preadv(fd, iovecs.as_ptr(), iovecs.len()) };

                if raw_result == -1 {
                    Err(fal::DeviceError::from(io::Error::last_os_error()))
                } else {
                    Ok(usize::try_from(result)?)
                }
            };
            #[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "netbsd", target_os = "openbsd", target_os = "dragonflybsd")))]
            let result = {
                // TODO: Check for overflow.
                let current_offset = offset + bytes_read;
                self.file.read_at(current_offset, &mut buffers[0])
            };

            match result {
                Ok(0) => {
                    if buffers.is_empty() {
                        return Ok(());
                    } else {
                        log::warn!("Device gave insufficient data, returning error.");
                        return Err(fal::DeviceError::from(io::Error::new(io::ErrorKind::UnexpectedEof, "device finished read even though there were buffers left")));
                    }
                }
                Err(n) => {
                    // TODO: Check for overflow.
                    bytes_read += n;
                    buffers = match fal::IoSliceMut::advance_within(buffers, n) {
                        Some(b) => b,
                        None => {
                            log::warn!("Device return value was higher than the bytes in the slice. Ignoring those.");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
    fn disk_info(&self) -> Result<fal::DiskInfo, fal::DeviceError> {
        let metadata = self.file.metadata()?;

        let disk_info = if metadata.file_type().is_block_device() {
            #[cfg(target_os = "linux")]
            fal::DiskInfo {
                block_size: unsafe {
                    linux_ioctls::get_blocksize(self.file.as_raw_fd() as libc::c_int)?
                },
                block_count: metadata.len(),
                sector_size: unsafe {
                    linux_blk::get_sectorsize(self.file.as_raw_fd() as libc::c_int)
                        .map(Into::into)
                        .ok()
                },
                is_rotational: unsafe {
                    linux_blk::is_rotational(self.file.as_raw_fd() as libc::c_int).ok()
                },
                minimal_io_align: None,
                minimal_io_size: unsafe {
                    linux_blk::get_minimum_io_size(self.file.as_raw_fd() as libc::c_int)
                },
                optimal_io_align: None,
                optimal_io_size: unsafe {
                    linux_blk::get_minimum_io_size(self.file.as_raw_fd() as libc::c_int)
                },
            }
        } else if metadata.file_type().is_char_device() {
            todo!("IIRC FreeBSD uses character devices over block devices")
        } else if metadata.file_type().is_file() {
            fal::DiskInfo {
                block_size: metadata.blksize() as u32, // FIXME
                block_count: metadata.blocks(),
                minimal_io_align: None,
                minimal_io_size: None,
                optimal_io_align: None,
                optimal_io_size: None,
                sector_size: None,
                is_rotational: None,
            }
        } else {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                "backing device is neither a regular file, character device, nor a block device",
            )
            .into());
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
    fn write_blocking(&self, offset: u64, mut buffers: &mut [fal::IoSlice]) -> Result<(), fal::DeviceError> {
        let mut bytes_written = 0;

        loop {
            #[cfg(any(target_os = "linux", target_os = "freebsd", target_os = "netbsd", target_os = "openbsd", target_os = "dragonflybsd"))]
            let result = {
                let raw_result = unsafe { libc::pwritev(fd, iovecs.as_ptr(), iovecs.len()) };

                if raw_result == -1 {
                    Err(fal::DeviceError::from(io::Error::last_os_error()))
                } else {
                    Ok(usize::try_from(result)?)
                }
            };
            #[cfg(not(any(target_os = "linux", target_os = "freebsd", target_os = "netbsd", target_os = "openbsd", target_os = "dragonflybsd")))]
            let result = {
                // TODO: Check for overflow.
                let current_offset = offset + bytes_written;
                self.file.write_at(current_offset, &buffers[0])
            };
            match result {
                Ok(0) => {
                    if buffers.is_empty() {
                        return Ok(());
                    } else {
                        log::warn!("Device didn't accept all data, returning error.");
                        return Err(fal::DeviceError::from(io::Error::new(io::ErrorKind::UnexpectedEof, "device finished write even though there were buffers left")));
                    }
                }
                Err(n) => {
                    // TODO: Check for overflow.
                    bytes_written += n;
                    buffers = match fal::IoSlice::advance_within(buffers, n) {
                        Some(b) => b,
                        None => {
                            log::warn!("Device return value was higher than the bytes in the slice. Ignoring those.");
                            return Ok(());
                        }
                    }
                }
            }
        }
    }
    fn discard(&self, start_block: u64, count: u64) -> Result<(), fal::DeviceError> {
        #[cfg(target_os = "linux")]
        unsafe {
            linux_ioctls::basic_discard_range(
                self.file.as_raw_fd() as libc::c_int,
                start_block,
                count,
            )?
        };

        Ok(())
    }
    // TODO: Secure erase, discard zero-out (refer to Linux ioctls)
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
        f.debug_struct("Device").finish()
    }
}
