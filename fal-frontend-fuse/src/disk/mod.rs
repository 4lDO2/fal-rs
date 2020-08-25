use std::collections::BTreeMap;
use std::fs::File;
use std::future::Future;
use std::io::prelude::*;
use std::ops::Range;
use std::pin::Pin;
use std::thread::ThreadId;
use std::{io, task};

use fal::DeviceRo as _;

use crossbeam_queue::ArrayQueue;
use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use std::os::unix::fs::{FileExt, FileTypeExt, MetadataExt};
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

// TODO: io_uring, when async fn works in traits and the io_uring ecosystem gets better.

#[derive(Clone, Copy, Debug, Eq, Ord, Hash, PartialEq, PartialOrd)]
pub struct ByteRangeOffHalf(u64);

#[derive(Clone, Copy, Debug, Eq, Ord, Hash, PartialEq, PartialOrd)]
pub struct ByteRangeInfoHalf {
    len: u64,
    wakers: Mutex<Vec<task::Waker>>,
}

pub struct Device {
    file: File,

    last_cached_disk_info: Mutex<Option<fal::DiskInfo>>,

    // TODO: Concurrent struct
    locked_ranges: RwLock<BTreeMap<ByteRangeOffHalf, ByteRangeInfoHalf>>,

    async_mode_ctx: AsyncModeCtx,
}

#[cfg(target_os = "linux")]
mod linux;

impl Device {
    pub fn initialize(file: File, async_mode: AsyncMode) -> Result<Self, fal::DeviceError> {
        Ok(Self {
            file,
            last_cached_disk_info: Mutex::new(None),
            locked_ranges: RwLock::new(Map::new()),
            async_mode_ctx,
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
    fn try_acquire_lock_inner(&self, start: u64, count: u64) -> (ByteRangeOffHalf, bool) {
        /*let read_guard = self.locked_ranges.upgradable_read();

        let partial_off_key = ByteRangeOffHalf(start);

        match read_guard.range(..=partial_off_key).next_back() {
            Some(k, v) => if k.0 + v.len < start {
                ()
            } else {
            }
        }*/
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
    fn sync(&self) -> Result<(), DeviceError> {
        Ok(self.file.flush()?)
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

pub enum AsyncMode {
    Threadpool,
    #[cfg(target_os = "linux")]
    Aio,
    #[cfg(target_os = "linux")]
    IoUring,
}
pub enum AsyncModeCtx {
    Threadpool(threadpool::Context),
    #[cfg(target_os = "linux")]
    Aio(linux::aio::Context),
    #[cfg(target_os = "linux")]
    IoUring(linux::io_uring::Context),
}

impl fal::AsyncDeviceRo for Device {
    type ReadFutureNoGat = ReadFuture;

    unsafe fn read_async_no_gat(&self, offset: u64, buffers: &mut [IoSliceMut]) -> Self::ReadFutureNoGat {
    }
}

// a generic future type for syscalls that return Result<usize> or Result<()>
enum GenericFuture {
    ThreadPool(ThreadPoolFuture),
    /*#[cfg(target_os = "linux")]
    Aio(linux::aio::AioFuture),
    #[cfg(target_os = "linux")]
    IoUring(linux::io_uring::IoUringFuture),*/
}
impl Unpin for GenericFuture {}

impl Future for GenericFuture {
    type Output = Result<usize>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let this = self.get_mut();

        match this {
            Self::ThreadPool(ref mut threadpool_future) => threadpool_future.poll(cx),
            //Self::Aio(ref mut aio_future) => aio_future.poll(cx),
            //Self::IoUring(ref mut iouring_future) => iouring_future.poll(cx),
        }
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
        f.debug_struct("Device").finish()
    }
}
