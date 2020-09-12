use std::collections::BTreeMap;
use std::convert::{TryFrom, TryInto};
use std::fs::File;
use std::future::Future;
use std::io::prelude::*;
use std::num::NonZeroUsize;
use std::ops::Range;
use std::pin::Pin;
use std::thread::ThreadId;
use std::{io, task};

use fal::DeviceRo as _;
use fal::{IoSlice, IoSliceMut};

use crossbeam_queue::ArrayQueue;
use parking_lot::{Mutex, RwLock};
use thiserror::Error;

use std::os::unix::fs::{FileExt, FileTypeExt, MetadataExt};
use std::os::unix::io::{AsRawFd, FromRawFd, RawFd};

#[cfg(all(feature = "io_uring", target_os = "linux"))]
use self::linux::io_uring;

#[derive(Clone, Copy, Debug, Eq, Ord, Hash, PartialEq, PartialOrd)]
pub struct ByteRangeOffHalf(u64);

#[derive(Debug)]
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

mod threadpool;

#[cfg(target_os = "linux")]
mod linux;

impl Device {
    pub fn initialize(file: File, async_mode: AsyncMode) -> Result<Self, fal::DeviceError> {
        Ok(Self {
            file,
            last_cached_disk_info: Mutex::new(None),
            locked_ranges: RwLock::new(BTreeMap::new()),
            async_mode_ctx: match async_mode {
                // TODO: Error handling with threadpool initialization?
                AsyncMode::Threadpool { thread_count } => AsyncModeCtx::Threadpool(self::threadpool::Context::new(thread_count)),
                #[cfg(all(feature = "aio", target_os = "linux"))]
                AsyncMode::Aio => AsyncModeCtx::Aio(todo!()),
                AsyncMode::IoUring(options) => AsyncModeCtx::IoUring(io_uring::Context::new(file.as_raw_fd(), options).map_err(fal::DeviceError::io_error)?),
            },
        })
    }
    fn block_size(&self) -> Result<u64, fal::DeviceError> {
        Ok(
            if let &Some(ref info) = &*self.last_cached_disk_info.lock() {
                info.block_size
            } else {
                self.disk_info_blocking()?.block_size
            },
        )
    }
    fn try_acquire_lock_inner(&self, start: u64, count: u64) -> (ByteRangeOffHalf, bool) {
        todo!();
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
    fn read_blocking(
        &self,
        offset: u64,
        mut buffers: &mut [fal::IoSliceMut],
    ) -> Result<(), fal::DeviceError> {

        let fd = self.file.as_raw_fd();

        let mut bytes_read = 0;

        loop {
            if buffers.is_empty() {
                return Ok(());
            }

            let iovecs = fal::IoSliceMut::as_raw_iovecs(buffers);

            // TODO: More platforms that support this.
            #[cfg(any(
                target_os = "linux",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd",
                target_os = "dragonflybsd",
            ))]
            let result = {
                let offset: libc::off_t =
                    libc::off_t::try_from(offset).or(Err(fal::DeviceError::InvalidOffset))?;


                let count: libc::c_int = libc::c_int::try_from(buffers.len()).or(Err(fal::DeviceError::TooManyIovecs))?;

                // TODO: More platforms. Note that this check is optional and will still be made by the OS
                // at some point.
                // TODO: Where is libc::IOV_MAX?
                #[cfg(any(target_os = "linux", target_os = "macos"))]
                if count > libc::_SC_IOV_MAX {
                    return Err(fal::DeviceError::TooManyIovecs);
                }

                // TODO: Maybe add read_at_vectored and write_at_vectored to std?
                let raw_result = unsafe { libc::preadv(fd, iovecs.as_ptr(), count, offset) };

                if raw_result == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    usize::try_from(raw_result).or(Err(
                        io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "device did not read full range",
                        ),
                    ))
                }
            };
            #[cfg(not(any(
                target_os = "linux",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd",
                target_os = "dragonflybsd",
            )))]
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
                        return Err(fal::DeviceError::io_error(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "device finished read even though there were buffers left",
                        )));
                    }
                }
                Ok(n) => {
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
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(fal::DeviceError::io_error(error)),
            }
        }
    }
    fn disk_info_blocking(&self) -> Result<fal::DiskInfo, fal::DeviceError> {
        let metadata = self.file.metadata().map_err(fal::DeviceError::io_error)?;

        let disk_info = if metadata.file_type().is_block_device() {
            #[cfg(target_os = "linux")]
            fal::DiskInfo {
                block_size: unsafe {
                    linux::ioctl::get_soft_blocksize(self.file.as_raw_fd() as libc::c_int)?.into()
                },
                block_count: metadata.len(),
                sector_size: unsafe {
                    linux::ioctl::get_logical_blocksize(self.file.as_raw_fd() as libc::c_int)
                        .map(Into::into)
                        .ok()
                },
                is_rotational: unsafe {
                    linux::ioctl::is_rotational(self.file.as_raw_fd() as libc::c_int).ok()
                },
                minimal_io_align: None,
                minimal_io_size: unsafe {
                    linux::ioctl::get_minimum_io_size(self.file.as_raw_fd() as libc::c_int).ok().map(Into::into)
                },
                optimal_io_align: None,
                optimal_io_size: unsafe {
                    // TODO: At least print errors
                    linux::ioctl::get_minimum_io_size(self.file.as_raw_fd() as libc::c_int).ok().map(Into::into)
                },
            }
        } else if metadata.file_type().is_char_device() {
            todo!("IIRC FreeBSD uses character devices over block devices")
        } else if metadata.file_type().is_file() {
            fal::DiskInfo {
                block_size: metadata.blksize(),
                block_count: metadata.blocks(),
                minimal_io_align: None,
                minimal_io_size: None,
                optimal_io_align: None,
                optimal_io_size: None,
                sector_size: None,
                is_rotational: None,
            }
        } else {
            return Err(fal::DeviceError::io_error(io::Error::new(
                io::ErrorKind::InvalidInput,
                "backing device is neither a regular file, character device, nor a block device",
            )));
        };
        if let Some(mut guard) = self.last_cached_disk_info.try_lock() {
            *guard = Some(disk_info);
        }
        Ok(disk_info)
    }
    fn seek_hint(&self, offset: u64) -> Result<(), fal::DeviceError> {
        (&self.file).seek(io::SeekFrom::Start(offset)).map_err(fal::DeviceError::io_error)?;
        Ok(())
    }
}
impl fal::Device for Device {
    fn write_blocking(
        &self,
        offset: u64,
        mut buffers: &mut [fal::IoSlice],
    ) -> Result<(), fal::DeviceError> {
        let mut bytes_written = 0;

        let fd = self.file.as_raw_fd();

        loop {
            #[cfg(any(
                target_os = "linux",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd",
                target_os = "dragonflybsd"
            ))]
            let result = {
                let buffers = fal::IoSlice::as_raw_iovecs(buffers);
                let offset: libc::off_t =
                    libc::off_t::try_from(offset).or(Err(fal::DeviceError::InvalidOffset))?;
                let len: libc::c_int =
                    libc::c_int::try_from(buffers.len()).or(Err(fal::DeviceError::TooManyIovecs))?;
                let raw_result =
                    unsafe { libc::pwritev(fd, buffers.as_ptr(), len, offset) };

                if raw_result == -1 {
                    Err(io::Error::last_os_error())
                } else {
                    usize::try_from(raw_result).map_err(|_| {
                        io::Error::new(
                            io::ErrorKind::Other,
                            "Overflow when converting signed result from pwritev",
                        )
                    })
                }
            };
            #[cfg(not(any(
                target_os = "linux",
                target_os = "freebsd",
                target_os = "netbsd",
                target_os = "openbsd",
                target_os = "dragonflybsd"
            )))]
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
                        return Err(fal::DeviceError::io_error(io::Error::new(
                            io::ErrorKind::UnexpectedEof,
                            "device finished write even though there were buffers left",
                        )));
                    }
                }
                Ok(n) => {
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
                Err(error) if error.kind() == io::ErrorKind::Interrupted => continue,
                Err(error) => return Err(fal::DeviceError::io_error(error)),
            }
        }
    }
    fn sync(&self) -> Result<(), fal::DeviceError> {
        self.file.flush().map_err(fal::DeviceError::io_error)
    }
    fn discard(&self, start_block: u64, count: u64) -> Result<(), fal::DeviceError> {
        #[cfg(target_os = "linux")]
        unsafe {
            linux::ioctl::discard_sector_range(
                self.file.as_raw_fd() as libc::c_int,
                start_block,
                count,
                false,
            )?
        };

        Ok(())
    }
    // TODO: Secure erase, discard zero-out (refer to Linux ioctls)
}

pub enum AsyncMode {
    Threadpool { thread_count: NonZeroUsize },
    #[cfg(target_os = "linux")]
    Aio,
    #[cfg(target_os = "linux")]
    IoUring(io_uring::IoUringOptions),
}
pub enum AsyncModeCtx {
    Threadpool(threadpool::Context),
    #[cfg(all(feature = "aio", target_os = "linux"))]
    Aio(linux::aio::Context),
    #[cfg(all(feature = "io_uring", target_os = "linux"))]
    IoUring(linux::io_uring::Context),
}

impl fal::AsyncDeviceRo for Device {
    type ReadFutureNoGat = ReadFuture;

    unsafe fn read_async_no_gat(
        &self,
        offset: u64,
        buffers: &mut [IoSliceMut],
    ) -> Self::ReadFutureNoGat {
        let generic = match self.async_mode_ctx {
            AsyncModeCtx::IoUring(ref io_uring_context) => GenericFuture(),
        }
    }
}
impl fal::AsyncDevice for Device {
    type WriteFutureNoGat = WriteFuture;

    unsafe fn write_async_no_gat(&self, offset: u64, bufs: &[IoSlice]) -> Self::WriteFutureNoGat {
    }
}

// a generic future type for syscalls that return Result<usize> or Result<()>
enum GenericFuture {
    ThreadPool(threadpool::ThreadpoolFuture),
    #[cfg(all(feature = "aio", target_os = "linux"))]
    Aio(linux::aio::AioFuture),
    #[cfg(all(feature = "io_uring", target_os = "linux"))]
    IoUring(linux::io_uring::IoUringFuture),
}
impl Unpin for GenericFuture {}

impl Future for GenericFuture {
    type Output = Result<usize, libc::c_int>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        let this = self.get_mut();

        match this {
            Self::ThreadPool(ref mut threadpool_future) => Pin::new(threadpool_future).poll(cx),
            #[cfg(all(feature = "aio", target_os = "linux"))]
            Self::Aio(ref mut aio_future) => aio_future.poll(cx),
            #[cfg(all(feature = "io_uring", target_os = "linux"))]
            Self::IoUring(ref mut iouring_future) => Pin::new(iouring_future).poll(cx),
        }
    }
}

pub struct ReadFuture {
    inner: GenericFuture,
}
pub struct WriteFuture {
    inner: GenericFuture,
}

impl Future for ReadFuture {
    type Output = Result<usize, fal::DeviceError>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner).poll(cx).map_err(|errno| fal::DeviceError::io_error(io::Error::from_raw_os_error(errno)))
    }
}
impl Future for WriteFuture {
    type Output = Result<usize, fal::DeviceError>;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        Pin::new(&mut self.get_mut().inner).poll(cx).map_err(|errno| fal::DeviceError::io_error(io::Error::from_raw_os_error(errno)))
    }
}

#[derive(Debug, Error)]
pub enum DiskError {
    #[error("disk i/o error: {0}")]
    IoError(#[from] io::Error),

    #[error("disk ioctl error: {0}")]
    #[cfg(target_os = "linux")]
    IoctlError(#[from] linux::ioctl::IoctlError),
}
impl std::fmt::Debug for Device {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Device").finish()
    }
}
