use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::{io, task};

struct ContextInner {
    ring: iou::IoUring,
    device_fd: libc::c_int,
}

pub struct Context {
    inner: Arc<ContextInner>,
}

pub struct IoUringOptions {
    entry_count: u32,
    flags: Option<iou::SetupFlags>,
}

impl Default for IoUringOptions {
    fn default() -> Self {
        Self {
            entry_count: DEFAULT_ENTRY_COUNT,
            flags: None,
        }
    }
}
const DEFAULT_ENTRY_COUNT: u32 = 64;

impl Context {
    pub fn new(device_fd: libc::c_int, options: IoUringOptions) -> io::Result<Self> {
        let inner = ContextInner {
            ring: match options.flags {
                Some(flags) => iou::IoUring::new_with_flags(options.entry_count, flags)?,
                None => iou::IoUring::new(options.entry_count)?,
            },
            device_fd,
        };

        Ok(Self {
            inner: Arc::new(inner),
        })
    }
    pub unsafe fn async_read(&self, buffers: &mut [fal::IoSliceMut]) -> IoUringFuture {
        IoUringFuture {
            inner: todo!(),
        }
    }
}

pub struct IoUringFuture {
    inner: Arc<ContextInner>,
}

impl Future for IoUringFuture {
    type Output = i32;

    fn poll(self: Pin<&mut Self>, cx: &mut task::Context<'_>) -> task::Poll<Self::Output> {
        todo!()
    }
}
