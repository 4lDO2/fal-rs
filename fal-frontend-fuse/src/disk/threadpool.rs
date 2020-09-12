use std::num::NonZeroUsize;
use std::future::Future;
use std::pin::Pin;
use std::{io, task, thread};

use parking_lot::Mutex;

pub struct Context {
    threads: Mutex<Vec<thread::JoinHandle<()>>>,
}

impl Context {
    pub fn new(thread_count: NonZeroUsize) -> Self {
        // TODO: map_while
        let threads = (0..thread_count.get()).map(|n| {
            let result = thread::Builder::new()
                .name(format!("fal-frontend-fuse threadpool #{}", n))
                // TODO: Small stacks since the threads don't do much
                .spawn(move || {
                });

            match result {
                Ok(thread) => Some(thread),
                Err(error) => {
                    log::warn!("Failed to spawn thread #{}: {}", n, error);
                    None
                }
            }
        }).take_while(Option::is_some).map(Option::unwrap).collect::<Vec<_>>();

        if threads.is_empty() {
            panic!("Failed to setup threadpool properly: no thread was spawned");
        }

        Self {
            threads: Mutex::new(threads),
        }
    }
}

pub struct ThreadpoolFuture {}
