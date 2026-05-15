use std::{
    hint::spin_loop,
    ops::Deref,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering},
    thread::yield_now,
};

use crate::orqestra::task_core::{ExecutableTask, OrqestraTaskTrait};

/// counter
#[repr(align(64))]
struct Counter {
    idx: AtomicU64,
}

impl Deref for Counter {
    type Target = AtomicU64;
    fn deref(&self) -> &Self::Target {
        &self.idx
    }
}

/// Space
#[repr(align(64))]
pub(crate) struct RingBufferSpace<T, O>
where
    T: OrqestraTaskTrait,
{
    task: Option<ExecutableTask<T, O>>,
    empty: AtomicBool,
}

///
pub struct RingBuffer<T, O, const RING_BUFFER_SIZE: usize>
where
    T: OrqestraTaskTrait,
{
    head: Counter,
    tail: Counter,
    queue: AtomicPtr<[RingBufferSpace<T, O>; RING_BUFFER_SIZE]>,
}

impl<T, O, const RING_BUFFER_SIZE: usize> RingBuffer<T, O, RING_BUFFER_SIZE>
where
    T: OrqestraTaskTrait,
{
    pub(crate) fn enqueue(&self, executable_task: ExecutableTask<T, O>) {
        let idx = self.head.fetch_add(1, Ordering::Relaxed) as usize & (RING_BUFFER_SIZE - 1);

        unsafe {
            let space = &mut (&mut (*self.queue.load(Ordering::Relaxed)))[idx];
            let mut yield_counter = 0;
            while !space.empty.load(Ordering::Relaxed) {
                spin_loop();
                if yield_counter >= 1000 {
                    yield_now();
                } else {
                    yield_counter += 1;
                }
            }
            space.task = Some(executable_task);
        }
    }
}
