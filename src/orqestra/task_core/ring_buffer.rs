use std::{
    hint::spin_loop,
    ops::Deref,
    sync::atomic::{AtomicBool, AtomicPtr, AtomicU64, Ordering},
    thread::yield_now,
};

use crate::orqestra::task_core::{ExecutableTask, OrqestraTaskTrait};

/// counter, sebagai wrapper dari AtomicU64
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

/// RingBufferSpace, berfungsi untuk sebagai space tempat ExecutableTask disimpan.
#[repr(align(64))]
pub(crate) struct RingBufferSpace<T, O>
where
    T: OrqestraTaskTrait,
{
    task: Option<ExecutableTask<T, O>>,
    empty: AtomicBool,
}

/// RingBuffer, struktur inti untuk membangun ring-buffer
/// yang akan menampung setiap ExecutableTask yang telah dibuat
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
    /// memasukkan ExecutableTask ke dalam ring-buffer
    /// ## Blocking
    /// Saat ring-buffer penuh, maka akan terjadi blocking hingga terdapat space untuk
    /// mengalokasikan ExecutableTask
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

    /// memasukkan ExecutableTask ke dalam ring-buffer
    /// ## non-Blocking
    /// Saat ring-buffer penuh, maka akan mengembalikan tipe data Result::Err(&'static str)
    pub(crate) fn try_enqueue(
        &self,
        executable_task: ExecutableTask<T, O>,
    ) -> Result<(), &'static str> {
        let idx = self.head.fetch_add(1, Ordering::Relaxed) as usize & (RING_BUFFER_SIZE - 1);

        unsafe {
            let space = &mut (&mut (*self.queue.load(Ordering::Relaxed)))[idx];
            if !space.empty.load(Ordering::Relaxed) {
                return Err("Cannot insert task because ring buffer is full");
            }

            space.task = Some(executable_task);
            Ok(())
        }
    }
}
