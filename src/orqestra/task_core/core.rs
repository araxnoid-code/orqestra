use std::sync::{Arc, atomic::AtomicU64};

use crate::orqestra::task_core::{OrqestraTaskTrait, RingBuffer};

/// The part responsible for processing tasks and jobs, creating ExecutableTask,
/// and managing the ring buffer. Adding(enqueue) and removing(dequeue) elements must be done
/// through the Ring Buffer in the TaskCore structure.
pub struct TaskCore<T, O, const RING_BUFFER_SIZE: usize>
where
    T: OrqestraTaskTrait<O> + 'static,
    O: 'static,
{
    /// count each task that has been registered into the ring-buffer
    pub(crate) in_task: AtomicU64,

    /// ring-buffer is a place to store ExecutableTask that will be executed by workers
    pub(crate) ring_buffer: Arc<RingBuffer<T, O, RING_BUFFER_SIZE>>,
}

impl<T, O, const RING_BUFFER_SIZE: usize> TaskCore<T, O, RING_BUFFER_SIZE>
where
    T: OrqestraTaskTrait<O>,
    O: 'static,
{
    /// initial TaskCore
    pub(crate) fn new() -> TaskCore<T, O, RING_BUFFER_SIZE> {
        Self {
            in_task: AtomicU64::new(0),
            ring_buffer: Arc::new(RingBuffer::new()),
        }
    }
}
