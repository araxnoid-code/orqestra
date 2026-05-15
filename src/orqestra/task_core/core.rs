use std::sync::atomic::AtomicU64;

use crate::orqestra::task_core::{OrqestraTaskTrait, RingBuffer};

///
pub struct TaskCore<T, O, const RING_BUFFER_SIZE: usize>
where
    T: OrqestraTaskTrait,
{
    pub(crate) in_task: AtomicU64,
    pub(crate) ring_buffer: RingBuffer<T, O, RING_BUFFER_SIZE>,
}
