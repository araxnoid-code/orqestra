use crate::{OrqestraTaskTrait, RingBuffer};
use std::sync::{Arc, atomic::AtomicBool};

///
pub(crate) struct Worker<T, O, const RING_BUFFER_SIZE: usize>
where
    T: OrqestraTaskTrait + 'static,
    O: 'static,
{
    _id: usize,
    join_flag: Arc<AtomicBool>,
    break_counter: usize,
    ring_buffer: Arc<RingBuffer<T, O, RING_BUFFER_SIZE>>,
}

impl<T, O, const RING_BUFFER_SIZE: usize> Worker<T, O, RING_BUFFER_SIZE>
where
    T: OrqestraTaskTrait,
{
    /// initial
    pub(crate) fn new(
        id: usize,
        join_flag: Arc<AtomicBool>,
        ring_buffer: Arc<RingBuffer<T, O, RING_BUFFER_SIZE>>,
    ) -> Worker<T, O, RING_BUFFER_SIZE> {
        Self {
            _id: id,
            join_flag,
            break_counter: 0,
            ring_buffer,
        }
    }

    /// task_running
    pub(crate) fn running_task(&self) {}
}
