use crate::{DequeueStatus, ExecutableTask, OrqestraTaskTrait, RingBuffer};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    thread::{park_timeout, yield_now},
    time::Duration,
};

///
pub(crate) struct Worker<T, O, const RING_BUFFER_SIZE: usize>
where
    T: OrqestraTaskTrait<O> + 'static,
    O: 'static,
{
    _id: usize,
    join_flag: Arc<AtomicBool>,
    break_counter: usize,
    ring_buffer: Arc<RingBuffer<T, O, RING_BUFFER_SIZE>>,
    order: Option<usize>,
}

impl<T, O, const RING_BUFFER_SIZE: usize> Worker<T, O, RING_BUFFER_SIZE>
where
    T: OrqestraTaskTrait<O>,
    O: 'static,
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
            order: None,
        }
    }

    ///
    pub(crate) fn running(&mut self) {
        loop {
            if self.join_flag.load(Ordering::Relaxed) {
                break;
            }

            let executable_task = if let Some(idx) = self.order {
                match self.ring_buffer.dequeue_via_order(idx) {
                    DequeueStatus::Ok(executable_task) => Some(executable_task),
                    DequeueStatus::Order(_) => None,
                }
            } else {
                match self.ring_buffer.dequeue() {
                    DequeueStatus::Ok(executable_task) => Some(executable_task),
                    DequeueStatus::Order(idx) => {
                        self.order = Some(idx);
                        None
                    }
                }
            };

            if let Some(executable_task) = executable_task {
                self.break_counter = 0;
                executable_task.execute_then_update();
            } else {
                if self.break_counter < 500 {
                    yield_now();
                } else {
                    park_timeout(Duration::from_millis(10));
                    continue;
                }
                self.break_counter += 1;
            }
        }
    }
}
