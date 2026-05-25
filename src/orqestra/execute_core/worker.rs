use crate::{DequeueStatus, ExecutableTask, OrqestraJobTrait, OrqestraTaskTrait, RingBufferCore};
use std::{
    collections::VecDeque,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    thread::{park_timeout, yield_now},
    time::Duration,
};

/// structure that functions to execute ExecutableTask on the ring-buffer
pub(crate) struct Worker<T, J, O, const RING_BUFFER_SIZE: usize>
where
    T: OrqestraTaskTrait<O> + 'static,
    J: OrqestraJobTrait<O> + 'static,
    O: 'static,
{
    /// identifier
    _id: usize,

    /// to count how many tasks have been completed
    done_task: Arc<AtomicU64>,

    /// serves to provide a signal to end the iteration
    join_flag: Arc<AtomicBool>,

    /// useful for entering idle mode
    break_counter: usize,

    /// to enqueue and dequeue an ExecutableTask
    ring_buffer: Arc<RingBufferCore<T, J, O, RING_BUFFER_SIZE>>,

    /// save the obtained index in the ring-buffer,
    /// but there is still no ExecutableTask in that index
    order: Option<usize>,

    ///
    saving_jobs: VecDeque<ExecutableTask<T, J, O>>,

    ///
    toggle: bool,
}

impl<T, J, O, const RING_BUFFER_SIZE: usize> Worker<T, J, O, RING_BUFFER_SIZE>
where
    T: OrqestraTaskTrait<O> + 'static,
    J: OrqestraJobTrait<O> + 'static,
    O: 'static,
{
    /// initial
    pub(crate) fn new(
        id: usize,
        join_flag: Arc<AtomicBool>,
        ring_buffer: Arc<RingBufferCore<T, J, O, RING_BUFFER_SIZE>>,
        done_task: Arc<AtomicU64>,
    ) -> Worker<T, J, O, RING_BUFFER_SIZE> {
        Self {
            _id: id,
            join_flag,
            break_counter: 0,
            ring_buffer,
            order: None,
            done_task,
            saving_jobs: VecDeque::with_capacity(32),
            toggle: false,
        }
    }

    /// The execution flow is as follows:
    /// 1. Checks whether any indexes have been previously reserved.
    /// If so, checks them using the RingBuffer::dequeue_via_order method:
    /// If DequeueStatus::Ok, it will receive an ExecutableTask.
    /// If DequeueStatus::Order, it will return the same index and store it for the next iteration.
    ///
    /// 2. If no indexes have been reserved, the worker will use the RingBuffer::dequeue method:
    /// If DequeueStatus::Ok, it will receive an ExecutableTask.
    /// If DequeueStatus::Order, it will store the index it received for checking in the next iteration.
    ///
    /// 3. If it receives an ExecutableTask,
    /// the worker will immediately execute it.
    ///
    /// 4. When the worker is not executing any ExecutableTasks,
    /// it will periodically update the break_counter until it enters idle mode,
    /// where it executes `yield_now` and `park_timeout` is incremented.
    /// When a worker receives an ExecutableTask, the break_counter is set to 0.
    pub(crate) fn running(&mut self) {
        loop {
            if self.join_flag.load(Ordering::Relaxed) {
                break;
            }

            let saving_job = if let Some(save_job) = self.saving_jobs.pop_front() {
                self.ring_buffer.swap_enqueue(save_job);
                None
            } else {
                None
            };

            let secondary_list = if self.toggle {
                // self.ring_buffer.secondary_list.pop()
                None
            } else {
                self.toggle = false;
                None
            };

            let executable_task = if let Some(executable_task) = secondary_list {
                Some(executable_task)
            } else if let (Some(idx), None) = (self.order, &saving_job) {
                match self.ring_buffer.dequeue_via_order(idx) {
                    DequeueStatus::Ok(executable_task) => Some(executable_task),
                    DequeueStatus::Order(_) => None,
                }
            } else if let Some(executable_task) = saving_job {
                Some(executable_task)
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
                executable_task.next_job(&mut self.saving_jobs);

                self.done_task.fetch_add(1, Ordering::Relaxed);
                self.toggle = false;
            } else {
                if self.break_counter < 500 {
                    yield_now();
                } else {
                    park_timeout(Duration::from_millis(10));
                    continue;
                }
                self.break_counter += 1;
                self.toggle = true;
            }
        }
    }
}
