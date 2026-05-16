use std::{
    array,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    },
    thread::{self, JoinHandle},
};

use crate::{OrqestraTaskTrait, RingBuffer, orqestra::execute_core::Worker};

///
pub struct ExecuteCore<const RING_BUFFER_SIZE: usize, const WORKERS_SIZE: usize> {
    pub(crate) done_task: Arc<AtomicU64>,
    pub(crate) join_flag: Arc<AtomicBool>,
    pub(crate) workers: [JoinHandle<()>; WORKERS_SIZE],
}

impl<const RING_BUFFER_SIZE: usize, const WORKERS_SIZE: usize>
    ExecuteCore<RING_BUFFER_SIZE, WORKERS_SIZE>
{
    pub(crate) fn new<T, O>(
        ring_buffer: Arc<RingBuffer<T, O, RING_BUFFER_SIZE>>,
    ) -> ExecuteCore<RING_BUFFER_SIZE, WORKERS_SIZE>
    where
        T: OrqestraTaskTrait<O> + 'static,
        O: 'static,
    {
        let join_flag = Arc::new(AtomicBool::new(false));
        let done_task = Arc::new(AtomicU64::new(0));

        let workers: [JoinHandle<()>; WORKERS_SIZE] = array::from_fn(|id| {
            let ring_buffer_clone = ring_buffer.clone();
            let join_flag_clone = join_flag.clone();
            let done_task_clone = done_task.clone();
            thread::spawn(move || {
                Worker::new(id, join_flag_clone, ring_buffer_clone, done_task_clone).running();
            })
        });

        Self {
            done_task,
            join_flag,
            workers,
        }
    }
}
