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
    done_task: AtomicU64,
    join_flag: AtomicBool,
    workers: [JoinHandle<()>; WORKERS_SIZE],
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

        let workers: [JoinHandle<()>; WORKERS_SIZE] = array::from_fn(|id| {
            let ring_buffer_clone = ring_buffer.clone();
            let join_flag_clone = join_flag.clone();
            thread::spawn(move || {
                Worker::new(id, join_flag_clone, ring_buffer_clone).running();
            })
        });

        Self {
            done_task: AtomicU64::new(0),
            join_flag: AtomicBool::new(false),
            workers,
        }
    }

    pub(crate) fn join(self) {
        for worker in self.workers {
            worker.join().unwrap();
        }
    }
}
