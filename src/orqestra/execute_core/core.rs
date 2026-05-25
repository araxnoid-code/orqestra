use std::{
    array,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64},
    },
    thread::{self, JoinHandle},
};

use crossbeam_queue::SegQueue;

use crate::{
    ExecutableTask, OrqestraJobTrait, OrqestraTaskTrait, RingBufferCore,
    orqestra::execute_core::Worker,
};

/// The part that functions as the task executor in the ring-buffer,
/// has a Thread Pool where each thread will access the ring-buffer simultaneously.
/// ## WORKERS_SIZE
/// The number of threads (workers) depends on the const value of WORKERS_SIZE,
/// manual initialization is required for WORKERS_SIZE
pub struct ExecuteCore<const RING_BUFFER_SIZE: usize, const WORKERS_SIZE: usize> {
    /// to count how many tasks have been completed
    pub(crate) done_task: Arc<AtomicU64>,

    /// serves to provide a signal to end the iteration
    pub(crate) join_flag: Arc<AtomicBool>,

    /// thread pool, stores threads (workers) that have been spawned
    pub(crate) workers: [JoinHandle<()>; WORKERS_SIZE],
}

impl<const RING_BUFFER_SIZE: usize, const WORKERS_SIZE: usize>
    ExecuteCore<RING_BUFFER_SIZE, WORKERS_SIZE>
{
    /// ExecuteCore initialization, Requires a generic Structure in the form of:
    ///
    /// ExecuteCore::new<T: OrqestraTaskTrait<O> + 'static, J:OrqestraJobTrait<O> + 'static, O: 'static>
    ///
    /// T, tipe data yang akan menjadi tugas
    /// J, data type that will be the job
    /// O, functions for the output of spawned tasks/jobs
    ///
    /// will immediately spawn threads of the number of WORKERS_SIZE and store them as a thread pool
    pub(crate) fn new<T, J, O>(
        ring_buffer: Arc<RingBufferCore<T, J, O, RING_BUFFER_SIZE>>,
    ) -> ExecuteCore<RING_BUFFER_SIZE, WORKERS_SIZE>
    where
        T: OrqestraTaskTrait<O> + 'static + Send,
        J: OrqestraJobTrait<O> + 'static + Send + Sync,
        O: 'static + Send,
    {
        let join_flag = Arc::new(AtomicBool::new(false));
        let done_task = Arc::new(AtomicU64::new(0));

        let workers: [JoinHandle<()>; WORKERS_SIZE] = array::from_fn(|id| {
            let ring_buffer_clone = ring_buffer.clone();
            let join_flag_clone = join_flag.clone();
            let done_task_clone = done_task.clone();
            let queue_clone = queueu.clone();

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
