use std::{
    hint::spin_loop,
    sync::atomic::Ordering,
    thread::{park_timeout, yield_now},
    time::Duration,
};

use crate::orqestra::{
    execute_core::ExecuteCore,
    task_core::{OrqestraTaskTrait, TaskCore},
};

pub struct Orqestra<T, O, const RING_BUFFER_SIZE: usize, const WORKERS_SIZE: usize>
where
    T: OrqestraTaskTrait<O> + 'static,
    O: 'static,
{
    task_core: TaskCore<T, O, RING_BUFFER_SIZE>,
    execute_core: ExecuteCore<RING_BUFFER_SIZE, WORKERS_SIZE>,
}

impl<T, O, const RING_BUFFER_SIZE: usize, const WORKERS_SIZE: usize>
    Orqestra<T, O, RING_BUFFER_SIZE, WORKERS_SIZE>
where
    T: OrqestraTaskTrait<O>,
{
    /// intial
    pub fn new() -> Orqestra<T, O, RING_BUFFER_SIZE, WORKERS_SIZE> {
        let task_core = TaskCore::new();
        let execute_core = ExecuteCore::new(task_core.ring_buffer.clone());
        Self {
            task_core,
            execute_core,
        }
    }

    ///
    pub fn spawn_task(&self, task: T) {
        self.task_core.spawn_task(task);
    }

    ///
    pub fn try_spawn_task(&self, task: T) -> Result<(), &str> {
        self.task_core.try_spawn_task(task)
    }

    /// join
    pub fn join(self) {
        let mut counter = 0;
        while self.execute_core.done_task.load(Ordering::Relaxed)
            < self.task_core.in_task.load(Ordering::Relaxed)
        {
            if counter < 500 {
                yield_now();
            } else {
                park_timeout(Duration::from_millis(10));
                continue;
            }
            counter += 1;
        }

        self.execute_core.join_flag.store(true, Ordering::Relaxed);

        for worker in self.execute_core.workers {
            worker.join().unwrap();
        }
    }
}
