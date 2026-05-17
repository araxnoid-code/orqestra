use std::{
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
    /// initial requires manual initialization of the data type as
    /// task, job and size of the ring-buffer and the number of workers to spawn
    pub fn new() -> Orqestra<T, O, RING_BUFFER_SIZE, WORKERS_SIZE> {
        let task_core = TaskCore::new();
        let execute_core = ExecuteCore::new(task_core.ring_buffer.clone());
        Self {
            task_core,
            execute_core,
        }
    }

    /// serves to spawn a task that will be executed by workers in Orqestra
    /// accepts data types that already implement the `OrqestraTaskTrait` trait
    /// ```rust
    /// use orqestra::{Orqestra, OrqestraTaskTrait};
    ///
    /// struct MyTask;
    /// impl OrqestraTaskTrait<()> for MyTask {
    ///     fn execute(&self) -> () {
    ///         println!("execute!");
    ///     }
    /// }
    ///
    /// fn main() {
    ///     let orqestra: Orqestra<MyTask, _, 32, 4> = Orqestra::new();
    ///
    ///     orqestra.spawn_task(MyTask);
    ///     orqestra.spawn_task(MyTask);
    ///
    ///     orqestra.join();
    /// }
    /// ```
    /// ## Blocking
    /// When the ring buffer is full, blocking will occur until there is space for the task that has been spawned.
    pub fn spawn_task(&self, task: T) {
        self.task_core.spawn_task(task);
    }

    /// serves to spawn a task that will be executed by workers in Orqestra
    /// accepts data types that already implement the `OrqestraTaskTrait` trait
    /// ```rust
    /// use orqestra::{Orqestra, OrqestraTaskTrait};
    ///
    /// struct MyTask;
    /// impl OrqestraTaskTrait<()> for MyTask {
    ///     fn execute(&self) -> () {
    ///         println!("execute!");
    ///     }
    /// }
    ///
    /// fn main() {
    ///     let orqestra: Orqestra<MyTask, _, 32, 4> = Orqestra::new();
    ///
    ///     orqestra.try_spawn_task(MyTask).unwrap();
    ///     orqestra.try_spawn_task(MyTask).unwrap();
    ///
    ///     orqestra.join();
    /// }
    /// ```
    /// ## non Blocking
    /// when the ring-buffer is full, it will give Result::Err.
    pub fn try_spawn_task(&self, task: T) -> Result<(), &str> {
        self.task_core.try_spawn_task(task)
    }

    /// At the end of the Orchestra flow, blocking will occur until all spawned
    /// tasks and jobs are completed and cleanup is carried out.
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

        self.task_core.ring_buffer.drop_queue();
    }
}
