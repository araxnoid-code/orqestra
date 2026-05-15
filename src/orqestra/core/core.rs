use crate::orqestra::{
    execute_core::ExecuteCore,
    task_core::{OrqestraTaskTrait, TaskCore},
};

pub struct Orqestra<T, O, const RING_BUFFER_SIZE: usize, const WORKERS_SIZE: usize>
where
    T: OrqestraTaskTrait + 'static,
    O: 'static,
{
    task_core: TaskCore<T, O, RING_BUFFER_SIZE>,
    execute_core: ExecuteCore<RING_BUFFER_SIZE, WORKERS_SIZE>,
}

impl<T, O, const RING_BUFFER_SIZE: usize, const WORKERS_SIZE: usize>
    Orqestra<T, O, RING_BUFFER_SIZE, WORKERS_SIZE>
where
    T: OrqestraTaskTrait,
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
    pub fn join(self) {}
}
