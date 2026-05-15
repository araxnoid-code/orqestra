use crate::orqestra::task_core::{OrqestraTaskTrait, TaskCore};

pub struct Orqestra<T, O, const RING_BUFFER_SIZE: usize>
where
    T: OrqestraTaskTrait,
{
    task_core: TaskCore<T, O, RING_BUFFER_SIZE>,
}

impl<T, O, const RING_BUFFER_SIZE: usize> Orqestra<T, O, RING_BUFFER_SIZE>
where
    T: OrqestraTaskTrait,
{
    /// intial
    pub fn new() -> Orqestra<T, O, RING_BUFFER_SIZE> {
        Self {
            task_core: TaskCore::new(),
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
}
