use std::sync::atomic::Ordering;

use crate::orqestra::task_core::{ExecutableTask, OrqestraTaskTrait, TaskCore, WaitingTask};

impl<T, O, const RING_BUFFER_SIZE: usize> TaskCore<T, O, RING_BUFFER_SIZE>
where
    T: OrqestraTaskTrait,
{
    pub fn spawn_task(&self, task: T) {
        self.in_task.fetch_add(1, Ordering::Relaxed);

        let executable_task = ExecutableTask::Task(WaitingTask::<_, O>::new(task));
        self.ring_buffer.enqueue(executable_task);
    }

    pub fn try_spawn_task(&self, task: T) -> Result<(), &'static str> {
        self.in_task.fetch_add(1, Ordering::Relaxed);

        let executable_task = ExecutableTask::Task(WaitingTask::<_, O>::new(task));
        self.ring_buffer
            .try_enqueue(executable_task)
            .map_err(|err| {
                self.in_task.fetch_sub(1, Ordering::Relaxed);
                err
            })?;

        Ok(())
    }
}
