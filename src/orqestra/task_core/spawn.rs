use std::sync::atomic::Ordering;

use crate::{
    OrqestraJobTrait,
    orqestra::task_core::{ExecutableTask, OrqestraTaskTrait, TaskCore, WaitingTask},
};

impl<T, J, O, const RING_BUFFER_SIZE: usize> TaskCore<T, J, O, RING_BUFFER_SIZE>
where
    T: OrqestraTaskTrait<O> + 'static,
    J: OrqestraJobTrait<O> + 'static,
    O: 'static,
{
    /// spawning task that will be saved into the ring-buffer
    /// will be executed by workers
    /// ## Blocking
    // When the ring buffer is full,
    // blocking occurs until there is space to allocate an ExecutableTask.
    pub(crate) fn spawn_task(&self, task: T) {
        self.in_task.fetch_add(1, Ordering::Relaxed);

        let executable_task = ExecutableTask::Task(WaitingTask::<_, O>::new(task));
        self.ring_buffer.enqueue(executable_task);
    }

    // spawning tasks that will be stored in the ring-buffer
    // will be executed by workers
    /// ## non Blocking
    /// When the ring-buffer is full, it will return the Result::Err data type.
    pub(crate) fn try_spawn_task(&self, task: T) -> Result<(), &'static str> {
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
