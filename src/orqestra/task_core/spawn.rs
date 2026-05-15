use std::sync::atomic::Ordering;

use crate::orqestra::task_core::{ExecutableTask, OrqestraTaskTrait, TaskCore, WaitingTask};

impl<T, O, const RING_BUFFER_SIZE: usize> TaskCore<T, O, RING_BUFFER_SIZE>
where
    T: OrqestraTaskTrait,
{
    /// spawning task yang akan disimpan ke dalam ring-buffer
    /// serta akan dieksekusi oleh workers
    /// ## Blocking
    /// Saat ring-buffer penuh, maka akan terjadi blocking hingga terdapat space untuk
    /// mengalokasikan ExecutableTask
    pub(crate) fn spawn_task(&self, task: T) {
        self.in_task.fetch_add(1, Ordering::Relaxed);

        let executable_task = ExecutableTask::Task(WaitingTask::<_, O>::new(task));
        self.ring_buffer.enqueue(executable_task);
    }

    /// spawning task yang akan disimpan ke dalam ring-buffer
    /// serta akan dieksekusi oleh workers
    /// ## non-Blocking
    /// Saat ring-buffer penuh, maka akan mengembalikan tipe data Result::Err(&'static str)
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
