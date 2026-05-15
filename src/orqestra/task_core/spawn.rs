use crate::orqestra::task_core::{ExecutableTask, OrqestraTaskTrait, TaskCore, WaitingTask};

impl<T, O, const RING_BUFFER_SIZE: usize> TaskCore<T, O, RING_BUFFER_SIZE>
where
    T: OrqestraTaskTrait,
{
    pub fn spawn_task(&self, task: T) {
        let executable_task = ExecutableTask::Task(WaitingTask::<_, O>::new(task));
        self.ring_buffer.enqueue(executable_task);
    }
}
