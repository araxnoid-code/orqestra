use std::sync::atomic::Ordering;

use crate::{OrqestraTaskTrait, WaitingTask};

/// ExecutableTask functions to store Tasks and Jobs in one enum data type.
/// useful for `Orqestra` to be able to process Task and job data types simultaneously.
pub(crate) enum ExecutableTask<T, O>
where
    T: OrqestraTaskTrait<O>,
    O: 'static,
{
    /// save the spawned Task
    Task(WaitingTask<T, O>),
}

impl<T, O> ExecutableTask<T, O>
where
    T: OrqestraTaskTrait<O> + 'static,
    O: 'static,
{
    /// execute ExecutableTask
    pub(crate) fn execute(&self) -> O {
        match self {
            ExecutableTask::Task(task) => task.f.execute(),
        }
    }

    /// update return_value based on parameter value
    pub(crate) fn update_value(&self, value: O) {
        match self {
            ExecutableTask::Task(task) => {
                task.return_value.1.store(true, Ordering::Relaxed);
                task.return_value.0.replace(Some(value));
            }
        };
    }

    /// execute ExecuteTask and save the value of the execution result
    pub(crate) fn execute_then_update(&self) {
        self.update_value(self.execute());
    }
}
