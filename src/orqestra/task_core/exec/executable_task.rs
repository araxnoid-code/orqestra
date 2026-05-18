use std::sync::{Arc, atomic::Ordering};

use crate::{Job, OrqestraJobTrait, OrqestraTaskTrait, WaitingTask};

/// ExecutableTask functions to store Tasks and Jobs in one enum data type.
/// useful for `Orqestra` to be able to process Task and job data types simultaneously.
pub(crate) enum ExecutableTask<T, J, O>
where
    J: OrqestraJobTrait<O> + 'static,
    T: OrqestraTaskTrait<O> + 'static,
    O: 'static,
{
    /// save the spawned Task
    Task(WaitingTask<T, O>),
    Job(Arc<Job<J, O>>),
}

impl<J, T, O> ExecutableTask<T, J, O>
where
    J: OrqestraJobTrait<O> + 'static,
    T: OrqestraTaskTrait<O> + 'static,
    O: 'static,
{
    /// execute ExecutableTask
    pub(crate) fn execute(&self) -> O {
        match self {
            ExecutableTask::Task(task) => task.f.execute(),
            ExecutableTask::Job(job) => job.inner.f.execute(),
        }
    }

    /// update return_value based on parameter value
    pub(crate) fn update_value(&self, value: O) {
        match self {
            ExecutableTask::Task(task) => {
                task.return_value.0.replace(Some(value));
                task.return_value.1.store(true, Ordering::Relaxed);
            }
            ExecutableTask::Job(job) => {
                job.inner.return_value.0.replace(Some(value));
                job.inner.return_value.1.store(true, Ordering::Relaxed);
            }
        };
    }

    /// execute ExecuteTask and save the value of the execution result
    pub(crate) fn execute_then_update(&self) {
        self.update_value(self.execute());
    }
}
