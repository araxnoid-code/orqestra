use std::sync::{Arc, atomic::Ordering};

use crate::{InnerJob, JobDep, OrqestraJobTrait, OrqestraTaskTrait, RingBufferTrait, WaitingTask};

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
    Job(Arc<InnerJob<J, O>>),
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
            ExecutableTask::Job(job) => job.f.execute(JobDep {
                vec: job.dep.take(),
            }),
        }
    }

    /// Every Job that is in `next_jobs` because it has been registered for scheduling will be processed.
    ///
    /// 1. exec_counter > 0, will not be included in the ring-buffer.
    /// 2. exec_counter == 0, will be put into the ring-buffer.
    pub fn next_job<R>(&self, ring_buffer: &R)
    where
        R: RingBufferTrait<T, J, O>,
    {
        if let ExecutableTask::Job(job) = self {
            for job in job.next_jobs.take() {
                if job.exec_counter.fetch_sub(1, Ordering::Relaxed) == 1 {
                    ring_buffer.enqueue(Self::Job(job));
                };
            }
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
                job.return_value.0.replace(Some(value));
                job.return_value.1.store(true, Ordering::Relaxed);
            }
        };
    }

    /// execute ExecuteTask and save the value of the execution result
    pub(crate) fn execute_then_update(&self) {
        self.update_value(self.execute());
    }
}
