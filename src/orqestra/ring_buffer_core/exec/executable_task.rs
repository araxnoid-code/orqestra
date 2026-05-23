use std::{
    collections::VecDeque,
    ops::Deref,
    ptr::{null, null_mut},
    sync::{
        Arc,
        atomic::{AtomicPtr, Ordering},
    },
};

use crate::{InnerJob, Job, JobDep, OrqestraJobTrait, OrqestraTaskTrait, WaitingTask};

pub(crate) struct NextExecutable<T, J, O>(AtomicPtr<ExecutableTask<T, J, O>>)
where
    J: OrqestraJobTrait<O> + 'static,
    T: OrqestraTaskTrait<O> + 'static,
    O: 'static;

impl<T, J, O> NextExecutable<T, J, O>
where
    J: OrqestraJobTrait<O> + 'static,
    T: OrqestraTaskTrait<O> + 'static,
    O: 'static,
{
    fn new() -> NextExecutable<T, J, O> {
        Self(AtomicPtr::new(null_mut()))
    }
}

pub(crate) struct PrevExecutable<T, J, O>(AtomicPtr<ExecutableTask<T, J, O>>)
where
    J: OrqestraJobTrait<O> + 'static,
    T: OrqestraTaskTrait<O> + 'static,
    O: 'static;

impl<T, J, O> PrevExecutable<T, J, O>
where
    J: OrqestraJobTrait<O> + 'static,
    T: OrqestraTaskTrait<O> + 'static,
    O: 'static,
{
    fn new() -> PrevExecutable<T, J, O> {
        Self(AtomicPtr::new(null_mut()))
    }
}

/// ExecutableTask functions to store Tasks and Jobs in one enum data type.
/// useful for `Orqestra` to be able to process Task and job data types simultaneously.
pub(crate) enum ExecutableTask<T, J, O>
where
    J: OrqestraJobTrait<O> + 'static,
    T: OrqestraTaskTrait<O> + 'static,
    O: 'static,
{
    /// save the spawned Task
    Task(WaitingTask<T, O>, NextExecutable<T, J, O>),
    Job(Arc<InnerJob<J, O>>, NextExecutable<T, J, O>),
    Dummy(NextExecutable<T, J, O>),
}

impl<J, T, O> ExecutableTask<T, J, O>
where
    J: OrqestraJobTrait<O> + 'static,
    T: OrqestraTaskTrait<O> + 'static,
    O: 'static,
{
    pub fn new_task(task: T) -> ExecutableTask<T, J, O> {
        Self::Task(WaitingTask::new(task), NextExecutable::new())
    }

    pub fn new_job(job: Job<J, O>) -> ExecutableTask<T, J, O> {
        Self::Job(job.inner.clone(), NextExecutable::new())
    }

    pub fn new_job_from_arc_inner(inner: Arc<InnerJob<J, O>>) -> ExecutableTask<T, J, O> {
        Self::Job(inner, NextExecutable::new())
    }

    pub fn new_dummy() -> ExecutableTask<T, J, O> {
        Self::Dummy(NextExecutable::new())
    }

    /// execute ExecutableTask
    pub(crate) fn execute(&self) -> O {
        match self {
            ExecutableTask::Task(task, _) => task.f.execute(),
            ExecutableTask::Job(job, _) => job.f.execute(JobDep {
                vec: job.dep.take(),
            }),
            ExecutableTask::Dummy(_) => panic!("Error, dummy cannot be executed"),
        }
    }

    /// Every Job that is in `next_jobs` because it has been registered for scheduling will be processed.
    ///
    /// 1. exec_counter > 0, will not be included in the ring-buffer.
    /// 2. exec_counter == 0, will be put into the ring-buffer.
    pub fn next_job(&self, saving_jobs: &mut VecDeque<ExecutableTask<T, J, O>>) {
        if let ExecutableTask::Job(job, _) = self {
            for job in job.next_jobs.take() {
                if job.exec_counter.fetch_sub(1, Ordering::Relaxed) == 1 {
                    saving_jobs.push_back(Self::new_job_from_arc_inner(job));
                };
            }
        }
    }

    /// update return_value based on parameter value
    pub(crate) fn update_value(&self, value: O) {
        match self {
            ExecutableTask::Task(task, _) => {
                task.return_value.0.replace(Some(value));
                task.return_value.1.store(true, Ordering::Relaxed);
            }
            ExecutableTask::Job(job, _) => {
                job.return_value.0.replace(Some(value));
                job.return_value.1.store(true, Ordering::Relaxed);
            }
            ExecutableTask::Dummy(_) => (),
        };
    }

    /// execute ExecuteTask and save the value of the execution result
    pub(crate) fn execute_then_update(&self) {
        self.update_value(self.execute());
    }

    ///
    pub(crate) fn next(&self) -> &AtomicPtr<ExecutableTask<T, J, O>> {
        match self {
            ExecutableTask::Task(_, next) => &next.0,
            ExecutableTask::Job(_, next) => &next.0,
            ExecutableTask::Dummy(next) => &next.0,
        }
    }
}
