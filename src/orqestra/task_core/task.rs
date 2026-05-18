use std::{
    cell::RefCell,
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
};

/// The `OrqestraTaskTrait` trait allows any data type that implements
/// it to be a task that can be executed by the `Orqestra`.
pub trait OrqestraTaskTrait<O>
where
    O: 'static,
{
    /// the main function that will be executed by the Worker
    fn execute(&self) -> O;
}

/// The `OrqestraJobTrait` trait allows any data type that implements
/// it to be a job that can be run by `Orqestra`
pub trait OrqestraJobTrait<O>
where
    O: 'static,
{
    /// the main function that will be executed by the Worker
    fn execute(&self) -> O;
}

/// Writing Task is a data type used when spawning a task.
/// WaitingTask is just an independent task without any scheduling capabilities.
pub struct WaitingTask<T, O>
where
    T: OrqestraTaskTrait<O>,
    O: 'static,
{
    /// Stores data types that implement the OrchestratorTaskTrait.
    /// Serves as the main part in executing spawned tasks.
    f: T,
    /// as a data type that stores values that will be updated
    /// when the task has been executed by the worker
    /// `AtomicBool` functions to provide a sign whether the return_value already has a completed value.
    return_value: Arc<(RefCell<Option<O>>, AtomicBool)>,
}

impl<T, O> WaitingTask<T, O>
where
    T: OrqestraTaskTrait<O>,
    O: 'static,
{
    /// create a WaitingTask
    pub fn new(f: T) -> WaitingTask<T, O> {
        Self {
            f,
            return_value: Arc::new((RefCell::new(None), AtomicBool::new(false))),
        }
    }
}

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
