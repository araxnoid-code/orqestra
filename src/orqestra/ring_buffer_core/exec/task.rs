use std::{
    cell::RefCell,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicPtr},
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

/// Writing Task is a data type used when spawning a task.
/// WaitingTask is just an independent task without any scheduling capabilities.
pub struct WaitingTask<T, O>
where
    T: OrqestraTaskTrait<O>,
    O: 'static,
{
    /// Stores data types that implement the OrchestratorTaskTrait.
    /// Serves as the main part in executing spawned tasks.
    pub(crate) f: T,

    /// as a data type that stores values that will be updated
    /// when the task has been executed by the worker
    /// `AtomicBool` functions to provide a sign whether the return_value already has a completed value.
    pub(crate) return_value: Arc<(RefCell<Option<O>>, AtomicBool)>,
}

impl<T, O> WaitingTask<T, O>
where
    T: OrqestraTaskTrait<O>,
    O: 'static,
{
    /// create a WaitingTask, accepts data types that implement OrqestraTaskTrait
    pub fn new(f: T) -> WaitingTask<T, O> {
        Self {
            f,
            return_value: Arc::new((RefCell::new(None), AtomicBool::new(false))),
        }
    }
}
