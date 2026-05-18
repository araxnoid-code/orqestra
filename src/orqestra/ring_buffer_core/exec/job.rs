use std::{
    cell::RefCell,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

///
pub struct InnerJob<J, O>
where
    J: OrqestraJobTrait<O>,
    O: 'static,
{
    pub(crate) exec_counter: AtomicUsize,
    pub(crate) f: J,
    pub(crate) return_value: Arc<(RefCell<Option<O>>, AtomicBool)>,
    pub(crate) next_jobs: RefCell<Vec<Arc<InnerJob<J, O>>>>,
    pub(crate) dep: RefCell<Vec<Arc<(RefCell<Option<O>>, AtomicBool)>>>,
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

impl<J, O> InnerJob<J, O>
where
    J: OrqestraJobTrait<O>,
    O: 'static,
{
    fn new(f: J) -> InnerJob<J, O> {
        InnerJob {
            exec_counter: AtomicUsize::new(0),
            f,
            return_value: Arc::new((RefCell::new(None), AtomicBool::new(false))),
            next_jobs: RefCell::new(Vec::with_capacity(4)),
            dep: RefCell::new(Vec::with_capacity(4)),
        }
    }
}

///
pub struct Job<J, O>
where
    J: OrqestraJobTrait<O>,
    O: 'static,
{
    pub(crate) inner: Arc<InnerJob<J, O>>,
}

impl<J, O> Job<J, O>
where
    J: OrqestraJobTrait<O>,
    O: 'static,
{
    pub fn new(f: J) -> Job<J, O> {
        Job {
            inner: Arc::new(InnerJob::new(f)),
        }
    }

    pub fn after(self, job: &Job<J, O>) -> Self {
        self.inner.exec_counter.fetch_add(1, Ordering::Relaxed);
        self.inner
            .dep
            .borrow_mut()
            .push(job.inner.return_value.clone());

        job.inner.next_jobs.borrow_mut().push(self.inner.clone());
        self
    }
}
