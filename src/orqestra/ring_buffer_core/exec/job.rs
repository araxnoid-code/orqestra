use std::{
    cell::{Ref, RefCell},
    sync::{
        Arc, Mutex, MutexGuard,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};

/// The `OrqestraJobTrait` trait allows any data type that implements
/// it to be a job that can be run by `Orqestra`
pub trait OrqestraJobTrait<O>
where
    O: 'static,
{
    /// the main function that will be executed by the Worker
    fn execute(&self, job_dep: JobDep<O>) -> O;
}

/// the structure that is the core of a Job,
/// handles the tasks to be executed,
/// stores scheduling and dependency values
/// ## Workflow
/// The 2 main things in scheduling that a job has are:
///
/// 1. next_jobs
/// 2. exec_counter
///
/// `next_jobs` stores jobs registered through Job::after to be executed after a job has been executed.
/// If a job has been executed, then the other jobs contained in next_job will be processed
/// to determine whether or not to continue being included in the ring buffer.
///
/// `exec_counter`, functions to provide a condition for whether a Job is ready to be executed or not,
/// the exec_counter will increase according to the number of dependencies it has,
/// every time a Job that is a dependency is completed, the job will reduce the exec_counter until
/// if there is an exec_counter = 0, then it will be put into the ring-buffer.
/// ```
///             Job_1
///     Job_1   next_jobs: [Job_2, Job_3]
///       |     exec_counter: 0
///       |
///      / \       Job_2
///     /   \      next_jobs: [job_4]
///    /     \     exec_counter: 1
/// Job_2   Job_3  Job_3
///    \     /     next_jobs: [job_4]
///     \   /      exec_counter: 1
///      \ /
///       |     Job_4
///      Job_4  next_jobs: [Job_2, Job_3]
///             exec_counter: 2
/// ```
pub struct InnerJob<J, O>
where
    J: OrqestraJobTrait<O>,
    O: 'static,
{
    /// Storing a counter determines whether the Job will be executed or not.
    pub(crate) exec_counter: AtomicUsize,

    /// Stores the Data Type that implements OrqestraJobTrait data to be executed
    pub(crate) f: J,

    /// Saving values after a Job is executed
    pub(crate) return_value: Arc<(Mutex<Option<O>>, AtomicBool)>,

    /// Saves another Job to be executed after this Job is executed
    pub(crate) next_jobs: Mutex<Vec<Arc<InnerJob<J, O>>>>,

    /// Stores the value of the Job that is a dependency
    pub(crate) dep: Mutex<Vec<Arc<(Mutex<Option<O>>, AtomicBool)>>>,
}

impl<J, O> InnerJob<J, O>
where
    J: OrqestraJobTrait<O>,
    O: 'static,
{
    /// Job initialization, requires a data type that implements OrqestraJobTrait
    fn new(f: J) -> InnerJob<J, O> {
        InnerJob {
            exec_counter: AtomicUsize::new(0),
            f,
            return_value: Arc::new((Mutex::new(None), AtomicBool::new(false))),
            next_jobs: Mutex::new(Vec::with_capacity(4)),
            dep: Mutex::new(Vec::with_capacity(4)),
        }
    }
}

/// structure that has an InnerJob,
/// functions to perform Job initialization and scheduling.
/// ## Workflow
/// The 2 main things in scheduling that a job has are:
///
/// 1. next_jobs
/// 2. exec_counter
///
/// next_jobs and exec_counter are stored inside `InnerJob`.
///
/// `next_jobs` stores jobs registered through Job::after to be executed after a job has been executed.
/// If a job has been executed, then the other jobs contained in next_job will be processed
/// to determine whether or not to continue being included in the ring buffer.
///
/// `exec_counter`, functions to provide a condition for whether a Job is ready to be executed or not,
/// the exec_counter will increase according to the number of dependencies it has,
/// every time a Job that is a dependency is completed, the job will reduce the exec_counter until
/// if there is an exec_counter = 0, then it will be put into the ring-buffer.
/// ```
///             Job_1
///     Job_1   next_jobs: [Job_2, Job_3]
///       |     exec_counter: 0
///       |
///      / \       Job_2
///     /   \      next_jobs: [job_4]
///    /     \     exec_counter: 1
/// Job_2   Job_3  Job_3
///    \     /     next_jobs: [job_4]
///     \   /      exec_counter: 1
///      \ /
///       |     Job_4
///      Job_4  next_jobs: [Job_2, Job_3]
///             exec_counter: 2
/// ```
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
    /// Job initialization, requires a data type that implements OrqestraJobTrait
    pub fn new(f: J) -> Job<J, O> {
        Job {
            inner: Arc::new(InnerJob::new(f)),
        }
    }

    /// Functions for scheduling jobs,
    /// The job entered as an argument in this method will become a dependency on the job that uses this method.
    /// ```rust
    /// fn main() {
    ///     let orqestra: Orqestra<MyTask, MyJob, _, 64, 4> = Orqestra::new();
    ///
    ///     let job_1 = Job::new(MyJob(|_| {
    ///         println!("job 1 done");
    ///         10
    ///     }));
    ///
    ///     let job_2 = Job::new(MyJob(|_| {
    ///         println!("job 2 done");
    ///         20
    ///     }))
    ///     .after(&job_1) // job_2 will be executed after job_1 has finished executing
    ///     ;
    ///
    ///     orqestra.job_exec(job_1);
    ///
    ///     orqestra.join();
    /// }
    /// ```
    pub fn after(self, job: &Job<J, O>) -> Self {
        self.inner.exec_counter.fetch_add(1, Ordering::Relaxed);
        self.inner
            .dep
            .lock()
            .unwrap()
            .push(job.inner.return_value.clone());

        job.inner.next_jobs.lock().unwrap().push(self.inner.clone());
        self
    }
}

/// Enum dedicated to catching errors in `JobDep`
#[derive(Debug)]
pub enum JobDepErr {
    IndexOutOfBounds,
    ValueIsnNotReady,
}

/// a useful structure for wrapping dependency values obtained after scheduling a job
pub struct JobDep<'a, O>
where
    O: 'static,
{
    pub(crate) vec: &'a Mutex<Vec<Arc<(Mutex<Option<O>>, AtomicBool)>>>,
}

impl<'a, O> JobDep<'a, O>
where
    O: 'static,
{
    /// Gets the value of a scheduled dependency value
    /// ## Index
    /// Accessing dependency values using an index based on the order in which the Job::after method is used on the job that is the dependency.
    /// ```rust
    /// fn main() {
    ///     let orqestra: Orqestra<MyTask, MyJob, _, 64, 4> = Orqestra::new();
    ///
    ///     let job_1 = Job::new(MyJob(|_| {
    ///         println!("job 1 done");
    ///         10
    ///     }));
    ///
    ///     let job_2 = Job::new(MyJob(|_| {
    ///         println!("job 2 done");
    ///         20
    ///     }));
    ///
    ///     let job_3 = Job::new(MyJob(|dep| {
    ///         // access job_1 value
    ///         let value_1 = dep.get(0).unwrap().unwrap();
    ///
    ///         // access job_2 value
    ///         let value_2 = dep.get(1).unwrap().unwrap();
    ///         println!("job 3 done with value {}", value_1 + value_2);
    ///         value_1 + value_2
    ///     }))
    ///     .after(&job_1) // index 0
    ///     .after(&job_2) // index 1
    ///     ;
    ///
    ///     orqestra.job_exec(job_1);
    ///     orqestra.job_exec(job_2);
    ///
    ///     orqestra.join();
    /// }
    /// ```
    pub fn get(&self, idx: usize) -> Result<MutexGuard<'_, Option<O>>, JobDepErr> {
        if let Some(arc_value) = self.vec.lock().unwrap().get(idx) {
            // Ok(arc_value.0.lock().unwrap())
        } else {
            // Err(JobDepErr::IndexOutOfBounds)
        }
        panic!()
    }
}
