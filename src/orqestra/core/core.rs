use std::{
    sync::{Arc, atomic::Ordering},
    thread::{park_timeout, yield_now},
    time::Duration,
};

use crate::{
    ExecutableTask, Job, OrqestraJobTrait, OrqestraTaskTrait, RingBufferCore, WaitingTask,
    orqestra::execute_core::ExecuteCore,
};

/// The main structure in managing the generated tasks,
/// allocation into ring-buffers, managing workflows
/// and synchronizing between ExecutableTask and worker management.
/// ## 2 Main Core
/// ### RingBufferCore
/// The part responsible for processing tasks and jobs, creating ExecutableTask,
/// and managing the ring buffer. Adding(enqueue) and removing(dequeue) elements must be done
/// through the Ring Buffer in the TaskCore structure.
/// ### ExecuteCore
/// The part that functions as the task executor in the ring-buffer,
/// has a Thread Pool where each thread will access the ring-buffer simultaneously.
pub struct Orqestra<T, J, O, const RING_BUFFER_SIZE: usize, const WORKERS_SIZE: usize>
where
    T: OrqestraTaskTrait<O> + 'static + Send,
    J: OrqestraJobTrait<O> + 'static + Send + Sync,
    O: 'static + Send,
{
    /// The part responsible for processing tasks and jobs, creating ExecutableTask,
    /// and managing the ring buffer. Adding(enqueue) and removing(dequeue) elements must be done
    /// through the Ring Buffer in the TaskCore structure.
    ring_buffer_core: Arc<RingBufferCore<T, J, O, RING_BUFFER_SIZE>>,

    /// The part that functions as the task executor in the ring-buffer,
    /// has a Thread Pool where each thread will access the ring-buffer
    execute_core: ExecuteCore<RING_BUFFER_SIZE, WORKERS_SIZE>,
}

impl<T, J, O, const RING_BUFFER_SIZE: usize, const WORKERS_SIZE: usize>
    Orqestra<T, J, O, RING_BUFFER_SIZE, WORKERS_SIZE>
where
    T: OrqestraTaskTrait<O> + 'static + Send,
    J: OrqestraJobTrait<O> + 'static + Send + Sync,
    O: 'static + Send,
{
    /// initial requires manual initialization of the data type as
    /// task, job and size of the ring-buffer and the number of workers to spawn
    /// ## initialization
    /// requires Initialization of data types that implement `OrqestraTaskTrait` and `OrqestraJobTrait`.
    /// requires size initialization for ring-buffer size and number of workers.
    /// ```rust
    /// // OrqestraTaskTrait
    /// struct MyTask(fn() -> usize);
    /// impl OrqestraTaskTrait<usize> for MyTask {
    ///     fn execute(&self) -> usize {
    ///         (self.0)()
    ///     }
    /// }
    ///
    /// // OrqestraJobTrait
    /// struct MyJob(fn(JobDep<usize>) -> usize);
    /// impl OrqestraJobTrait<usize> for MyJob {
    ///     fn execute(&self, job_dep: JobDep<usize>) -> usize {
    ///         (self.0)(job_dep)
    ///     }
    /// }
    ///
    /// fn main() {
    ///    // Orqestra<T: OrqestraTaskTrait, J:OrqestraJobTrait, O: 'static, RING_BUFFER_SIZE, WORKERS_SIZE>
    ///    let orqestra: Orqestra<MyTask, MyJob, usize, 64, 4> = Orqestra::new();
    /// }
    /// ```
    /// ##
    pub fn new() -> Orqestra<T, J, O, RING_BUFFER_SIZE, WORKERS_SIZE> {
        let ring_buffer_core = Arc::new(RingBufferCore::new());
        let execute_core = ExecuteCore::new(ring_buffer_core.clone());
        Self {
            ring_buffer_core,
            execute_core,
        }
    }

    /// serves to spawn a task that will be executed by workers in Orqestra
    /// accepts data types that already implement the `OrqestraTaskTrait` trait
    /// ```rust
    /// use orqestra::{Orqestra, OrqestraTaskTrait};
    ///
    /// struct MyTask;
    /// impl OrqestraTaskTrait<()> for MyTask {
    ///     fn execute(&self) -> () {
    ///         println!("execute!");
    ///     }
    /// }
    ///
    /// fn main() {
    ///     let orqestra: Orqestra<MyTask, _, 32, 4> = Orqestra::new();
    ///
    ///     orqestra.spawn_task(MyTask);
    ///     orqestra.spawn_task(MyTask);
    ///
    ///     orqestra.join();
    /// }
    /// ```
    /// ## Blocking
    /// When the ring buffer is full, blocking will occur until there is space for the task that has been spawned.
    pub fn spawn_task(&self, task: T) {
        self.ring_buffer_core
            .enqueue(ExecutableTask::new_task(task));
    }

    /// serves to spawn a task that will be executed by workers in Orqestra
    /// accepts data types that already implement the `OrqestraTaskTrait` trait
    /// ```rust
    /// use orqestra::{Orqestra, OrqestraTaskTrait};
    ///
    /// struct MyTask;
    /// impl OrqestraTaskTrait<()> for MyTask {
    ///     fn execute(&self) -> () {
    ///         println!("execute!");
    ///     }
    /// }
    ///
    /// fn main() {
    ///     let orqestra: Orqestra<MyTask, _, 32, 4> = Orqestra::new();
    ///
    ///     orqestra.try_spawn_task(MyTask).unwrap();
    ///     orqestra.try_spawn_task(MyTask).unwrap();
    ///
    ///     orqestra.join();
    /// }
    /// ```
    /// ## non Blocking
    /// when the ring-buffer is full, it will give Result::Err.
    pub fn try_spawn_task(&self, task: T) -> Result<(), &str> {
        self.ring_buffer_core
            .try_enqueue(ExecutableTask::new_task(task))
    }

    /// Executes a Job that has been initialized from the Job struct
    /// This method must be used on a Job that is the initialization of a graph schedule.
    /// ```rust
    /// fn main() {
    ///     let orqestra: Orqestra<MyTask, MyJob, (), 64, 4> = Orqestra::new();
    ///
    ///     let job_1 = Job::new(MyJob(|_| {
    ///         println!("job 1 done");
    ///     }));
    ///
    ///     let job_2 = Job::new(MyJob(|_| {
    ///         println!("job 2 done");
    ///     }));
    ///
    ///     let job_3 = Job::new(MyJob(|dep| {
    ///         println!("job 3 done");
    ///     }))
    ///     .after(&job_1)
    ///     .after(&job_2);
    ///
    ///     orqestra.job_exec(job_1);
    ///     orqestra.job_exec(job_2);
    ///
    ///     orqestra.join();
    /// }
    /// ```
    ///
    /// ```
    /// The code above will create a graph like this
    ///
    ///  Job_1  Job_2
    ///    \     /
    ///     \   /
    ///      \ /
    ///     Job_3
    /// ```
    ///
    /// Because job_1 and job_2 are the beginning/initial of the graph,
    /// only job_1 and job_2 are executed using Orqestra::job_exec
    /// ```rust
    /// //...
    /// orqestra.job_exec(job_1);
    /// orqestra.job_exec(job_2);
    /// //...
    /// ```
    pub fn job_exec(&self, job: Job<J, O>) {
        self.ring_buffer_core.enqueue(ExecutableTask::new_job(job));
    }

    ///
    pub fn secondary_spawn(&self, task: T) {
        self.ring_buffer_core
            .secondary_push(ExecutableTask::new_task(task));
    }

    /// At the end of the Orchestra flow, blocking will occur until all spawned
    /// tasks and jobs are completed and cleanup is carried out.
    pub fn join(self) {
        let mut counter = 0;
        while self.execute_core.done_task.load(Ordering::Relaxed)
            < self.ring_buffer_core.in_task.load(Ordering::Relaxed)
        {
            if counter < 500 {
                yield_now();
            } else {
                park_timeout(Duration::from_millis(10));
                continue;
            }
            counter += 1;
        }

        self.execute_core.join_flag.store(true, Ordering::Relaxed);

        for worker in self.execute_core.workers {
            worker.join().unwrap();
        }

        self.ring_buffer_core.drop_queue();
    }

    pub fn get_ring_buffer_core(&self) -> &RingBufferCore<T, J, O, RING_BUFFER_SIZE> {
        &*self.ring_buffer_core
    }

    pub fn get_execute_core(&self) -> &ExecuteCore<RING_BUFFER_SIZE, WORKERS_SIZE> {
        &self.execute_core
    }
}
