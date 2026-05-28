use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    thread::sleep,
    time::Duration,
};

use orqestra::{Job, JobDep, Orqestra, OrqestraJobTrait, OrqestraTaskTrait};
struct MyTask;
impl OrqestraTaskTrait<()> for MyTask {
    fn execute(&self) -> () {
        sleep(Duration::from_millis(500));
        println!("execute!");
    }
}

struct MyJob(
    fn(JobDep<()>, usize, Arc<AtomicUsize>) -> (),
    usize,
    Arc<AtomicUsize>,
);
impl OrqestraJobTrait<()> for MyJob {
    fn execute(&self, job_dep: JobDep<()>) -> () {
        (self.0)(job_dep, self.1, self.2.clone())
    }
}

fn main() {
    let orqestra: Orqestra<MyTask, MyJob, _, 16, 8> = Orqestra::new();
    let job_count = Arc::new(AtomicUsize::new(0));
    let child_job_count = Arc::new(AtomicUsize::new(0));

    for i in 0..1000 {
        let job = Job::new(MyJob(
            |_, idx, counter| {
                sleep(Duration::from_millis(250));
                // println!("done job {}", idx);
                // counter.fetch_add(1, Ordering::Relaxed);
            },
            i,
            job_count.clone(),
        ));

        Job::new(MyJob(
            |_, idx, counter| {
                sleep(Duration::from_millis(250));
                // println!("done child job {}", idx);
                // counter.fetch_add(1, Ordering::Relaxed);
            },
            i,
            child_job_count.clone(),
        ))
        .after(&job);

        orqestra.job_exec(job);
    }

    orqestra.join();
}
