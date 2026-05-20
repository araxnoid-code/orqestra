use orqestra::{Job, JobDep, Orqestra, OrqestraJobTrait, OrqestraTaskTrait};

struct MyTask(fn() -> ());
impl OrqestraTaskTrait<()> for MyTask {
    fn execute(&self) -> () {
        (self.0)()
    }
}

struct MyJob(fn(JobDep<()>) -> ());
impl OrqestraJobTrait<()> for MyJob {
    fn execute(&self, job_dep: JobDep<()>) -> () {
        (self.0)(job_dep)
    }
}

fn main() {
    let orqestra: Orqestra<MyTask, MyJob, (), 64, 4> = Orqestra::new();

    let job_1 = Job::new(MyJob(|_| {
        println!("job 1 done");
    }));

    let job_2 = Job::new(MyJob(|_| {
        println!("job 2 done");
    }));

    let job_3 = Job::new(MyJob(|dep| {
        println!("job 3 done");
    }))
    .after(&job_1)
    .after(&job_2);

    orqestra.job_exec(job_1);
    orqestra.job_exec(job_2);

    orqestra.join();
}
