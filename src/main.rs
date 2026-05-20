use orqestra::{Job, JobDep, Orqestra, OrqestraJobTrait, OrqestraTaskTrait};

struct MyTask(fn() -> usize);
impl OrqestraTaskTrait<usize> for MyTask {
    fn execute(&self) -> usize {
        (self.0)()
    }
}

struct MyJob(fn(JobDep<usize>) -> usize);
impl OrqestraJobTrait<usize> for MyJob {
    fn execute(&self, job_dep: JobDep<usize>) -> usize {
        (self.0)(job_dep)
    }
}

fn main() {
    let orqestra: Orqestra<MyTask, MyJob, _, 64, 4> = Orqestra::new();

    let job_1 = Job::new(MyJob(|_| {
        println!("job 1 done");
        10
    }));

    let job_2 = Job::new(MyJob(|_| {
        println!("job 2 done");
        20
    }))
    .after(&job_1);

    orqestra.job_exec(job_1);

    orqestra.join();
}
