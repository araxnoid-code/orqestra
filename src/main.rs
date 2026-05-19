use orqestra::{Job, JobDep, Orqestra, OrqestraJobTrait, OrqestraTaskTrait};

type MyOutput = usize;
struct MyTask(fn() -> MyOutput);

impl OrqestraTaskTrait<MyOutput> for MyTask {
    fn execute(&self) -> MyOutput {
        (self.0)()
    }
}

struct MyJob(fn(JobDep<MyOutput>) -> MyOutput);
impl OrqestraJobTrait<MyOutput> for MyJob {
    fn execute(&self, job_dep: JobDep<MyOutput>) -> MyOutput {
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
    }));

    let job_3 = Job::new(MyJob(|dep| {
        let value_1 = dep.get(0).unwrap().unwrap();
        let value_2 = dep.get(1).unwrap().unwrap();
        println!("job 3 done with value {}", value_1 + value_2);
        value_1 + value_2
    }))
    .after(&job_1)
    .after(&job_2);

    let job_4 = Job::new(MyJob(|dep| {
        let value_2 = dep.get(0).unwrap().unwrap();
        let value_3 = dep.get(1).unwrap().unwrap();
        println!("job 4 done with value {}", value_2 + value_3);
        value_2 + value_3
    }))
    .after(&job_2)
    .after(&job_3);

    let job_5 = Job::new(MyJob(|dep| {
        let value_3 = dep.get(0).unwrap().unwrap();
        let value_4 = dep.get(1).unwrap().unwrap();
        println!("job 4 done with value {}", value_3 + value_4);
        0
    }))
    .after(&job_3)
    .after(&job_4);

    orqestra.job_exec(job_1);
    orqestra.job_exec(job_2);

    orqestra.join();
}
