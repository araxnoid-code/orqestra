use std::{thread::sleep, time::Duration};

use orqestra::{Job, JobDep, Orqestra, OrqestraJobTrait, OrqestraTaskTrait};

struct MyTask(fn() -> ());
impl OrqestraTaskTrait<()> for MyTask {
    fn execute(&self) -> () {
        (self.0)()
    }
}

struct MyJob((fn(&Self, JobDep<()>) -> (), usize));
impl OrqestraJobTrait<()> for MyJob {
    fn execute(&self, job_dep: JobDep<()>) -> () {
        self.0.0(self, job_dep)
    }
}

fn main() {
    let orqestra: Orqestra<MyTask, MyJob, (), 4, 2> = Orqestra::new();

    for i in 0..5 {
        let job = Job::new(MyJob((
            |this, _| {
                sleep(Duration::from_millis(1000));
                println!("job {} done", this.0.1);
            },
            i,
        )));

        Job::new(MyJob((
            |this, _| {
                sleep(Duration::from_millis(1000));
                println!("inner job {} done", this.0.1);
            },
            i,
        )))
        .after(&job);

        orqestra.job_exec(job);
    }

    orqestra.join();
}
