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
    let orqestra: Orqestra<MyTask, MyJob, (), 8, 1> = Orqestra::new();

    let job = Job::new(MyJob((
        |this, _| {
            sleep(Duration::from_millis(2000));
            println!("task {} done", this.0.1)
        },
        0,
    )));

    Job::new(MyJob((
        |this, _| println!("chile task {} done", this.0.1),
        8,
    )))
    .after(&job);

    orqestra.job_exec(job);

    for i in 0..8 {
        let job = Job::new(MyJob((
            |this, _| {
                sleep(Duration::from_millis(1000));
                println!("task {} done", this.0.1)
            },
            i,
        )));

        orqestra.job_exec(job);
    }

    orqestra.join();
}
