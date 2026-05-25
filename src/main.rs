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

    for i in 0..4 {
        orqestra.secondary_spawn(MyTask(|| {
            sleep(Duration::from_millis(1000));
            println!("done");
        }));
    }

    orqestra.join();
}
