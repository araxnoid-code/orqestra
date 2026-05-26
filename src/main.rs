use std::{sync::atomic::Ordering, thread::sleep, time::Duration};

use orqestra::{Job, JobDep, Orqestra, OrqestraJobTrait, OrqestraTaskTrait};

struct MyTask(fn(usize) -> (), usize);
impl OrqestraTaskTrait<()> for MyTask {
    fn execute(&self) -> () {
        (self.0)(self.1)
    }
}

struct MyJob((fn(&Self, JobDep<()>) -> (), usize));
impl OrqestraJobTrait<()> for MyJob {
    fn execute(&self, job_dep: JobDep<()>) -> () {
        self.0.0(self, job_dep)
    }
}

fn main() {
    let orqestra: Orqestra<MyTask, MyJob, (), 64, 16> = Orqestra::new();

    orqestra.join();
}
