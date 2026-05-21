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
    //  |
    // [0, 0, 0, 0, 0, 0, 0, 0]
    //  |
    //  p

    let orqestra: Orqestra<MyTask, MyJob, (), 8, 1> = Orqestra::new();

    orqestra.spawn_task(MyTask(|| {
        sleep(Duration::from_millis(1000));
        println!("dummy done");
    }));

    for i in 0..8 {
        orqestra.spawn_task(MyTask(|| {
            sleep(Duration::from_millis(1000));
            println!("dummy done");
        }));
    }

    let _ = orqestra.try_spawn_task(MyTask(|| {
        println!("done 1");
        sleep(Duration::from_millis(1000));
    }));

    let _ = orqestra.spawn_task(MyTask(|| {
        println!("done 2");
        sleep(Duration::from_millis(1000));
    }));

    orqestra.join();
}
