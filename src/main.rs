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
    let orqestra: Orqestra<MyTask, MyJob, (), 1024, 16> = Orqestra::new();
    let ring_buffer = orqestra.get_ring_buffer_core();

    for i in 0..1000 {
        orqestra.spawn_task(MyTask(|| {
            sleep(Duration::from_millis(500));
            println!("done");
        }));
    }

    loop {
        // let counter = ring_buffer
        //     .registered
        //     .load(std::sync::atomic::Ordering::Acquire);

        // println!("registered: {}", counter);

        // if counter == 0 {
        //     break;
        // }
    }

    orqestra.join();
}
