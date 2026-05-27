use orqestra::{JobDep, Orqestra, OrqestraJobTrait, OrqestraTaskTrait};
struct MyTask;
impl OrqestraTaskTrait<()> for MyTask {
    fn execute(&self) -> () {
        println!("execute!");
    }
}

struct MyJob(fn(JobDep<()>) -> ());
impl OrqestraJobTrait<()> for MyJob {
    fn execute(&self, job_dep: JobDep<()>) -> () {
        (self.0)(job_dep)
    }
}

fn main() {
    let orqestra: Orqestra<MyTask, MyJob, _, 32, 4> = Orqestra::new();

    orqestra.secondary_spawn_task(MyTask);
    orqestra.secondary_spawn_task(MyTask);

    orqestra.join();
}
