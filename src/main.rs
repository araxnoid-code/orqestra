use orqestra::{Job, Orqestra, OrqestraJobTrait, OrqestraTaskTrait};

struct MyTask(usize);
impl OrqestraTaskTrait<()> for MyTask {
    fn execute(&self) -> () {
        println!("execute!");
    }
}

impl OrqestraJobTrait<()> for MyTask {
    fn execute(&self) -> () {
        println!("job execute! from {}", self.0);
    }
}

fn main() {
    let orqestra: Orqestra<MyTask, MyTask, _, 32, 4> = Orqestra::new();

    let job_1 = Job::new(MyTask(0));
    let job_2 = Job::new(MyTask(1));

    let job_3 = Job::new(MyTask(2)).after(&job_1).after(&job_2);
    let job_4 = Job::new(MyTask(3)).after(&job_3).after(&job_2);

    let job_5 = Job::new(MyTask(4)).after(&job_3).after(&job_4);

    orqestra.job_exec(job_1);
    orqestra.job_exec(job_2);

    orqestra.join();
}
