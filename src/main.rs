use orqestra::{Orqestra, OrqestraTaskTrait};

struct MyTask;
impl OrqestraTaskTrait<()> for MyTask {
    fn execute(&self) -> () {
        println!("execute!");
    }
}

fn main() {
    let orqestra: Orqestra<MyTask, _, 32, 4> = Orqestra::new();

    orqestra.try_spawn_task(MyTask).unwrap();
    orqestra.try_spawn_task(MyTask).unwrap();

    orqestra.join();
}
