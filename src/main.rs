use orqestra::{Orqestra, OrqestraTaskTrait};

struct MyTask(usize);
impl OrqestraTaskTrait<()> for MyTask {
    fn execute(&self) -> () {
        println!("execute! {}", self.0);
    }
}

fn main() {
    let orqestra: Orqestra<MyTask, _, 64, 4> = Orqestra::new();

    for i in 0..100 {
        orqestra.spawn_task(MyTask(i));
    }

    orqestra.join();
}
