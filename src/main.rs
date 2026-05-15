use orqestra::{Orqestra, OrqestraTaskTrait};

struct MyTask;
impl OrqestraTaskTrait for MyTask {}

// impl OrqestraTaskTrait for MyTask {}

fn main() {
    let orqestra: Orqestra<MyTask, usize, 64> = Orqestra::new();

    for i in 0..65 {
        orqestra.try_spawn_task(MyTask).unwrap();
    }

    println!("done");
}
