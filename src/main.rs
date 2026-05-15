fn main() {
    let zoo = vec![Cat::new(), Dog::new()];
}

trait AnimalTrait {
    fn walk(&self) {
        println!("animal walking")
    }
}

struct Cat;
impl Cat {
    fn new() -> Animal {
        Animal::Cat(Cat)
    }
}
impl AnimalTrait for Cat {}

struct Dog;
impl Dog {
    fn new() -> Animal {
        Animal::Dog(Dog)
    }
}
impl AnimalTrait for Dog {}

enum Animal {
    Cat(Cat),
    Dog(Dog),
}

impl AnimalTrait for Animal {
    fn walk(&self) {
        match self {
            Animal::Cat(cat) => cat.walk(),
            Animal::Dog(dog) => dog.walk(),
        }
    }
}
