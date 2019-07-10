pub trait Bool {
    const VALUE: bool;
}

pub struct True;

impl Bool for True {
    const VALUE: bool = true;
}

pub struct False;

impl Bool for False {
    const VALUE: bool = false;
}
