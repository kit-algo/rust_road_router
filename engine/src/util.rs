use std::cmp::Ordering;

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

#[derive(PartialEq, PartialOrd)]
pub struct NonNan(f32);

impl NonNan {
    pub fn new(val: f32) -> Option<NonNan> {
        if val.is_nan() {
            None
        } else {
            Some(NonNan(val))
        }
    }
}

impl Eq for NonNan {}

impl Ord for NonNan {
    fn cmp(&self, other: &NonNan) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub trait TapOps: Sized {
    fn tap<R, F>(self, f: F) -> Self
    where
        F: FnOnce(&mut Self) -> R;
}

impl<T> TapOps for T
where
    T: Sized,
{
    fn tap<R, F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut Self) -> R,
    {
        let _ = f(&mut self);
        self
    }
}
