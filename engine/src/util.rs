use std::cmp::Ordering;

pub mod in_range_option;

/// Poor mans const generic bools, while waiting for actual support.
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

/// Yes rust, I know floats are dangerous but I have to sort them anyway.
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

/// Util function to chain unchainable function calls.
///
/// ```
/// # use bmw_routing_engine::util::TapOps;
/// assert_eq!(vec![2,7,4].tap(|slf| slf.sort()).tap(|slf| slf.pop()), vec![2,4]);
/// ```
pub trait TapOps: Sized {
    fn tap<R, F>(mut self, f: F) -> Self
    where
        F: FnOnce(&mut Self) -> R,
    {
        let _ = f(&mut self);
        self
    }
}

impl<T> TapOps for T where T: Sized {}
