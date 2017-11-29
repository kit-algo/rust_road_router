use std::fmt::Debug;
use std::{u32, u64, usize};

pub trait Sentinel: PartialEq + Copy {
    const SENTINEL: Self;
}

impl Sentinel for u32 {
    const SENTINEL: u32 = u32::MAX;
}

impl Sentinel for u64 {
    const SENTINEL: u64 = u64::MAX;
}

impl Sentinel for usize {
    const SENTINEL: usize = usize::MAX;
}

#[derive(Debug, Clone, Copy)]
pub struct InrangeOption<T: Sentinel + Debug>(T);

impl<T: Sentinel + Debug> InrangeOption<T> {
    pub fn new(value: Option<T>) -> InrangeOption<T> {
        match value {
            Some(value) => {
                assert_ne!(value, T::SENTINEL, "InrangeOption::new: Got sentinel as a value");
                InrangeOption(value)
            },
            None => InrangeOption(T::SENTINEL),
        }
    }

    pub fn value(&self) -> Option<T> {
        let &InrangeOption(value) = self;
        if value != T::SENTINEL {
            Some(value)
        } else {
            None
        }
    }
}
