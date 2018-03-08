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

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InRangeOption<T: Sentinel + Debug>(T);

impl<T: Sentinel + Debug> InRangeOption<T> {
    pub fn new(value: Option<T>) -> InRangeOption<T> {
        match value {
            Some(value) => {
                assert_ne!(value, T::SENTINEL, "InRangeOption::new: Got sentinel as a value");
                InRangeOption(value)
            },
            None => InRangeOption(T::SENTINEL),
        }
    }

    pub fn value(&self) -> Option<T> {
        let &InRangeOption(value) = self;
        if value != T::SENTINEL {
            Some(value)
        } else {
            None
        }
    }
}

impl<T: Sentinel + Debug> Default for InRangeOption<T> {
    fn default() -> Self {
        Self::new(None)
    }
}
