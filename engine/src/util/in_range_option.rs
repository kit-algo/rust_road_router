//! No space overhead `Option`s for types with sentinels

use std::fmt::Debug;
use std::{i32, u32, u64, usize};

/// Trait to define sentiel values for types used with `InRangeOption`.
pub trait Sentinel: PartialEq + Copy {
    const SENTINEL: Self;
}

impl Sentinel for u32 {
    const SENTINEL: u32 = u32::MAX;
}

impl Sentinel for i32 {
    const SENTINEL: i32 = i32::MIN;
}

impl Sentinel for u64 {
    const SENTINEL: u64 = u64::MAX;
}

impl Sentinel for usize {
    const SENTINEL: usize = usize::MAX;
}

/// A struct to get `Option`s without space overhead.
///
/// This is conceptually similar to the `NonNull` types rust provides
/// or `Option`s of references.
/// This type is slightly less ergonomic but allows for other sentinel values than null.
///
/// `InRangeOptions` are constructed from real `Options`.
/// To work with the encapsulated data, the type has to be converted back into an actual `Option` through the `value` method.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct InRangeOption<T: Sentinel + Debug>(T);

impl<T: Sentinel + Debug> InRangeOption<T> {
    #[inline]
    pub fn new(value: Option<T>) -> InRangeOption<T> {
        match value {
            Some(value) => {
                assert_ne!(value, T::SENTINEL, "InRangeOption::new: Got sentinel as a value");
                InRangeOption(value)
            }
            None => InRangeOption(T::SENTINEL),
        }
    }

    #[inline]
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
