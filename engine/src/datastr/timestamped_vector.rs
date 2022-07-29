//! A fast resettable vector based on timestamps.

use crate::{datastr::graph::*, util::in_range_option::*};
use std::ops::{Index, IndexMut};

pub trait Reset: Clone {
    const DEFAULT: Self;
    fn reset(&mut self) {
        *self = Self::DEFAULT;
    }
}

impl Reset for Weight {
    const DEFAULT: Self = INFINITY;
}
impl Reset for (Weight, Weight) {
    const DEFAULT: Self = (INFINITY, INFINITY);
}
impl Reset for (Weight, Weight, Weight) {
    const DEFAULT: Self = (INFINITY, INFINITY, INFINITY);
}
impl<T: Sentinel + std::fmt::Debug> Reset for InRangeOption<T> {
    const DEFAULT: Self = InRangeOption::NONE;
}
impl Reset for bool {
    const DEFAULT: Self = false;
}
impl<T: Clone> Reset for Vec<T> {
    const DEFAULT: Self = vec![];
    fn reset(&mut self) {
        self.clear()
    }
}

/// A fast resettable vector based on 32bit timestamps.
/// When only few entries are modified, a clearlist based approach may actually be preferable
/// The elements can be modified through the index traits.
/// Other modifications are not permitted.
#[derive(Clone)]
pub struct TimestampedVector<T> {
    data: Vec<T>,
    // timestamp for current iteration. Up to date values will have this one
    current: u32,
    // current timestamp for each entry.
    timestamps: Vec<u32>,
    default: T,
}

impl<T: Reset> TimestampedVector<T> {
    /// Create a new `TimestampedVector` with `size` elements of the default
    pub fn new(size: usize) -> TimestampedVector<T> {
        TimestampedVector {
            data: vec![T::DEFAULT; size],
            current: 0,
            timestamps: vec![0; size],
            default: T::DEFAULT,
        }
    }

    /// Reset all elements to the default.
    /// Amortized O(1).
    pub fn reset(&mut self) {
        let (new, overflow) = self.current.overflowing_add(1);
        self.current = new;

        // we have to reset all values manually on overflow, because we now might encounter old timestamps again
        if overflow {
            for element in &mut self.data {
                element.reset();
            }
        }
    }

    /// Update an individual element.
    /// Slightly more efficient than going through `index_mut` because no branching is involved.
    pub fn set(&mut self, index: usize, value: T) {
        self.data[index] = value;
        // Unconditionally update to the current time stamp
        self.timestamps[index] = self.current;
    }

    /// Number of elements in the data structure
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Are there no elements in the data structure
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    pub unsafe fn get_unchecked(&self, index: usize) -> &T {
        debug_assert!(index < self.len());
        // If element is from the current iteration use the element, otherwise the default
        if *self.timestamps.get_unchecked(index) == self.current {
            self.data.get_unchecked(index)
        } else {
            // fine since immutable
            &self.default
        }
    }

    pub unsafe fn get_unchecked_mut(&mut self, index: usize) -> &mut T {
        debug_assert!(index < self.len());
        let timestamp = self.timestamps.get_unchecked_mut(index);
        let val = self.data.get_unchecked_mut(index);
        if *timestamp != self.current {
            *timestamp = self.current;
            val.reset();
        }
        val
    }
}

impl<T: Reset> Index<usize> for TimestampedVector<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        // If element is from the current iteration use the element, otherwise the default
        if self.timestamps[index] == self.current {
            &self.data[index]
        } else {
            // fine since immutable
            &self.default
        }
    }
}

impl<T: Reset> IndexMut<usize> for TimestampedVector<T> {
    fn index_mut(&mut self, index: usize) -> &mut T {
        if self.timestamps[index] != self.current {
            self.timestamps[index] = self.current;
            self.data[index].reset();
        }
        &mut self.data[index]
    }
}

use std::sync::atomic::{AtomicU64, Ordering};

pub struct AtomicDists {
    data: Box<[std::sync::atomic::AtomicU64]>,
    current: u32,
}

impl AtomicDists {
    pub fn new(size: usize) -> Self {
        AtomicDists {
            data: (0..size).map(|_| AtomicU64::new(from_pair((INFINITY, 0)))).collect(),
            current: 0,
        }
    }

    /// Reset all elements to the default.
    /// Amortized O(1).
    pub fn reset(&mut self) {
        let (new, overflow) = self.current.overflowing_add(1);
        self.current = new;

        // we have to reset all values manually on overflow, because we now might encounter old timestamps again
        if overflow {
            for element in &self.data[..] {
                element.store(from_pair((INFINITY, 0)), Ordering::Relaxed);
            }
        }
    }

    pub fn set(&self, index: usize, value: Weight) {
        self.data[index].store(from_pair((value, self.current)), Ordering::Relaxed);
    }

    pub fn get(&self, index: usize) -> Weight {
        let (w, ts) = to_pair(self.data[index].load(Ordering::Relaxed));
        if ts == self.current {
            w
        } else {
            INFINITY
        }
    }

    /// Number of elements in the data structure
    pub fn len(&self) -> usize {
        self.data.len()
    }

    /// Are there no elements in the data structure
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

fn from_pair((w, ts): (Weight, u32)) -> u64 {
    unsafe { std::mem::transmute((w, ts)) }
}
fn to_pair(combined: u64) -> (Weight, u32) {
    unsafe { std::mem::transmute(combined) }
}
