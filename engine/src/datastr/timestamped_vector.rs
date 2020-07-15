//! A fast resettable vector based on timestamps.

use std::ops::{Index, IndexMut};

/// A fast resettable vector based on 32bit timestamps.
/// When only few entries are modified, a clearlist based approach may actually be preferable
/// The elements can be modified through the index traits.
/// Other modifications are not permitted.
#[derive(Debug)]
pub struct TimestampedVector<T> {
    data: Vec<T>,
    // timestamp for current iteration. Up to date values will have this one
    current: u32,
    // current timestamp for each entry.
    timestamps: Vec<u32>,
    default: T,
}

impl<T: Clone> TimestampedVector<T> {
    /// Create a new `TimestampedVector` with `size` elements of the default
    pub fn new(size: usize, default: T) -> TimestampedVector<T> {
        TimestampedVector {
            data: vec![default.clone(); size],
            current: 0,
            timestamps: vec![0; size],
            default,
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
                *element = self.default.clone();
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
}

impl<T: Clone> Index<usize> for TimestampedVector<T> {
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

impl<T: Clone> IndexMut<usize> for TimestampedVector<T> {
    fn index_mut(&mut self, index: usize) -> &mut T {
        if self.timestamps[index] != self.current {
            self.set(index, self.default.clone());
        }
        &mut self.data[index]
    }
}
