//! A fast resettable vector.

use std::ops::{Index, IndexMut};

/// A fast resettable vector based on a clearlist.
/// Resetting is efficient when few entries where modified.
/// The elements can be modified through the index traits.
/// Other modifications are not permitted.
pub struct ClearlistVector<T> {
    data: Vec<T>,
    clearlist: Vec<usize>,
    default: T,
}

impl<T: Clone> ClearlistVector<T> {
    /// Create a new `ClearlistVector` with size elements of the given default.
    pub fn new(size: usize, default: T) -> ClearlistVector<T> {
        ClearlistVector {
            data: vec![default.clone(); size],
            clearlist: Vec::new(),
            default,
        }
    }

    /// Reset all elements to the default value
    pub fn reset(&mut self) {
        for &idx in &self.clearlist {
            self.data[idx] = self.default.clone();
        }
        self.clearlist.clear();
    }
}

impl<T> Index<usize> for ClearlistVector<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        &self.data[index]
    }
}

impl<T: PartialEq> IndexMut<usize> for ClearlistVector<T> {
    fn index_mut(&mut self, index: usize) -> &mut T {
        if self.data[index] == self.default {
            self.clearlist.push(index)
        }
        &mut self.data[index]
    }
}
