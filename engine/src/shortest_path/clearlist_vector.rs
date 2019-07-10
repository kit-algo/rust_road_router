use std::ops::{Index, IndexMut};

#[derive(Debug)]
pub struct ClearlistVector<T> {
    data: Vec<T>,
    clearlist: Vec<usize>,
    default: T
}

impl<T: Clone> ClearlistVector<T> {
    pub fn new(size: usize, default: T) -> ClearlistVector<T> {
        ClearlistVector {
            data: vec![default.clone(); size],
            clearlist: Vec::new(),
            default
        }
    }

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
