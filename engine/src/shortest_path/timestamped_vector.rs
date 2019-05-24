use std::ops::{Index, IndexMut};
use std::sync::atomic::compiler_fence;
use std::sync::atomic::Ordering::SeqCst;

#[derive(Debug)]
pub struct TimestampedVector<T: Clone> {
    data: Vec<T>,
    // choose something small, as overflows are not a problem
    current: u32,
    timestamps: Vec<u32>,
    default: T
}

impl<T: Clone> TimestampedVector<T> {
    pub fn new(size: usize, default: T) -> TimestampedVector<T> {
        TimestampedVector {
            data: vec![default.clone(); size],
            current: 0,
            timestamps: vec![0; size],
            default
        }
    }

    pub fn reset(&mut self) {
        let (new, overflow) = self.current.overflowing_add(1);
        self.current = new;

        if overflow {
            for element in &mut self.data {
                *element = self.default.clone();
            }
        }
    }

    pub fn set(&mut self, index: usize, value: T) {
        self.data[index] = value;
        // we are going to access this container from two different threads
        // unsyncrhnoized and with one thread modifying it - completely unsafe
        // but fine for this case, we only need to make sure, that the timestamp
        // is always modified after the value - which is the cause for this memory fence
        compiler_fence(SeqCst);
        self.timestamps[index] = self.current;
    }
}

impl<T: Clone> Index<usize> for TimestampedVector<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        if self.timestamps[index] == self.current {
            &self.data[index]
        } else {
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
