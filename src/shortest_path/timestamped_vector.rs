use std::ops::Index;

#[derive(Debug)]
pub struct TimestampedVector<T: Copy> {
    data: Vec<T>,
    // choose something small, as overflows are not a problem
    current: u8,
    timestamps: Vec<u8>,
    default: T
}

impl<T: Copy> TimestampedVector<T> {
    pub fn new(size: usize, default: T) -> TimestampedVector<T> {
        TimestampedVector {
            data: vec![default; size],
            current: 0,
            timestamps: vec![0; size],
            default: default
        }
    }

    pub fn reset(&mut self) {
        self.current += 1;
    }

    pub fn set(&mut self, index: usize, value: T) {
        self.data[index] = value;
        self.timestamps[index] = self.current;
    }
}

impl<T: Copy> Index<usize> for TimestampedVector<T> {
    type Output = T;

    fn index(&self, index: usize) -> &T {
        if self.timestamps[index] == self.current {
            &self.data[index]
        } else {
            &self.default
        }
    }
}
