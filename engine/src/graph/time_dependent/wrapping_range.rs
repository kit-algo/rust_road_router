use super::*;

// Struct to represent a range of numbers within a certain rest class.
// Similar to Rusts struct the range is half open, including the start and excluding the end value.
// But when the end value is smaller than the start value this range will include [start..period()) and [0..end).
// This range cannot be empty. If start == end this will be interpretated as the full possible range.
#[derive(Debug, Clone)]
pub struct WrappingRange {
    range: Range<Timestamp>,
}

impl WrappingRange {
    pub fn new(range: Range<Timestamp>) -> WrappingRange {
        debug_assert!(range.start < period());
        debug_assert!(range.end < period());
        WrappingRange { range }
    }

    pub fn start(&self) -> Timestamp {
        self.range.start
    }

    pub fn end(&self) -> Timestamp {
        self.range.end
    }

    pub fn contains(&self, item: Timestamp) -> bool {
        debug_assert!(item < period());

        if self.start() < self.end() {
            item >= self.start() && item < self.end()
        } else {
            item >= self.start() || item < self.end()
        }
    }

    pub fn shift_start(&mut self, start: Timestamp) {
        debug_assert!(self.contains(start));
        if self.full_range() {
            self.range.end = start;
        }
        self.range.start = start;
    }

    pub fn full_range(&self) -> bool {
        self.range.start == self.range.end
    }
}
