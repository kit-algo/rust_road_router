use super::*;

// Struct to represent a range of numbers within a certain rest class.
// Similar to Rusts struct the range is half open, including the start and excluding the end value.
// But when the end value is smaller than the start value this range will include [start..wrap_around) and [0..end).
// This range cannot be empty. If start == end this will be interpretated as the full possible range.
#[derive(Debug, Clone)]
pub struct WrappingRange<Idx: PartialOrd + Copy> {
    range: Range<Idx>,
    wrap_around_at: Idx
}

impl<Idx: PartialOrd + Copy> WrappingRange<Idx> {
    pub fn new(range: Range<Idx>, wrap_around_at: Idx) -> WrappingRange<Idx> {
        debug_assert!(range.start < wrap_around_at);
        debug_assert!(range.end < wrap_around_at);
        WrappingRange { range, wrap_around_at }
    }

    pub fn start(&self) -> &Idx {
        &self.range.start
    }

    pub fn end(&self) -> &Idx {
        &self.range.end
    }

    pub fn wrap_around(&self) -> &Idx {
        &self.wrap_around_at
    }

    pub fn contains(&self, item: Idx) -> bool {
        debug_assert!(item < self.wrap_around_at);

        if self.start() < self.end() {
            item >= *self.start() && item < *self.end()
        } else {
            item >= *self.start() || item < *self.end()
        }
    }

    pub fn shift_start(&mut self, start: Idx) {
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
