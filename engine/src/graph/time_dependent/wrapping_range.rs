use super::*;

// Struct to represent a range of numbers within a certain rest class.
// Similar to Rusts struct the range is half open, including the start and excluding the end value.
// But when the end value is smaller than the start value this range will include [start..wrap_around) and [0..end).
// This range cannot be empty. If start == end this will be interpretated as the full possible range.
// The full range can also be represented with start = 0 and end = wrap_around.
#[derive(Debug)]
pub struct WrappingRange<Idx: PartialOrd> {
    range: Range<Idx>,
    wrap_around_at: Idx
}

impl<Idx: PartialOrd> WrappingRange<Idx> {
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
}
