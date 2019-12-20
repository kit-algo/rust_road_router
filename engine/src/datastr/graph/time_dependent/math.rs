//! A few handy methods for ranges

use std::cmp::{max, min};
use std::ops::Range;

pub trait RangeExtensions: Sized {
    type Idx;

    fn intersection(&self, other: &Self) -> Self;
    fn is_intersection_empty(&self, other: &Self) -> bool;
    fn split(self, split_at: Self::Idx) -> (Self, Self);
}

impl<T: Ord + Copy> RangeExtensions for Range<T> {
    type Idx = T;

    fn intersection(&self, other: &Self) -> Self {
        Range {
            start: max(self.start, other.start),
            end: min(self.end, other.end),
        }
    }

    fn is_intersection_empty(&self, other: &Self) -> bool {
        let intersection = self.intersection(other);
        intersection.start >= intersection.end
    }

    fn split(self, split_at: T) -> (Self, Self) {
        if split_at < self.start {
            (
                Self {
                    start: split_at,
                    end: split_at,
                },
                self,
            )
        } else if split_at >= self.end {
            (
                self,
                Self {
                    start: split_at,
                    end: split_at,
                },
            )
        } else {
            (
                Self {
                    start: self.start,
                    end: split_at,
                },
                Self {
                    start: split_at,
                    end: self.end,
                },
            )
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_range_splitting() {
        assert_eq!(Range { start: 5, end: 10 }.split(8), (Range { start: 5, end: 8 }, Range { start: 8, end: 10 }));
        assert_eq!(Range { start: 5, end: 10 }.split(3), (Range { start: 3, end: 3 }, Range { start: 5, end: 10 }));
        assert_eq!(
            Range { start: 5, end: 10 }.split(12),
            (Range { start: 5, end: 10 }, Range { start: 12, end: 12 })
        );
    }
}
