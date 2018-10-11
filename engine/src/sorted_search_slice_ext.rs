use std::ops::Range;
use std::cmp::Ordering;
use crate::graph::time_dependent::*;
use crate::graph::time_dependent::period;

pub trait SortedSearchSliceExt {
    type Item;

    fn sorted_search(&self, x: &Self::Item) -> Result<usize, usize>
        where Self::Item: Ord
    {
        self.sorted_search_by(|p| p.cmp(x))
    }
    fn sorted_search_by<'a, F>(&'a self, f: F) -> Result<usize, usize>
        where F: FnMut(&'a Self::Item) -> Ordering;

    fn sorted_search_by_key<'a, B, F>(&'a self, b: &B, mut f: F) -> Result<usize, usize>
        where F: FnMut(&'a Self::Item) -> B, B: Ord
    {
        self.sorted_search_by(|k| f(k).cmp(b))
    }
}

impl<T> SortedSearchSliceExt for [T] {
    type Item = T;

    #[inline]
    fn sorted_search_by<'a, F>(&'a self, f: F) -> Result<usize, usize>
        where F: FnMut(&'a Self::Item) -> Ordering {

        // if self.len() > 30 {
            self.binary_search_by(f)
        // } else {
        //     for (i, el) in self.iter().enumerate() {
        //         use std::cmp::Ordering::*;
        //         match f(el) {
        //             Equal => return Ok(i),
        //             Greater => return Err(i),
        //             Less => {}
        //         }
        //     }
        //     Err(self.len())
        // }
    }
}

#[derive(Debug)]
pub enum Location {
    On(usize),
    Between(usize, usize),
}

pub trait FullPeriodTimestampSliceExt : SortedSearchSliceExt {
    fn index_ranges<'a, F>(&'a self, range: &WrappingRange, f: F) -> (Range<usize>, Range<usize>)
        where F: FnMut(&'a Self::Item) -> Timestamp;

    fn index_range<'a, F>(&'a self, range: &Range<Timestamp>, f: F) -> Range<usize>
        where F: FnMut(&'a Self::Item) -> Timestamp;

    fn locate<'a, F>(&'a self, time: &Timestamp, f: F) -> Location
        where F: FnMut(&'a Self::Item) -> Timestamp;
}

impl<T> FullPeriodTimestampSliceExt for [T] {
    #[inline]
    fn locate<'a, F>(&'a self, time: &Timestamp, f: F) -> Location
        where F: FnMut(&'a Self::Item) -> Timestamp
    {
        if *time == 0 {
            return Location::On(0);
        }
        if *time == period() {
            return Location::On(self.len() - 1);
        }
        match self.sorted_search_by_key(time, f) {
            Ok(index) => Location::On(index),
            Err(upper_index) => {
                debug_assert!(upper_index > 0);
                debug_assert!(upper_index < self.len());
                let lower_index = upper_index - 1;
                Location::Between(lower_index, upper_index)
            },
        }
    }

    fn index_ranges<'a, F>(&'a self, range: &WrappingRange, mut f: F) -> (Range<usize>, Range<usize>)
        where F: FnMut(&'a Self::Item) -> Timestamp
    {
        if range.full_range() {
            // I'm pretty sure, we don't need this extra if, so I commented it out
            // when taking else we will get 0..1 as the second range, which is fine, since we only iterate in windows of 2
            // Anyway, leaving this here in case my assumptions turn our to be wrong...
            // if range.start() == 0 {
            //     ((0..self.len()), (0..0))
            // } else {
            debug_assert!(range.start() < period());
            match self.locate(&range.start(), f) {
                Location::On(index) => ((index..self.len()), (0..index+1)),
                Location::Between(lower_index, upper_index) => {
                    ((lower_index..self.len()), (0..upper_index+1))
                },
            }
            // }
        } else {
            let start_index = match self.locate(&range.start(), |el| f(el)) {
                Location::On(index) => index,
                Location::Between(lower_index, _upper_index) => {
                    lower_index
                },
            };
            let end_index = match self.locate(&range.end(), |el| f(el)) {
                Location::On(index) => index + 1,
                Location::Between(_lower_index, upper_index) => {
                    upper_index + 1
                },
            };

            if range.end() < range.start() {
                ((start_index..self.len()), (0..end_index))
            } else {
                ((start_index..end_index), (0..0))
            }
        }
    }

    fn index_range<'a, F>(&'a self, range: &Range<Timestamp>, mut f: F) -> Range<usize>
        where F: FnMut(&'a Self::Item) -> Timestamp
    {
        if range.start == range.end {
            (0..0)
        } else {
            let start_index = match self.locate(&range.start, |el| f(el)) {
                Location::On(index) => index,
                Location::Between(lower_index, _upper_index) => {
                    lower_index
                },
            };
            let end_index = match self.locate(&range.end, |el| f(el)) {
                Location::On(index) => index + 1,
                Location::Between(_lower_index, upper_index) => {
                    upper_index + 1
                },
            };

            (start_index..end_index)
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_sorted_search() {
        let s = [0, 1, 1, 1, 1, 2, 3, 5, 8, 13, 21, 34, 55];

        assert_eq!(s.sorted_search(&13),  Ok(9));
        assert_eq!(s.sorted_search(&4),   Err(7));
        assert_eq!(s.sorted_search(&100), Err(13));
        let r = s.sorted_search(&1);
        assert!(match r { Ok(1...4) => true, _ => false, });
    }
}
