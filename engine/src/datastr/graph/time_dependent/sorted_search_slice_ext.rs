use crate::datastr::graph::time_dependent::period;
use crate::datastr::graph::time_dependent::*;
use std::ops::Range;

#[derive(Debug)]
pub enum Location {
    On(usize),
    Between(usize, usize),
}

pub trait FullPeriodTimestampSliceExt {
    type Item;

    fn index_range<'a, F>(&'a self, range: &Range<Timestamp>, f: F) -> Range<usize>
    where
        F: FnMut(&'a Self::Item) -> Timestamp;

    fn locate<'a, F>(&'a self, time: Timestamp, f: F) -> Location
    where
        F: FnMut(&'a Self::Item) -> Timestamp;
}

impl<T> FullPeriodTimestampSliceExt for [T] {
    type Item = T;

    #[inline]
    fn locate<'a, F>(&'a self, time: Timestamp, f: F) -> Location
    where
        F: FnMut(&'a Self::Item) -> Timestamp,
    {
        if time == 0 {
            return Location::On(0);
        }
        if time == period() {
            return Location::On(self.len() - 1);
        }
        match self.binary_search_by_key(&time, f) {
            Ok(index) => Location::On(index),
            Err(upper_index) => {
                debug_assert!(upper_index > 0);
                debug_assert!(upper_index < self.len());
                let lower_index = upper_index - 1;
                Location::Between(lower_index, upper_index)
            }
        }
    }

    fn index_range<'a, F>(&'a self, range: &Range<Timestamp>, mut f: F) -> Range<usize>
    where
        F: FnMut(&'a Self::Item) -> Timestamp,
    {
        if range.start == range.end {
            (0..0)
        } else {
            let start_index = match self.locate(range.start, |el| f(el)) {
                Location::On(index) => index,
                Location::Between(lower_index, _upper_index) => lower_index,
            };
            let end_index = match self.locate(range.end, |el| f(el)) {
                Location::On(index) => index + 1,
                Location::Between(_lower_index, upper_index) => upper_index + 1,
            };

            (start_index..end_index)
        }
    }
}
