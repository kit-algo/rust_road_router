use super::*;

#[derive(Debug)]
pub struct WrappingSliceIter<'a> {
    range: WrappingRange<Timestamp>,
    slice: &'a [TTIpp],
    next_index: Option<usize>,
    initial_index: usize
}

impl<'a> WrappingSliceIter<'a> {
    pub fn new(slice: &'a [TTIpp], range: WrappingRange<Timestamp>) -> WrappingSliceIter<'a> {
        if slice.len() == 0 {
            return WrappingSliceIter { range, slice, next_index: None, initial_index: 0 }
        }

        if let Some(last) = slice.last() {
            debug_assert!(last.at < range.wrap_around());
        }
        for values in slice.windows(2) {
            debug_assert!(values[0].at < values[1].at);
        }

        let next_index = match slice.binary_search_by_key(&range.start(), |ipp| ipp.at) {
            Ok(index) => index,
            Err(index) => index,
        };

        WrappingSliceIter { range, slice, next_index: Some(next_index), initial_index: next_index }
    }
}

impl<'a> Iterator for WrappingSliceIter<'a> {
    type Item = &'a TTIpp;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_index.map(|current_index| {
            let next = (current_index + 1) % self.slice.len();
            debug_assert!(next < self.slice.len());
            if next != self.initial_index && self.range.contains(unsafe { self.slice.get_unchecked(next).at }) {
                self.next_index = Some(next);
            } else {
                self.next_index = None
            }
            debug_assert!(current_index < self.slice.len());
            unsafe { self.slice.get_unchecked(current_index) }
        })
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_full_range_starting_at_zero() {
        let range = WrappingRange::new(Range { start: 0, end: 0 }, 10);
        let vec: Vec<_> = vec![(0,0), (2,5), (8,7), (9,4)].into_iter().map(|(at, val)| TTIpp::new(at, val)).collect();

        let elems: Vec<_> = WrappingSliceIter::new(&vec, range).cloned().collect();
        assert_eq!(vec, elems);
    }

    #[test]
    fn test_full_range_starting_in_middle() {
        let range = WrappingRange::new(Range { start: 5, end: 5 }, 10);
        let vec: Vec<_> = vec![(0,0), (2,5), (8,7), (9,4)].into_iter().map(|(at, val)| TTIpp::new(at, val)).collect();

        let elems: Vec<_> = WrappingSliceIter::new(&vec, range).cloned().map(TTIpp::as_tuple).collect();
        assert_eq!(elems, vec![(8,7), (9,4), (0,0), (2,5)]);
    }

    #[test]
    fn test_full_range_starting_on_elem() {
        let range = WrappingRange::new(Range { start: 8, end: 8 }, 10);
        let vec: Vec<_> = vec![(0,0), (2,5), (8,7), (9,4)].into_iter().map(|(at, val)| TTIpp::new(at, val)).collect();

        let elems: Vec<_> = WrappingSliceIter::new(&vec, range).cloned().map(TTIpp::as_tuple).collect();
        assert_eq!(elems, vec![(8,7), (9,4), (0,0), (2,5)]);
    }

    #[test]
    fn test_partial_range() {
        let range = WrappingRange::new(Range { start: 5, end: 1 }, 10);
        let vec: Vec<_> = vec![(0,0), (2,5), (8,7), (9,4)].into_iter().map(|(at, val)| TTIpp::new(at, val)).collect();

        let elems: Vec<_> = WrappingSliceIter::new(&vec, range).cloned().map(TTIpp::as_tuple).collect();
        assert_eq!(elems, vec![(8,7), (9,4), (0,0)]);
    }

    #[test]
    fn test_empty_slice() {
        let range = WrappingRange::new(Range { start: 5, end: 5 }, 10);
        let vec: Vec<_> = vec![];

        let elems: Vec<_> = WrappingSliceIter::new(&vec, range).cloned().collect();
        assert_eq!(elems, vec);
    }
}
