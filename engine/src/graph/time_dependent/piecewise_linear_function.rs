use super::*;

#[derive(Debug)]
pub struct PiecewiseLinearFunction<'a> {
    departure_time: &'a [Timestamp],
    travel_time: &'a [Weight],
    period: Timestamp
}

impl<'a> PiecewiseLinearFunction<'a> {
    pub fn new(departure_time: &'a [Timestamp], travel_time: &'a [Weight], period: Weight) -> PiecewiseLinearFunction<'a> {
        debug_assert_eq!(departure_time.len(), travel_time.len());
        debug_assert!(!departure_time.is_empty());
        for dt in departure_time.iter() {
            debug_assert!(*dt < period);
        }
        for pair in departure_time.windows(2) {
            debug_assert!(pair[0] < pair[1]);
        }
        // TODO FIFO

        PiecewiseLinearFunction {
            departure_time, travel_time, period
        }
    }

    pub fn lower_bound(&self) -> Weight {
        *self.travel_time.iter().min().unwrap()
    }

    pub fn upper_bound(&self) -> Weight {
        *self.travel_time.iter().max().unwrap()
    }

    pub fn bounds(&self) -> (Weight, Weight) {
        (self.lower_bound(), self.upper_bound())
    }

    pub fn evaluate(&self, mut departure: Timestamp) -> Weight {
        debug_assert!(departure < self.period);
        if self.departure_time.len() == 1 {
            return unsafe { *self.travel_time.get_unchecked(0) }
        }

        match self.departure_time.sorted_search(&departure) {
            Ok(departure_index) => unsafe { *self.travel_time.get_unchecked(departure_index) },
            Err(upper_index) => {
                let upper_index = upper_index % self.departure_time.len();
                let lower_index = if upper_index > 0 { upper_index - 1 } else { self.departure_time.len() - 1 };
                let lf = unsafe {
                    Line::new(
                        TTIpp::new(*self.departure_time.get_unchecked(lower_index), *self.travel_time.get_unchecked(lower_index)),
                        TTIpp::new(*self.departure_time.get_unchecked(upper_index), *self.travel_time.get_unchecked(upper_index)))
                };
                if departure < lf.from.at {
                    departure += self.period;
                }
                lf.into_monotone_at_line(self.period).interpolate_tt(departure)
            },
        }
    }

    pub fn ipp_iter(&self, range: WrappingRange<Timestamp>) -> Iter<'a> {
        debug_assert_eq!(range.wrap_around(), self.period);
        Iter::new(self.departure_time, self.travel_time, range)
    }

    pub(super) fn seg_iter(&self, range: WrappingRange<Timestamp>) -> SegmentIter<'a> {
        debug_assert_eq!(range.wrap_around(), self.period);
        SegmentIter::new(self.departure_time, self.travel_time, range)
    }

    fn subtract_wrapping(&self, x: Weight, y: Weight) -> Weight {
        (self.period + x - y) % self.period
    }

    pub fn debug_to_s(&self, indent: usize) -> String {
        let mut s = String::from("PLF: ");
        for data in self.departure_time.iter().zip(self.travel_time.iter()) {
            s.push('\n');
            for _ in 0..indent {
                s.push(' ');
                s.push(' ');
            }
            s = s + &format!("{:?}", data);
        }
        s
    }
}

pub fn interpolate(delta_x: Weight, y1: Weight, y2: Weight, x: Timestamp) -> Weight {
    debug_assert!(x <= delta_x);
    debug_assert_ne!(delta_x, 0);
    let delta_y = y2 + delta_x - y1;
    let result = u64::from(y1) + (u64::from(x) * u64::from(delta_y) / u64::from(delta_x));
    result as Weight - x
}

#[derive(Debug)]
pub struct Iter<'a> {
    departure_time: &'a [Timestamp],
    travel_time: &'a [Weight],
    range: WrappingRange<Timestamp>,
    current_index: usize,
    initial_index: usize,
    done: bool
}

impl<'a> Iter<'a> {
    fn new(departure_time: &'a [Timestamp], travel_time: &'a [Weight], range: WrappingRange<Timestamp>) -> Iter<'a> {
        if departure_time.len() <= 1 {
            return Iter { departure_time, travel_time, range, current_index: 0, initial_index: 0, done: true }
        }

        let current_index = match departure_time.sorted_search(&range.start()) {
            Ok(index) => index,
            Err(index) => index
        } % departure_time.len();

        Iter { departure_time, travel_time, range, current_index, initial_index: current_index, done: false }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = TTIpp;

    fn next(&mut self) -> Option<Self::Item> {
        let ipp = unsafe { *self.departure_time.get_unchecked(self.current_index) };

        if !self.done && self.range.contains(ipp) {
            let tt = unsafe { *self.travel_time.get_unchecked(self.current_index) };
            self.current_index = (self.current_index + 1) % self.departure_time.len();
            if self.current_index == self.initial_index {
                self.done = true;
            }
            Some(TTIpp::new(ipp, tt))
        } else {
            None
        }
    }
}

impl<'a> SegmentIter<'a> {
    fn new(departure_time: &'a [Timestamp], travel_time: &'a [Weight], range: WrappingRange<Timestamp>) -> SegmentIter<'a> {
        if departure_time.len() <= 1 {
            return SegmentIter { departure_time, travel_time, range, current_index: 0, initial_index: 0, done: true, done_after_next: true }
        }

        let current_index = match departure_time.sorted_search(&range.start()) {
            Ok(index) => index,
            Err(index) => index + departure_time.len() - 1
        } % departure_time.len();

        SegmentIter { departure_time, travel_time, range, current_index, initial_index: current_index, done: false, done_after_next: false }
    }
}

#[derive(Debug)]
pub(super) struct SegmentIter<'a> {
    departure_time: &'a [Timestamp],
    travel_time: &'a [Weight],
    range: WrappingRange<Timestamp>,
    current_index: usize,
    initial_index: usize,
    done: bool,
    done_after_next: bool
}

impl<'a> Iterator for SegmentIter<'a> {
    type Item = TTFSeg;

    fn next(&mut self) -> Option<Self::Item> {
        let ipp = unsafe { *self.departure_time.get_unchecked(self.current_index) };
        let next_index = (self.current_index + 1) % self.departure_time.len();
        let next_ipp = unsafe { *self.departure_time.get_unchecked(next_index) };

        if !self.done && (self.range.contains(ipp) || self.range.contains(next_ipp)) {
            let apply_valid_from = self.current_index == self.initial_index && !self.done_after_next;

            let tt = unsafe { *self.travel_time.get_unchecked(self.current_index) };
            self.current_index = next_index;

            let mut segment = TTFSeg::new((ipp, tt), (next_ipp, unsafe { *self.travel_time.get_unchecked(self.current_index) }));

            if self.done_after_next || !self.range.contains(next_ipp) {
                self.done = true;
                segment.valid.end = self.range.end();
            }

            if self.current_index == self.initial_index {
                self.done_after_next = true;
                if next_ipp == self.range.start() {
                    self.done = true;
                }
            }

            if apply_valid_from { segment.valid.start = self.range.start() }

            Some(segment)
        } else {
            None
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bounds() {
        let departure_time = vec![6, 9, 14, 17, 20];
        let travel_time =    vec![1, 3, 2,  4,  1];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        assert_eq!(ttf.lower_bound(), 1);
        assert_eq!(ttf.upper_bound(), 4);
    }

    #[test]
    fn test_eval_on_ipp() {
        let departure_time = vec![6, 9, 14, 17, 20];
        let travel_time =    vec![1, 3, 2,  4,  1];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        assert_eq!(ttf.evaluate(14), 2);
        assert_eq!(ttf.evaluate(17), 4);
    }

    #[test]
    fn test_interpolating_eval() {
        let departure_time = vec![5, 9, 14, 17, 20];
        let travel_time =    vec![1, 3, 2,  4,  1];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        assert_eq!(ttf.evaluate(0), 1);
        assert_eq!(ttf.evaluate(6), 1);
        assert_eq!(ttf.evaluate(7), 2);
        assert_eq!(ttf.evaluate(8), 2);
        assert_eq!(ttf.evaluate(10), 2);
        assert_eq!(ttf.evaluate(11), 2);
        assert_eq!(ttf.evaluate(12), 2);
        assert_eq!(ttf.evaluate(13), 2);
        assert_eq!(ttf.evaluate(15), 2);
        assert_eq!(ttf.evaluate(16), 3);
        assert_eq!(ttf.evaluate(18), 3);
        assert_eq!(ttf.evaluate(19), 2);
        assert_eq!(ttf.evaluate(23), 1);
    }

    #[test]
    fn test_full_range_ipp_iter() {
        let departure_time = vec![0, 5, 14, 20];
        let travel_time =    vec![2, 1, 2,  1];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        let all_ipps: Vec<_> = ttf.ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, 24)).map(TTIpp::as_tuple).collect();
        assert_eq!(all_ipps, vec![(0,2), (5,1), (14,2), (20,1)]);
    }

    #[test]
    fn test_wrapping_range_ipp_iter() {
        let departure_time = vec![0, 5, 14, 20];
        let travel_time =    vec![2, 1, 2,  1];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        let all_ipps: Vec<_> = ttf.ipp_iter(WrappingRange::new(Range { start: 17, end: 17 }, 24)).map(TTIpp::as_tuple).collect();
        assert_eq!(all_ipps, vec![(20,1), (0,2), (5,1), (14,2)]);
    }

    #[test]
    fn test_partial_range_ipp_iter() {
        let departure_time = vec![0, 5, 14, 20];
        let travel_time =    vec![2, 1, 2,  1];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        let all_ipps: Vec<_> = ttf.ipp_iter(WrappingRange::new(Range { start: 10, end: 21 }, 24)).map(TTIpp::as_tuple).collect();
        assert_eq!(all_ipps, vec![(14,2), (20,1)]);
    }

    #[test]
    fn test_static_weight_iter() {
        let departure_time = vec![0];
        let travel_time =    vec![2];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        let all_ipps: Vec<_> = ttf.ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, 24)).collect();
        assert_eq!(all_ipps, vec![]);
    }

    #[test]
    fn test_full_range_seg_iter() {
        let departure_time = vec![0, 5, 14, 20];
        let travel_time =    vec![2, 1, 2,  1];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        let all_segments: Vec<TTFSeg> = ttf.seg_iter(WrappingRange::new(Range { start: 0, end: 0 }, 24)).collect();
        assert_eq!(all_segments, vec![TTFSeg::new((0,2), (5,1)), TTFSeg::new((5,1), (14,2)), TTFSeg::new((14,2), (20,1)), TTFSeg::new((20,1), (0,2))]);
    }

    #[test]
    fn test_wrapping_range_seg_iter() {
        let departure_time = vec![0, 5, 14, 20];
        let travel_time =    vec![2, 1, 2,  1];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        let all_ipps: Vec<TTFSeg> = ttf.seg_iter(WrappingRange::new(Range { start: 17, end: 17 }, 24)).collect();
        let mut first_segment = TTFSeg::new((14,2), (20,1));
        first_segment.valid.start = 17;
        let mut last_segment = TTFSeg::new((14,2), (20,1));
        last_segment.valid.end = 17;
        assert_eq!(all_ipps, vec![first_segment, TTFSeg::new((20,1), (0,2)), TTFSeg::new((0,2), (5,1)), TTFSeg::new((5,1), (14,2)), last_segment]);
    }

    #[test]
    fn test_partial_range_seg_iter() {
        let departure_time = vec![0, 5, 14, 20];
        let travel_time =    vec![2, 1, 2,  1];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        let all_ipps: Vec<TTFSeg> = ttf.seg_iter(WrappingRange::new(Range { start: 10, end: 21 }, 24)).collect();
        let mut first_segment = TTFSeg::new((5,1), (14,2));
        first_segment.valid.start = 10;
        let mut last_segment = TTFSeg::new((20,1), (0,2));
        last_segment.valid.end = 21;
        assert_eq!(all_ipps, vec![first_segment, TTFSeg::new((14,2), (20,1)), last_segment]);
    }

    #[test]
    fn test_static_weight_seg_iter() {
        let departure_time = vec![0];
        let travel_time =    vec![2];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        let all_ipps: Vec<TTFSeg> = ttf.seg_iter(WrappingRange::new(Range { start: 0, end: 0 }, 24)).collect();
        assert_eq!(all_ipps, vec![]);
    }
}
