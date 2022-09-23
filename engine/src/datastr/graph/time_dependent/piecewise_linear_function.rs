use super::math::*;
use super::sorted_search_slice_ext::*;
use super::*;

/// A struct borrowing data of a single PLF and exposing some methods to work with it.
pub struct PiecewiseLinearFunction<'a> {
    pub departure_time: &'a [Timestamp],
    pub travel_time: &'a [Weight],
}

impl<'a> PiecewiseLinearFunction<'a> {
    /// Create from two slices and make sure certain invariants hold.
    #[inline(always)]
    pub fn new(departure_time: &'a [Timestamp], travel_time: &'a [Weight]) -> PiecewiseLinearFunction<'a> {
        debug_assert_eq!(departure_time.len(), travel_time.len());
        debug_assert!(!departure_time.is_empty());
        debug_assert_eq!(departure_time[0], 0, "{:?}", departure_time);
        // debug_assert_eq!(*departure_time.last().unwrap(), period());
        debug_assert_eq!(*travel_time.last().unwrap(), travel_time[0]);
        for dt in &departure_time[0..departure_time.len() - 1] {
            debug_assert!(*dt < period());
        }
        for (dts, tts) in departure_time.windows(2).zip(travel_time.windows(2)) {
            debug_assert!(dts[0] < dts[1]);
            debug_assert!(dts[0] + tts[0] <= dts[1] + tts[1]);
        }

        PiecewiseLinearFunction { departure_time, travel_time }
    }

    /// Calculate average Weight over a given time range.
    pub fn average(&self, range: WrappingRange) -> Weight {
        let monotone_range = range.monotonize();
        let total_time = monotone_range.end - monotone_range.start;
        let (first_range, second_range) = monotone_range.split(period());
        let mut sum: u64 = 0;
        for seg in self.non_wrapping_seg_iter(first_range).chain(self.non_wrapping_seg_iter(second_range)) {
            let delta = seg.valid.end - seg.valid.start;
            sum += u64::from(seg.line.line().from.val) * u64::from(delta);
            sum += u64::from(seg.line.line().to.val) * u64::from(delta);
        }
        (sum / 2 / u64::from(total_time)) as Weight
    }

    /// Evaluate function at an arbitrary point in time
    #[inline(always)]
    pub fn eval(&self, departure: Timestamp) -> Weight {
        self.evaluate(departure % period())
    }

    /// Find the lowest value of the function
    pub fn lower_bound(&self) -> Weight {
        *self.travel_time.iter().min().unwrap()
    }

    /// Calculate min Weight over a given time range.
    pub fn lower_bound_in_range(&self, range: Range<Timestamp>) -> Weight {
        if let &[const_tt] = self.travel_time {
            return const_tt;
        }

        let (first_range, mut second_range) = range.split(period());
        second_range.start %= period();
        second_range.end %= period();
        std::cmp::min(self.lower_bound_in_included(first_range), self.lower_bound_in_included(second_range))
    }

    pub fn lower_bound_in_included(&self, range: Range<Timestamp>) -> Weight {
        if range.is_empty() {
            return INFINITY;
        }
        let (first_idx, first_lower) = match self.departure_time.locate(range.start, |&dt| dt) {
            Location::On(index) => (index, INFINITY),
            Location::Between(_lower_index, upper_index) => (upper_index, self.evaluate(range.start)),
        };

        let (last_idx, last_lower) = match self.departure_time.locate(range.end, |&dt| dt) {
            Location::On(index) => (index, INFINITY),
            Location::Between(lower_index, _upper_index) => (lower_index, self.evaluate(range.end)),
        };

        self.travel_time[first_idx..=last_idx]
            .iter()
            .copied()
            .chain(std::iter::once(first_lower))
            .chain(std::iter::once(last_lower))
            .min()
            .unwrap()
    }

    pub fn upper_bound_in_range(&self, range: Range<Timestamp>) -> Weight {
        if let &[const_tt] = self.travel_time {
            return const_tt;
        }

        let (first_range, mut second_range) = range.split(period());
        second_range.start %= period();
        second_range.end %= period();
        std::cmp::max(self.upper_bound_in_included(first_range), self.upper_bound_in_included(second_range))
    }

    pub fn upper_bound_in_included(&self, range: Range<Timestamp>) -> Weight {
        if range.is_empty() {
            return INFINITY;
        }
        let (first_idx, first_lower) = match self.departure_time.locate(range.start, |&dt| dt) {
            Location::On(index) => (index, INFINITY),
            Location::Between(_lower_index, upper_index) => (upper_index, self.evaluate(range.start)),
        };

        let (last_idx, last_lower) = match self.departure_time.locate(range.end, |&dt| dt) {
            Location::On(index) => (index, INFINITY),
            Location::Between(lower_index, _upper_index) => (lower_index, self.evaluate(range.end)),
        };

        self.travel_time[first_idx..=last_idx]
            .iter()
            .copied()
            .chain(std::iter::once(first_lower))
            .chain(std::iter::once(last_lower))
            .max()
            .unwrap()
    }

    /// Find the highest value of the function
    pub fn upper_bound(&self) -> Weight {
        *self.travel_time.iter().max().unwrap()
    }

    /// Evaluate for a point in time within period!
    #[inline(always)]
    pub(super) fn evaluate(&self, departure: Timestamp) -> Weight {
        debug_assert!(departure <= period());
        if self.departure_time.len() <= 2 {
            return unsafe { *self.travel_time.get_unchecked(0) };
        }

        match self.departure_time.locate(departure, |&dt| dt) {
            Location::On(index) => unsafe { *self.travel_time.get_unchecked(index) },
            Location::Between(lower_index, upper_index) => {
                let lf = unsafe {
                    MonotoneLine::<TTIpp>::new(Line::new(
                        TTIpp::new(*self.departure_time.get_unchecked(lower_index), *self.travel_time.get_unchecked(lower_index)),
                        TTIpp::new(*self.departure_time.get_unchecked(upper_index), *self.travel_time.get_unchecked(upper_index)),
                    ))
                };
                lf.into_monotone_at_line().interpolate_tt_in_range(departure)
            }
        }
    }

    fn non_wrapping_seg_iter(&self, range: Range<Timestamp>) -> impl Iterator<Item = PLFSeg> + 'a {
        debug_assert!(self.departure_time.len() > 1);
        let index_range = self.departure_time.index_range(&range, |&dt| dt);

        self.departure_time[index_range.clone()]
            .windows(2)
            .zip(self.travel_time[index_range].windows(2))
            .map(move |(dts, tts)| PLFSeg {
                line: MonotoneLine::<TTIpp>::new(Line {
                    from: TTIpp::new(dts[0], tts[0]),
                    to: TTIpp::new(dts[1], tts[1]),
                }),
                valid: (dts[0]..dts[1]).intersection(&range),
            })
    }

    pub fn min_slope(&self) -> f64 {
        self.departure_time
            .windows(2)
            .zip(self.travel_time.windows(2))
            .map(|(dts, tts)| (tts[1] as i32 - tts[0] as i32) as f64 / (dts[1] - dts[0]) as f64)
            .min_by(|x, y| x.partial_cmp(y).unwrap())
            .unwrap_or(0.0)
    }

    pub fn max_slope(&self) -> f64 {
        self.departure_time
            .windows(2)
            .zip(self.travel_time.windows(2))
            .map(|(dts, tts)| (tts[1] as i32 - tts[0] as i32) as f64 / (dts[1] - dts[0]) as f64)
            .max_by(|x, y| x.partial_cmp(y).unwrap())
            .unwrap_or(0.0)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_eval_on_ipp() {
        run_test_with_periodicity(24, || {
            let departure_time = vec![0, 6, 9, 14, 17, 20, 24];
            let travel_time = vec![2, 1, 3, 2, 4, 1, 2];
            let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time);
            assert_eq!(ttf.evaluate(14), 2);
            assert_eq!(ttf.evaluate(17), 4);
        });
    }

    #[test]
    fn test_interpolating_eval() {
        run_test_with_periodicity(24, || {
            let departure_time = vec![0, 5, 9, 14, 17, 20, 24];
            let travel_time = vec![1, 1, 3, 2, 4, 1, 1];
            let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time);
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
        });
    }

    #[test]
    fn test_full_range_seg_iter() {
        run_test_with_periodicity(24, || {
            let departure_time = vec![0, 5, 14, 20, 24];
            let travel_time = vec![2, 1, 2, 1, 2];
            let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time);
            let all_segments: Vec<PLFSeg> = ttf.non_wrapping_seg_iter(0..24).collect();
            assert_eq!(
                all_segments,
                vec![
                    PLFSeg::from_point_tuples((0, 2), (5, 1)),
                    PLFSeg::from_point_tuples((5, 1), (14, 2)),
                    PLFSeg::from_point_tuples((14, 2), (20, 1)),
                    PLFSeg::from_point_tuples((20, 1), (24, 2))
                ]
            );
        });
    }

    #[test]
    fn test_partial_range_seg_iter() {
        run_test_with_periodicity(24, || {
            let departure_time = vec![0, 5, 14, 20, 24];
            let travel_time = vec![2, 1, 2, 1, 2];
            let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time);
            let all_ipps: Vec<PLFSeg> = ttf.non_wrapping_seg_iter(10..21).collect();
            let mut first_segment = PLFSeg::from_point_tuples((5, 1), (14, 2));
            first_segment.valid.start = 10;
            let mut last_segment = PLFSeg::from_point_tuples((20, 1), (24, 2));
            last_segment.valid.end = 21;
            assert_eq!(all_ipps, vec![first_segment, PLFSeg::from_point_tuples((14, 2), (20, 1)), last_segment]);
        });
    }

    #[test]
    fn test_static_weight_seg_iter() {
        run_test_with_periodicity(24, || {
            let departure_time = vec![0, 24];
            let travel_time = vec![2, 2];
            let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time);
            let all_ipps: Vec<PLFSeg> = ttf.non_wrapping_seg_iter(0..24).collect();
            assert_eq!(all_ipps, vec![PLFSeg::from_point_tuples((0, 2), (24, 2))]);

            let all_ipps: Vec<PLFSeg> = ttf.non_wrapping_seg_iter(10..24).collect();
            let mut seg = PLFSeg::from_point_tuples((0, 2), (24, 2));
            seg.valid.start = 10;
            assert_eq!(all_ipps, vec![seg]);

            let all_ipps: Vec<PLFSeg> = ttf.non_wrapping_seg_iter(0..10).collect();
            let mut seg = PLFSeg::from_point_tuples((0, 2), (24, 2));
            seg.valid.end = 10;
            assert_eq!(all_ipps, vec![seg]);
        });
    }
}
