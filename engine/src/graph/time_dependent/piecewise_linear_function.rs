use super::*;
use math::RangeExtensions;
use ::sorted_search_slice_ext::*;

#[derive(Debug)]
pub struct PiecewiseLinearFunction<'a> {
    departure_time: &'a [Timestamp],
    travel_time: &'a [Weight],
}

impl<'a> PiecewiseLinearFunction<'a> {
    pub fn new(departure_time: &'a [Timestamp], travel_time: &'a [Weight]) -> PiecewiseLinearFunction<'a> {
        // debug_assert_eq!(departure_time.len(), travel_time.len());
        // debug_assert!(!departure_time.is_empty());
        // debug_assert_eq!(departure_time[0], 0, "{:?}", departure_time);
        // debug_assert_eq!(*departure_time.last().unwrap(), period());
        // debug_assert_eq!(*travel_time.last().unwrap(), travel_time[0]);
        // for dt in &departure_time[0..departure_time.len()-1] {
        //     debug_assert!(*dt < period());
        // }
        // for pair in departure_time.windows(2) {
        //     debug_assert!(pair[0] < pair[1]);
        // }
        // for (dts, tts) in departure_time.windows(2).zip(travel_time.windows(2)) {
        //     debug_assert!(dts[0] + tts[0] <= dts[1] + tts[1]);
        // }

        PiecewiseLinearFunction {
            departure_time, travel_time
        }
    }

    pub fn lower_bound(&self) -> Weight {
        *self.travel_time.iter().min().unwrap()
    }

    pub fn upper_bound(&self) -> Weight {
        *self.travel_time.iter().max().unwrap()
    }

    pub fn bounds(&self) -> (Weight, Weight) {
        debug_assert!(self.lower_bound() < INFINITY);
        debug_assert!(self.upper_bound() < INFINITY);
        (self.lower_bound(), self.upper_bound())
    }

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

    pub fn eval(&self, departure: Timestamp) -> Weight {
        self.evaluate(departure % period())
    }

    // pub(super) fn evaluate(&self, departure: Timestamp) -> Weight {
    //     debug_assert!(departure < period());
    //     if self.departure_time.len() == 2 {
    //         return unsafe { *self.travel_time.get_unchecked(0) }
    //     }

    //     match self.departure_time.locate(&departure, |&dt| dt) {
    //         Location::On(index) => unsafe { *self.travel_time.get_unchecked(index) },
    //         Location::Between(lower_index , upper_index) => {
    //             let lf = unsafe {
    //                 MonotoneLine::<TTIpp>::new(Line::new(
    //                     TTIpp::new(*self.departure_time.get_unchecked(lower_index), *self.travel_time.get_unchecked(lower_index)),
    //                     TTIpp::new(*self.departure_time.get_unchecked(upper_index), *self.travel_time.get_unchecked(upper_index))))
    //             };
    //             // TODO optimize - no div_euc necessary
    //             lf.into_monotone_at_line().interpolate_tt(departure)
    //         },
    //     }
    // }

    pub(super) fn evaluate(&self, departure: Timestamp) -> Weight {
        debug_assert!(departure < period());
        if self.departure_time.len() == 1 {
            return unsafe { *self.travel_time.get_unchecked(0) }
        }

        let departure = departure % period();
        match self.departure_time.sorted_search(&departure) {
            Ok(departure_index) => unsafe { *self.travel_time.get_unchecked(departure_index) },
            Err(upper_index) => {
                let upper_index = upper_index % self.departure_time.len();
                let lower_index = if upper_index > 0 { upper_index - 1 } else { self.departure_time.len() - 1 };
                unsafe {
                    let delta_dt = self.subtract_wrapping(*self.departure_time.get_unchecked(upper_index), *self.departure_time.get_unchecked(lower_index));
                    let relative_x = self.subtract_wrapping(departure, *self.departure_time.get_unchecked(lower_index));
                    interpolate(delta_dt, *self.travel_time.get_unchecked(lower_index), *self.travel_time.get_unchecked(upper_index), relative_x)
                }
            },
        }
    }

    fn subtract_wrapping(&self, x: Weight, y: Weight) -> Weight {
        if x > y {
            x - y
        } else {
            period() + x - y
        }
    }

    pub(super) fn non_wrapping_seg_iter(&self, range: Range<Timestamp>) -> impl Iterator<Item = PLFSeg> + 'a {
        let index_range = self.departure_time.index_range(&range, |&dt| dt);

        self.departure_time[index_range.clone()].windows(2).zip(self.travel_time[index_range].windows(2)).map(move |(dts, tts)| {
            PLFSeg { line: MonotoneLine::<TTIpp>::new(Line { from: TTIpp::new(dts[0], tts[0]), to: TTIpp::new(dts[1], tts[1]) }), valid: (dts[0]..dts[1]).intersection(&range) }
        })
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_bounds() {
        run_test_with_periodicity(24, || {
            let departure_time = vec![0, 6, 9, 14, 17, 20, 24];
            let travel_time =    vec![2, 1, 3, 2,  4,  1,  2];
            let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time);
            assert_eq!(ttf.lower_bound(), 1);
            assert_eq!(ttf.upper_bound(), 4);
        });
    }

    #[test]
    fn test_eval_on_ipp() {
        run_test_with_periodicity(24, || {
            let departure_time = vec![0, 6, 9, 14, 17, 20, 24];
            let travel_time =    vec![2, 1, 3, 2,  4,  1,  2];
            let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time);
            assert_eq!(ttf.evaluate(14), 2);
            assert_eq!(ttf.evaluate(17), 4);
        });
    }

    #[test]
    fn test_interpolating_eval() {
        run_test_with_periodicity(24, || {
            let departure_time = vec![0, 5, 9, 14, 17, 20, 24];
            let travel_time =    vec![1, 1, 3, 2,  4,  1,  1];
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
            let travel_time =    vec![2, 1, 2,  1,  2];
            let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time);
            let all_segments: Vec<PLFSeg> = ttf.non_wrapping_seg_iter(0..24).collect();
            assert_eq!(all_segments, vec![PLFSeg::from_point_tuples((0,2), (5,1)), PLFSeg::from_point_tuples((5,1), (14,2)), PLFSeg::from_point_tuples((14,2), (20,1)), PLFSeg::from_point_tuples((20,1), (24,2))]);
        });
    }

    #[test]
    fn test_partial_range_seg_iter() {
        run_test_with_periodicity(24, || {
            let departure_time = vec![0, 5, 14, 20, 24];
            let travel_time =    vec![2, 1, 2,  1,  2];
            let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time);
            let all_ipps: Vec<PLFSeg> = ttf.non_wrapping_seg_iter(10..21).collect();
            let mut first_segment = PLFSeg::from_point_tuples((5,1), (14,2));
            first_segment.valid.start = 10;
            let mut last_segment = PLFSeg::from_point_tuples((20,1), (24,2));
            last_segment.valid.end = 21;
            assert_eq!(all_ipps, vec![first_segment, PLFSeg::from_point_tuples((14,2), (20,1)), last_segment]);
        });
    }

    #[test]
    fn test_static_weight_seg_iter() {
        run_test_with_periodicity(24, || {
            let departure_time = vec![0, 24];
            let travel_time =    vec![2, 2];
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
