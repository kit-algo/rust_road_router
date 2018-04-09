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

    pub fn evaluate(&self, departure: Timestamp) -> Weight {
        if self.departure_time.len() == 1 {
            return self.travel_time[0]
        }

        let departure = departure % self.period;
        match self.departure_time.binary_search(&departure) {
            Ok(departure_index) => self.travel_time[departure_index],
            Err(upper_index) => {
                let upper_index = upper_index % self.departure_time.len();
                let lower_index = if upper_index > 0 { upper_index - 1 } else { self.departure_time.len() - 1 };
                let delta_dt = self.subtract_wrapping(self.departure_time[upper_index], self.departure_time[lower_index]);
                let relative_x = self.subtract_wrapping(departure, self.departure_time[lower_index]);
                interpolate(delta_dt, self.travel_time[lower_index], self.travel_time[upper_index], relative_x)
            },
        }
    }

    pub fn ipp_iter(&self, range: WrappingRange<Timestamp>) -> Iter<'a> {
        debug_assert_eq!(*range.wrap_around(), self.period);
        Iter::new(self.departure_time, self.travel_time, range)
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

fn interpolate(delta_x: Weight, y1: Weight, y2: Weight, x: Timestamp) -> Weight {
    debug_assert!(x <= delta_x);
    debug_assert_ne!(delta_x, 0);
    let delta_y = y2 + delta_x - y1;
    let result = y1 as u64 + (x as u64 * delta_y as u64 / delta_x as u64);
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
        let current_index = match departure_time.binary_search(range.start()) {
            Ok(index) => index,
            Err(index) => index
        } % departure_time.len();

        Iter { departure_time, travel_time, range, current_index, initial_index: current_index, done: false }
    }
}

impl<'a> Iterator for Iter<'a> {
    type Item = (Timestamp, Weight);

    fn next(&mut self) -> Option<(Timestamp, Weight)> {
        let ipp = self.departure_time[self.current_index];

        if !self.done && self.range.contains(ipp) {
            let tt = self.travel_time[self.current_index];
            self.current_index = (self.current_index + 1) % self.departure_time.len();
            if self.current_index == self.initial_index {
                self.done = true;
            }
            Some((ipp, tt))
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
        let all_ipps: Vec<(Timestamp, Weight)> = ttf.ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, 24)).collect();
        assert_eq!(all_ipps, vec![(0,2), (5,1), (14,2), (20,1)]);
    }

    #[test]
    fn test_wrapping_range_ipp_iter() {
        let departure_time = vec![0, 5, 14, 20];
        let travel_time =    vec![2, 1, 2,  1];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        let all_ipps: Vec<(Timestamp, Weight)> = ttf.ipp_iter(WrappingRange::new(Range { start: 17, end: 17 }, 24)).collect();
        assert_eq!(all_ipps, vec![(20,1), (0,2), (5,1), (14,2)]);
    }

    #[test]
    fn test_partial_range_ipp_iter() {
        let departure_time = vec![0, 5, 14, 20];
        let travel_time =    vec![2, 1, 2,  1];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        let all_ipps: Vec<(Timestamp, Weight)> = ttf.ipp_iter(WrappingRange::new(Range { start: 10, end: 21 }, 24)).collect();
        assert_eq!(all_ipps, vec![(14,2), (20,1)]);
    }

    #[test]
    fn test_static_weight_iter() {
        let departure_time = vec![0];
        let travel_time =    vec![2];
        let ttf = PiecewiseLinearFunction::new(&departure_time, &travel_time, 24);
        let all_ipps: Vec<(Timestamp, Weight)> = ttf.ipp_iter(WrappingRange::new(Range { start: 0, end: 0 }, 24)).collect();
        assert_eq!(all_ipps, vec![(0,2)]);
    }
}
