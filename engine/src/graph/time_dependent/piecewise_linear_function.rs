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

    pub fn next_ipp_greater_eq(&self, time: Timestamp) -> Option<Timestamp> {
        match self.departure_time.binary_search(&time) {
            Ok(_) => Some(time),
            Err(index) => {
                if index < self.departure_time.len() {
                    Some(self.departure_time[index])
                } else {
                    None
                }
            }
        }
    }

    fn subtract_wrapping(&self, x: Weight, y: Weight) -> Weight {
        (self.period + x - y) % self.period
    }
}

fn interpolate(delta_x: Weight, y1: Weight, y2: Weight, x: Timestamp) -> Weight {
    debug_assert!(x <= delta_x);
    debug_assert_ne!(delta_x, 0);
    let delta_y = y2 as i64 - y1 as i64;
    let result = y1 as i64 + (x as i64 * delta_y / delta_x as i64);
    debug_assert!(result >= 0);
    result as Weight
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
        assert_eq!(ttf.evaluate(15), 2);
        assert_eq!(ttf.evaluate(16), 3);
        assert_eq!(ttf.evaluate(23), 1);
    }
}
