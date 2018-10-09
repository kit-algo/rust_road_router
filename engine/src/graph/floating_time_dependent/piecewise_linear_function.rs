use super::*;
use std::cmp::{min, max, Ordering};

#[derive(Debug)]
pub struct PiecewiseLinearFunction<'a> {
    ipps: &'a [Point],
}

impl<'a> PiecewiseLinearFunction<'a> {
    pub fn new(ipps: &'a [Point]) -> PiecewiseLinearFunction<'a> {
        debug_assert!(ipps.first().unwrap().at == Timestamp::zero(), "{:?}", ipps);
        debug_assert!(ipps.first().unwrap().val.fuzzy_eq(ipps.last().unwrap().val), "{:?}", ipps);
        debug_assert!(ipps.len() == 1 || ipps.last().unwrap().at == period(), "{:?}", ipps);

        for points in ipps.windows(2) {
            debug_assert!(points[0].at < points[1].at);
        }

        PiecewiseLinearFunction { ipps }
    }

    pub fn lower_bound(&self) -> FlWeight {
        self.ipps.iter().map(|p| p.val).min().unwrap()
    }

    pub fn upper_bound(&self) -> FlWeight {
        self.ipps.iter().map(|p| p.val).max().unwrap()
    }

    pub fn eval(&self, t: Timestamp) -> FlWeight {
        debug_assert!(t < period());

        if self.ipps.len() == 1 {
            return self.ipps.first().unwrap().val
        }

        let pos = self.ipps.binary_search_by(|p| {
            if p.at.fuzzy_eq(t) {
                Ordering::Equal
            } else if p.at < t {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        });

        match pos {
            Ok(i) => unsafe { self.ipps.get_unchecked(i).val },
            Err(i) => {
                let prev = unsafe { self.ipps.get_unchecked(i-1) };
                let next = unsafe { self.ipps.get_unchecked(i) };

                let frac = (t - prev.at) / (next.at - prev.at);
                prev.val + (next.val - prev.val) * frac
            },
        }
    }

    pub fn link(&self, other: &Self) -> Vec<Point> {
        if let [Point { val, .. }] = &self.ipps {
            if let [Point { val: other, .. }] = &other.ipps {
                return vec![Point { at: Timestamp::zero(), val: val + other }]
            } else {
                let zero_val = other.eval(val.into());
                return std::iter::once(Point { at: Timestamp::zero(), val: zero_val + val })
                    .chain(
                        other.ipps.iter().filter(|p| p.at > val.into()).map(|p| Point { at: p.at - val, val: p.val + val })
                    ).chain(
                        other.ipps.iter().filter(|p| p.at < val.into()).map(|p| Point { at: p.at + FlWeight::from(period()) - val, val: p.val + val })
                    ).chain(std::iter::once(Point { at: period(), val: zero_val + val }))
                    .fold(Vec::with_capacity(other.ipps.len() + 2), |mut acc, p| {
                        Self::append_point(&mut acc, p);
                        acc
                    })
            }
        }
        if let [Point { val, .. }] = &other.ipps {
            return self.ipps.iter().map(|p| Point { at: p.at, val: p.val + val }).collect()
        }

        let mut result = Vec::with_capacity(self.ipps.len() + other.ipps.len() + 1);

        let mut f = Cursor::new(&self.ipps);
        let mut g = Cursor::starting_at(&other.ipps, Timestamp::zero() + self.ipps[0].val);

        loop {
            let mut x;
            let y;

            if g.cur().at.fuzzy_eq(f.cur().at + f.cur().val) {
                x = f.cur().at;
                y = g.cur().val + f.cur().val;

                g.advance();
                f.advance();
            } else if g.cur().at < f.cur().at + f.cur().val {
                debug_assert!(g.cur().at.fuzzy_lt(f.cur().at + f.cur().val));

                let m_arr_f_inverse = (f.cur().at - f.prev().at) / (f.cur().at + f.cur().val - f.prev().at - f.prev().val);
                x = m_arr_f_inverse * (g.cur().at - f.prev().at - f.prev().val) + f.prev().at;
                y = g.cur().at + g.cur().val - x;

                g.advance();
            } else {
                debug_assert!((f.cur().at + f.cur().val).fuzzy_lt(g.cur().at));

                x = f.cur().at;
                let m_g = (g.cur().val - g.prev().val) / (g.cur().at - g.prev().at);
                y = g.prev().val + m_g * (f.cur().at + f.cur().val - g.prev().at) + f.cur().val;

                f.advance();
            }

            if period().fuzzy_lt(x) { break }

            x = min(x, period());
            x = max(x, Timestamp::zero());

            Self::append_point(&mut result, Point { at: x, val: y });
        }

        debug_assert!(result.len() <= self.ipps.len() + other.ipps.len() + 1);
        Self::new(&result);

        result
    }

    #[allow(clippy::cyclomatic_complexity)]
    pub fn merge(&self, other: &Self) -> (Vec<Point>, Vec<(Timestamp, bool)>) {
        if self.upper_bound() < other.lower_bound() {
            return (self.ipps.to_vec(), vec![(Timestamp::zero(), true)])
        } else if other.upper_bound()  < self.lower_bound() {
            return (self.ipps.to_vec(), vec![(Timestamp::zero(), false)])
        }

        let mut result = Vec::with_capacity(2 * self.ipps.len() + 2 * other.ipps.len() + 2);
        let mut better = Vec::new();

        let mut f = Cursor::new(&self.ipps);
        let mut g = Cursor::new(&other.ipps);

        if f.cur().val.fuzzy_eq(g.cur().val) {
            better.push((Timestamp::zero(), counter_clockwise(&f.cur(), &f.next(), &g.next())));
        } else {
            better.push((Timestamp::zero(), f.cur().val < g.cur().val));
        }

        while f.cur().at < period() || g.cur().at < period() {

            if intersect(&f.prev(), &f.cur(), &g.prev(), &g.cur()) {
                let intersection = intersection_point(&f.prev(), &f.cur(), &g.prev(), &g.cur());
                if intersection.at >= Timestamp::zero() {
                    better.push((intersection.at, counter_clockwise(&intersection, &f.cur(), &g.cur())));
                    Self::append_point(&mut result, intersection);
                }
            }

            if f.cur().at.fuzzy_eq(g.cur().at) {
                if f.cur().val.fuzzy_eq(g.cur().val) {
                    Self::append_point(&mut result, f.cur().clone());

                    if clockwise(&g.prev(), &f.prev(), &f.cur()) && counter_clockwise(&f.cur(), &f.next(), &g.next()) {
                        debug_assert!(!better.last().unwrap().1);
                        better.push((f.cur().at, true));
                    }

                    if counter_clockwise(&g.prev(), &f.prev(), &f.cur()) && clockwise(&f.cur(), &f.next(), &g.next()) {
                        debug_assert!(better.last().unwrap().1);
                        better.push((f.cur().at, false));
                    }

                } else if f.cur().val < g.cur().val {

                    Self::append_point(&mut result, f.cur().clone());
                    debug_assert!(f.cur().val.fuzzy_lt(g.cur().val));
                    debug_assert!(better.last().unwrap().1);

                } else {

                    Self::append_point(&mut result, g.cur().clone());
                    debug_assert!(g.cur().val < f.cur().val);
                    debug_assert!(!better.last().unwrap().1);
                }

                f.advance();
                g.advance();

            } else if f.cur().at < g.cur().at {

                debug_assert!(f.cur().at.fuzzy_lt(g.cur().at));

                if counter_clockwise(&g.prev(), &f.cur(), &g.cur()) {
                    Self::append_point(&mut result, f.cur().clone());
                    debug_assert!(better.last().unwrap().1);

                } else if colinear(&g.prev(), &f.cur(), &g.cur()) {

                    if counter_clockwise(&g.prev(), &f.prev(), &f.cur()) || counter_clockwise(&f.cur(), &f.next(), &g.cur()) {
                        Self::append_point(&mut result, f.cur().clone());
                        debug_assert!(better.last().unwrap().1);
                    } else if result.is_empty() {
                        Self::append_point(&mut result, f.cur().clone());
                        panic!("wtf?");
                    }

                    if clockwise(&g.prev(), &f.prev(), &f.cur()) || clockwise(&f.cur(), &f.next(), &g.cur()) {
                        debug_assert!(!better.last().unwrap().1);
                    }
                }

                f.advance();

            } else {

                debug_assert!(g.cur().at < f.cur().at);

                if counter_clockwise(&f.prev(), &g.cur(), &f.cur()) {
                    Self::append_point(&mut result, g.cur().clone());
                    debug_assert!(!better.last().unwrap().1);

                } else if colinear(&f.prev(), &g.cur(), &f.cur()) {
                    if counter_clockwise(&g.prev(), &g.cur(), &f.prev()) || counter_clockwise(&g.cur(), &g.next(), &f.cur()) {
                        Self::append_point(&mut result, g.cur().clone());
                        debug_assert!(!better.last().unwrap().1);
                    }
                    if result.is_empty() {
                        Self::append_point(&mut result, g.cur().clone());
                        panic!("wtf?");
                    }

                    if clockwise(&g.prev(), &g.cur(), &f.prev()) || clockwise(&g.cur(), &g.next(), &f.cur()) {
                        debug_assert!(better.last().unwrap().1);
                    }
                }

                g.advance();
            }
        };

        debug_assert_eq!(f.cur().at, period());
        debug_assert_eq!(g.cur().at, period());

        if intersect(&f.prev(), &f.cur(), &g.prev(), &g.cur()) {
            let intersection = intersection_point(&f.prev(), &f.cur(), &g.prev(), &g.cur());
            if intersection.at < period() {
                better.push((intersection.at, counter_clockwise(&intersection, &f.cur(), &g.cur())));
                Self::append_point(&mut result, intersection);
            }
        }

        if result.len() > 1 {
            let p = Point { at: period(), val: result[0].val };
            Self::append_point(&mut result, p);
        }

        debug_assert!(result.len() <= 2 * self.ipps.len() + 2 * other.ipps.len() + 2);
        Self::new(&result);

        (result, better)
    }

    fn append_point(points: &mut Vec<Point>, mut point: Point) {
        debug_assert!(point.val > FlWeight::new(0.0));

        if let Some(prev) = points.last_mut() {
            if prev.at.fuzzy_eq(point.at) {
                if prev.val.fuzzy_eq(point.val) { return }
                println!("Jumping TTF");
                let early = min(prev.at, point.at);
                let late = max(prev.at, point.at) + FlWeight::new(EPSILON);
                prev.at = early;
                point.at = late;
            }

            if point.at < prev.at {
                println!("Point insert before prev");
                std::mem::swap(&mut point.at, &mut prev.at);
            }
        }
        if points.len() > 1 && colinear(&points[points.len() - 2], &points[points.len() - 1], &point) {
            points.pop();
        }

        points.push(point)
    }
}

#[derive(Debug)]
struct Cursor<'a> {
    ipps: &'a [Point],
    current_index: usize,
    offset: FlWeight,
}

impl<'a> Cursor<'a> {
    fn new(ipps: &'a [Point]) -> Self {
        Cursor { ipps, current_index: 0, offset: FlWeight::new(0.0) }
    }

    fn starting_at(ipps: &'a [Point], t: Timestamp) -> Self {
        debug_assert!(t < period());

        if ipps.len() == 1 {
            return Self::new(ipps)
        }

        let pos = ipps.binary_search_by(|p| {
            if p.at.fuzzy_eq(t) {
                Ordering::Equal
            } else if p.at < t {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        });

        match pos {
            Ok(i) => Cursor { ipps, current_index: i, offset: FlWeight::new(0.0) },
            Err(i) => Cursor { ipps, current_index: i-1, offset: FlWeight::new(0.0) },
        }
    }

    fn cur(&self) -> Point {
        self.ipps[self.current_index].shifted(self.offset)
    }

    fn next(&self) -> Point {
        if self.ipps.len() == 1 {
            self.ipps.first().unwrap().shifted(self.offset + FlWeight::from(period()))
        } else {
            self.ipps[self.current_index + 1].shifted(self.offset)
        }
    }

    fn prev(&self) -> Point {
        if self.ipps.len() == 1 {
            self.ipps.first().unwrap().shifted(self.offset - FlWeight::from(period()))
        } else if self.current_index == 0 {
            let offset = self.offset - FlWeight::from(period());
            self.ipps[self.ipps.len() - 2].shifted(offset)
        } else {
            self.ipps[self.current_index - 1].shifted(self.offset)
        }
    }

    fn advance(&mut self) {
        self.current_index += 1;
        if self.current_index % self.ipps.len() == self.ipps.len() - 1 || self.ipps.len() == 1 {
            self.offset = self.offset + FlWeight::from(period());
            self.current_index = 0;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_fn_cursor() {
        run_test_with_periodicity(Timestamp::new(10.0), || {
            let ipps = [Point { at: Timestamp::zero(), val: FlWeight::new(5.0) }];
            let mut cursor = Cursor::new(&ipps);
            assert_eq!(cursor.cur(), Point { at: Timestamp::zero(), val: FlWeight::new(5.0) });
            assert_eq!(cursor.next(), Point { at: period(), val: FlWeight::new(5.0) });
            assert_eq!(cursor.prev(), Point { at: Timestamp::zero() - FlWeight::from(period()), val: FlWeight::new(5.0) });
            cursor.advance();
            assert_eq!(cursor.cur(), Point { at: period(), val: FlWeight::new(5.0) });
            assert_eq!(cursor.next(), Point { at: Timestamp::new(20.0), val: FlWeight::new(5.0) });
            assert_eq!(cursor.prev(), Point { at: Timestamp::zero(), val: FlWeight::new(5.0) });
        });
    }

    #[test]
    fn test_dyn_fn_cursor() {
        run_test_with_periodicity(Timestamp::new(10.0), || {
            let ipps = [Point { at: Timestamp::zero(), val: FlWeight::new(5.0) }, Point { at: Timestamp::new(5.0), val: FlWeight::new(7.0) }, Point { at: period(), val: FlWeight::new(5.0) }];
            let mut cursor = Cursor::new(&ipps);
            assert_eq!(cursor.cur(), Point { at: Timestamp::zero(), val: FlWeight::new(5.0) });
            assert_eq!(cursor.next(), Point { at: Timestamp::new(5.0), val: FlWeight::new(7.0) });
            assert_eq!(cursor.prev(), Point { at: Timestamp::new(-5.0), val: FlWeight::new(7.0) });
            cursor.advance();
            assert_eq!(cursor.cur(), Point { at: Timestamp::new(5.0), val: FlWeight::new(7.0) });
            assert_eq!(cursor.next(), Point { at: period(), val: FlWeight::new(5.0) });
            assert_eq!(cursor.prev(), Point { at: Timestamp::zero(), val: FlWeight::new(5.0) });
            cursor.advance();
            assert_eq!(cursor.cur(), Point { at: period(), val: FlWeight::new(5.0) });
            assert_eq!(cursor.next(), Point { at: Timestamp::new(15.0), val: FlWeight::new(7.0) });
            assert_eq!(cursor.prev(), Point { at: Timestamp::new(5.0), val: FlWeight::new(7.0) });
        });
    }
}
