use super::*;
use std::cmp::{min, max, Ordering};
use self::debug::debug_merge;

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
        let mut g = Cursor::starting_at_or_after(&other.ipps, Timestamp::zero() + self.ipps[0].val);

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

            if !x.fuzzy_lt(period()) { break }
            debug_assert!(!x.fuzzy_lt(Timestamp::zero()), "{:?} {:?} {:?}", x, y, debug_merge(&f, &g, &result, &[]));

            x = min(x, period());
            x = max(x, Timestamp::zero());

            Self::append_point(&mut result, Point { at: x, val: y });
        }

        let zero_val = result[0].val;
        Self::append_point(&mut result, Point { at: period(), val: zero_val });

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

        // non fuzzy cmp on purpose!
        if f.cur().val == g.cur().val {
            better.push((Timestamp::zero(), !clockwise(&f.cur(), &f.next(), &g.next())));
        } else {
            better.push((Timestamp::zero(), f.cur().val < g.cur().val));
        }

        while f.cur().at < period() || g.cur().at < period() {

            if intersect(&f.prev(), &f.cur(), &g.prev(), &g.cur()) {
                let intersection = intersection_point(&f.prev(), &f.cur(), &g.prev(), &g.cur());
                if intersection.at >= Timestamp::zero() {
                    debug_assert_ne!(better.last().unwrap().1, counter_clockwise(&g.prev(), &f.cur(), &g.cur()), "{:?}", debug_merge(&f, &g, &result, &better));
                    better.push((intersection.at, counter_clockwise(&g.prev(), &f.cur(), &g.cur())));
                    Self::append_point(&mut result, intersection);
                }
            }

            if f.cur().at.fuzzy_eq(g.cur().at) {
                if f.cur().val.fuzzy_eq(g.cur().val) {
                    if better.last().unwrap().1 {
                        Self::append_point(&mut result, f.cur().clone());
                    } else {
                        Self::append_point(&mut result, g.cur().clone());
                    }

                    if !better.last().unwrap().1 && counter_clockwise(&f.cur(), &f.next(), &g.next()) {
                        debug_assert!(!counter_clockwise(&g.prev(), &f.prev(), &f.cur()));
                        better.push((f.cur().at, true));
                    }

                    if better.last().unwrap().1 && clockwise(&f.cur(), &f.next(), &g.next()) {
                        debug_assert!(!clockwise(&g.prev(), &f.prev(), &f.cur()));
                        better.push((f.cur().at, false));
                    }

                } else if f.cur().val < g.cur().val {

                    Self::append_point(&mut result, f.cur().clone());
                    debug_assert!(f.cur().val.fuzzy_lt(g.cur().val));
                    debug_assert!(better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));

                } else {

                    Self::append_point(&mut result, g.cur().clone());
                    debug_assert!(g.cur().val < f.cur().val);
                    debug_assert!(!better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));
                }

                f.advance();
                g.advance();

            } else if f.cur().at < g.cur().at {

                debug_assert!(f.cur().at.fuzzy_lt(g.cur().at), "f {:?} g {:?}", f.cur().at, g.cur().at);

                if counter_clockwise(&g.prev(), &f.cur(), &g.cur()) {
                    Self::append_point(&mut result, f.cur().clone());
                    debug_assert!(better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));

                } else if colinear(&g.prev(), &f.cur(), &g.cur()) {
                    if !better.last().unwrap().1 && counter_clockwise(&f.cur(), &f.next(), &g.cur()) {
                        better.push((f.cur().at, true))
                    }

                    if counter_clockwise(&g.prev(), &f.prev(), &f.cur()) || counter_clockwise(&f.cur(), &f.next(), &g.cur()) {
                        Self::append_point(&mut result, f.cur().clone());
                        debug_assert!(better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));
                    } else if result.is_empty() {
                        Self::append_point(&mut result, f.cur().clone());
                        debug_assert!(better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));
                        panic!("wtf?");
                    }

                    if better.last().unwrap().1 && clockwise(&f.cur(), &f.next(), &g.cur()) {
                        better.push((f.cur().at, false))
                    }
                }

                f.advance();

            } else {

                debug_assert!(g.cur().at < f.cur().at);

                if counter_clockwise(&f.prev(), &g.cur(), &f.cur()) {
                    Self::append_point(&mut result, g.cur().clone());
                    debug_assert!(!better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));
                    // debug_assert!(!better.last().unwrap().1, "{:?}\n\n\nf: {:?}\n\n\ng: {:?}\n\n\nmerged {:?}", debug_merge(&f, &g, &result, &better), f.ipps, g.ipps, result);

                } else if colinear(&f.prev(), &g.cur(), &f.cur()) {
                    if better.last().unwrap().1 && counter_clockwise(&g.cur(), &g.next(), &f.cur()) {
                        better.push((g.cur().at, false))
                    }

                    if counter_clockwise(&g.prev(), &g.cur(), &f.prev()) || counter_clockwise(&g.cur(), &g.next(), &f.cur()) {
                        Self::append_point(&mut result, g.cur().clone());
                        debug_assert!(!better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));
                    }
                    if result.is_empty() {
                        Self::append_point(&mut result, g.cur().clone());
                        debug_assert!(!better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));
                        panic!("wtf?");
                    }

                    if !better.last().unwrap().1 && clockwise(&g.cur(), &g.next(), &f.cur()) {
                        better.push((g.cur().at, true))
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
        debug_assert!(point.val > FlWeight::new(0.0), "{:?}", point);

        if let Some(prev) = points.last_mut() {
            if prev.at.fuzzy_eq(point.at) {
                if prev.val.fuzzy_eq(point.val) { return }
                println!("Jumping TTF");
                let early = min(prev.at, point.at);
                let late = max(prev.at, point.at) + FlWeight::new(EPSILON);
                debug_assert!(early.fuzzy_lt(late), "{:?} {:?}", prev, point);
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

        debug_assert!(points.last().map(|p| p.at.fuzzy_lt(point.at)).unwrap_or(true));
        points.push(point)
    }
}

#[derive(Debug)]
pub struct Cursor<'a> {
    ipps: &'a [Point],
    current_index: usize,
    offset: FlWeight,
}

impl<'a> Cursor<'a> {
    fn new(ipps: &'a [Point]) -> Self {
        Cursor { ipps, current_index: 0, offset: FlWeight::new(0.0) }
    }

    fn starting_at_or_after(ipps: &'a [Point], t: Timestamp) -> Self {
        debug_assert!(t.fuzzy_lt(period()));

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
            Err(i) => {
                if i == ipps.len() - 1 {
                    Cursor { ipps, current_index: 0, offset: period().into() }
                } else {
                    Cursor { ipps, current_index: i, offset: FlWeight::new(0.0) }
                }
            }
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

mod debug {
    use super::*;

    use std::io::Write;
    use std::process::{Command, Stdio};

    pub fn debug_merge(f: &Cursor, g: &Cursor, merged: &[Point], better: &[(Timestamp, bool)]) {
        if let Ok(mut child) = Command::new("python").stdin(Stdio::piped()).spawn() {
            if let Some(mut stdin) = child.stdin.as_mut() {
                stdin.write_all(
b"
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn

def plot_coords(coords, *args, **kwargs):
  x, y = zip(*coords)
  plt.plot(list(x), list(y), *args, **kwargs)

"
                ).expect("broken pipe, could not write to python");
                write!(&mut stdin, "plot_coords([");
                for p in f.ipps {
                    write!(&mut stdin, "({}, {}), ", f64::from(p.at), f64::from(p.val));
                }
                writeln!(&mut stdin, "], 'r+-', label='f', linewidth=1, markersize=5)");

                write!(&mut stdin, "plot_coords([");
                for p in g.ipps {
                    write!(&mut stdin, "({}, {}), ", f64::from(p.at), f64::from(p.val));
                }
                writeln!(&mut stdin, "], 'gx-', label='g', linewidth=1, markersize=5)");

                writeln!(&mut stdin, "plot_coords([({}, {})], 'rs', markersize=10)", f64::from(f.cur().at), f64::from(f.cur().val));
                writeln!(&mut stdin, "plot_coords([({}, {})], 'gs', markersize=10)", f64::from(g.cur().at), f64::from(g.cur().val));

                if !merged.is_empty() {
                    write!(&mut stdin, "plot_coords([");
                    for p in merged {
                        write!(&mut stdin, "({}, {}), ", f64::from(p.at), f64::from(p.val));
                    }
                    writeln!(&mut stdin, "], 'bo-', label='merged', linewidth=1, markersize=1)");
                }

                let max_val = f.ipps.iter().map(|p| p.val).max().unwrap();
                let max_val = max(g.ipps.iter().map(|p| p.val).max().unwrap(), max_val);

                let min_val = f.ipps.iter().map(|p| p.val).min().unwrap();
                let min_val = min(g.ipps.iter().map(|p| p.val).min().unwrap(), min_val);

                for &(t, f_better) in better {
                    writeln!(&mut stdin, "plt.vlines({}, {}, {}, '{}', linewidth=1)", f64::from(t), f64::from(min_val), f64::from(max_val), if f_better { 'r' } else { 'g' });
                }

                writeln!(&mut stdin, "plt.legend()");
                writeln!(&mut stdin, "plt.show()");
            }
        }
    }
}
