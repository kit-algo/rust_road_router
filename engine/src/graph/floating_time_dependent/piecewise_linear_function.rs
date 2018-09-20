use super::*;
use std::cmp::{min, max};

#[derive(Debug)]
pub struct PiecewiseLinearFunction<'a> {
    ipps: &'a [Point],
}

impl<'a> PiecewiseLinearFunction<'a> {
    pub fn new(ipps: &'a [Point]) -> PiecewiseLinearFunction<'a> {
        debug_assert!(ipps.first().unwrap().at == Timestamp::zero());
        debug_assert!(ipps.first().unwrap().val == ipps.last().unwrap().val);
        debug_assert!(ipps.len() == 1 || ipps.last().unwrap().at == period());
        PiecewiseLinearFunction { ipps }
    }

    pub fn lower_bound(&self) -> FlWeight {
        self.ipps.iter().map(|p| p.val).min().unwrap()
    }

    pub fn upper_bound(&self) -> FlWeight {
        self.ipps.iter().map(|p| p.val).max().unwrap()
    }

    pub fn eval(&self, _t: Timestamp) -> FlWeight {
        unimplemented!();
    }

    pub fn link(&self, other: &Self) -> Vec<Point> {
        if let [Point { val, .. }] = &self.ipps {
            if let [Point { val: other, .. }] = &other.ipps {
                return vec![Point { at: Timestamp::zero(), val: val + other }]
            } else {
                let zero_val = other.eval(val.into());
                return other.ipps.iter().filter(|p| p.at > val.into()).map(|p| Point { at: p.at - val, val: p.val + val })
                    .chain(std::iter::once(Point { at: Timestamp::zero(), val: zero_val + val }))
                    .chain(
                        other.ipps.iter().filter(|p| p.at < val.into()).map(|p| Point { at: p.at + FlWeight::from(period()) - val, val: p.val + val })
                    ).chain(std::iter::once(Point { at: period(), val: zero_val + val })).collect()
            }
        }
        if let [Point { val, .. }] = &other.ipps {
            return self.ipps.iter().map(|p| Point { at: p.at, val: p.val + val }).collect()
        }

        let mut result = Vec::with_capacity(self.ipps.len() + other.ipps.len() + 1);

        let f = &self.ipps;
        let g = &other.ipps;

        let mut i = other.first_index_greater_or_equal(self.ipps[0].val);
        let mut j = 0;

        loop {
            let mut x;
            let y;

            if g[i].at.fuzzy_eq(f[j].at + f[j].val) {
                x = f[j].at;
                y = g[i].val + f[j].val;

                i += 1;
                j += 1;
            }
            else if g[i].at < f[j].at + f[j].val {
                debug_assert!(g[i].at.fuzzy_lt(f[j].at + f[j].val));

                let m_arr_f_inverse = (f[j].at - f[j-1].at) / (f[j].at + f[j].val - f[j-1].at - f[j-1].val);
                x = m_arr_f_inverse * (g[i].at - f[j-1].at - f[j-1].val) + f[j-1].at;
                y = g[i].at + g[i].val - x;

                i += 1;
            } else {
                debug_assert!((f[j].at + f[j].val).fuzzy_lt(g[i].at));

                x = f[j].at;
                let m_g = (g[i].val - g[i-1].val) / (g[i].at - g[i-1].at);
                y = g[i-1].val + m_g * (f[j].at + f[j].val - g[i-1].at) + f[j].val;

                j += 1;
            }

            if period().fuzzy_lt(x) { break }

            x = min(x, period());
            x = max(x, Timestamp::zero());

            Self::append_point(&mut result, Point { at: x, val: y });
        }

        debug_assert!(result.len() <= self.ipps.len() + other.ipps.len() + 1);
        result
    }

    pub fn merge(&self, other: &Self) -> (Vec<Point>, Vec<(Timestamp, bool)>) {
        if self.upper_bound() < other.lower_bound() {
            return (self.ipps.to_vec(), vec![(Timestamp::zero(), true)])
        } else if other.upper_bound()  < self.lower_bound() {
            return (self.ipps.to_vec(), vec![(Timestamp::zero(), false)])
        }

        let mut result = Vec::with_capacity(2 * self.ipps.len() + 2 * other.ipps.len() + 2);

        let f = &self.ipps;
        let g = &other.ipps;

        let mut i = 0;
        let mut j = 0;

        while i < f.len() || j < g.len() {

            if intersect(&f[i - 1], &f[i], &g[j - 1], &g[j]) {
                let intersection = intersection_point(&f[i-1], &f[i], &g[j-1], &g[j]);
                if intersection.at >= Timestamp::zero() { Self::append_point(&mut result, intersection); }
                // TODO which is the better one?
            }

            if f[i].at.fuzzy_eq(g[j].at) {
                if f[i].val.fuzzy_eq(g[j].val) {
                    Self::append_point(&mut result, f[i].clone());

                    if counter_clockwise(&g[j - 1], &f[i - 1], &f[i]) || counter_clockwise(&f[i], &f[i + 1], &g[j + 1]) {
                        unimplemented!(); // f better
                    }

                    if clockwise(&g[j-1], &f[i-1], &f[i]) || clockwise(&f[i], &f[i+1], &g[j+1]) {
                        unimplemented!() // g better
                    }

                } else if f[i].val < g[j].val {

                    Self::append_point(&mut result, f[i].clone());

                    debug_assert!(f[i].val.fuzzy_lt(g[j].val));
                    unimplemented!(); // f better

                } else {

                    debug_assert!(g[j].val < f[i].val);
                    Self::append_point(&mut result, g[j].clone());
                    unimplemented!() // g better
                }

                i += 1;
                j += 1;

            } else if f[i].at < g[j].at {

                debug_assert!(f[i].at.fuzzy_lt(g[j].at));

                if counter_clockwise(&g[j - 1], &f[i], &g[j]) {
                    Self::append_point(&mut result, f[i].clone());
                    unimplemented!(); // f better

                } else if colinear(&g[j - 1], &f[i], &g[j]) {

                    if counter_clockwise(&g[j - 1], &f[i - 1], &f[i]) || counter_clockwise(&f[i], &f[i + 1], &g[j]) {
                        Self::append_point(&mut result, f[i].clone());
                        unimplemented!(); // f better
                    } else if result.is_empty() {
                        Self::append_point(&mut result, f[i].clone());
                    }

                    if clockwise(&g[j - 1], &f[i - 1], &f[i]) || clockwise(&f[i], &f[i + 1], &g[j]) {
                        unimplemented!() // g better
                    }
                }

                i += 1;

            } else {

                debug_assert!(g[j].at < f[i].at);

                if counter_clockwise(&f[i - 1], &g[j], &f[i]) {
                    Self::append_point(&mut result, g[j].clone());
                    unimplemented!() // g better

                } else if colinear(&f[i - 1], &g[j], &f[i]) {
                    if counter_clockwise(&g[j - 1], &g[j], &f[i - 1]) || counter_clockwise(&g[j], &g[j + 1], &f[i]) {
                        Self::append_point(&mut result, g[j].clone());
                        unimplemented!() // g better
                    }
                    if result.is_empty() {
                        Self::append_point(&mut result, g[j].clone());
                    }

                    if clockwise(&g[j - 1], &g[j], &f[i - 1]) || clockwise(&g[j], &g[j + 1], &f[i]) {
                        unimplemented!(); // f better
                    }
                }

                j += 1;
            }
        };

        let n = f.len();
        let m = g.len();

        if intersect(&f[n - 1], &f[n], &g[m - 1], &g[m]) {
            let intersection = intersection_point(&f[n-1], &f[n], &g[m-1], &g[m]);
            if intersection.at < period() { Self::append_point(&mut result, intersection); }

            unimplemented!(); // TODO which is better?
        }

        (result, vec![])
    }

    fn first_index_greater_or_equal(&self, _val: FlWeight) -> usize {
        unimplemented!();
    }

    fn append_point(points: &mut Vec<Point>, point: Point) {
        points.push(point)
    }
}

fn intersect(f1: &Point, f2: &Point, g1: &Point, g2: &Point) -> bool {
    unimplemented!();
}

fn intersection_point(f1: &Point, f2: &Point, g1: &Point, g2: &Point) -> Point {
    unimplemented!();
}

fn counter_clockwise(p: &Point, q: &Point, r: &Point) -> bool {
    unimplemented!();
}

fn clockwise(p: &Point, q: &Point, r: &Point) -> bool {
    unimplemented!();
}

fn colinear(p: &Point, q: &Point, r: &Point) -> bool {
    unimplemented!();
}
