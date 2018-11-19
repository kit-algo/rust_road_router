use std::ops::{Sub, Mul};
use super::*;

#[derive(Debug, Clone, PartialEq)]
pub struct TTFPoint {
    pub at: Timestamp,
    pub val: FlWeight,
}

impl TTFPoint {
    pub fn shifted(&self, offset: FlWeight) -> TTFPoint {
        TTFPoint { at: self.at + offset, val: self.val }
    }
}

impl Default for TTFPoint {
    fn default() -> Self {
        TTFPoint { at: Timestamp::zero(), val: FlWeight::new(0.0) }
    }
}

impl<'a> Sub for &'a TTFPoint {
    type Output = TTFPoint;

    fn sub(self, other: Self) -> Self::Output {
        TTFPoint { at: (self.at - other.at).into(), val: self.val - other.val }
    }
}

impl<'a> Mul<f64> for &'a TTFPoint {
    type Output = TTFPoint;

    fn mul(self, scalar: f64) -> Self::Output {
        TTFPoint { at: Timestamp::new(f64::from(self.at) * scalar), val: scalar * self.val }
    }
}

pub fn interpolate_linear(prev: &TTFPoint, next: &TTFPoint, t: Timestamp) -> FlWeight {
    let frac = (t - prev.at) / (next.at - prev.at);
    prev.val + (next.val - prev.val) * frac
}

pub fn intersect(f1: &TTFPoint, f2: &TTFPoint, g1: &TTFPoint, g2: &TTFPoint) -> bool {
    if ccw(f1, f2, g1) == 0               { return false }
    if ccw(f1, f2, g2) == 0               { return false }
    if ccw(g1, g2, f1) == 0               { return false }
    if ccw(g1, g2, f2) == 0               { return false }
    if ccw(f1, f2, g1) == ccw(f1, f2, g2) { return false }
    if ccw(g1, g2, f1) == ccw(g1, g2, f2) { return false }

    true
}

pub fn intersection_point(f1: &TTFPoint, f2: &TTFPoint, g1: &TTFPoint, g2: &TTFPoint) -> TTFPoint {
    let nom = perp_dot_product(&(g1 - g2), &(g1 - f1));
    let div = perp_dot_product(&(g1 - g2), &(f2 - f1));

    #[allow(clippy::float_cmp)]
    debug_assert!(div != 0.0);
    let frac = nom / div;

    TTFPoint {
        at: f1.at + frac * (f2.at - f1.at),
        val: f1.val + frac * (f2.val - f1.val),
    }
}

pub fn counter_clockwise(p: &TTFPoint, q: &TTFPoint, r: &TTFPoint) -> bool {
    ccw(p,q,r) == -1
}

pub fn clockwise(p: &TTFPoint, q: &TTFPoint, r: &TTFPoint) -> bool {
    ccw(p,q,r) == 1
}

pub fn colinear_ordered(p: &TTFPoint, q: &TTFPoint, r: &TTFPoint) -> bool {
    debug_assert!(p.at.fuzzy_lt(q.at));
    debug_assert!(q.at.fuzzy_lt(r.at));

    let v = r - p;
    q.val.fuzzy_eq(p.val + (f64::from(q.at - p.at) / f64::from(v.at)) * v.val)
}

fn ccw(a: &TTFPoint, b: &TTFPoint, c: &TTFPoint) -> i32 {
    if a.at.fuzzy_eq(b.at) && a.val.fuzzy_eq(b.val) { return 0 }
    if a.at.fuzzy_eq(c.at) && a.val.fuzzy_eq(c.val) { return 0 }
    if b.at.fuzzy_eq(c.at) && b.val.fuzzy_eq(c.val) { return 0 }

    let v1 = c - a;
    let v2 = b - a;

    let x = perp_dot_product(&v1, &v2);

    if x.abs() < 0.000_000_000_1 { return 0 }

    if x.abs() < 0.1 && (v1.val.fuzzy_eq((f64::from(v1.at) / f64::from(v2.at)) * v2.val) || v2.val.fuzzy_eq((f64::from(v2.at) / f64::from(v1.at)) * v1.val)) {
        return 0
    }

    if x > 0.0 {
        1
    } else {
        -1
    }
}

fn perp_dot_product(p: &TTFPoint, q: &TTFPoint) -> f64 {
    f64::from(p.at) * f64::from(q.val) - f64::from(q.at) * f64::from(p.val)
}
