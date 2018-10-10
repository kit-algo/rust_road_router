use std::ops::{Sub, Mul};
use super::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Point {
    pub at: Timestamp,
    pub val: FlWeight,
}

impl Point {
    pub fn shifted(&self, offset: FlWeight) -> Point {
        Point { at: self.at + offset, val: self.val }
    }

    fn length(&self) -> f64 {
        (f64::from(self.at) * f64::from(self.at) + f64::from(self.val) * f64::from(self.val)).sqrt()
    }
}

impl<'a> Sub for &'a Point {
    type Output = Point;

    fn sub(self, other: Self) -> Self::Output {
        Point { at: (self.at - other.at).into(), val: self.val - other.val }
    }
}

impl<'a> Mul<f64> for &'a Point {
    type Output = Point;

    fn mul(self, scalar: f64) -> Self::Output {
        Point { at: Timestamp::new(f64::from(self.at) * scalar), val: scalar * self.val }
    }
}

pub fn intersect(f1: &Point, f2: &Point, g1: &Point, g2: &Point) -> bool {
    if ccw(f1, f2, g1) == 0               { return false }
    if ccw(f1, f2, g2) == 0               { return false }
    if ccw(g1, g2, f1) == 0               { return false }
    if ccw(g1, g2, f2) == 0               { return false }
    if ccw(f1, f2, g1) == ccw(f1, f2, g2) { return false }
    if ccw(g1, g2, f1) == ccw(g1, g2, f2) { return false }

    true
}

pub fn intersection_point(f1: &Point, f2: &Point, g1: &Point, g2: &Point) -> Point {
    let nom = perp_dot_product(&(g1 - g2), &(g1 - f1));
    let div = perp_dot_product(&(g1 - g2), &(f2 - f1));

    #[allow(clippy::float_cmp)]
    debug_assert!(div != 0.0);
    let frac = nom / div;

    Point {
        at: f1.at + frac * (f2.at - f1.at),
        val: f1.val + frac * (f2.val - f1.val),
    }
}

pub fn counter_clockwise(p: &Point, q: &Point, r: &Point) -> bool {
    ccw(p,q,r) == -1
}

pub fn clockwise(p: &Point, q: &Point, r: &Point) -> bool {
    ccw(p,q,r) == 1
}

pub fn colinear(p: &Point, q: &Point, r: &Point) -> bool {
    ccw(p,q,r) == 0
}

fn ccw(a: &Point, b: &Point, c: &Point) -> i32 {
    if a.at.fuzzy_eq(b.at) && a.val.fuzzy_eq(b.val) { return 0 }
    if a.at.fuzzy_eq(c.at) && a.val.fuzzy_eq(c.val) { return 0 }
    if b.at.fuzzy_eq(c.at) && b.val.fuzzy_eq(c.val) { return 0 }

    let v1 = c - a;
    let v2 = b - a;

    let x = perp_dot_product(&v1, &v2);

    if x.abs() < 0.000_000_01 {
        0
    } else if x.abs() > 0.1 {
        if x > 0.0 {
            1
        } else {
            -1
        }
    } else {
        let v1 = &v1 * (1.0 / if EPSILON > v1.length() { EPSILON } else { v1.length() });
        let v2 = &v2 * (1.0 / if EPSILON > v2.length() { EPSILON } else { v2.length() });

        let x = perp_dot_product(&v1, &v2);

        if x.abs() < 0.000_000_000_1 { return 0 }

        if x > 0.0 {
            1
        } else {
            -1
        }
    }
}

fn perp_dot_product(p: &Point, q: &Point) -> f64 {
    f64::from(p.at) * f64::from(q.val) - f64::from(q.at) * f64::from(p.val)
}
