use super::*;
use std::ops::Sub;

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct TTIpp {
    pub at: Timestamp,
    pub val: Weight,
}

impl TTIpp {
    pub fn new(at: Timestamp, val: Weight) -> TTIpp {
        TTIpp { at, val }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ATIpp {
    pub at: Timestamp,
    pub val: Timestamp,
}

impl ATIpp {
    pub fn new(at: Timestamp, val: Weight) -> ATIpp {
        debug_assert!(val >= at);
        ATIpp { at, val }
    }

    pub fn shift(&mut self) {
        self.at += period();
        self.val += period();
    }
}

#[derive(Debug)]
pub struct Point {
    pub x: i64, pub y: i64
}

impl Sub for ATIpp {
    type Output = Point;

    fn sub(self, other: Self) -> Self::Output {
        Point { x: i64::from(self.at) - i64::from(other.at), y: i64::from(self.val) - i64::from(other.val) }
    }
}
