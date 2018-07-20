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

    pub fn as_tuple(self) -> (Timestamp, Weight) {
        (self.at, self.val)
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

    pub fn into_ttipp(self) -> TTIpp {
        let ATIpp { at, val } = self;
        debug_assert!(at <= val);
        TTIpp { at, val: val - at }
    }

    pub fn as_tuple(self) -> (Timestamp, Timestamp) {
        (self.at, self.val)
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
