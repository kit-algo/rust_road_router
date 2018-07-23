use super::*;
use std::ops::Range;

#[derive(Debug, Clone, PartialEq)]
pub struct Segment<LineType> {
    pub line: LineType,
    pub valid: Range<Timestamp>,
}

impl<LineType> Segment<LineType> {
    pub fn new(line: LineType, valid: Range<Timestamp>) -> Self {
        Segment { line, valid }
    }
}

pub type MATSeg = Segment<MonotoneLine<ATIpp>>;

impl MATSeg {
    #[cfg(test)]
    pub fn from_point_tuples((from_at, from_val): (Timestamp, Weight), (to_at, to_val): (Timestamp, Weight)) -> Self {
        MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(from_at, from_val), ATIpp::new(to_at, to_val))), from_at..to_at)
    }

    pub fn valid_value_range(&self, staring_at: Timestamp) -> Range<Timestamp> {
        debug_assert!(self.valid.contains(&staring_at));
        self.line.interpolate_at(staring_at)..self.line.interpolate_at(self.valid.end)
    }

    pub fn shift(&mut self) {
        self.valid.start += period();
        self.valid.end += period();
        self.line.shift();
    }

    pub fn start_of_valid_at_val(&self) -> Timestamp {
        self.line.interpolate_at(self.valid.start) % period()
    }

    pub fn end_of_valid_at_val(&self) -> Timestamp {
        self.line.interpolate_at(self.valid.end) % period()
    }

    pub fn eval(&self, x: Timestamp) -> Weight {
        debug_assert!(self.valid.start <= x);
        debug_assert!(self.valid.end >= x);
        self.line.interpolate_tt(x)
    }

    pub fn combine(&mut self, other: &MATSeg) -> bool {
        if self.line == other.line && self.valid.end == other.valid.start {
            self.valid.end = other.valid.end;
            return true
        }
        false
    }

    pub fn line(&self) -> &Line<ATIpp> {
        self.line.line()
    }

    pub fn intersect(&self, other: &MATSeg) -> Option<Timestamp> {
        let x1 = i128::from(self.line().from.at);
        let x2 = i128::from(self.line().to.at);
        let x3 = i128::from(other.line().from.at);
        let x4 = i128::from(other.line().to.at);
        let y1 = i128::from(self.line().from.val);
        let y2 = i128::from(self.line().to.val);
        let y3 = i128::from(other.line().from.val);
        let y4 = i128::from(other.line().to.val);

        let numerator = (x1 * y2 - y1 * x2) * (x3 - x4) - (x1 - x2) * (x3 * y4 - y3 * x4);
        let denominator = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);

        if denominator == 0 {
            return None;
        }

        let result = if numerator >= 0 {
            (numerator + denominator - 1) / denominator
        } else {
            (numerator + denominator + 1) / denominator
        };
        let unrounded = numerator / denominator;

        if result >= i128::from(INFINITY) {
            return None
        }

        let result = result as Timestamp;
        let unrounded = unrounded as Timestamp;

        if !self.valid.contains(&result) || !other.valid.contains(&result) || unrounded < self.valid.start || unrounded < other.valid.start {
            return None
        }

        Some(result as Timestamp)
    }

    #[cfg(test)]
    pub fn is_equivalent_to(&self, other: &Self) -> bool {
        if self == other { return true }
        if self.valid != other.valid { return false }
        if self.line.delta_x() * other.line.delta_y() != self.line.delta_y() * other.line.delta_x() { return false }
        let delta = if self.line().from == other.line().from {
            self.line().to - other.line().to
        } else {
            self.line().from - other.line().from
        };
        return delta.x * i64::from(self.line.delta_y()) == delta.y * i64::from(self.line.delta_x())
    }
}

pub type PLFSeg = Segment<MonotoneLine<TTIpp>>;

impl PLFSeg {
    #[cfg(test)]
    pub fn from_point_tuples((from_at, from_val): (Timestamp, Weight), (to_at, to_val): (Timestamp, Weight)) -> Self {
        Segment { line: MonotoneLine::<TTIpp>::new(Line { from: TTIpp::new(from_at, from_val), to: TTIpp::new(to_at, to_val) }), valid: from_at..to_at }
    }

    pub fn into_atfseg(self) -> MATSeg {
        MATSeg::new(self.line.into_monotone_at_line(), self.valid)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intersection() {
        assert_eq!(MATSeg::from_point_tuples((0,0), (5,5))    .intersect(&MATSeg::from_point_tuples((0,8), (8,16))),     None);
        assert_eq!(MATSeg::from_point_tuples((0,0), (5,5))    .intersect(&MATSeg::from_point_tuples((0,8), (8,15))),     None);
        assert_eq!(MATSeg::from_point_tuples((0,0), (2,4))    .intersect(&MATSeg::from_point_tuples((0,2), (2,2))),      Some(1));
        assert_eq!(MATSeg::from_point_tuples((0,0), (3,5))    .intersect(&MATSeg::from_point_tuples((0,2), (3,3))),      Some(2));
        assert_eq!(MATSeg::from_point_tuples((0,2), (3,3))    .intersect(&MATSeg::from_point_tuples((0,0), (3,5))),      Some(2));
        assert_eq!(MATSeg::from_point_tuples((7,7+8), (9,9+6)).intersect(&MATSeg::from_point_tuples((6,8), (16,16+12))), None);
        assert_eq!(MATSeg::from_point_tuples((9,9+6), (10,20)).intersect(&MATSeg::from_point_tuples((6,8), (16,16+12))), None);
    }
}
