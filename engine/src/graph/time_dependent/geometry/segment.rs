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

    pub fn valid_value_range(&self) -> Range<Timestamp> {
        self.line.interpolate_at(self.valid.start)..self.line.interpolate_at(self.valid.end)
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
        debug_assert!(self.valid.contains(&x));
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
