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

pub type TTFSeg = Segment<Line<TTIpp>>; // TODO always monotone

impl TTFSeg {
    pub fn from_point_tuples((from_at, from_val): (Timestamp, Weight), (to_at, to_val): (Timestamp, Weight)) -> Self {
        TTFSeg::new(Line { from: TTIpp::new(from_at, from_val), to: TTIpp::new(to_at, to_val) }, from_at..to_at)
    }

    #[cfg(test)]
    pub fn is_equivalent_to(&self, other: &Self) -> bool {
        if self == other { return true }
        if self.valid != other.valid { return false }
        let self_line = self.line.clone().into_monotone_tt_line();
        let other_line = other.line.clone().into_monotone_tt_line();
        if self_line.delta_x() * other_line.delta_y() != self_line.delta_y() * other_line.delta_x()  { return false }
        let delta = if self.line.from != other.line.from {
            self.line.from - other.line.from
        } else {
            self.line.to - other.line.to
        };
        if delta.x * i64::from(self_line.delta_y()) != delta.y * i64::from(self_line.delta_x()) { return false }
        true
    }

    pub fn into_monotone_at_segment(self) -> Segment<MonotoneLine<ATIpp>> {
        let Segment { line, mut valid } = self;
        let line = line.into_monotone_at_line();
        if valid.end < valid.start { // TODO <= ?
            valid.end += period();
        }
        Segment { line, valid }
    }


}

pub type MATSeg = Segment<MonotoneLine<ATIpp>>;

impl MATSeg {
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
}

pub type PLFSeg = Segment<MonotoneLine<TTIpp>>;

impl PLFSeg {
    pub fn from_point_tuples((from_at, from_val): (Timestamp, Weight), (to_at, to_val): (Timestamp, Weight)) -> Self {
        Segment { line: MonotoneLine::<TTIpp>::new(Line { from: TTIpp::new(from_at, from_val), to: TTIpp::new(to_at, to_val) }), valid: from_at..to_at }
    }

    pub fn into_atfseg(self) -> MATSeg {
        MATSeg::new(self.line.into_monotone_at_line(), self.valid)
    }
}
