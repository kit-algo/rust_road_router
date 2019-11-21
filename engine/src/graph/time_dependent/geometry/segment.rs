use super::*;
use std::ops::Range;

#[derive(Debug, Clone, PartialEq)]
pub struct Segment<LineType> {
    pub line: LineType,
    pub valid: Range<Timestamp>,
}

pub type PLFSeg = Segment<MonotoneLine<TTIpp>>;

impl PLFSeg {
    #[cfg(test)]
    pub fn from_point_tuples((from_at, from_val): (Timestamp, Weight), (to_at, to_val): (Timestamp, Weight)) -> Self {
        Segment {
            line: MonotoneLine::<TTIpp>::new(Line {
                from: TTIpp::new(from_at, from_val),
                to: TTIpp::new(to_at, to_val),
            }),
            valid: from_at..to_at,
        }
    }
}
