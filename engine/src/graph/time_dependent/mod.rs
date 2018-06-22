use super::*;
use std::{
    iter::Peekable,
    cmp::{min, max}
};

use ::sorted_search_slice_ext::SortedSearchSliceExt;

mod piecewise_linear_function;
use self::piecewise_linear_function::*;

mod shortcut_source;
use self::shortcut_source::*;

mod shortcut;
pub use self::shortcut::*;

mod linked;
pub use self::linked::*;

mod wrapping_range;
pub use self::wrapping_range::*;

mod graph;
pub use self::graph::Graph as TDGraph;

pub mod shortcut_graph;
pub use self::shortcut_graph::ShortcutGraph;

mod wrapping_slice_iter;
use self::wrapping_slice_iter::WrappingSliceIter;

mod intersections;
use self::intersections::*;


pub type Timestamp = Weight;

const TOLERANCE: Weight = 10;

fn abs_diff(x: Weight, y: Weight) -> Weight {
    max(x, y) - min(x, y)
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct TTIpp {
    at: Timestamp,
    val: Weight,
}

impl TTIpp {
    fn new(at: Timestamp, val: Weight) -> Ipp {
        Ipp { at, val }
    }

    fn as_tuple(self) -> (Timestamp, Weight) {
        (self.at, self.val)
    }
}

type Ipp = TTIpp;

#[derive(Debug, Clone, Copy, PartialEq)]
struct ATIpp {
    at: Timestamp,
    val: Timestamp,
}

impl ATIpp {
    fn new(at: Timestamp, val: Weight) -> ATIpp {
        ATIpp { at, val }
    }

    fn as_tuple(self) -> (Timestamp, Weight) {
        (self.at, self.val)
    }
}

impl TTIpp {
    fn into_atipp(self, period: Timestamp) -> ATIpp {
        let TTIpp { at, val } = self;
        debug_assert!(at < period);
        ATIpp{ at, val: (at + val) % period }
    }
}

#[derive(Debug, Clone, PartialEq)]
struct Line<Point> {
    from: Point,
    to: Point,
}

#[derive(Debug, Clone, PartialEq)]
struct Segment<LineType> {
    line: LineType,
    valid: Range<Timestamp>,
}

type TTFSeg = Segment<Line<Ipp>>;

impl TTFSeg {
    fn new((from_at, from_val): (Timestamp, Weight), (to_at, to_val): (Timestamp, Weight)) -> Self {
        Segment { line: Line { from: Ipp::new(from_at, from_val), to: Ipp::new(to_at, to_val) }, valid: Range { start: from_at, end: to_at } }
    }
}

impl Line<TTIpp> {
    fn into_monotone_at_line(self, period: Timestamp) -> MonotoneLine<ATIpp> {
        let Line { from, mut to } = self;
        if to.at < from.at {
            to.at += period;
        }
        MonotoneLine(Line { from: ATIpp { at: from.at, val: from.at + from.val }, to: ATIpp { at: to.at, val: to.at + to.val } })
    }
}

#[derive(Debug, Clone, PartialEq)]
struct MonotoneLine<Point>(Line<Point>);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ttipp_to_atipp() {
        assert_eq!(TTIpp::new(2, 2).into_atipp(10), ATIpp::new(2, 4));
        assert_eq!(TTIpp::new(6, 5).into_atipp(10), ATIpp::new(6, 1));
    }

    #[test]
    fn test_tt_line_to_monotone_at_line() {
        assert_eq!(Line { from: TTIpp::new(2, 2), to: TTIpp::new(4, 5) }.into_monotone_at_line(10),
            MonotoneLine(Line { from: ATIpp::new(2, 4), to: ATIpp::new(4, 9) }));
        assert_eq!(Line { from: TTIpp::new(4, 5), to: TTIpp::new(2, 2) }.into_monotone_at_line(10),
            MonotoneLine(Line { from: ATIpp::new(4, 9), to: ATIpp::new(12, 14) }));
        assert_eq!(Line { from: TTIpp::new(8, 3), to: TTIpp::new(2, 2) }.into_monotone_at_line(10),
            MonotoneLine(Line { from: ATIpp::new(8, 11), to: ATIpp::new(12, 14) }));
    }
}
