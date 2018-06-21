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
struct Ipp {
    at: Timestamp,
    val: Weight,
}

impl Ipp {
    fn new(at: Timestamp, val: Weight) -> Ipp {
        Ipp { at, val }
    }

    fn as_tuple(self) -> (Timestamp, Weight) {
        (self.at, self.val)
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
struct TTIpp(Ipp);
#[derive(Debug, Clone, Copy, PartialEq)]
struct ATIpp(Ipp);

impl TTIpp {
    fn into_atipp(self, period: Timestamp) -> ATIpp {
        let TTIpp(Ipp { at, val }) = self;
        debug_assert!(at < period);
        ATIpp(Ipp { at, val: (at + val) % period })
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

#[derive(Debug, Clone, PartialEq)]
struct MonotoneLine<Point>(Line<Point>);

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_ttipp_to_atipp() {
        assert_eq!(TTIpp(Ipp::new(2, 2)).into_atipp(10), ATIpp(Ipp::new(2, 4)));
        assert_eq!(TTIpp(Ipp::new(6, 5)).into_atipp(10), ATIpp(Ipp::new(6, 1)));
    }

    #[test]
    fn test_tt_line_to_monotone_at_line() {
    }
}
