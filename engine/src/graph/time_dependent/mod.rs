use super::*;
use std::{
    iter::Peekable,
    cmp::{min, max}
};

pub type Timestamp = Weight;

const TOLERANCE: Weight = 10;

fn abs_diff(x: Weight, y: Weight) -> Weight {
    max(x, y) - min(x, y)
}

mod piecewise_linear_function;
use self::piecewise_linear_function::*;

mod shortcut_source;
use self::shortcut_source::*;

mod shortcut;
pub use self::shortcut::*;

mod linked;
pub use self::linked::*;

mod wrapping_range;
use self::wrapping_range::*;

mod graph;
pub use self::graph::Graph as TDGraph;

pub mod shortcut_graph;
pub use self::shortcut_graph::ShortcutGraph;

mod wrapping_slice_iter;
use self::wrapping_slice_iter::WrappingSliceIter;

mod intersections;
use self::intersections::*;
