use super::*;

pub type Timestamp = Weight;

mod piecewise_linear_function;
use self::piecewise_linear_function::*;

mod shortcut_source;
use self::shortcut_source::*;

mod shortcut;
use self::shortcut::*;

mod linked;
use self::linked::*;

mod graph;
use self::graph::Graph as TDGraph;

mod shortcut_graph;
use self::shortcut_graph::*;
