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
pub use self::graph::Graph as TDGraph;

pub mod shortcut_graph;
pub use self::shortcut_graph::ShortcutGraph;
