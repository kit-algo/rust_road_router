use super::*;
use crate::shortest_path::node_order::NodeOrder;

pub mod cch_graph;
mod contraction;
use contraction::*;

use self::cch_graph::CCHGraph;

pub fn contract<Graph: for<'a> LinkIterGraph<'a>>(graph: &Graph, node_order: NodeOrder) -> CCHGraph {
    CCHGraph::new(ContractionGraph::new(graph, node_order).contract())
}
