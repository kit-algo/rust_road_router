use crate::datastr::graph::*;

mod floating_td_stepped_dijkstra;
mod floating_td_stepped_elimination_tree;
pub mod stepped_dijkstra;
mod stepped_elimination_tree;
mod td_stepped_dijkstra;

use self::stepped_dijkstra::{QueryProgress, State, SteppedDijkstra};

pub mod contraction_hierarchy;
pub mod customizable_contraction_hierarchy;
pub mod query;
pub mod topocore;

#[derive(Debug, Clone, Copy)]
pub struct Query {
    pub from: NodeId,
    pub to: NodeId,
}

#[derive(Debug, Clone, Copy)]
pub struct TDQuery {
    from: NodeId,
    to: NodeId,
    departure_time: Weight,
}

#[derive(Debug, Clone, Copy)]
pub struct FlTDQuery {
    from: NodeId,
    to: NodeId,
    departure_time: crate::datastr::graph::floating_time_dependent::Timestamp,
}
