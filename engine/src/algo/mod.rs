use crate::datastr::graph::*;

use self::dijkstra::stepped_dijkstra::{QueryProgress, State, SteppedDijkstra};

pub mod catchup;
pub mod ch_potentials;
pub mod contraction_hierarchy;
pub mod customizable_contraction_hierarchy;
pub mod dijkstra;
pub mod time_dependent_sampling;
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
