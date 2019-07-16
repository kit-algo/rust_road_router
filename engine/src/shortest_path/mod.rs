use crate::graph::*;

pub mod timestamped_vector;
pub mod clearlist_vector;
mod stepped_dijkstra;
mod stepped_elimination_tree;
mod td_stepped_dijkstra;
mod floating_td_stepped_dijkstra;
mod floating_td_stepped_elimination_tree;

use self::stepped_dijkstra::{SteppedDijkstra, QueryProgress, State};

pub mod node_order;
pub mod contraction_hierarchy;
pub mod customizable_contraction_hierarchy;
pub mod query;

#[derive(Debug, Clone, Copy)]
pub struct Query {
    from: NodeId,
    to: NodeId
}

#[derive(Debug, Clone, Copy)]
pub struct TDQuery {
    from: NodeId,
    to: NodeId,
    departure_time: Weight
}


#[derive(Debug, Clone, Copy)]
pub struct FlTDQuery {
    from: NodeId,
    to: NodeId,
    departure_time: crate::graph::floating_time_dependent::Timestamp
}
