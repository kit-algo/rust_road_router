use graph::*;

mod timestamped_vector;
mod stepped_dijkstra;

use self::stepped_dijkstra::{SteppedDijkstra, QueryProgress, State};

pub mod contraction_hierarchy;
pub mod query;

#[derive(Debug, Clone, Copy)]
pub struct Query {
    from: NodeId,
    to: NodeId
}
