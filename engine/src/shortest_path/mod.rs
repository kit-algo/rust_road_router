use super::graph::*;
use self::stepped_dijkstra::{SteppedDijkstra, QueryProgress, State};

mod timestamped_vector;
mod stepped_dijkstra;
pub mod contraction_hierarchy;
pub mod query;

#[derive(Debug, Clone, Copy)]
pub struct Query {
    from: NodeId,
    to: NodeId
}
