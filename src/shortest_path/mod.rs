use super::graph::*;
use self::stepped_dijkstra::{SteppedDijkstra, QueryProgress, State};

mod timestamped_vector;
mod stepped_dijkstra;
pub mod server;

#[derive(Debug)]
pub struct Query {
    from: NodeId,
    to: NodeId
}
