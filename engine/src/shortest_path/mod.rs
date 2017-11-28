use super::graph::*;
use self::stepped_dijkstra::{SteppedDijkstra, QueryProgress, State};

mod timestamped_vector;
mod stepped_dijkstra;
pub mod contraction_hierarchy;
pub mod customizable_contraction_hierarchy;
pub mod query;

#[derive(Debug, Clone, Copy)]
pub struct Query {
    from: NodeId,
    to: NodeId
}

pub trait DijkstrableGraph {
    fn num_nodes(&self) -> usize;
    // not particularily liking this interface, would be much nicer to return an iterator
    // sadly we would have to box it, which would be problematic in terms of performance
    // even the impl trait functionality on nightly won't allow generic return types on traits
    // which makes sense when you think about it, because it would need to return something
    // which does dynamic dispath, which is exactly what the boxing would do...
    fn for_each_neighbor(&self, node: NodeId, f: &mut FnMut(Link));
}
