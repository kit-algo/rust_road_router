use std;
use shortest_path::DijkstrableGraph;

pub mod first_out_graph;

pub type NodeId = u32;
pub type Weight = u32;
pub const INFINITY: u32 = std::u32::MAX / 2;

#[derive(Debug, Copy, Clone)]
pub struct Link {
    pub node: NodeId,
    pub weight: Weight
}
