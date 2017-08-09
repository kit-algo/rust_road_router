use std;

pub mod first_out_graph;
pub mod adjacency_lists_graph;

pub type NodeId = u32;
pub type Weight = u32;
pub const INFINITY: u32 = std::u32::MAX / 2;

#[derive(Debug)]
pub struct Link {
    pub node: NodeId,
    pub weight: Weight
}
