use std;

pub mod first_out_graph;

pub use self::first_out_graph::{OwnedGraph, FirstOutGraph};

pub type NodeId = u32;
pub type Weight = u32;
pub const INFINITY: u32 = std::u32::MAX / 2;

#[derive(Debug, Copy, Clone)]
pub struct Link {
    pub node: NodeId,
    pub weight: Weight
}

pub trait Graph {
    fn num_nodes(&self) -> usize;
}

pub trait LinkIterGraph<'a>: Graph {
    type Iter: Iterator<Item = Link> + 'a;

    fn neighbor_iter(&'a self, node: NodeId) -> Self::Iter;

    fn reverse(&'a self) -> OwnedGraph {
        // vector of adjacency lists for the reverse graph
        let mut reversed: Vec<Vec<Link>> = (0..self.num_nodes()).map(|_| Vec::<Link>::new() ).collect();

        // iterate over all edges and insert them in the reversed structure
        for node in 0..(self.num_nodes() as NodeId) {
            for Link { node: neighbor, weight } in self.neighbor_iter(node) {
                reversed[neighbor as usize].push(Link { node, weight });
            }
        }

        OwnedGraph::from_adjancecy_lists(reversed)
    }

    fn ch_split(&'a self, node_ranks: &Vec<u32>) -> (OwnedGraph, OwnedGraph) {
        let mut up: Vec<Vec<Link>> = (0..self.num_nodes()).map(|_| Vec::<Link>::new() ).collect();
        let mut down: Vec<Vec<Link>> = (0..self.num_nodes()).map(|_| Vec::<Link>::new() ).collect();

        // iterate over all edges and insert them in the reversed structure
        for node in 0..(self.num_nodes() as NodeId) {
            for Link { node: neighbor, weight } in self.neighbor_iter(node) {
                if node_ranks[node as usize] < node_ranks[neighbor as usize] {
                    up[node as usize].push(Link { node: neighbor, weight });
                } else {
                    down[neighbor as usize].push(Link { node, weight });
                }
            }
        }

        (OwnedGraph::from_adjancecy_lists(up), OwnedGraph::from_adjancecy_lists(down))
    }
}

pub trait RandomLinkAccessGraph {
    fn edge_index(&self, from: NodeId, to: NodeId) -> Option<usize>;
}
