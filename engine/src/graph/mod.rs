use std;
use std::ops::Range;

pub mod first_out_graph;
pub mod floating_time_dependent;
pub mod link_id_to_tail_mapper;
pub mod time_dependent;

pub use self::first_out_graph::{FirstOutGraph, OwnedGraph};

pub type NodeId = u32;
pub type EdgeId = u32;
pub type Weight = u32;
pub const INFINITY: Weight = std::u32::MAX / 2;

#[derive(Debug, Copy, Clone)]
pub struct Link {
    pub node: NodeId,
    pub weight: Weight,
}

pub trait Graph {
    fn num_nodes(&self) -> usize;
    fn num_arcs(&self) -> usize;
}

pub trait LinkIterGraph<'a>: Graph {
    type Iter: Iterator<Item = Link> + 'a; // fix with https://github.com/rust-lang/rfcs/pull/1598

    fn neighbor_iter(&'a self, node: NodeId) -> Self::Iter;

    fn reverse(&'a self) -> OwnedGraph {
        // vector of adjacency lists for the reverse graph
        let mut reversed: Vec<Vec<Link>> = (0..self.num_nodes()).map(|_| Vec::<Link>::new()).collect();

        // iterate over all edges and insert them in the reversed structure
        for node in 0..(self.num_nodes() as NodeId) {
            for Link { node: neighbor, weight } in self.neighbor_iter(node) {
                reversed[neighbor as usize].push(Link { node, weight });
            }
        }

        OwnedGraph::from_adjancecy_lists(reversed)
    }

    fn ch_split(&'a self, node_ranks: &[u32]) -> (OwnedGraph, OwnedGraph) {
        let mut up: Vec<Vec<Link>> = (0..self.num_nodes()).map(|_| Vec::<Link>::new()).collect();
        let mut down: Vec<Vec<Link>> = (0..self.num_nodes()).map(|_| Vec::<Link>::new()).collect();

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

pub trait MutWeightLinkIterGraph<'a>: Graph {
    type Iter: Iterator<Item = (&'a NodeId, &'a mut Weight)> + 'a;
    fn mut_weight_link_iter(&'a mut self, node: NodeId) -> Self::Iter;
}

pub fn unify_parallel_edges<G: for<'a> MutWeightLinkIterGraph<'a>>(graph: &mut G) {
    let mut weight_cache = vec![INFINITY; graph.num_nodes()];
    for node in 0..graph.num_nodes() {
        for (&node, weight) in graph.mut_weight_link_iter(node as NodeId) {
            weight_cache[node as usize] = std::cmp::min(weight_cache[node as usize], *weight);
        }
        for (&node, weight) in graph.mut_weight_link_iter(node as NodeId) {
            *weight = weight_cache[node as usize];
        }
        for (&node, _weight) in graph.mut_weight_link_iter(node as NodeId) {
            weight_cache[node as usize] = INFINITY;
        }
    }
}

pub trait RandomLinkAccessGraph {
    fn link(&self, edge_id: EdgeId) -> Link;
    fn edge_index(&self, from: NodeId, to: NodeId) -> Option<EdgeId>;
    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId>;

    fn neighbor_edge_indices_usize(&self, node: NodeId) -> Range<usize> {
        let range = self.neighbor_edge_indices(node);
        Range {
            start: range.start as usize,
            end: range.end as usize,
        }
    }
}
