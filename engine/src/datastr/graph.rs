//! Something with graphs 🤷‍♂️
//!
//! Several traits and structs for working with graphs.

use crate::datastr::node_order::NodeOrder;
use std::ops::Range;

pub mod first_out_graph;
pub mod floating_time_dependent;
pub mod link_id_to_tail_mapper;
pub mod time_dependent;

pub use self::first_out_graph::{FirstOutGraph, OwnedGraph, ReversedGraphWithEdgeIds, UnweightedFirstOutGraph, UnweightedOwnedGraph};

/// Node ids are 32bit unsigned ints
pub type NodeId = u32;
/// Edge ids are 32bit unsigned ints
pub type EdgeId = u32;
/// Basic weights are 32bit unsigned ints
pub type Weight = u32;
/// A sufficiently large infinity constant.
/// Set to `u32::MAX / 2` so that `INFINITY + x` for `x <= INFINITY` does not overflow.
pub const INFINITY: Weight = std::u32::MAX / 2;

pub trait Arc {
    fn head(&self) -> NodeId;
}

/// Simple struct for weighted links.
/// No behaviour, just a pure data struct.
#[derive(Debug, Copy, Clone)]
pub struct Link {
    pub node: NodeId,
    pub weight: Weight,
}

impl Arc for Link {
    #[inline(always)]
    fn head(&self) -> NodeId {
        self.node
    }
}

impl Arc for (NodeId, EdgeId) {
    #[inline(always)]
    fn head(&self) -> NodeId {
        self.0
    }
}

#[derive(Debug, Copy, Clone)]
pub struct LinkWithId {
    pub node: NodeId,
    pub edge_id: EdgeId,
}

impl Arc for LinkWithId {
    #[inline(always)]
    fn head(&self) -> NodeId {
        self.node
    }
}

/// Base trait for graphs.
/// Interesting behaviour will be added through subtraits.
pub trait Graph {
    fn num_nodes(&self) -> usize;
    fn num_arcs(&self) -> usize;
    fn degree(&self, node: NodeId) -> usize;
}

pub trait LinkIterable<Link>: Graph {
    /// Type of the outgoing neighbor iterator.
    type Iter<'a>: Iterator<Item = Link>;

    /// Get a iterator over the outgoing links of the given node.
    fn link_iter(&self, node: NodeId) -> Self::Iter<'_>;
}

pub trait MutLinkIterable<'a, Link>: Graph {
    /// Type of the outgoing neighbor iterator.
    type Iter: Iterator<Item = Link> + 'a;

    /// Get a iterator over the outgoing links of the given node.
    fn link_iter_mut(&'a mut self, node: NodeId) -> Self::Iter;
}

/// Trait for graph data structures which allow iterating over outgoing links of a node.
pub trait LinkIterGraph: LinkIterable<Link> {
    /// Split the graph in an upward graph of outgoing edges and a
    /// downward graph of incoming edges based on the node order passed.
    fn ch_split(&self, order: &NodeOrder) -> (OwnedGraph, OwnedGraph) {
        let mut up: Vec<Vec<Link>> = (0..self.num_nodes()).map(|_| Vec::<Link>::new()).collect();
        let mut down: Vec<Vec<Link>> = (0..self.num_nodes()).map(|_| Vec::<Link>::new()).collect();

        // iterate over all edges and insert them in the reversed structure
        for node in 0..(self.num_nodes() as NodeId) {
            for Link { node: neighbor, weight } in self.link_iter(node) {
                if order.rank(node) < order.rank(neighbor) {
                    up[node as usize].push(Link { node: neighbor, weight });
                } else {
                    down[neighbor as usize].push(Link { node, weight });
                }
            }
        }

        (OwnedGraph::from_adjancecy_lists(up), OwnedGraph::from_adjancecy_lists(down))
    }
}

impl<G: LinkIterable<Link>> LinkIterGraph for G {}

/// Utility function for preprocessing graphs with parallel edges.
/// Will ensure, that all parallel edges have the lowest weight.
/// Currently used to preprocess OSM graphs before CCH customization
/// because the respecting will not necessarily use the parallel edge with the lowest weight.
pub fn unify_parallel_edges<G: for<'a> MutLinkIterable<'a, (&'a NodeId, &'a mut Weight)>>(graph: &mut G) {
    let mut weight_cache = vec![INFINITY; graph.num_nodes()];
    for node in 0..graph.num_nodes() {
        for (&node, weight) in graph.link_iter_mut(node as NodeId) {
            weight_cache[node as usize] = std::cmp::min(weight_cache[node as usize], *weight);
        }
        for (&node, weight) in graph.link_iter_mut(node as NodeId) {
            *weight = weight_cache[node as usize];
        }
        for (&node, _weight) in graph.link_iter_mut(node as NodeId) {
            weight_cache[node as usize] = INFINITY;
        }
    }
}

/// Trait for graph types which allow random access to links based on edge ids.
pub trait RandomLinkAccessGraph: Graph {
    /// Get the link with the given id.
    fn link(&self, edge_id: EdgeId) -> Link;
    /// Find the id of the edge from `from` to `to` if it exists.
    fn edge_index(&self, from: NodeId, to: NodeId) -> Option<EdgeId>;
    /// Get the range of edge ids which make up the outgoing edges of `node`
    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId>;

    /// Get the range of edge ids which make up the outgoing edges of `node` as a `Range<usize>`
    #[inline(always)]
    fn neighbor_edge_indices_usize(&self, node: NodeId) -> Range<usize> {
        let range = self.neighbor_edge_indices(node);
        Range {
            start: range.start as usize,
            end: range.end as usize,
        }
    }

    /// Build the line graph (the turn expanded graph).
    /// The callback should return the turn costs between the two links
    /// with the given ids and `None` if the turn is forbidden.
    fn line_graph(&self, mut turn_costs: impl FnMut(EdgeId, EdgeId) -> Option<Weight>) -> OwnedGraph {
        let mut first_out = Vec::with_capacity(self.num_arcs() + 1);
        first_out.push(0);
        let mut head = Vec::new();
        let mut weight = Vec::new();
        let mut num_turns = 0;

        for edge_id in 0..self.num_arcs() {
            let link = self.link(edge_id as EdgeId);
            for next_link_id in self.neighbor_edge_indices(link.node) {
                if let Some(turn_cost) = turn_costs(edge_id as EdgeId, next_link_id) {
                    head.push(next_link_id);
                    weight.push(link.weight + turn_cost);
                    num_turns += 1;
                }
            }
            first_out.push(num_turns as EdgeId);
        }

        OwnedGraph::new(first_out, head, weight)
    }
}

/// Generic Trait for building reversed graphs.
/// Type setup similar to `FromIter` for `std::iter::collect`.
pub trait BuildReversed<G> {
    /// Create a new graph with all edges reversed
    fn reversed(graph: &G) -> Self;
}

/// Generic Trait for building permutated graphs.
/// Type setup similar to `FromIter` for `std::iter::collect`.
pub trait BuildPermutated<G>: Sized {
    /// Build an isomorph graph with node ids permutated according to the given order.
    fn permutated(graph: &G, order: &NodeOrder) -> Self {
        Self::permutated_filtered(graph, order, Box::new(|_, _| true))
    }

    /// Build an isomorph graph with node ids permutated according to the given order while filtering out edges.
    /// Predicate takes edges as NodeId pairs with NodeIds according to the permutated graph.
    fn permutated_filtered(graph: &G, order: &NodeOrder, predicate: Box<dyn FnMut(NodeId, NodeId) -> bool>) -> Self;
}

pub struct InfinityFilteringGraph<G>(pub G);

impl<G: Graph> Graph for InfinityFilteringGraph<G> {
    fn degree(&self, node: NodeId) -> usize {
        self.0.degree(node)
    }
    fn num_nodes(&self) -> usize {
        self.0.num_nodes()
    }
    fn num_arcs(&self) -> usize {
        self.0.num_arcs()
    }
}

impl<G: LinkIterable<Link>> LinkIterable<Link> for InfinityFilteringGraph<G> {
    type Iter<'a> = std::iter::Filter<<G as LinkIterable<Link>>::Iter<'a>, fn(&Link) -> bool>;

    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        self.0.link_iter(node).filter(|l| l.weight < INFINITY)
    }
}

impl<G: LinkIterable<Link>> LinkIterable<NodeId> for InfinityFilteringGraph<G> {
    type Iter<'a> = std::iter::Map<<Self as LinkIterable<Link>>::Iter<'a>, fn(Link) -> NodeId>;

    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        LinkIterable::<Link>::link_iter(self, node).map(|l| l.node)
    }
}
