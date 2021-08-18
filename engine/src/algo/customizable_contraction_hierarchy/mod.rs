//! Implementation of Customizable Contraction Hierarchies.

use super::*;
use crate::{
    datastr::node_order::NodeOrder,
    io::*,
    report::benchmark::*,
    util::{in_range_option::InRangeOption, *},
};
use std::{cmp::Ordering, ops::Range};

mod contraction;
use contraction::*;
mod customization;
pub use customization::ftd as ftd_cch;
pub use customization::{customize, customize_directed};
pub mod separator_decomposition;
use separator_decomposition::*;
mod reorder;
pub use reorder::*;
pub mod query;

/// Execute first phase, that is metric independent preprocessing.
pub fn contract<Graph: LinkIterable<NodeIdT> + EdgeIdGraph>(graph: &Graph, node_order: NodeOrder) -> CCH {
    CCH::new(ContractionGraph::new(graph, node_order).contract())
}

/// A struct containing all metric independent preprocessing data of CCHs.
/// This includes on top of the chordal supergraph (the "contracted" graph),
/// several other structures like the elimination tree, a mapping from cch edge ids to original edge ids and the inverted graph.
pub struct CCH {
    first_out: Vec<EdgeId>,
    head: Vec<NodeId>,
    tail: Vec<NodeId>,
    node_order: NodeOrder,
    cch_edge_to_orig_arc: Vec<(Vec<EdgeIdT>, Vec<EdgeIdT>)>,
    elimination_tree: Vec<InRangeOption<NodeId>>,
    inverted: ReversedGraphWithEdgeIds,
}

impl Deconstruct for CCH {
    fn store_each(&self, store: &dyn Fn(&str, &dyn Store) -> std::io::Result<()>) -> std::io::Result<()> {
        store("cch_first_out", &self.first_out)?;
        store("cch_head", &self.head)?;
        Ok(())
    }
}

pub struct CCHReconstrctor<'g, Graph>(pub &'g Graph);

impl<'g, Graph: EdgeIdGraph> ReconstructPrepared<CCH> for CCHReconstrctor<'g, Graph> {
    fn reconstruct_with(self, loader: Loader) -> std::io::Result<CCH> {
        let node_order = NodeOrder::reconstruct_from(&loader.path())?;
        let head: Vec<NodeId> = loader.load("cch_head")?;
        let cch_graph = UnweightedOwnedGraph::new(loader.load("cch_first_out")?, head);
        assert_eq!(cch_graph.num_nodes(), self.0.num_nodes());
        Ok(CCH::new_from(self.0, node_order, cch_graph))
    }
}

impl CCH {
    pub fn fix_order_and_build(graph: &(impl LinkIterable<NodeIdT> + EdgeIdGraph), order: NodeOrder) -> Self {
        let cch = contract(graph, order);
        let order = CCHReordering {
            cch: &cch,
            latitude: &[],
            longitude: &[],
        }
        .reorder_for_seperator_based_customization();
        contract(graph, order)
    }

    fn new<Graph: EdgeIdGraph>(contracted_graph: ContractedGraph<Graph>) -> CCH {
        let (cch, order, orig) = contracted_graph.decompose();
        Self::new_from(orig, order, cch)
    }

    // this method creates all the other structures from the contracted graph
    fn new_from<Graph: EdgeIdGraph>(original_graph: &Graph, node_order: NodeOrder, contracted_graph: UnweightedOwnedGraph) -> Self {
        let elimination_tree = Self::build_elimination_tree(&contracted_graph);
        let n = contracted_graph.num_nodes() as NodeId;
        let m = contracted_graph.num_arcs();
        let mut tail = vec![0; m];

        let cch_edge_to_orig_arc = (0..n)
            .flat_map(|node| {
                let node_order = &node_order;
                LinkIterable::<NodeIdT>::link_iter(&contracted_graph, node).map(move |NodeIdT(neighbor)| {
                    (
                        original_graph.edge_indices(node_order.node(node), node_order.node(neighbor)).collect(),
                        original_graph.edge_indices(node_order.node(neighbor), node_order.node(node)).collect(),
                    )
                })
            })
            .collect();

        for node in 0..n {
            tail[contracted_graph.neighbor_edge_indices_usize(node)]
                .iter_mut()
                .for_each(|tail| *tail = node);
        }

        let inverted = ReversedGraphWithEdgeIds::reversed(&contracted_graph);
        let (first_out, head) = contracted_graph.decompose();

        CCH {
            first_out,
            head,
            node_order,
            cch_edge_to_orig_arc,
            elimination_tree,
            tail,
            inverted,
        }
    }

    /// Reconstruct the separators of the nested dissection order.
    pub fn separators(&self) -> SeparatorTree {
        SeparatorTree::new(&self.elimination_tree)
    }

    fn build_elimination_tree(graph: &UnweightedOwnedGraph) -> Vec<InRangeOption<NodeId>> {
        (0..graph.num_nodes())
            .map(|node_id| LinkIterable::<NodeIdT>::link_iter(graph, node_id as NodeId).map(|NodeIdT(n)| n).min())
            .map(InRangeOption::new)
            .collect()
    }

    /// Get the tail node for an edge id
    pub fn edge_id_to_tail(&self, edge_id: EdgeId) -> NodeId {
        self.tail[edge_id as usize]
    }

    /// Get chordal supergraph `first_out` as slice
    pub fn first_out(&self) -> &[EdgeId] {
        &self.first_out
    }

    /// Get chordal supergraph `head` as slice
    pub fn head(&self) -> &[NodeId] {
        &self.head
    }

    #[inline]
    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        (self.first_out[node as usize])..(self.first_out[(node + 1) as usize])
    }

    #[inline]
    fn edge_indices_range(&self, nodes: Range<NodeId>) -> Range<EdgeId> {
        (self.first_out[nodes.start as usize] as EdgeId)..(self.first_out[nodes.end as usize] as EdgeId)
    }

    #[inline]
    fn neighbor_edge_indices_usize(&self, node: NodeId) -> Range<usize> {
        let range = self.neighbor_edge_indices(node);
        Range {
            start: range.start as usize,
            end: range.end as usize,
        }
    }

    #[inline]
    fn edge_indices_range_usize(&self, nodes: Range<NodeId>) -> Range<usize> {
        let range = self.edge_indices_range(nodes);
        Range {
            start: range.start as usize,
            end: range.end as usize,
        }
    }

    #[inline]
    pub fn neighbor_iter(&self, node: NodeId) -> std::iter::Cloned<std::slice::Iter<NodeId>> {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().cloned()
    }

    /// Transform into a directed CCH which is more efficient
    /// for turn expanded graphs because many edges can be removed.
    pub fn into_directed_cch(self) -> DirectedCCH {
        // identify arcs which are always infinity and can be removed
        let customized = customization::always_infinity(&self);
        let forward = customized.forward_graph();
        let backward = customized.backward_graph();

        let mut forward_first_out = Vec::with_capacity(self.first_out.len());
        forward_first_out.push(0);
        let mut forward_head = Vec::with_capacity(self.head.len());
        let mut forward_cch_edge_to_orig_arc = Vec::with_capacity(self.head.len());

        let mut backward_first_out = Vec::with_capacity(self.first_out.len());
        backward_first_out.push(0);
        let mut backward_head = Vec::with_capacity(self.head.len());
        let mut backward_cch_edge_to_orig_arc = Vec::with_capacity(self.head.len());

        let mut forward_edge_counter = 0;
        let mut backward_edge_counter = 0;

        for node in 0..self.num_nodes() as NodeId {
            let orig_arcs = &self.cch_edge_to_orig_arc[self.neighbor_edge_indices_usize(node)];
            for (link, (forward_orig_arcs, _)) in LinkIterable::<Link>::link_iter(&forward, node).zip(orig_arcs.iter()) {
                if link.weight < INFINITY {
                    forward_head.push(link.node);
                    forward_cch_edge_to_orig_arc.push(forward_orig_arcs.clone());
                    forward_edge_counter += 1;
                }
            }
            for (link, (_, backward_orig_arcs)) in LinkIterable::<Link>::link_iter(&backward, node).zip(orig_arcs.iter()) {
                if link.weight < INFINITY {
                    backward_head.push(link.node);
                    backward_cch_edge_to_orig_arc.push(backward_orig_arcs.clone());
                    backward_edge_counter += 1;
                }
            }
            forward_first_out.push(forward_edge_counter);
            backward_first_out.push(backward_edge_counter);
        }

        let forward_inverted = ReversedGraphWithEdgeIds::reversed(&UnweightedFirstOutGraph::new(&forward_first_out[..], &forward_head[..]));
        let backward_inverted = ReversedGraphWithEdgeIds::reversed(&UnweightedFirstOutGraph::new(&backward_first_out[..], &backward_head[..]));

        DirectedCCH {
            forward_first_out,
            forward_head,
            backward_first_out,
            backward_head,
            node_order: self.node_order,
            forward_cch_edge_to_orig_arc,
            backward_cch_edge_to_orig_arc,
            elimination_tree: self.elimination_tree,
            forward_inverted,
            backward_inverted,
        }
    }
}

impl Graph for CCH {
    fn num_arcs(&self) -> usize {
        self.head.len()
    }

    fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }

    fn degree(&self, node: NodeId) -> usize {
        let node = node as usize;
        (self.first_out[node + 1] - self.first_out[node]) as usize
    }
}

/// Trait for directed and undirected CCHs
pub trait CCHT {
    fn forward_first_out(&self) -> &[EdgeId];
    fn backward_first_out(&self) -> &[EdgeId];
    fn forward_head(&self) -> &[NodeId];
    fn backward_head(&self) -> &[NodeId];
    fn forward_inverted(&self) -> &ReversedGraphWithEdgeIds;
    fn backward_inverted(&self) -> &ReversedGraphWithEdgeIds;

    /// Get elimination tree (actually forest).
    /// The tree is represented as a slice of length `n`.
    /// The entry with index `x` contains the parent node in the tree of node `x`.
    /// If there is no parent, `x` is a root node.
    fn elimination_tree(&self) -> &[InRangeOption<NodeId>];

    /// Borrow node order
    fn node_order(&self) -> &NodeOrder;

    /// Check for a node pair and a weight if there is a corresponding lower triangle.
    /// If so, return the id of the middle node and the weights of both lower edges.
    fn unpack_arc(&self, from: NodeId, to: NodeId, weight: Weight, upward: &[Weight], downward: &[Weight]) -> Option<(NodeId, Weight, Weight)> {
        // `inverted` contains the downward neighbors sorted ascending.
        // We do a coordinated linear sweep over both neighborhoods.
        // Whenever we find a common neighbor, we have a lower triangle.
        let mut current_iter = self.backward_inverted().link_iter(from).peekable();
        let mut other_iter = self.forward_inverted().link_iter(to).peekable();

        while let (
            Some(&(NodeIdT(lower_from_first), Reversed(EdgeIdT(edge_from_first_id)))),
            Some(&(NodeIdT(lower_from_second), Reversed(EdgeIdT(edge_from_second_id)))),
        ) = (current_iter.peek(), other_iter.peek())
        {
            match lower_from_first.cmp(&lower_from_second) {
                Ordering::Less => current_iter.next(),
                Ordering::Greater => other_iter.next(),
                Ordering::Equal => {
                    if downward[edge_from_first_id as usize] + upward[edge_from_second_id as usize] == weight {
                        return Some((lower_from_first, downward[edge_from_first_id as usize], upward[edge_from_second_id as usize]));
                    }

                    current_iter.next();
                    other_iter.next()
                }
            };
        }

        None
    }
}

/// A struct containing all metric independent preprocessing data of CCHs.
/// This includes on top of the chordal supergraph (the "contracted" graph),
/// several other structures like the elimination tree, a mapping from cch edge ids to original edge ids and the inverted graph.
impl CCHT for CCH {
    fn forward_first_out(&self) -> &[EdgeId] {
        &self.first_out[..]
    }
    fn backward_first_out(&self) -> &[EdgeId] {
        &self.first_out[..]
    }
    fn forward_head(&self) -> &[NodeId] {
        &self.head[..]
    }
    fn backward_head(&self) -> &[NodeId] {
        &self.head[..]
    }
    fn forward_inverted(&self) -> &ReversedGraphWithEdgeIds {
        &self.inverted
    }
    fn backward_inverted(&self) -> &ReversedGraphWithEdgeIds {
        &self.inverted
    }

    fn node_order(&self) -> &NodeOrder {
        &self.node_order
    }

    fn elimination_tree(&self) -> &[InRangeOption<NodeId>] {
        &self.elimination_tree[..]
    }
}

/// A struct containing the results of the second preprocessing phase.
#[derive(Debug)]
pub struct Customized<'c, CCH> {
    cch: &'c CCH,
    upward: Vec<Weight>,
    downward: Vec<Weight>,
}

impl<'c, CCH: CCHT> Customized<'c, CCH> {
    pub fn forward_graph(&self) -> FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]> {
        FirstOutGraph::new(self.cch.forward_first_out(), self.cch.forward_head(), &self.upward)
    }

    pub fn backward_graph(&self) -> FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]> {
        FirstOutGraph::new(self.cch.backward_first_out(), self.cch.backward_head(), &self.downward)
    }

    pub fn cch(&self) -> &'c CCH {
        self.cch
    }
}

pub struct DirectedCCH {
    forward_first_out: Vec<EdgeId>,
    forward_head: Vec<NodeId>,
    backward_first_out: Vec<EdgeId>,
    backward_head: Vec<NodeId>,
    node_order: NodeOrder,
    forward_cch_edge_to_orig_arc: Vec<Vec<EdgeIdT>>,
    backward_cch_edge_to_orig_arc: Vec<Vec<EdgeIdT>>,
    elimination_tree: Vec<InRangeOption<NodeId>>,
    forward_inverted: ReversedGraphWithEdgeIds,
    backward_inverted: ReversedGraphWithEdgeIds,
}

impl DirectedCCH {
    fn num_nodes(&self) -> usize {
        self.forward_first_out.len() - 1
    }

    fn forward(&self) -> Slcs<EdgeId, NodeId> {
        Slcs(&self.forward_first_out, &self.forward_head)
    }

    fn backward(&self) -> Slcs<EdgeId, NodeId> {
        Slcs(&self.forward_first_out, &self.forward_head)
    }

    /// Reconstruct the separators of the nested dissection order.
    pub fn separators(&self) -> SeparatorTree {
        SeparatorTree::new(&self.elimination_tree)
    }
}

impl CCHT for DirectedCCH {
    fn forward_first_out(&self) -> &[EdgeId] {
        &self.forward_first_out[..]
    }
    fn backward_first_out(&self) -> &[EdgeId] {
        &self.backward_first_out[..]
    }
    fn forward_head(&self) -> &[NodeId] {
        &self.forward_head[..]
    }
    fn backward_head(&self) -> &[NodeId] {
        &self.backward_head[..]
    }
    fn forward_inverted(&self) -> &ReversedGraphWithEdgeIds {
        &self.forward_inverted
    }
    fn backward_inverted(&self) -> &ReversedGraphWithEdgeIds {
        &self.backward_inverted
    }

    fn node_order(&self) -> &NodeOrder {
        &self.node_order
    }

    fn elimination_tree(&self) -> &[InRangeOption<NodeId>] {
        &self.elimination_tree[..]
    }
}
