//! Implementation of Customizable Contraction Hierarchies.

use super::*;
use crate::{
    datastr::{graph::first_out_graph::BorrowedGraph, node_order::NodeOrder},
    io::*,
    report::{benchmark::*, block_reporting},
    util::{in_range_option::InRangeOption, *},
};
use std::{cmp::Ordering, ops::Range};

mod contraction;
use contraction::*;
pub mod customization;
pub use customization::ftd as ftd_cch;
pub use customization::{customize, customize_directed, customize_perfect};
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
    forward_cch_edge_to_orig_arc: Vecs<EdgeIdT>,
    backward_cch_edge_to_orig_arc: Vecs<EdgeIdT>,
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
        let contracted = {
            let _blocked = block_reporting();
            ContractionGraph::new(graph, order.clone()).contract()
        };
        let order = reorder_for_seperator_based_customization(&order, SeparatorTree::new(&contracted.elimination_tree()));
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

        let order = &node_order;
        let forward_cch_edge_to_orig_arc = Vecs::from_iters((0..n).flat_map(|node| {
            LinkIterable::<NodeIdT>::link_iter(&contracted_graph, node)
                .map(move |NodeIdT(neighbor)| original_graph.edge_indices(order.node(node), order.node(neighbor)))
        }));
        let backward_cch_edge_to_orig_arc = Vecs::from_iters((0..n).flat_map(|node| {
            LinkIterable::<NodeIdT>::link_iter(&contracted_graph, node)
                .map(move |NodeIdT(neighbor)| original_graph.edge_indices(order.node(neighbor), order.node(node)))
        }));

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
            forward_cch_edge_to_orig_arc,
            backward_cch_edge_to_orig_arc,
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
            .map(|node_id| LinkIterable::<NodeIdT>::link_iter(graph, node_id as NodeId).map(|NodeIdT(n)| n).next())
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
    fn neighbor_edge_indices_usize(&self, node: NodeId) -> Range<usize> {
        let range = self.neighbor_edge_indices(node);
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
        let mut forward_tail = Vec::with_capacity(self.head.len());

        let mut backward_first_out = Vec::with_capacity(self.first_out.len());
        backward_first_out.push(0);
        let mut backward_head = Vec::with_capacity(self.head.len());
        let mut backward_tail = Vec::with_capacity(self.head.len());

        let forward_cch_edge_to_orig_arc = Vecs::from_iters(
            self.forward_cch_edge_to_orig_arc
                .iter()
                .zip(forward.weight().iter())
                .filter(|(_, w)| **w < INFINITY)
                .map(|(slc, _)| slc.iter().copied()),
        );
        let backward_cch_edge_to_orig_arc = Vecs::from_iters(
            self.backward_cch_edge_to_orig_arc
                .iter()
                .zip(backward.weight().iter())
                .filter(|(_, w)| **w < INFINITY)
                .map(|(slc, _)| slc.iter().copied()),
        );

        for node in 0..self.num_nodes() as NodeId {
            forward_head.extend(LinkIterable::<Link>::link_iter(&forward, node).filter(|l| l.weight < INFINITY).map(|l| l.node));
            backward_head.extend(LinkIterable::<Link>::link_iter(&backward, node).filter(|l| l.weight < INFINITY).map(|l| l.node));
            forward_tail.extend(LinkIterable::<Link>::link_iter(&forward, node).filter(|l| l.weight < INFINITY).map(|_| node));
            backward_tail.extend(LinkIterable::<Link>::link_iter(&backward, node).filter(|l| l.weight < INFINITY).map(|_| node));
            forward_first_out.push(forward_head.len() as EdgeId);
            backward_first_out.push(backward_head.len() as EdgeId);
        }

        let forward_inverted = ReversedGraphWithEdgeIds::reversed(&UnweightedFirstOutGraph::new(&forward_first_out[..], &forward_head[..]));
        let backward_inverted = ReversedGraphWithEdgeIds::reversed(&UnweightedFirstOutGraph::new(&backward_first_out[..], &backward_head[..]));

        DirectedCCH {
            forward_first_out,
            forward_head,
            forward_tail,
            backward_first_out,
            backward_head,
            backward_tail,
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
    fn forward_tail(&self) -> &[NodeId];
    fn backward_tail(&self) -> &[NodeId];
    fn forward_inverted(&self) -> &ReversedGraphWithEdgeIds;
    fn backward_inverted(&self) -> &ReversedGraphWithEdgeIds;

    /// Get elimination tree (actually forest).
    /// The tree is represented as a slice of length `n`.
    /// The entry with index `x` contains the parent node in the tree of node `x`.
    /// If there is no parent, `x` is a root node.
    fn elimination_tree(&self) -> &[InRangeOption<NodeId>];

    /// Borrow node order
    fn node_order(&self) -> &NodeOrder;

    /// Reconstruct the separators of the nested dissection order.
    fn separators(&self) -> SeparatorTree {
        SeparatorTree::new(self.elimination_tree())
    }
}

pub fn unpack_arc(
    from: NodeId,
    to: NodeId,
    weight: Weight,
    upward: &[Weight],
    downward: &[Weight],
    forward_inverted: &ReversedGraphWithEdgeIds,
    backward_inverted: &ReversedGraphWithEdgeIds,
) -> Option<(NodeId, Weight, Weight)> {
    // `inverted` contains the downward neighbors sorted ascending.
    // We do a coordinated linear sweep over both neighborhoods.
    // Whenever we find a common neighbor, we have a lower triangle.
    let mut current_iter = backward_inverted.link_iter(from).peekable();
    let mut other_iter = forward_inverted.link_iter(to).peekable();

    debug_assert_eq!(upward.len(), forward_inverted.num_arcs());
    debug_assert_eq!(downward.len(), backward_inverted.num_arcs());

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
    fn forward_tail(&self) -> &[NodeId] {
        &self.tail[..]
    }
    fn backward_tail(&self) -> &[NodeId] {
        &self.tail[..]
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

pub trait Customized {
    type CCH: CCHT;
    fn forward_graph(&self) -> BorrowedGraph;
    fn backward_graph(&self) -> BorrowedGraph;
    fn cch(&self) -> &Self::CCH;

    fn unpack_outgoing(&self, edge: EdgeIdT) -> Option<(EdgeIdT, EdgeIdT, NodeIdT)>;
    fn unpack_incoming(&self, edge: EdgeIdT) -> Option<(EdgeIdT, EdgeIdT, NodeIdT)>;

    fn forward_tail(&self) -> &[NodeId];
    fn backward_tail(&self) -> &[NodeId];
    fn forward_unpacking(&self) -> &[(InRangeOption<EdgeId>, InRangeOption<EdgeId>)];
    fn backward_unpacking(&self) -> &[(InRangeOption<EdgeId>, InRangeOption<EdgeId>)];
}

/// A struct containing the results of the second preprocessing phase.
pub struct CustomizedBasic<'a, CCH> {
    cch: &'a CCH,
    upward: Vec<Weight>,
    downward: Vec<Weight>,
    up_unpacking: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
    down_unpacking: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
}

impl<'a, C: CCHT> CustomizedBasic<'a, C> {
    fn new(
        cch: &'a C,
        upward: Vec<Weight>,
        downward: Vec<Weight>,
        up_unpacking: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
        down_unpacking: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
    ) -> Self {
        Self {
            cch,
            upward,
            downward,
            up_unpacking,
            down_unpacking,
        }
    }
}
impl<'a, C: CCHT> Customized for CustomizedBasic<'a, C> {
    type CCH = C;
    fn forward_graph(&self) -> BorrowedGraph {
        FirstOutGraph::new(self.cch.forward_first_out(), self.cch.forward_head(), &self.upward)
    }
    fn backward_graph(&self) -> BorrowedGraph {
        FirstOutGraph::new(self.cch.backward_first_out(), self.cch.backward_head(), &self.downward)
    }
    fn cch(&self) -> &C {
        self.cch
    }
    fn forward_tail(&self) -> &[NodeId] {
        self.cch().forward_tail()
    }
    fn backward_tail(&self) -> &[NodeId] {
        self.cch().backward_tail()
    }
    fn unpack_outgoing(&self, EdgeIdT(edge): EdgeIdT) -> Option<(EdgeIdT, EdgeIdT, NodeIdT)> {
        let (down, up) = self.up_unpacking[edge as usize];
        down.value()
            .map(|down| (EdgeIdT(down), EdgeIdT(up.value().unwrap()), NodeIdT(self.backward_tail()[down as usize])))
    }
    fn unpack_incoming(&self, EdgeIdT(edge): EdgeIdT) -> Option<(EdgeIdT, EdgeIdT, NodeIdT)> {
        let (down, up) = self.down_unpacking[edge as usize];
        down.value()
            .map(|down| (EdgeIdT(down), EdgeIdT(up.value().unwrap()), NodeIdT(self.backward_tail()[down as usize])))
    }
    fn forward_unpacking(&self) -> &[(InRangeOption<EdgeId>, InRangeOption<EdgeId>)] {
        &self.up_unpacking
    }
    fn backward_unpacking(&self) -> &[(InRangeOption<EdgeId>, InRangeOption<EdgeId>)] {
        &self.down_unpacking
    }
}

pub struct CustomizedPerfect<'a, CCH> {
    cch: &'a CCH,
    upward: OwnedGraph,
    downward: OwnedGraph,
    up_unpacking: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
    down_unpacking: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
    forward_tail: Vec<NodeId>,
    backward_tail: Vec<NodeId>,
}

impl<'a, C: CCHT> CustomizedPerfect<'a, C> {
    fn new(
        cch: &'a C,
        upward: OwnedGraph,
        downward: OwnedGraph,
        up_unpacking: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
        down_unpacking: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
        forward_tail: Vec<NodeId>,
        backward_tail: Vec<NodeId>,
    ) -> Self {
        Self {
            cch,
            upward,
            downward,
            up_unpacking,
            down_unpacking,
            forward_tail,
            backward_tail,
        }
    }
}
impl<'a, C: CCHT> Customized for CustomizedPerfect<'a, C> {
    type CCH = C;
    fn forward_graph(&self) -> BorrowedGraph {
        self.upward.borrowed()
    }
    fn backward_graph(&self) -> BorrowedGraph {
        self.downward.borrowed()
    }
    fn cch(&self) -> &C {
        self.cch
    }
    fn forward_tail(&self) -> &[NodeId] {
        &self.forward_tail
    }
    fn backward_tail(&self) -> &[NodeId] {
        &self.backward_tail
    }
    fn unpack_outgoing(&self, EdgeIdT(edge): EdgeIdT) -> Option<(EdgeIdT, EdgeIdT, NodeIdT)> {
        let (down, up) = self.up_unpacking[edge as usize];
        down.value()
            .map(|down| (EdgeIdT(down), EdgeIdT(up.value().unwrap()), NodeIdT(self.backward_tail()[down as usize])))
    }
    fn unpack_incoming(&self, EdgeIdT(edge): EdgeIdT) -> Option<(EdgeIdT, EdgeIdT, NodeIdT)> {
        let (down, up) = self.down_unpacking[edge as usize];
        down.value()
            .map(|down| (EdgeIdT(down), EdgeIdT(up.value().unwrap()), NodeIdT(self.backward_tail()[down as usize])))
    }
    fn forward_unpacking(&self) -> &[(InRangeOption<EdgeId>, InRangeOption<EdgeId>)] {
        &self.up_unpacking
    }
    fn backward_unpacking(&self) -> &[(InRangeOption<EdgeId>, InRangeOption<EdgeId>)] {
        &self.down_unpacking
    }
}

pub struct DirectedCCH {
    forward_first_out: Vec<EdgeId>,
    forward_head: Vec<NodeId>,
    forward_tail: Vec<NodeId>,
    backward_first_out: Vec<EdgeId>,
    backward_head: Vec<NodeId>,
    backward_tail: Vec<NodeId>,
    node_order: NodeOrder,
    forward_cch_edge_to_orig_arc: Vecs<EdgeIdT>,
    backward_cch_edge_to_orig_arc: Vecs<EdgeIdT>,
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
    fn forward_tail(&self) -> &[NodeId] {
        &self.forward_tail[..]
    }
    fn backward_tail(&self) -> &[NodeId] {
        &self.backward_tail[..]
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
