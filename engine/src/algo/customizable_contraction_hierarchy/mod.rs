//! Implementation of Customizable Contraction Hierarchies.

use super::*;
use crate::{
    datastr::{graph::first_out_graph::degrees_to_first_out, node_order::NodeOrder},
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
mod separator_decomposition;
use separator_decomposition::*;
mod reorder;
pub use reorder::*;
pub mod query;

/// Execute first phase, that is metric independent preprocessing.
pub fn contract<Graph: for<'a> LinkIterable<'a, NodeId> + RandomLinkAccessGraph>(graph: &Graph, node_order: NodeOrder) -> CCH {
    CCH::new(ContractionGraph::new(graph, node_order).contract())
}

/// A struct containing all metric independent preprocessing data of CCHs.
/// This includes on top of the chordal supergraph (the "contracted" graph),
/// several other structures like the elimination tree, a mapping from cch edge ids to original edge ids and the inverted graph.
#[derive(Debug)]
pub struct CCH {
    first_out: Vec<EdgeId>,
    head: Vec<NodeId>,
    tail: Vec<NodeId>,
    node_order: NodeOrder,
    cch_edge_to_orig_arc: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
    elimination_tree: Vec<InRangeOption<NodeId>>,
    inverted: OwnedGraph,
}

impl Deconstruct for CCH {
    fn store_each(&self, store: &dyn Fn(&str, &dyn Store) -> std::io::Result<()>) -> std::io::Result<()> {
        store("cch_first_out", &self.first_out)?;
        store("cch_head", &self.head)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct CCHReconstrctor<'g, Graph> {
    pub original_graph: &'g Graph,
    pub node_order: NodeOrder,
}

impl<'g, Graph: RandomLinkAccessGraph> ReconstructPrepared<CCH> for CCHReconstrctor<'g, Graph> {
    fn reconstruct_with(self, loader: Loader) -> std::io::Result<CCH> {
        let head: Vec<NodeId> = loader.load("cch_head")?;
        let m = head.len();
        let cch_graph = OwnedGraph::new(loader.load("cch_first_out")?, head, vec![INFINITY; m]);
        assert_eq!(cch_graph.num_nodes(), self.original_graph.num_nodes());
        Ok(CCH::new_from(self.original_graph, self.node_order, cch_graph))
    }
}

impl CCH {
    fn new<Graph: RandomLinkAccessGraph>(contracted_graph: ContractedGraph<Graph>) -> CCH {
        let (cch, order, orig) = contracted_graph.decompose();
        Self::new_from(orig, order, cch)
    }

    // this method creates all the other structures from the contracted graph
    fn new_from<Graph: RandomLinkAccessGraph>(original_graph: &Graph, node_order: NodeOrder, contracted_graph: OwnedGraph) -> Self {
        let elimination_tree = Self::build_elimination_tree(&contracted_graph);
        let n = contracted_graph.num_nodes() as NodeId;
        let m = contracted_graph.num_arcs();
        let mut tail = vec![0; m];

        let cch_edge_to_orig_arc = (0..n)
            .flat_map(|node| {
                let node_order = &node_order;
                LinkIterable::<Link>::link_iter(&contracted_graph, node).map(move |Link { node: neighbor, .. }| {
                    (
                        InRangeOption::new(original_graph.edge_index(node_order.node(node), node_order.node(neighbor))),
                        InRangeOption::new(original_graph.edge_index(node_order.node(neighbor), node_order.node(node))),
                    )
                })
            })
            .collect();

        for node in 0..n {
            tail[contracted_graph.neighbor_edge_indices_usize(node)]
                .iter_mut()
                .for_each(|tail| *tail = node);
        }

        let inverted = inverted_with_orig_edge_ids_as_weights(&contracted_graph);
        let (first_out, head, _) = contracted_graph.decompose();

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

    fn build_elimination_tree(graph: &OwnedGraph) -> Vec<InRangeOption<NodeId>> {
        (0..graph.num_nodes())
            .map(|node_id| LinkIterable::<NodeId>::link_iter(graph, node_id as NodeId).min())
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
        (self.first_out[node as usize] as EdgeId)..(self.first_out[(node + 1) as usize] as EdgeId)
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
    fn neighbor_iter(&self, node: NodeId) -> std::iter::Cloned<std::slice::Iter<NodeId>> {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().cloned()
    }

    /// Transform into a directed CCH which is more efficient
    /// for turn expanded graphs because many edges can be removed.
    pub fn into_directed_cch(self) -> DirectedCCH {
        // identify arcs which are always infinity and can be removed
        let (forward, backward) = customization::always_infinity(&self).into_ch_graphs();

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
            for (link, &(forward_orig_arc, _)) in LinkIterable::<Link>::link_iter(&forward, node).zip(orig_arcs.iter()) {
                if link.weight < INFINITY {
                    forward_head.push(link.node);
                    forward_cch_edge_to_orig_arc.push(forward_orig_arc);
                    forward_edge_counter += 1;
                }
            }
            for (link, &(_, backward_orig_arc)) in LinkIterable::<Link>::link_iter(&backward, node).zip(orig_arcs.iter()) {
                if link.weight < INFINITY {
                    backward_head.push(link.node);
                    backward_cch_edge_to_orig_arc.push(backward_orig_arc);
                    backward_edge_counter += 1;
                }
            }
            forward_first_out.push(forward_edge_counter);
            backward_first_out.push(backward_edge_counter);
        }

        let mut forward_tail = vec![0; forward_head.len()];
        let mut backward_tail = vec![0; backward_head.len()];

        for node in 0..(self.num_nodes() as NodeId) {
            SlcsMut::new(&forward_first_out, &mut forward_tail)[node as usize]
                .iter_mut()
                .for_each(|tail| *tail = node);
            SlcsMut::new(&backward_first_out, &mut backward_tail)[node as usize]
                .iter_mut()
                .for_each(|tail| *tail = node);
        }

        let forward_inverted = inverted_with_orig_edge_ids_as_weights(&FirstOutGraph::new(&forward_first_out[..], &forward_head[..], &forward_head[..]));
        let backward_inverted = inverted_with_orig_edge_ids_as_weights(&FirstOutGraph::new(&backward_first_out[..], &backward_head[..], &backward_head[..]));

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

fn inverted_with_orig_edge_ids_as_weights<'a>(graph: &'a (impl RandomLinkAccessGraph + LinkIterGraph<'a>)) -> OwnedGraph {
    let mut inverted = vec![Vec::new(); graph.num_nodes()];
    for current_node in 0..(graph.num_nodes() as NodeId) {
        for (Link { node, .. }, edge_id) in graph.link_iter(current_node).zip(graph.neighbor_edge_indices(current_node)) {
            // the heads of inverted will be sorted ascending for each node, because current node goes from 0 to n
            inverted[node as usize].push((current_node, edge_id));
        }
    }

    let down_first_out: Vec<EdgeId> = {
        let degrees = inverted.iter().map(|neighbors| neighbors.len() as EdgeId);
        degrees_to_first_out(degrees).collect()
    };
    debug_assert_eq!(down_first_out.len(), graph.num_nodes() as usize + 1);

    let (down_head, down_up_edge_ids): (Vec<_>, Vec<_>) = inverted.into_iter().flat_map(|neighbors| neighbors.into_iter()).unzip();

    OwnedGraph::new(down_first_out, down_head, down_up_edge_ids)
}

/// Trait for directed and undirected CCHs
pub trait CCHT {
    fn forward_first_out(&self) -> &[EdgeId];
    fn backward_first_out(&self) -> &[EdgeId];
    fn forward_head(&self) -> &[NodeId];
    fn backward_head(&self) -> &[NodeId];
    fn forward_inverted(&self) -> &OwnedGraph;
    fn backward_inverted(&self) -> &OwnedGraph;

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
        let mut current_iter = LinkIterable::<Link>::link_iter(self.backward_inverted(), from).peekable();
        let mut other_iter = LinkIterable::<Link>::link_iter(self.forward_inverted(), to).peekable();

        while let (
            Some(&Link {
                node: lower_from_first,
                weight: edge_from_first_id,
            }),
            Some(&Link {
                node: lower_from_second,
                weight: edge_from_second_id,
            }),
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
    fn forward_inverted(&self) -> &OwnedGraph {
        &self.inverted
    }
    fn backward_inverted(&self) -> &OwnedGraph {
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
    /// Decompose into an upward and a downward graph which could be used for a CH query.
    #[allow(clippy::type_complexity)]
    pub fn into_ch_graphs(
        self,
    ) -> (
        FirstOutGraph<&'c [EdgeId], &'c [NodeId], Vec<Weight>>,
        FirstOutGraph<&'c [EdgeId], &'c [NodeId], Vec<Weight>>,
    ) {
        (
            FirstOutGraph::new(self.cch.forward_first_out(), self.cch.forward_head(), self.upward),
            FirstOutGraph::new(self.cch.backward_first_out(), &self.cch.backward_head(), self.downward),
        )
    }
}

#[derive(Debug)]
pub struct DirectedCCH {
    forward_first_out: Vec<EdgeId>,
    forward_head: Vec<NodeId>,
    forward_tail: Vec<NodeId>,
    backward_first_out: Vec<EdgeId>,
    backward_head: Vec<NodeId>,
    backward_tail: Vec<NodeId>,
    node_order: NodeOrder,
    forward_cch_edge_to_orig_arc: Vec<InRangeOption<EdgeId>>,
    backward_cch_edge_to_orig_arc: Vec<InRangeOption<EdgeId>>,
    elimination_tree: Vec<InRangeOption<NodeId>>,
    forward_inverted: OwnedGraph,
    backward_inverted: OwnedGraph,
}

impl DirectedCCH {
    fn num_nodes(&self) -> usize {
        self.forward_first_out.len() - 1
    }

    fn forward(&self) -> Slcs<EdgeId, NodeId> {
        Slcs::new(&self.forward_first_out, &self.forward_head)
    }

    fn backward(&self) -> Slcs<EdgeId, NodeId> {
        Slcs::new(&self.forward_first_out, &self.forward_head)
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
    fn forward_inverted(&self) -> &OwnedGraph {
        &self.forward_inverted
    }
    fn backward_inverted(&self) -> &OwnedGraph {
        &self.backward_inverted
    }

    fn node_order(&self) -> &NodeOrder {
        &self.node_order
    }

    fn elimination_tree(&self) -> &[InRangeOption<NodeId>] {
        &self.elimination_tree[..]
    }
}
