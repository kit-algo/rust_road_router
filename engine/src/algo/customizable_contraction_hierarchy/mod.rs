//! Implementation of Customizable Contraction Hierarchies.

use super::*;
use crate::{
    datastr::{graph::first_out_graph::degrees_to_first_out, node_order::NodeOrder},
    io::*,
    report::benchmark::*,
    util::in_range_option::InRangeOption,
};
use std::{cmp::Ordering, ops::Range};

mod contraction;
use contraction::*;
mod customization;
pub use customization::customize;
pub use customization::ftd as ftd_cch;
mod separator_decomposition;
use separator_decomposition::*;
mod reorder;
pub use reorder::*;
pub mod query;

/// Execute first phase, that is metric independent preprocessing.
pub fn contract<Graph: for<'a> LinkIterGraph<'a> + RandomLinkAccessGraph>(graph: &Graph, node_order: NodeOrder) -> CCH {
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
pub struct CCHReconstrctor<'g, Graph: for<'a> LinkIterGraph<'a>> {
    pub original_graph: &'g Graph,
    pub node_order: NodeOrder,
}

impl<'g, Graph: for<'a> LinkIterGraph<'a> + RandomLinkAccessGraph> ReconstructPrepared<CCH> for CCHReconstrctor<'g, Graph> {
    fn reconstruct_with(self, loader: Loader) -> std::io::Result<CCH> {
        let head: Vec<NodeId> = loader.load("cch_head")?;
        let m = head.len();
        let cch_graph = OwnedGraph::new(loader.load("cch_first_out")?, head, vec![INFINITY; m]);
        assert_eq!(cch_graph.num_nodes(), self.original_graph.num_nodes());
        Ok(CCH::new_from(self.original_graph, self.node_order, cch_graph))
    }
}

impl CCH {
    fn new<Graph: for<'a> LinkIterGraph<'a> + RandomLinkAccessGraph>(contracted_graph: ContractedGraph<Graph>) -> CCH {
        let (cch, order, orig) = contracted_graph.decompose();
        Self::new_from(orig, order, cch)
    }

    // this method creates all the other structures from the contracted graph
    fn new_from<Graph: for<'a> LinkIterGraph<'a> + RandomLinkAccessGraph>(original_graph: &Graph, node_order: NodeOrder, contracted_graph: OwnedGraph) -> Self {
        let elimination_tree = Self::build_elimination_tree(&contracted_graph);
        let n = contracted_graph.num_nodes() as NodeId;
        let m = contracted_graph.num_arcs();
        let mut tail = vec![0; m];

        let cch_edge_to_orig_arc = (0..n)
            .flat_map(|node| {
                let node_order = &node_order;
                contracted_graph.neighbor_iter(node).map(move |Link { node: neighbor, .. }| {
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

        let mut inverted = vec![Vec::new(); n as usize];
        for current_node in 0..n {
            for (Link { node, .. }, edge_id) in contracted_graph
                .neighbor_iter(current_node)
                .zip(contracted_graph.neighbor_edge_indices(current_node))
            {
                // the heads of inverted will be sorted ascending for each node, because current node goes from 0 to n
                inverted[node as usize].push((current_node, edge_id));
            }
        }

        let down_first_out: Vec<EdgeId> = {
            let degrees = inverted.iter().map(|neighbors| neighbors.len() as EdgeId);
            degrees_to_first_out(degrees).collect()
        };
        debug_assert_eq!(down_first_out.len(), n as usize + 1);

        let (down_head, down_up_edge_ids): (Vec<_>, Vec<_>) = inverted.into_iter().flat_map(|neighbors| neighbors.into_iter()).unzip();

        let inverted = OwnedGraph::new(down_first_out, down_head, down_up_edge_ids);
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
        SeparatorTree::new(&self)
    }

    fn build_elimination_tree(graph: &OwnedGraph) -> Vec<InRangeOption<NodeId>> {
        (0..graph.num_nodes())
            .map(|node_id| graph.neighbor_iter(node_id as NodeId).map(|l| l.node).min())
            .map(InRangeOption::new)
            .collect()
    }

    /// Borrow node order
    pub fn node_order(&self) -> &NodeOrder {
        &self.node_order
    }

    /// Get elimination tree (actually forest).
    /// The tree is represented as a slice of length `n`.
    /// The entry with index `x` contains the parent node in the tree of node `x`.
    /// If there is no parent, `x` is a root node.
    pub fn elimination_tree(&self) -> &[InRangeOption<NodeId>] {
        &self.elimination_tree[..]
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

    /// Get number of upward going edges in chordal supergraph for given node.
    pub fn degree(&self, node: NodeId) -> usize {
        let range = self.neighbor_edge_indices_usize(node);
        range.end - range.start
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

    /// Check for a node pair and a weight if there is a corresponding lower triangle.
    /// If so, return the id of the middle node and the weights of both lower edges.
    pub fn unpack_arc(&self, from: NodeId, to: NodeId, weight: Weight, upward: &[Weight], downward: &[Weight]) -> Option<(NodeId, Weight, Weight)> {
        // `inverted` contains the downward neighbors sorted ascending.
        // We do a coordinated linear sweep over both neighborhoods.
        // Whenever we find a common neighbor, we have a lower triangle.
        let mut current_iter = self.inverted.neighbor_iter(from).peekable();
        let mut other_iter = self.inverted.neighbor_iter(to).peekable();

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

impl Graph for CCH {
    fn num_arcs(&self) -> usize {
        self.head.len()
    }

    fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }
}

/// A struct containing the results of the second preprocessing phase.
#[derive(Debug)]
pub struct Customized<'c> {
    cch: &'c CCH,
    upward: Vec<Weight>,
    downward: Vec<Weight>,
}

impl<'c> Customized<'c> {
    /// Decompose into an upward and a downward graph which could be used for a CH query.
    #[allow(clippy::type_complexity)]
    pub fn into_ch_graphs(
        self,
    ) -> (
        FirstOutGraph<&'c [EdgeId], &'c [NodeId], Vec<Weight>>,
        FirstOutGraph<&'c [EdgeId], &'c [NodeId], Vec<Weight>>,
    ) {
        (
            FirstOutGraph::new(&self.cch.first_out[..], &self.cch.head[..], self.upward),
            FirstOutGraph::new(&self.cch.first_out[..], &self.cch.head[..], self.downward),
        )
    }
}
