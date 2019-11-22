use super::*;
use crate::{benchmark::*, datastr::node_order::NodeOrder, graph::first_out_graph::degrees_to_first_out, in_range_option::InRangeOption, io::*};
use std::ops::Range;

mod contraction;
use contraction::*;
mod customization;
pub use customization::customize;
pub use customization::ftd as ftd_cch;
mod separator_decomposition;
use separator_decomposition::*;
mod reorder;
pub use reorder::*;

pub fn contract<Graph: for<'a> LinkIterGraph<'a> + RandomLinkAccessGraph>(graph: &Graph, node_order: NodeOrder) -> CCH {
    CCH::new(ContractionGraph::new(graph, node_order).contract())
}

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
    pub(super) fn new<Graph: for<'a> LinkIterGraph<'a> + RandomLinkAccessGraph>(contracted_graph: ContractedGraph<Graph>) -> CCH {
        let (cch, order, orig) = contracted_graph.decompose();
        Self::new_from(orig, order, cch)
    }

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

    pub fn separators(&self) -> SeparatorTree {
        SeparatorTree::new(&self)
    }

    fn build_elimination_tree(graph: &OwnedGraph) -> Vec<InRangeOption<NodeId>> {
        (0..graph.num_nodes())
            .map(|node_id| graph.neighbor_iter(node_id as NodeId).map(|l| l.node).min())
            .map(InRangeOption::new)
            .collect()
    }

    pub fn node_order(&self) -> &NodeOrder {
        &self.node_order
    }

    pub fn elimination_tree(&self) -> &[InRangeOption<NodeId>] {
        &self.elimination_tree[..]
    }

    pub fn edge_id_to_tail(&self, edge_id: EdgeId) -> NodeId {
        self.tail[edge_id as usize]
    }

    pub fn first_out(&self) -> &[EdgeId] {
        &self.first_out
    }

    pub fn head(&self) -> &[NodeId] {
        &self.head
    }

    #[inline]
    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        (self.first_out[node as usize] as EdgeId)..(self.first_out[(node + 1) as usize] as EdgeId)
    }

    pub fn degree(&self, node: NodeId) -> usize {
        let range = self.neighbor_edge_indices_usize(node);
        range.end - range.start
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
    fn neighbor_iter(&self, node: NodeId) -> std::iter::Cloned<std::slice::Iter<u32>> {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().cloned()
    }

    pub fn num_arcs(&self) -> usize {
        self.head.len()
    }

    pub fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }

    pub fn unpack_arc(&self, from: NodeId, to: NodeId, weight: Weight, upward: &[Weight], downward: &[Weight]) -> Option<(NodeId, Weight, Weight)> {
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
            if lower_from_first < lower_from_second {
                current_iter.next();
            } else if lower_from_second < lower_from_first {
                other_iter.next();
            } else {
                if downward[edge_from_first_id as usize] + upward[edge_from_second_id as usize] == weight {
                    return Some((lower_from_first, downward[edge_from_first_id as usize], upward[edge_from_second_id as usize]));
                }

                current_iter.next();
                other_iter.next();
            }
        }

        None
    }
}
