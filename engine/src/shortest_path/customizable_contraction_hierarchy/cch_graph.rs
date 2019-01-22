use super::*;
use crate::{
    graph::first_out_graph::degrees_to_first_out,
    shortest_path::node_order::NodeOrder,
    in_range_option::InRangeOption,
    benchmark::*,
    io::*,
};

use std;
use std::ops::Range;

#[derive(Debug)]
pub struct SeparatorTree {
    pub nodes: Vec<NodeId>,
    pub children: Vec<SeparatorTree>,
}

#[derive(Debug)]
pub struct CCHGraph {
    first_out: Vec<EdgeId>,
    head: Vec<NodeId>,
    tail: Vec<NodeId>,
    node_order: NodeOrder,
    original_edge_to_ch_edge: Vec<EdgeId>,
    elimination_tree: Vec<InRangeOption<NodeId>>,
}

impl Deconstruct for CCHGraph {
    fn store_each(&self, store: &Fn(&str, &dyn Store) -> std::io::Result<()>) -> std::io::Result<()> {
        store("cch_first_out", &self.first_out)?;
        store("cch_head", &self.head)?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct CCHGraphReconstrctor<'g, Graph: for<'a> LinkIterGraph<'a>> {
    pub original_graph: &'g Graph,
    pub node_order: NodeOrder,
}

impl<'g, Graph: for<'a> LinkIterGraph<'a>> ReconstructPrepared<CCHGraph> for CCHGraphReconstrctor<'g, Graph> {
    fn reconstruct_with(self, loader: Loader) -> std::io::Result<CCHGraph> {
        let head: Vec<NodeId> = loader.load("cch_head")?;
        let m = head.len();
        let cch_graph = OwnedGraph::new(loader.load("cch_first_out")?, head, vec![INFINITY; m]);
        assert_eq!(cch_graph.num_nodes(), self.original_graph.num_nodes());
        Ok(CCHGraph::new_from(self.original_graph, self.node_order, cch_graph))
    }
}

impl CCHGraph {
    pub(super) fn new<Graph: for<'a> LinkIterGraph<'a>>(contracted_graph: ContractedGraph<Graph>) -> CCHGraph {
        let ContractedGraph(contracted_graph) = contracted_graph;
        let graph = Self::adjancecy_lists_to_first_out_graph(contracted_graph.nodes);
        Self::new_from(contracted_graph.original_graph, contracted_graph.node_order, graph)
    }

    fn new_from<Graph: for<'a> LinkIterGraph<'a>>(original_graph: &Graph, node_order: NodeOrder, contracted_graph: OwnedGraph) -> Self {
        let elimination_tree = Self::build_elimination_tree(&contracted_graph);
        let n = contracted_graph.num_nodes() as NodeId;
        let m = contracted_graph.num_arcs();
        let mut tail = vec![0; m];

        let original_edge_to_ch_edge = (0..n).flat_map(|node| {
            {
                let contracted_graph = &contracted_graph;
                let node_order = &node_order;

                original_graph.neighbor_iter(node).map(move |Link { node: neighbor, .. }| {
                    let node_rank = node_order.rank(node);
                    let neighbor_rank = node_order.rank(neighbor);
                    if node_rank < neighbor_rank {
                        contracted_graph.edge_index(node_rank, neighbor_rank).unwrap()
                    } else {
                        contracted_graph.edge_index(neighbor_rank, node_rank).unwrap()
                    }
                })
            }
        }).collect();

        for node in 0..n {
            tail[contracted_graph.neighbor_edge_indices_usize(node)].iter_mut().for_each(|tail| *tail = node);
        }

        let (first_out, head, _) = contracted_graph.decompose();

        CCHGraph {
            first_out,
            head,
            node_order,
            original_edge_to_ch_edge,
            elimination_tree,
            tail
        }
    }

    pub fn separators(&self) -> SeparatorTree {
        let mut children = vec![Vec::new(); self.num_nodes()];
        let mut roots = Vec::new();

        for (node_index, parent) in self.elimination_tree.iter().enumerate() {
            if let Some(parent) = parent.value() {
                children[parent as usize].push(node_index as NodeId);
            } else {
                roots.push(node_index as NodeId);
            }
        }

        SeparatorTree {
            nodes: Vec::new(),
            children: roots.into_iter().map(|root| Self::aggregate_chain(root, &children)).collect()
        }
    }

    fn aggregate_chain(root: NodeId, children: &[Vec<NodeId>]) -> SeparatorTree {
        let mut node = root;
        let mut nodes = vec![root];

        #[allow(clippy::match_ref_pats)]
        while let &[only_child] = &children[node as usize][..] {
            node = only_child;
            nodes.push(only_child)
        }

        SeparatorTree {
            nodes,
            children: children[node as usize].iter().map(|&child| Self::aggregate_chain(child, children)).collect()
        }
    }

    fn adjancecy_lists_to_first_out_graph(adjancecy_lists: Vec<Node>) -> OwnedGraph {
        let n = adjancecy_lists.len();

        let first_out: Vec<EdgeId> = {
            let degrees = adjancecy_lists.iter().map(|neighbors| neighbors.edges.len() as EdgeId);
            degrees_to_first_out(degrees).collect()
        };
        debug_assert_eq!(first_out.len(), n + 1);

        let head: Vec<NodeId> = adjancecy_lists
            .into_iter()
            .flat_map(|neighbors| neighbors.edges.into_iter())
            .collect();

        let m = head.len();
        OwnedGraph::new(first_out, head, vec![INFINITY; m])
    }

    fn build_elimination_tree(graph: &OwnedGraph) -> Vec<InRangeOption<NodeId>> {
        (0..graph.num_nodes()).map(|node_id| graph.neighbor_iter(node_id as NodeId).map(|l| l.node).min()).map(InRangeOption::new).collect()
    }

    pub fn customize<Graph: for<'a> LinkIterGraph<'a> + RandomLinkAccessGraph>(&self, metric: &Graph) ->
        (
            FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>,
            FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>,
            Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
            Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
        )
    {
        let n = (self.first_out.len() - 1) as NodeId;
        let m = self.head.len();

        let mut upward_shortcut_expansions = vec![(InRangeOption::new(None), InRangeOption::new(None)); m];
        let mut downward_shortcut_expansions = vec![(InRangeOption::new(None), InRangeOption::new(None)); m];

        let mut upward_weights = vec![INFINITY; m];
        let mut downward_weights = vec![INFINITY; m];

        report_time("CCH apply weights", || {
            for node in 0..n {
                for (edge_id, Link { node: neighbor, weight }) in metric.neighbor_edge_indices(node).zip(metric.neighbor_iter(node)) {
                    let ch_edge_id = self.original_edge_to_ch_edge[edge_id as usize];

                    if self.node_order.rank(node) < self.node_order.rank(neighbor) {
                        upward_weights[ch_edge_id as usize] = weight;
                    } else {
                        downward_weights[ch_edge_id as usize] = weight;
                    }
                }
            }
        });

        let mut upward = FirstOutGraph::new(&self.first_out[..], &self.head[..], upward_weights);
        let mut downward = FirstOutGraph::new(&self.first_out[..], &self.head[..], downward_weights);

        report_time("CCH Customization", || {
            let mut node_outgoing_weights = vec![(INFINITY, InRangeOption::new(None)); n as usize];
            let mut node_incoming_weights = vec![(INFINITY, InRangeOption::new(None)); n as usize];

            for current_node in 0..n {
                for (Link { node, weight }, edge_id) in downward.neighbor_iter(current_node).zip(downward.neighbor_edge_indices(current_node)) {
                    node_incoming_weights[node as usize] = (weight, InRangeOption::new(Some(edge_id)));
                    debug_assert_eq!(downward.link(edge_id).node, node);
                }
                for (Link { node, weight }, edge_id) in upward.neighbor_iter(current_node).zip(upward.neighbor_edge_indices(current_node)) {
                    node_outgoing_weights[node as usize] = (weight, InRangeOption::new(Some(edge_id)));
                    debug_assert_eq!(upward.link(edge_id).node, node);
                }

                for (Link { node, weight }, edge_id) in downward.neighbor_iter(current_node).zip(downward.neighbor_edge_indices(current_node)) {
                    debug_assert_eq!(self.edge_id_to_tail(edge_id), current_node);
                    let shortcut_edge_ids = upward.neighbor_edge_indices(node);
                    for ((&target, shortcut_weight), shortcut_edge_id) in upward.mut_weight_link_iter(node).zip(shortcut_edge_ids) {
                        debug_assert_eq!(self.edge_id_to_tail(shortcut_edge_id), node);
                        if weight + node_outgoing_weights[target as usize].0 < *shortcut_weight {
                            *shortcut_weight = weight + node_outgoing_weights[target as usize].0;
                            debug_assert!(node_outgoing_weights[target as usize].1.value().is_some());
                            upward_shortcut_expansions[shortcut_edge_id as usize] = (InRangeOption::new(Some(edge_id)), node_outgoing_weights[target as usize].1)
                        }
                    }
                }
                for (Link { node, weight }, edge_id) in upward.neighbor_iter(current_node).zip(upward.neighbor_edge_indices(current_node)) {
                    debug_assert_eq!(self.edge_id_to_tail(edge_id), current_node);
                    let shortcut_edge_ids = downward.neighbor_edge_indices(node);
                    for ((&target, shortcut_weight), shortcut_edge_id) in downward.mut_weight_link_iter(node).zip(shortcut_edge_ids) {
                        debug_assert_eq!(self.edge_id_to_tail(shortcut_edge_id), node);
                        if weight + node_incoming_weights[target as usize].0 < *shortcut_weight {
                            *shortcut_weight = weight + node_incoming_weights[target as usize].0;
                            debug_assert!(node_incoming_weights[target as usize].1.value().is_some());
                            downward_shortcut_expansions[shortcut_edge_id as usize] = (node_incoming_weights[target as usize].1, InRangeOption::new(Some(edge_id)))
                        }
                    }
                }

                for Link { node, .. } in downward.neighbor_iter(current_node) {
                    node_incoming_weights[node as usize] = (INFINITY, InRangeOption::new(None));
                }
                for Link { node, .. } in upward.neighbor_iter(current_node) {
                    node_outgoing_weights[node as usize] = (INFINITY, InRangeOption::new(None));
                }
            }
        });

        (upward, downward, upward_shortcut_expansions, downward_shortcut_expansions)
    }

    pub fn customize_td<'a, 'b: 'a>(&'a self, metric: &'b time_dependent::TDGraph) -> time_dependent::ShortcutGraph<'a> {
        use crate::graph::time_dependent::*;

        let n = (self.first_out.len() - 1) as NodeId;
        let m = self.head.len();

        let mut upward_weights = vec![Shortcut::new(None); m];
        let mut downward_weights = vec![Shortcut::new(None); m];

        report_time("TD-CCH apply weights", || {
            for node in 0..n {
                for (edge_id, neighbor) in metric.neighbor_edge_indices(node).zip(metric.neighbor_iter(node).map(|link| link.node)) {
                    let ch_edge_id = self.original_edge_to_ch_edge[edge_id as usize];

                    if self.node_order.rank(node) < self.node_order.rank(neighbor) {
                        upward_weights[ch_edge_id as usize] = Shortcut::new(Some((edge_id, metric.travel_time_function(edge_id))));
                    } else {
                        downward_weights[ch_edge_id as usize] = Shortcut::new(Some((edge_id, metric.travel_time_function(edge_id))));
                    }
                }
            }
        });

        let mut shortcut_graph = ShortcutGraph::new(metric, &self.first_out, &self.head, upward_weights, downward_weights);

        report_time("TD-CCH Customization", || {
            let mut node_edge_ids = vec![InRangeOption::new(None); n as usize];

            let timer = Timer::new();

            for current_node in 0..n {
                if current_node % 100_000 == 0 {
                    println!("t: {}s customizing from node {}, degree: {}, current_num_segements: {}", timer.get_passed_ms() / 1000, current_node, self.degree(current_node), shortcut_graph.total_num_segments());
                }
                for (node, edge_id) in self.neighbor_iter(current_node).zip(self.neighbor_edge_indices(current_node)) {
                    node_edge_ids[node as usize] = InRangeOption::new(Some(edge_id));
                }

                for (node, edge_id) in self.neighbor_iter(current_node).zip(self.neighbor_edge_indices(current_node)) {
                    debug_assert_eq!(self.edge_id_to_tail(edge_id), current_node);
                    let shortcut_edge_ids = self.neighbor_edge_indices(node);
                    for (target, shortcut_edge_id) in self.neighbor_iter(node).zip(shortcut_edge_ids) {
                        debug_assert_eq!(self.edge_id_to_tail(shortcut_edge_id), node);
                        if let Some(other_edge_id) = node_edge_ids[target as usize].value() {
                            debug_assert!(shortcut_edge_id > edge_id);
                            debug_assert!(shortcut_edge_id > other_edge_id);
                            shortcut_graph.borrow_mut_outgoing(shortcut_edge_id, |shortcut, shortcut_graph| shortcut.merge((edge_id, other_edge_id), shortcut_graph));
                            shortcut_graph.borrow_mut_incoming(shortcut_edge_id, |shortcut, shortcut_graph| shortcut.merge((other_edge_id, edge_id), shortcut_graph));
                        }
                    }
                }

                for node in self.neighbor_iter(current_node) {
                    node_edge_ids[node as usize] = InRangeOption::new(None);
                }
            }

            for current_node in (0..n).rev() {
                if current_node % 100_000 == 0 {
                    println!("t: {}s perfect customizing from node {}, degree: {}, current_num_segements: {}", timer.get_passed_ms() / 1000, current_node, self.degree(current_node), shortcut_graph.total_num_segments());
                }
                for (node, edge_id) in self.neighbor_iter(current_node).zip(self.neighbor_edge_indices(current_node)) {
                    node_edge_ids[node as usize] = InRangeOption::new(Some(edge_id));
                }

                for (node, edge_id) in self.neighbor_iter(current_node).zip(self.neighbor_edge_indices(current_node)) {
                    let shortcut_edge_ids = self.neighbor_edge_indices(node);
                    for (target, shortcut_edge_id) in self.neighbor_iter(node).zip(shortcut_edge_ids) {
                        if let Some(other_edge_id) = node_edge_ids[target as usize].value() {
                            shortcut_graph.borrow_mut_outgoing(other_edge_id, |shortcut, shortcut_graph| {
                                let alternative = Linked::new(shortcut_graph.get_outgoing(edge_id), shortcut_graph.get_outgoing(shortcut_edge_id));
                                shortcut.remove_dominated_by(shortcut_graph, &alternative);
                            });

                            shortcut_graph.borrow_mut_outgoing(edge_id, |shortcut, shortcut_graph| {
                                let alternative = Linked::new(shortcut_graph.get_outgoing(other_edge_id), shortcut_graph.get_incoming(shortcut_edge_id));
                                shortcut.remove_dominated_by(shortcut_graph, &alternative);
                            });

                            shortcut_graph.borrow_mut_incoming(other_edge_id, |shortcut, shortcut_graph| {
                                let alternative = Linked::new(shortcut_graph.get_incoming(edge_id), shortcut_graph.get_incoming(shortcut_edge_id));
                                shortcut.remove_dominated_by(shortcut_graph, &alternative);
                            });

                            shortcut_graph.borrow_mut_incoming(edge_id, |shortcut, shortcut_graph| {
                                let alternative = Linked::new(shortcut_graph.get_incoming(other_edge_id), shortcut_graph.get_outgoing(shortcut_edge_id));
                                shortcut.remove_dominated_by(shortcut_graph, &alternative);
                            });
                        }
                    }
                }

                for node in self.neighbor_iter(current_node) {
                    shortcut_graph.borrow_mut_incoming(node_edge_ids[node as usize].value().unwrap(), Shortcut::remove_dominated);
                    shortcut_graph.borrow_mut_outgoing(node_edge_ids[node as usize].value().unwrap(), Shortcut::remove_dominated);
                    node_edge_ids[node as usize] = InRangeOption::new(None);
                }
            }
        });

        shortcut_graph
    }

    pub fn customize_floating_td<'a, 'b: 'a>(&'a self, metric: &'b floating_time_dependent::TDGraph) -> floating_time_dependent::ShortcutGraph<'a> {
        use crate::graph::floating_time_dependent::*;
        use crate::report::*;
        use std::cmp::min;

        report!("algo", "Floating TDCCH Customization");

        let n = (self.first_out.len() - 1) as NodeId;
        let m = self.head.len();

        let mut upward = vec![Shortcut::new(None, metric); m];
        let mut downward = vec![Shortcut::new(None, metric); m];

        let subctxt = push_context("weight_applying".to_string());
        report_time("TD-CCH apply weights", || {
            for node in 0..n {
                for (edge_id, neighbor) in metric.neighbor_edge_indices(node).zip(metric.neighbor_iter(node).map(|link| link.node)) {
                    let ch_edge_id = self.original_edge_to_ch_edge[edge_id as usize];

                    if self.node_order.rank(node) < self.node_order.rank(neighbor) {
                        upward[ch_edge_id as usize] = Shortcut::new(Some(edge_id), metric);
                    } else {
                        downward[ch_edge_id as usize] = Shortcut::new(Some(edge_id), metric);
                    }
                }
            }
        });
        drop(subctxt);

        let subctxt = push_context("precustomization".to_string());
        report_time("TD-CCH Pre-Customization", || {
            let mut node_edge_ids = vec![InRangeOption::new(None); n as usize];

            for current_node in 0..n {
                for (node, edge_id) in self.neighbor_iter(current_node).zip(self.neighbor_edge_indices(current_node)) {
                    upward[edge_id as usize].update_is_constant();
                    downward[edge_id as usize].update_is_constant();
                    node_edge_ids[node as usize] = InRangeOption::new(Some(edge_id));
                }

                for (node, edge_id) in self.neighbor_iter(current_node).zip(self.neighbor_edge_indices(current_node)) {
                    debug_assert_eq!(self.edge_id_to_tail(edge_id), current_node);
                    let shortcut_edge_ids = self.neighbor_edge_indices(node);
                    for (target, shortcut_edge_id) in self.neighbor_iter(node).zip(shortcut_edge_ids) {
                        debug_assert_eq!(self.edge_id_to_tail(shortcut_edge_id), node);
                        if let Some(other_edge_id) = node_edge_ids[target as usize].value() {
                            debug_assert!(shortcut_edge_id > edge_id);
                            debug_assert!(shortcut_edge_id > other_edge_id);
                            upward[shortcut_edge_id as usize].upper_bound = min(upward[shortcut_edge_id as usize].upper_bound, downward[edge_id as usize].upper_bound + upward[other_edge_id as usize].upper_bound);
                            upward[shortcut_edge_id as usize].lower_bound = min(upward[shortcut_edge_id as usize].lower_bound, downward[edge_id as usize].lower_bound + upward[other_edge_id as usize].lower_bound);
                            downward[shortcut_edge_id as usize].upper_bound = min(downward[shortcut_edge_id as usize].upper_bound, downward[other_edge_id as usize].upper_bound + upward[edge_id as usize].upper_bound);
                            downward[shortcut_edge_id as usize].lower_bound = min(downward[shortcut_edge_id as usize].lower_bound, downward[other_edge_id as usize].lower_bound + upward[edge_id as usize].lower_bound);
                        }
                    }
                }

                for node in self.neighbor_iter(current_node) {
                    node_edge_ids[node as usize] = InRangeOption::new(None);
                }
            }

            let upward_preliminary_bounds: Vec<_> = upward.iter().map(|s| s.lower_bound).collect();
            let downward_preliminary_bounds: Vec<_> = downward.iter().map(|s| s.lower_bound).collect();

            for current_node in (0..n).rev() {
                for (node, edge_id) in self.neighbor_iter(current_node).zip(self.neighbor_edge_indices(current_node)) {
                    node_edge_ids[node as usize] = InRangeOption::new(Some(edge_id));
                }

                for (node, edge_id) in self.neighbor_iter(current_node).zip(self.neighbor_edge_indices(current_node)) {
                    let shortcut_edge_ids = self.neighbor_edge_indices(node);
                    for (target, shortcut_edge_id) in self.neighbor_iter(node).zip(shortcut_edge_ids) {
                        if let Some(other_edge_id) = node_edge_ids[target as usize].value() {
                            upward[other_edge_id as usize].upper_bound = min(upward[other_edge_id as usize].upper_bound, upward[edge_id as usize].upper_bound + upward[shortcut_edge_id as usize].upper_bound);
                            upward[other_edge_id as usize].lower_bound = min(upward[other_edge_id as usize].lower_bound, upward[edge_id as usize].lower_bound + upward[shortcut_edge_id as usize].lower_bound);

                            upward[edge_id as usize].upper_bound = min(upward[edge_id as usize].upper_bound, upward[other_edge_id as usize].upper_bound + downward[shortcut_edge_id as usize].upper_bound);
                            upward[edge_id as usize].lower_bound = min(upward[edge_id as usize].lower_bound, upward[other_edge_id as usize].lower_bound + downward[shortcut_edge_id as usize].lower_bound);

                            downward[other_edge_id as usize].upper_bound = min(downward[other_edge_id as usize].upper_bound, downward[edge_id as usize].upper_bound + downward[shortcut_edge_id as usize].upper_bound);
                            downward[other_edge_id as usize].lower_bound = min(downward[other_edge_id as usize].lower_bound, downward[edge_id as usize].lower_bound + downward[shortcut_edge_id as usize].lower_bound);

                            downward[edge_id as usize].upper_bound = min(downward[edge_id as usize].upper_bound, downward[other_edge_id as usize].upper_bound + upward[shortcut_edge_id as usize].upper_bound);
                            downward[edge_id as usize].lower_bound = min(downward[edge_id as usize].lower_bound, downward[other_edge_id as usize].lower_bound + upward[shortcut_edge_id as usize].lower_bound);
                        }
                    }
                }

                for node in self.neighbor_iter(current_node) {
                    node_edge_ids[node as usize] = InRangeOption::new(None);
                }
            }

            for (shortcut, lower_bound) in upward.iter_mut().zip(upward_preliminary_bounds.into_iter()) {
                if shortcut.upper_bound.fuzzy_lt(lower_bound) {
                    shortcut.required = false;
                    shortcut.lower_bound = FlWeight::INFINITY;
                    shortcut.upper_bound = FlWeight::INFINITY;
                }
            }

            for (shortcut, lower_bound) in downward.iter_mut().zip(downward_preliminary_bounds.into_iter()) {
                if shortcut.upper_bound.fuzzy_lt(lower_bound) {
                    shortcut.required = false;
                    shortcut.lower_bound = FlWeight::INFINITY;
                    shortcut.upper_bound = FlWeight::INFINITY;
                }
            }
        });
        drop(subctxt);

        let mut shortcut_graph = ShortcutGraph::new(metric, &self.first_out, &self.head, upward, downward);

        let mut merge_count = 0;

        let subctxt = push_context("main".to_string());
        report_time("TD-CCH Customization", || {
            let mut events_ctxt = push_collection_context("events".to_string());

            let timer = Timer::new();

            let mut node_edge_ids = vec![InRangeOption::new(None); n as usize];

            for current_node in 0..n {
                if current_node > 0 && current_node % (n / 100) == 0 || self.degree(current_node) > 50 {
                    let _event = events_ctxt.push_collection_item();

                    report!("at_s", timer.get_passed_ms() / 1000);
                    report!("current_rank", current_node);
                    report!("current_node_degree", self.degree(current_node));
                    report!("num_ipps_stored", IPP_COUNT.with(|count| count.get()));
                    report!("num_shortcuts_active", ACTIVE_SHORTCUTS.with(|count| count.get()));
                    report!("num_ipps_reduced_by_approx", SAVED_BY_APPROX.with(|count| count.get()));
                    report!("num_ipps_considered_for_approx", CONSIDERED_FOR_APPROX.with(|count| count.get()));
                    report!("num_shortcut_merge_points", PATH_SOURCES_COUNT.with(|count| count.get()));
                    report!("percent_close_ipps", CLOSE_IPPS_COUNT.with(|count| count.get()) as f64 / f64::from(merge_count) / 100.0);
                    report!("num_performed_merges", ACTUALLY_MERGED.with(|count| count.get()));
                    report!("num_performed_links", ACTUALLY_LINKED.with(|count| count.get()));
                    report!("num_performed_unnecessary_links", UNNECESSARY_LINKED.with(|count| count.get()));
                    report!("num_processed_triangles", merge_count);
                }

                for (node, edge_id) in self.neighbor_iter(current_node).zip(self.neighbor_edge_indices(current_node)) {
                    shortcut_graph.borrow_mut_outgoing(edge_id, |shortcut, shortcut_graph| shortcut.finalize_bounds(shortcut_graph));
                    shortcut_graph.borrow_mut_incoming(edge_id, |shortcut, shortcut_graph| shortcut.finalize_bounds(shortcut_graph));
                    node_edge_ids[node as usize] = InRangeOption::new(Some(edge_id));
                }

                for (node, edge_id) in self.neighbor_iter(current_node).zip(self.neighbor_edge_indices(current_node)) {
                    debug_assert_eq!(self.edge_id_to_tail(edge_id), current_node);
                    let shortcut_edge_ids = self.neighbor_edge_indices(node);
                    for (target, shortcut_edge_id) in self.neighbor_iter(node).zip(shortcut_edge_ids) {
                        debug_assert_eq!(self.edge_id_to_tail(shortcut_edge_id), node);
                        if let Some(other_edge_id) = node_edge_ids[target as usize].value() {
                            debug_assert!(shortcut_edge_id > edge_id);
                            debug_assert!(shortcut_edge_id > other_edge_id);
                            shortcut_graph.borrow_mut_outgoing(shortcut_edge_id, |shortcut, shortcut_graph| shortcut.merge((edge_id, other_edge_id), shortcut_graph));
                            shortcut_graph.borrow_mut_incoming(shortcut_edge_id, |shortcut, shortcut_graph| shortcut.merge((other_edge_id, edge_id), shortcut_graph));
                            merge_count += 2;
                        }
                    }
                }

                for (node, edge_id) in self.neighbor_iter(current_node).zip(self.neighbor_edge_indices(current_node)) {
                    shortcut_graph.borrow_mut_outgoing(edge_id, |shortcut, _| shortcut.clear_plf());
                    shortcut_graph.borrow_mut_incoming(edge_id, |shortcut, _| shortcut.clear_plf());
                    node_edge_ids[node as usize] = InRangeOption::new(None);
                }
            }

        });
        drop(subctxt);

        report!("num_ipps_stored", IPP_COUNT.with(|count| count.get()));
        report!("num_shortcuts_active", ACTIVE_SHORTCUTS.with(|count| count.get()));
        report!("num_ipps_reduced_by_approx", SAVED_BY_APPROX.with(|count| count.get()));
        report!("num_ipps_considered_for_approx", CONSIDERED_FOR_APPROX.with(|count| count.get()));
        report!("num_shortcut_merge_points", PATH_SOURCES_COUNT.with(|count| count.get()));
        report!("percent_close_ipps", CLOSE_IPPS_COUNT.with(|count| count.get()) as f64 / f64::from(merge_count) / 100.0);
        report!("num_performed_merges", ACTUALLY_MERGED.with(|count| count.get()));
        report!("num_performed_links", ACTUALLY_LINKED.with(|count| count.get()));
        report!("num_performed_unnecessary_links", UNNECESSARY_LINKED.with(|count| count.get()));
        report!("num_processed_triangles", merge_count);

        shortcut_graph
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

    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        (self.first_out[node as usize] as EdgeId)..(self.first_out[(node + 1) as usize] as EdgeId)
    }

    pub fn degree(&self, node: NodeId) -> usize {
        let range = self.neighbor_edge_indices_usize(node);
        range.end - range.start
    }

    fn neighbor_edge_indices_usize(&self, node: NodeId) -> Range<usize> {
        let range = self.neighbor_edge_indices(node);
        Range { start: range.start as usize, end: range.end as usize }
    }

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
}
