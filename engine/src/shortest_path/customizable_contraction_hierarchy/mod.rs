use super::*;
// use crate::shortest_path::node_order::NodeOrder;

pub mod cch_graph;
mod contraction;
use contraction::*;


pub fn contract<Graph: for<'a> LinkIterGraph<'a>>(graph: &Graph, node_order: NodeOrder) -> CCH {
    CCH::new(ContractionGraph::new(graph, node_order).contract())
}


use crate::{
    graph::first_out_graph::degrees_to_first_out,
    shortest_path::node_order::NodeOrder,
    in_range_option::InRangeOption,
    benchmark::*,
    io::*,
    util::NonNan,
};

use std;
use std::ops::Range;
use std::cell::RefCell;
use std::sync::atomic::Ordering;

use rayon::prelude::*;

#[derive(Debug)]
pub struct SeparatorTree {
    nodes: Vec<NodeId>,
    children: Vec<SeparatorTree>,
    num_nodes: usize,
}

impl SeparatorTree {
    fn validate_for_parallelization(&self) {
        for nodes in self.nodes.windows(2) {
            assert_eq!(nodes[0], nodes[1] + 1, "Disconnected ID Ranges in nested dissection separator")
        }

        let mut child_range_start = self.nodes.first().cloned().unwrap_or(self.num_nodes as u32 - 1) + 1 - self.num_nodes as u32;
        for child in &self.children {
            assert_eq!(child_range_start, child.nodes[0] + 1 - child.num_nodes as u32, "Disconnected ID Ranges in nested dissection cells");
            child_range_start += child.num_nodes as u32;
        }

        for children in self.children.windows(2) {
            assert!(children[0].num_nodes >= children[1].num_nodes, "Sub cells not sorted by descending size");
        }

        for child in &self.children {
            child.validate_for_parallelization();
        }
    }

    fn new(cch: &CCH) -> Self {
        let mut children = vec![Vec::new(); cch.num_nodes()];
        let mut roots = Vec::new();

        for (node_index, parent) in cch.elimination_tree.iter().enumerate() {
            if let Some(parent) = parent.value() {
                children[parent as usize].push(node_index as NodeId);
            } else {
                roots.push(node_index as NodeId);
            }
        }

        let children: Vec<_> = roots.into_iter().map(|root| SeparatorTree::new_subtree(root, &children)).collect();
        let num_nodes = children.iter().map(|child| child.num_nodes).sum::<usize>();
        debug_assert_eq!(num_nodes, cch.num_nodes());

        SeparatorTree { nodes: Vec::new(), children, num_nodes }
    }

    fn new_subtree(root: NodeId, children: &[Vec<NodeId>]) -> SeparatorTree {
        let mut node = root;
        let mut nodes = vec![root];

        #[allow(clippy::match_ref_pats)]
        while let &[only_child] = &children[node as usize][..] {
            node = only_child;
            nodes.push(only_child)
        }

        let children: Vec<_> = children[node as usize].iter().map(|&child| { debug_assert!(child < node); Self::new_subtree(child, children) }).collect();
        let num_nodes = nodes.len() + children.iter().map(|child| child.num_nodes).sum::<usize>();

        debug_assert!(num_nodes > 0);
        debug_assert!(!nodes.is_empty());

        SeparatorTree {
            nodes,
            children,
            num_nodes,
        }
    }
}

#[derive(Debug)]
pub struct CCH {
    first_out: Vec<EdgeId>,
    head: Vec<NodeId>,
    tail: Vec<NodeId>,
    node_order: NodeOrder,
    original_edge_to_ch_edge: Vec<EdgeId>,
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

impl<'g, Graph: for<'a> LinkIterGraph<'a>> ReconstructPrepared<CCH> for CCHReconstrctor<'g, Graph> {
    fn reconstruct_with(self, loader: Loader) -> std::io::Result<CCH> {
        let head: Vec<NodeId> = loader.load("cch_head")?;
        let m = head.len();
        let cch_graph = OwnedGraph::new(loader.load("cch_first_out")?, head, vec![INFINITY; m]);
        assert_eq!(cch_graph.num_nodes(), self.original_graph.num_nodes());
        Ok(CCH::new_from(self.original_graph, self.node_order, cch_graph))
    }
}

impl CCH {
    pub(super) fn new<Graph: for<'a> LinkIterGraph<'a>>(contracted_graph: ContractedGraph<Graph>) -> CCH {
        let (cch, order, orig) = contracted_graph.decompose();
        Self::new_from(orig, order, cch)
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

        let mut inverted = vec![Vec::new(); n as usize];
        for current_node in 0..n {
            for (Link { node, .. }, edge_id) in contracted_graph.neighbor_iter(current_node).zip(contracted_graph.neighbor_edge_indices(current_node)) {
                inverted[node as usize].push((current_node, edge_id));
            }
        }

        let down_first_out: Vec<EdgeId> = {
            let degrees = inverted.iter().map(|neighbors| neighbors.len() as EdgeId);
            degrees_to_first_out(degrees).collect()
        };
        debug_assert_eq!(down_first_out.len(), n as usize + 1);

        let (down_head, down_up_edge_ids): (Vec<_>, Vec<_>) = inverted
            .into_iter()
            .flat_map(|neighbors| neighbors.into_iter())
            .unzip();

        let inverted = OwnedGraph::new(down_first_out, down_head, down_up_edge_ids);
        let (first_out, head, _) = contracted_graph.decompose();

        CCH {
            first_out,
            head,
            node_order,
            original_edge_to_ch_edge,
            elimination_tree,
            tail,
            inverted
        }
    }

    pub fn separators(&self) -> SeparatorTree {
        SeparatorTree::new(&self)
    }

    fn build_elimination_tree(graph: &OwnedGraph) -> Vec<InRangeOption<NodeId>> {
        (0..graph.num_nodes()).map(|node_id| graph.neighbor_iter(node_id as NodeId).map(|l| l.node).min()).map(InRangeOption::new).collect()
    }

    pub fn customize<Graph: for<'a> LinkIterGraph<'a> + RandomLinkAccessGraph>(&self, metric: &Graph) ->
        (FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>,FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>)
    {
        let n = self.num_nodes() as NodeId;
        let m = self.num_arcs();

        UPWARD_WORKSPACE.with(|node_outgoing_weights| {
            let mut node_outgoing_weights = node_outgoing_weights.borrow_mut();
            node_outgoing_weights.resize(n as usize, INFINITY);
        });

        DOWNWARD_WORKSPACE.with(|node_incoming_weights| {
            let mut node_incoming_weights = node_incoming_weights.borrow_mut();
            node_incoming_weights.resize(n as usize, INFINITY);
        });

        let separators = self.separators();

        let customize = |nodes: Range<usize>, offset, upward_weights: &mut [Weight], downward_weights: &mut [Weight]| {
            UPWARD_WORKSPACE.with(|node_outgoing_weights| {
                let mut node_outgoing_weights = node_outgoing_weights.borrow_mut();
                if node_outgoing_weights.len() != n as usize { node_outgoing_weights.resize(n as usize, INFINITY) }

                DOWNWARD_WORKSPACE.with(|node_incoming_weights| {
                    let mut node_incoming_weights = node_incoming_weights.borrow_mut();
                    if node_incoming_weights.len() != n as usize { node_incoming_weights.resize(n as usize, INFINITY) }

                    for current_node in nodes {
                        let current_node = current_node as NodeId;
                        let mut edges = self.neighbor_edge_indices_usize(current_node);
                        edges.start -= offset;
                        edges.end -= offset;
                        for ((node, &down_weight), &up_weight) in self.neighbor_iter(current_node).zip(&downward_weights[edges.clone()]).zip(&upward_weights[edges.clone()]) {
                            node_incoming_weights[node as usize] = down_weight;
                            node_outgoing_weights[node as usize] = up_weight;
                        }

                        for Link { node: low_node, weight: first_edge_id } in self.inverted.neighbor_iter(current_node) {
                            let first_down_weight = downward_weights[first_edge_id as usize - offset];
                            let first_up_weight = upward_weights[first_edge_id as usize - offset];
                            let mut low_up_edges = self.neighbor_edge_indices_usize(low_node);
                            low_up_edges.start -= offset;
                            low_up_edges.end -= offset;
                            for ((node, upward_weight), downward_weight) in self.neighbor_iter(low_node).rev().zip(upward_weights[low_up_edges.clone()].iter().rev()).zip(downward_weights[low_up_edges].iter().rev()) {
                                if node <= current_node { break; }

                                let relax = unsafe { node_outgoing_weights.get_unchecked_mut(node as usize) };
                                *relax = std::cmp::min(*relax, upward_weight + first_down_weight);
                                let relax = unsafe { node_incoming_weights.get_unchecked_mut(node as usize) };
                                *relax = std::cmp::min(*relax, downward_weight + first_up_weight);
                            }
                        }

                        for (((node, downward_weight), upward_weight), _edge_id) in self.neighbor_iter(current_node).zip(&mut downward_weights[edges.clone()]).zip(&mut upward_weights[edges.clone()]).zip(edges) {
                            *downward_weight = node_incoming_weights[node as usize];
                            *upward_weight = node_outgoing_weights[node as usize];
                        }
                    }
                });
            });
        };

        if !cfg!(feature = "cch-disable-par") {
            separators.validate_for_parallelization();
        }

        let customization = SeperatorBasedParallelCustomization {
            cch: &self,
            separators,
            customize_cell: customize,
            customize_separator: customize,
            _t: std::marker::PhantomData::<Weight>,
        };

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

        report_time("CCH Customization", || {
            customization.customize(&mut upward_weights, &mut downward_weights);
        });

        let upward = FirstOutGraph::new(&self.first_out[..], &self.head[..], upward_weights);
        let downward = FirstOutGraph::new(&self.first_out[..], &self.head[..], downward_weights);
        (upward, downward)
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
        Range { start: range.start as usize, end: range.end as usize }
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

        while let (Some(&Link { node: lower_from_first, weight: edge_from_first_id }), Some(&Link { node: lower_from_second, weight: edge_from_second_id })) = (current_iter.peek(), other_iter.peek()) {
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

thread_local! { static UPWARD_WORKSPACE: RefCell<Vec<Weight>> = RefCell::new(Vec::new()); }
thread_local! { static DOWNWARD_WORKSPACE: RefCell<Vec<Weight>> = RefCell::new(Vec::new()); }

struct SeperatorBasedParallelCustomization<'a, T, F, G> {
    cch: &'a CCH,
    separators: SeparatorTree,
    customize_cell: F,
    customize_separator: G,
    _t: std::marker::PhantomData<T>,
}

impl<'a, T, F, G> SeperatorBasedParallelCustomization<'a, T, F, G> where
    T: Send + Sync,
    F: Sync + Fn(Range<usize>, usize, &mut [T], &mut [T]),
    G: Sync + Fn(Range<usize>, usize, &mut [T], &mut [T]),
{
    pub fn customize(&self, upward: &'a mut [T], downward: &'a mut [T]) {
        self.customize_cell(&self.separators, 0, upward, downward);
    }

    fn customize_cell(&self, sep_tree: &SeparatorTree, offset: usize, upward: &'a mut [T], downward: &'a mut [T]) {
        let edge_offset = self.cch.first_out[offset] as usize;

        if cfg!(feature = "cch-disable-par") || sep_tree.num_nodes < self.cch.num_nodes() / (32 * rayon::current_num_threads()) {
            (self.customize_cell)(offset..offset + sep_tree.num_nodes, edge_offset, upward, downward);
        } else {
            let mut sub_offset = offset;
            let mut sub_edge_offset = edge_offset;
            let mut sub_upward = &mut upward[..];
            let mut sub_downward = &mut downward[..];

            rayon::scope(|s| {
                for sub in &sep_tree.children {
                    let (this_sub_up, rest_up) = (move || { sub_upward })().split_at_mut(self.cch.first_out[sub_offset + sub.num_nodes] as usize - sub_edge_offset);
                    let (this_sub_down, rest_down) = (move || { sub_downward })().split_at_mut(self.cch.first_out[sub_offset + sub.num_nodes] as usize - sub_edge_offset);
                    sub_edge_offset += this_sub_up.len();
                    if sub.num_nodes < self.cch.num_nodes() / (32 * rayon::current_num_threads()) {
                        self.customize_cell(sub, sub_offset, this_sub_up, this_sub_down);
                    } else {
                        s.spawn(move |_| self.customize_cell(sub, sub_offset, this_sub_up, this_sub_down));
                    }
                    sub_offset += sub.num_nodes;
                    sub_upward = rest_up;
                    sub_downward = rest_down;
                }
            });

            (self.customize_separator)(sub_offset..offset + sep_tree.num_nodes, edge_offset, upward, downward)
        }
    }
}

pub mod ftd_cch {
    use super::*;
    use floating_time_dependent::*;

    pub fn customize<'a, 'b: 'a>(cch: &'a CCH, metric: &'b TDGraph) -> ShortcutGraph<'a> {
        use crate::report::*;
        use std::cmp::min;

        report!("algo", "Floating TDCCH Customization");

        let n = (cch.first_out.len() - 1) as NodeId;
        let m = cch.head.len();

        let mut upward: Vec<_> = std::iter::repeat_with(|| Shortcut::new(None, metric)).take(m).collect();
        let mut downward: Vec<_> = std::iter::repeat_with(|| Shortcut::new(None, metric)).take(m).collect();

        let subctxt = push_context("weight_applying".to_string());
        report_time("TD-CCH apply weights", || {
            for node in 0..n {
                for (edge_id, neighbor) in metric.neighbor_edge_indices(node).zip(metric.neighbor_iter(node).map(|link| link.node)) {
                    let ch_edge_id = cch.original_edge_to_ch_edge[edge_id as usize];

                    if cch.node_order.rank(node) < cch.node_order.rank(neighbor) {
                        upward[ch_edge_id as usize] = Shortcut::new(Some(edge_id), metric);
                    } else {
                        downward[ch_edge_id as usize] = Shortcut::new(Some(edge_id), metric);
                    }
                }
            }
        });
        drop(subctxt);

        if cfg!(feature = "tdcch-precustomization") {
            let _subctxt = push_context("precustomization".to_string());
            report_time("TD-CCH Pre-Customization", || {
                let mut node_edge_ids = vec![InRangeOption::new(None); n as usize];

                for current_node in 0..n {
                    for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                        upward[edge_id as usize].update_is_constant();
                        downward[edge_id as usize].update_is_constant();
                        node_edge_ids[node as usize] = InRangeOption::new(Some(edge_id));
                    }

                    for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                        debug_assert_eq!(cch.edge_id_to_tail(edge_id), current_node);
                        let shortcut_edge_ids = cch.neighbor_edge_indices(node);
                        for (target, shortcut_edge_id) in cch.neighbor_iter(node).zip(shortcut_edge_ids) {
                            debug_assert_eq!(cch.edge_id_to_tail(shortcut_edge_id), node);
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

                    for node in cch.neighbor_iter(current_node) {
                        node_edge_ids[node as usize] = InRangeOption::new(None);
                    }
                }

                let upward_preliminary_bounds: Vec<_> = upward.iter().map(|s| s.lower_bound).collect();
                let downward_preliminary_bounds: Vec<_> = downward.iter().map(|s| s.lower_bound).collect();

                for current_node in (0..n).rev() {
                    for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                        node_edge_ids[node as usize] = InRangeOption::new(Some(edge_id));
                    }

                    for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                        let shortcut_edge_ids = cch.neighbor_edge_indices(node);
                        for (target, shortcut_edge_id) in cch.neighbor_iter(node).zip(shortcut_edge_ids) {
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

                    for node in cch.neighbor_iter(current_node) {
                        node_edge_ids[node as usize] = InRangeOption::new(None);
                    }
                }

                for (shortcut, lower_bound) in upward.iter_mut().zip(upward_preliminary_bounds.into_iter()) {
                    if shortcut.upper_bound.fuzzy_lt(lower_bound) {
                        shortcut.required = false;
                        shortcut.lower_bound = FlWeight::INFINITY;
                        shortcut.upper_bound = FlWeight::INFINITY;
                    } else {
                        shortcut.lower_bound = lower_bound;
                    }
                }

                for (shortcut, lower_bound) in downward.iter_mut().zip(downward_preliminary_bounds.into_iter()) {
                    if shortcut.upper_bound.fuzzy_lt(lower_bound) {
                        shortcut.required = false;
                        shortcut.lower_bound = FlWeight::INFINITY;
                        shortcut.upper_bound = FlWeight::INFINITY;
                    } else {
                        shortcut.lower_bound = lower_bound;
                    }
                }
            });
        }

        {
            let subctxt = push_context("main".to_string());

            use std::thread;
            use std::sync::mpsc::{channel, RecvTimeoutError};

            let (tx, rx) = channel();
            let (events_tx, events_rx) = channel();

            report_time("TD-CCH Customization", || {
                thread::spawn(move || {
                    let timer = Timer::new();

                    let mut events = Vec::new();

                    loop {
                        report!("at_s", timer.get_passed_ms() / 1000);
                        report!("nodes_customized", NODES_CUSTOMIZED.load(Ordering::Relaxed));
                        if cfg!(feature = "detailed-stats") {
                            report!("num_ipps_stored", IPP_COUNT.load(Ordering::Relaxed));
                            report!("num_shortcuts_active", ACTIVE_SHORTCUTS.load(Ordering::Relaxed));
                            report!("num_ipps_reduced_by_approx", SAVED_BY_APPROX.load(Ordering::Relaxed));
                            report!("num_ipps_considered_for_approx", CONSIDERED_FOR_APPROX.load(Ordering::Relaxed));
                            report!("num_shortcut_merge_points", PATH_SOURCES_COUNT.load(Ordering::Relaxed));
                            report!("num_performed_merges", ACTUALLY_MERGED.load(Ordering::Relaxed));
                            report!("num_performed_links", ACTUALLY_LINKED.load(Ordering::Relaxed));
                            report!("num_performed_unnecessary_links", UNNECESSARY_LINKED.load(Ordering::Relaxed));
                        }

                        if cfg!(feature = "detailed-stats") {
                            events.push((timer.get_passed_ms() / 1000,
                                         NODES_CUSTOMIZED.load(Ordering::Relaxed),
                                         IPP_COUNT.load(Ordering::Relaxed),
                                         ACTIVE_SHORTCUTS.load(Ordering::Relaxed),
                                         SAVED_BY_APPROX.load(Ordering::Relaxed),
                                         CONSIDERED_FOR_APPROX.load(Ordering::Relaxed),
                                         PATH_SOURCES_COUNT.load(Ordering::Relaxed),
                                         ACTUALLY_MERGED.load(Ordering::Relaxed),
                                         ACTUALLY_LINKED.load(Ordering::Relaxed),
                                         UNNECESSARY_LINKED.load(Ordering::Relaxed)));
                        } else {
                            events.push((timer.get_passed_ms() / 1000, NODES_CUSTOMIZED.load(Ordering::Relaxed), 0, 0, 0, 0, 0, 0, 0, 0));
                        }


                        if let Ok(()) | Err(RecvTimeoutError::Disconnected) = rx.recv_timeout(std::time::Duration::from_secs(3)) {
                            events_tx.send(events).unwrap();
                            break;
                        }
                    }
                });

                SeperatorBasedParallelCustomization {
                    cch: &cch,
                    separators: cch.separators(),
                    customize_cell: ftd_cch::create_customization_fn(&cch, metric, SeqIter(&cch)),
                    customize_separator: ftd_cch::create_customization_fn(&cch, metric, ParIter(&cch)),
                    _t: std::marker::PhantomData,
                }.customize(&mut upward, &mut downward);
            });

            tx.send(()).unwrap();

            for events in events_rx {
                let mut events_ctxt = push_collection_context("events".to_string());

                for event in events {
                    let _event = events_ctxt.push_collection_item();

                    report_silent!("at_s", event.0);
                    report_silent!("nodes_customized", event.1);
                    if cfg!(feature = "detailed-stats") {
                        report_silent!("num_ipps_stored", event.2);
                        report_silent!("num_shortcuts_active", event.3);
                        report_silent!("num_ipps_reduced_by_approx", event.4);
                        report_silent!("num_ipps_considered_for_approx", event.5);
                        report_silent!("num_shortcut_merge_points", event.6);
                        report_silent!("num_performed_merges", event.7);
                        report_silent!("num_performed_links", event.8);
                        report_silent!("num_performed_unnecessary_links", event.9);
                    }
                }
            }

            drop(subctxt);
        }

        if cfg!(feature = "detailed-stats") {
            report!("num_ipps_stored", IPP_COUNT.load(Ordering::Relaxed));
            report!("num_shortcuts_active", ACTIVE_SHORTCUTS.load(Ordering::Relaxed));
            report!("num_ipps_reduced_by_approx", SAVED_BY_APPROX.load(Ordering::Relaxed));
            report!("num_ipps_considered_for_approx", CONSIDERED_FOR_APPROX.load(Ordering::Relaxed));
            report!("num_shortcut_merge_points", PATH_SOURCES_COUNT.load(Ordering::Relaxed));
            report!("num_performed_merges", ACTUALLY_MERGED.load(Ordering::Relaxed));
            report!("num_performed_links", ACTUALLY_LINKED.load(Ordering::Relaxed));
            report!("num_performed_unnecessary_links", UNNECESSARY_LINKED.load(Ordering::Relaxed));
        }
        report!("approx", f64::from(APPROX));
        report!("approx_threshold", APPROX_THRESHOLD);

        if cfg!(feature = "tdcch-postcustomization") {
            let _subctxt = push_context("postcustomization".to_string());
            report_time("TD-CCH Post-Customization", || {
                let mut removed_by_perfection = 0;
                let mut node_edge_ids = vec![InRangeOption::new(None); n as usize];

                let upward_preliminary_bounds: Vec<_> = upward.iter().map(|s| s.lower_bound).collect();
                let downward_preliminary_bounds: Vec<_> = downward.iter().map(|s| s.lower_bound).collect();

                for current_node in (0..n).rev() {
                    for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                        node_edge_ids[node as usize] = InRangeOption::new(Some(edge_id));
                    }

                    for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                        let shortcut_edge_ids = cch.neighbor_edge_indices(node);
                        for (target, shortcut_edge_id) in cch.neighbor_iter(node).zip(shortcut_edge_ids) {
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

                    for node in cch.neighbor_iter(current_node) {
                        node_edge_ids[node as usize] = InRangeOption::new(None);
                    }
                }

                for (shortcut, lower_bound) in upward.iter_mut().zip(upward_preliminary_bounds.into_iter()) {
                    if shortcut.upper_bound.fuzzy_lt(lower_bound) {
                        if shortcut.required { removed_by_perfection += 1; }
                        shortcut.required = false;
                        shortcut.lower_bound = FlWeight::INFINITY;
                        shortcut.upper_bound = FlWeight::INFINITY;
                    } else {
                        shortcut.lower_bound = lower_bound;
                    }
                }

                for (shortcut, lower_bound) in downward.iter_mut().zip(downward_preliminary_bounds.into_iter()) {
                    if shortcut.upper_bound.fuzzy_lt(lower_bound) {
                        if shortcut.required { removed_by_perfection += 1; }
                        shortcut.required = false;
                        shortcut.lower_bound = FlWeight::INFINITY;
                        shortcut.upper_bound = FlWeight::INFINITY;
                    } else {
                        shortcut.lower_bound = lower_bound;
                    }
                }

                for current_node in 0..n {
                    let (upward_below, upward_above) = upward.split_at_mut(cch.first_out[current_node as usize] as usize);
                    let upward_active = &mut upward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
                    let (downward_below, downward_above) = downward.split_at_mut(cch.first_out[current_node as usize] as usize);
                    let downward_active = &mut downward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
                    let shortcut_graph = PartialShortcutGraph::new(metric, upward_below, downward_below, 0);

                    for shortcut in &mut upward_active[..] {
                        shortcut.invalidate_unneccesary_sources(&shortcut_graph);
                    }

                    for shortcut in &mut downward_active[..] {
                        shortcut.invalidate_unneccesary_sources(&shortcut_graph);
                    }
                }

                report!("removed_by_perfection", removed_by_perfection);
            });
        }

        ShortcutGraph::new(metric, &cch.first_out, &cch.head, upward, downward)
    }

    fn create_customization_fn<'s, F: 's>(cch: &'s CCH, metric: &'s TDGraph, merge_iter: F) -> impl Fn(Range<usize>, usize, &mut [Shortcut], &mut [Shortcut]) + 's where
        for <'p> F: ForEachIter<'p, 's>,
    {
        move |nodes, edge_offset, upward: &mut [Shortcut], downward: &mut [Shortcut]| {
            for current_node in nodes {

                let (upward_below, upward_above) = upward.split_at_mut(cch.first_out[current_node as usize] as usize - edge_offset);
                let upward_active = &mut upward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
                let (downward_below, downward_above) = downward.split_at_mut(cch.first_out[current_node as usize] as usize - edge_offset);
                let downward_active = &mut downward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
                let shortcut_graph = PartialShortcutGraph::new(metric, upward_below, downward_below, edge_offset);

                debug_assert_eq!(upward_active.len(), cch.degree(current_node as NodeId));
                debug_assert_eq!(downward_active.len(), cch.degree(current_node as NodeId));

                merge_iter.for_each(current_node as NodeId, upward_active, downward_active, |((&node, upward_shortcut), downward_shortcut)| {
                    MERGE_BUFFERS.with(|buffers| {
                        let mut buffers = buffers.borrow_mut();

                        let mut triangles = Vec::new();

                        let mut current_iter = cch.inverted.neighbor_iter(current_node as NodeId).peekable();
                        let mut other_iter = cch.inverted.neighbor_iter(node as NodeId).peekable();

                        while let (Some(Link { node: lower_from_current, weight: edge_from_cur_id }), Some(Link { node: lower_from_other, weight: edge_from_oth_id })) = (current_iter.peek(), other_iter.peek()) {
                            debug_assert_eq!(cch.head()[*edge_from_cur_id as usize], current_node as NodeId);
                            debug_assert_eq!(cch.head()[*edge_from_oth_id as usize], node);
                            debug_assert_eq!(cch.edge_id_to_tail(*edge_from_cur_id), *lower_from_current);
                            debug_assert_eq!(cch.edge_id_to_tail(*edge_from_oth_id), *lower_from_other);

                            if lower_from_current < lower_from_other {
                                current_iter.next();
                            } else if lower_from_other < lower_from_current {
                                other_iter.next();
                            } else {
                                triangles.push((*edge_from_cur_id, *edge_from_oth_id));

                                current_iter.next();
                                other_iter.next();
                            }
                        }
                        if cfg!(feature = "tdcch-triangle-sorting") {
                            triangles.sort_by_key(|&(down, up)| shortcut_graph.get_incoming(down).lower_bound + shortcut_graph.get_outgoing(up).lower_bound);
                        }
                        for &edges in &triangles {
                            upward_shortcut.merge(edges, &shortcut_graph, &mut buffers);
                        }
                        upward_shortcut.finalize_bounds(&shortcut_graph);

                        if cfg!(feature = "tdcch-triangle-sorting") {
                            triangles.sort_by_key(|&(up, down)| shortcut_graph.get_incoming(down).lower_bound + shortcut_graph.get_outgoing(up).lower_bound);
                        }
                        for &(up, down) in &triangles {
                            downward_shortcut.merge((down, up), &shortcut_graph, &mut buffers);
                        }
                        downward_shortcut.finalize_bounds(&shortcut_graph);
                    });
                });

                for Link { weight: edge_id, .. } in cch.inverted.neighbor_iter(current_node as NodeId) {
                    upward[edge_id as usize - edge_offset].clear_plf();
                    downward[edge_id as usize - edge_offset].clear_plf();
                }

                NODES_CUSTOMIZED.fetch_add(1, Ordering::Relaxed);
            }
        }
    }

    thread_local! { static MERGE_BUFFERS: RefCell<MergeBuffers> = RefCell::new(MergeBuffers::new()); }

    trait ForEachIter<'s, 'c> {
        fn for_each(&self, current_node: NodeId, upward_active: &'s mut [Shortcut], downward_active: &'s mut [Shortcut], f: impl Send + Sync + Fn(((&'c NodeId, &'s mut Shortcut), &'s mut Shortcut)));
    }

    struct SeqIter<'c>(&'c CCH);

    impl<'s, 'c> ForEachIter<'s, 'c> for SeqIter<'c> {
        fn for_each(&self, current_node: NodeId, upward_active: &'s mut [Shortcut], downward_active: &'s mut [Shortcut], f: impl Send + Sync + Fn(((&'c NodeId, &'s mut Shortcut), &'s mut Shortcut))) {
            self.0.head[self.0.neighbor_edge_indices_usize(current_node)].iter()
                .zip(upward_active.iter_mut())
                .zip(downward_active.iter_mut())
                .for_each(f);
        }
    }

    struct ParIter<'c>(&'c CCH);

    impl<'s, 'c> ForEachIter<'s, 'c> for ParIter<'c> {
        fn for_each(&self, current_node: NodeId, upward_active: &'s mut [Shortcut], downward_active: &'s mut [Shortcut], f: impl Send + Sync + Fn(((&'c NodeId, &'s mut Shortcut), &'s mut Shortcut))) {
            self.0.head[self.0.neighbor_edge_indices_usize(current_node)].par_iter()
                .zip_eq(upward_active.par_iter_mut())
                .zip_eq(downward_active.par_iter_mut())
                .for_each(f);
        }
    }
}

#[derive(Debug)]
pub struct CCHReordering<'a> {
    pub node_order: NodeOrder,
    pub latitude: &'a [f32],
    pub longitude: &'a [f32],
}

impl<'a> CCHReordering<'a> {
    fn distance (&self, n1: NodeId, n2: NodeId) -> NonNan {
        use nav_types::WGS84;
        NonNan::new(WGS84::new(self.latitude[self.node_order.node(n1) as usize], self.longitude[self.node_order.node(n1) as usize], 0.0)
            .distance(&WGS84::new(self.latitude[self.node_order.node(n2) as usize], self.longitude[self.node_order.node(n2) as usize], 0.0))).unwrap()
    }

    fn reorder_sep(&self, nodes: &mut [NodeId]) {
        let furthest = nodes.first().map(|&first| {
            nodes.iter().max_by_key(|&&node| self.distance(first, node)).unwrap()
        });

        if let Some(&furthest) = furthest {
            nodes.sort_by_key(|&node| self.distance(node, furthest))
        }
    }

    fn reorder_tree(&self, separators: &mut SeparatorTree, level: usize) {
        if level > 2 { return }

        self.reorder_sep(&mut separators.nodes);
        for child in &mut separators.children {
            self.reorder_tree(child, level + 1);
            // if let Some(&first) = child.nodes.first() {
            //     if let Some(&last) = child.nodes.last() {
            //         if let Some(&node) = separators.nodes.first() {
            //             if self.distance(first, node) < self.distance(last, node) {
            //                 child.nodes.reverse()
            //             }
            //         }
            //     }
            // }
        }
    }

    fn reorder_children_by_size(&self, separators: &mut SeparatorTree) {
        separators.children.sort_unstable_by_key(|c| c.num_nodes);

        for child in &mut separators.children {
            self.reorder_children_by_size(child);
        }
    }

    fn to_ordering(&self, seperators: SeparatorTree, order: &mut Vec<NodeId>) {
        order.extend(seperators.nodes);
        for child in seperators.children {
            self.to_ordering(child, order);
        }
    }

    pub fn reorder(self, mut separators: SeparatorTree) -> NodeOrder {
        self.reorder_tree(&mut separators, 0);
        let mut order = Vec::new();
        self.to_ordering(separators, &mut order);

        for rank in &mut order {
            *rank = self.node_order.node(*rank);
        }
        order.reverse();

        NodeOrder::from_node_order(order)
    }

    pub fn reorder_for_seperator_based_customization(self, mut separators: SeparatorTree) -> NodeOrder {
        self.reorder_children_by_size(&mut separators);
        let mut order = Vec::new();
        self.to_ordering(separators, &mut order);

        for rank in &mut order {
            *rank = self.node_order.node(*rank);
        }
        order.reverse();

        NodeOrder::from_node_order(order)
    }
}
