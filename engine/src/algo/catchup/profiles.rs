//! CATCHUp query algorithm.

use super::*;

pub use crate::algo::customizable_contraction_hierarchy::ftd_cch::customize;

use floating_td_stepped_elimination_tree::{QueryProgress, *};

use crate::algo::customizable_contraction_hierarchy::*;
use crate::datastr::graph::floating_time_dependent::*;
use crate::datastr::rank_select_map::{BitVec, FastClearBitVec};
#[cfg(feature = "tdcch-query-detailed-timing")]
use crate::report::benchmark::Timer;
// use time::Duration;

/// Query server struct for CATCHUp.
/// Implements the common query trait.
pub struct Server<'a> {
    // Corridor elimination tree query
    forward: FloatingTDSteppedEliminationTree<'a, 'a>,
    backward: FloatingTDSteppedEliminationTree<'a, 'a>,
    // static CCH stuff
    cch_graph: &'a CCH,
    // CATCHUp preprocessing
    customized_graph: &'a CustomizedGraph<'a>,

    // Middle nodes in corridor
    meeting_nodes: Vec<(NodeId, FlWeight)>,

    // For storing the paths through the elimination tree from the root to source/destination.
    // Because elimination only allows us to efficiently walk from node to root, but not the other way around
    forward_tree_path: Vec<NodeId>,
    backward_tree_path: Vec<NodeId>,
    // bitsets to mark the subset of nodes (subset of tree_path) in the elimination tree shortest path corridor
    backward_tree_mask: BitVec,
    forward_tree_mask: BitVec,

    // Bitset to mark all (upward) edges in the search space
    relevant_upward: FastClearBitVec,
    // Bitset to mark all (upward) edges in the search space
    relevant_original: FastClearBitVec,

    from: NodeId,
    to: NodeId,

    outgoing_profiles: Vec<Option<TTFCache<Box<[TTFPoint]>>>>,
    incoming_profiles: Vec<Option<TTFCache<Box<[TTFPoint]>>>>,
    buffers: MergeBuffers,

    downward_shortcut_offsets: Vec<usize>,
    upward_shortcut_offsets: Vec<usize>,
}

impl<'a> Server<'a> {
    pub fn new(cch_graph: &'a CCH, customized_graph: &'a CustomizedGraph<'a>) -> Self {
        let n = customized_graph.original_graph.num_nodes();
        let m_orig = customized_graph.original_graph.num_arcs();
        let m = cch_graph.num_arcs();

        Self {
            forward: FloatingTDSteppedEliminationTree::new(customized_graph.upward_bounds_graph(), cch_graph.elimination_tree()),
            backward: FloatingTDSteppedEliminationTree::new(customized_graph.downward_bounds_graph(), cch_graph.elimination_tree()),
            cch_graph,
            meeting_nodes: Vec::new(),
            customized_graph,
            forward_tree_path: Vec::new(),
            backward_tree_path: Vec::new(),
            forward_tree_mask: BitVec::new(n),
            backward_tree_mask: BitVec::new(n),
            relevant_upward: FastClearBitVec::new(m),
            relevant_original: FastClearBitVec::new(m_orig),
            from: 0,
            to: 0,
            outgoing_profiles: (0..customized_graph.outgoing.head().len()).map(|_| None).collect(),
            incoming_profiles: (0..customized_graph.incoming.head().len()).map(|_| None).collect(),
            buffers: MergeBuffers::new(),

            downward_shortcut_offsets: vec![n; n],
            upward_shortcut_offsets: vec![n; n],
        }
    }

    #[allow(clippy::collapsible_if)]
    #[allow(clippy::cognitive_complexity)]
    pub fn distance(&mut self, from_node: NodeId, to_node: NodeId) -> Shortcut {
        report!("algo", "Floating TDCCH Profile Query");

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let timer = Timer::new();

        self.from = self.cch_graph.node_order().rank(from_node);
        self.to = self.cch_graph.node_order().rank(to_node);

        let n = self.customized_graph.original_graph.num_nodes();

        // initialize
        let mut tentative_distance = (FlWeight::INFINITY, FlWeight::INFINITY);
        self.meeting_nodes.clear();
        self.forward.initialize_query(self.from);
        self.backward.initialize_query(self.to);
        self.relevant_upward.clear();
        self.relevant_original.clear();

        self.forward_tree_path.clear();
        self.backward_tree_path.clear();

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let init_time = timer.get_passed();

        // stats
        let mut nodes_in_elimination_tree_search_space = 0;
        let mut relaxed_elimination_tree_arcs = 0;

        // elimination tree corridor query
        while self.forward.peek_next().is_some() || self.backward.peek_next().is_some() {
            // advance the direction which currently is at the lower rank
            if self.forward.peek_next().unwrap_or(n as NodeId) <= self.backward.peek_next().unwrap_or(n as NodeId) {
                // experimental stall on demand
                // only works for forward search
                // currently deactivated by default
                let stall = || {
                    for ((target, _), (_, shortcut_upper_bound)) in
                        self.customized_graph.downward_bounds_graph().neighbor_iter(self.forward.peek_next().unwrap())
                    {
                        if self.forward.node_data(target).upper_bound + shortcut_upper_bound
                            < self.forward.node_data(self.forward.peek_next().unwrap()).lower_bound
                        {
                            return true;
                        }
                    }
                    false
                };

                // skip the node if it cannot possibly be in the shortest path corridor
                if self.forward.node_data(self.forward.peek_next().unwrap()).lower_bound > tentative_distance.1 || (cfg!(tdcch_stall_on_demand) && stall()) {
                    self.forward.skip_next();
                } else if let QueryProgress::Progress(node) = self.forward.next_step() {
                    if cfg!(feature = "detailed-stats") {
                        nodes_in_elimination_tree_search_space += 1;
                    }
                    if cfg!(feature = "detailed-stats") {
                        relaxed_elimination_tree_arcs += self.customized_graph.outgoing.degree(node);
                    }

                    // while we're here, clean up the forward_tree_mask
                    // this is fine because we only check the flag for nodes on the tree path
                    // we only set this to true when the node is guaranteed to be in the corridor.
                    self.forward_tree_mask.unset_all_around(node as usize);
                    // push node to tree path so we can efficiently walk back down later
                    self.forward_tree_path.push(node);

                    let lower_bound = self.forward.node_data(node).lower_bound + self.backward.node_data(node).lower_bound;
                    let upper_bound = self.forward.node_data(node).upper_bound + self.backward.node_data(node).upper_bound;

                    // improve tentative distance if possible
                    if !tentative_distance.1.fuzzy_lt(lower_bound) {
                        tentative_distance.0 = min(tentative_distance.0, lower_bound);
                        tentative_distance.1 = min(tentative_distance.1, upper_bound);
                        debug_assert!(!tentative_distance.1.fuzzy_lt(tentative_distance.0), "{:?}", tentative_distance);
                        self.meeting_nodes.push((node, lower_bound));
                    }
                } else {
                    unreachable!("inconsistent elimination tree state");
                }
            } else {
                // skip the node if it cannot possibly be in the shortest path corridor
                if self.backward.node_data(self.backward.peek_next().unwrap()).lower_bound > tentative_distance.1 {
                    self.backward.skip_next();
                } else if let QueryProgress::Progress(node) = self.backward.next_step() {
                    if cfg!(feature = "detailed-stats") {
                        nodes_in_elimination_tree_search_space += 1;
                    }
                    if cfg!(feature = "detailed-stats") {
                        relaxed_elimination_tree_arcs += self.customized_graph.incoming.degree(node);
                    }

                    // while we're here, clean up the forward_tree_mask
                    // this is fine because we only check the flag for nodes on the tree path
                    // we only set this to true when the node is guaranteed to be in the corridor.
                    self.backward_tree_mask.unset_all_around(node as usize);
                    // push node to tree path so we can efficiently walk back down later
                    self.backward_tree_path.push(node);

                    // improve tentative distance if possible
                    let lower_bound = self.forward.node_data(node).lower_bound + self.backward.node_data(node).lower_bound;
                    if !tentative_distance.1.fuzzy_lt(lower_bound) {
                        tentative_distance.0 = min(tentative_distance.0, lower_bound);
                        tentative_distance.1 = min(
                            tentative_distance.1,
                            self.forward.node_data(node).upper_bound + self.backward.node_data(node).upper_bound,
                        );
                        debug_assert!(!tentative_distance.1.fuzzy_lt(tentative_distance.0), "{:?}", tentative_distance);
                        self.meeting_nodes.push((node, lower_bound));
                    }
                } else {
                    unreachable!("inconsistent elimination tree state");
                }
            }
        }

        if cfg!(feature = "detailed-stats") {
            report!("num_nodes_in_elimination_tree_search_space", nodes_in_elimination_tree_search_space);
            report!("num_relaxed_elimination_tree_arcs", relaxed_elimination_tree_arcs);
            report!("num_meeting_nodes", self.meeting_nodes.len());
        }

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let elimination_tree_time = timer.get_passed();

        // elimination tree query done, now we want to retrieve the corridor

        let tentative_upper_bound = tentative_distance.1;

        // all meeting nodes are in the corridor
        self.meeting_nodes.retain(|(_, lower_bound)| !tentative_upper_bound.fuzzy_lt(*lower_bound));
        for &(node, _) in &self.meeting_nodes {
            self.forward_tree_mask.set(node as usize);
            self.backward_tree_mask.set(node as usize);
        }

        let mut profile_graph = ProfileGraph {
            customized_graph: &self.customized_graph,
            outgoing_cache: &mut self.outgoing_profiles,
            incoming_cache: &mut self.incoming_profiles,
        };

        // for all nodes on the path from root to target
        for &node in self.backward_tree_path.iter().rev() {
            // if the node is in the corridor
            if self.backward_tree_mask.get(node as usize) {
                let upper_bound = self.backward.node_data(node).upper_bound;

                // for all labels (parent pointers)
                for label in self
                    .backward
                    .node_data(node)
                    .labels
                    .iter()
                    // lazy label filtering
                    .filter(|label| !upper_bound.fuzzy_lt(label.lower_bound))
                {
                    // mark parent as in corridor
                    self.backward_tree_mask.set(label.parent as usize);

                    profile_graph.cache(ShortcutId::Incoming(label.shortcut_id), &mut self.buffers);
                }
            }
        }

        // for all nodes on the path from root to origin
        for &node in self.forward_tree_path.iter().rev() {
            // if the node is in the corridor
            if self.forward_tree_mask.get(node as usize) {
                // for all nodes that were also in the backward search space,
                // (the ones where forward and backward path overlap)
                // we already know a lower bound to target.
                // we are going to propagate this downward the corridor towards the source and all nodes on the way
                let upper_bound = self.forward.node_data(node).upper_bound;

                // for all labels (parent points)
                for label in self
                    .forward
                    .node_data(node)
                    .labels
                    .iter()
                    // lazy label filtering
                    .filter(|label| !upper_bound.fuzzy_lt(label.lower_bound))
                {
                    debug_assert!(self.customized_graph.outgoing.bounds()[label.shortcut_id as usize]
                        .0
                        .fuzzy_eq(label.lower_bound - self.forward.node_data(label.parent).lower_bound));
                    // mark parent as in corridor
                    self.forward_tree_mask.set(label.parent as usize);
                    // mark edge as in search space
                    self.relevant_upward.set(label.shortcut_id as usize);
                    profile_graph.cache(ShortcutId::Outgoing(label.shortcut_id), &mut self.buffers);
                }
            }
        }

        // TODO compute profiles of unpacked edges more efficiently

        let original_graph = &self.customized_graph.original_graph;
        let forward_tree_mask = &self.forward_tree_mask;
        let backward_tree_mask = &self.backward_tree_mask;
        let to = self.to;
        let from = self.from;
        self.forward_tree_path.retain(|&node| forward_tree_mask.get(node as usize) && node != to);
        self.backward_tree_path.retain(|&node| backward_tree_mask.get(node as usize) && node != from);

        for (offset, &node) in self.forward_tree_path[1..].iter().enumerate() {
            self.downward_shortcut_offsets[node as usize] = offset;
        }
        for (offset, &node) in self.backward_tree_path[1..].iter().enumerate() {
            self.upward_shortcut_offsets[node as usize] = offset;
        }

        let mut down_shortcuts: Vec<_> = self.forward_tree_path[1..].iter().map(|_| Shortcut::new(None, original_graph)).collect();
        let mut up_shortcuts: Vec<_> = self.backward_tree_path[1..].iter().map(|_| Shortcut::new(None, original_graph)).collect();

        for ((head, edge_id), _) in self.customized_graph.upward_bounds_graph().neighbor_iter(self.from) {
            if self.forward_tree_mask.get(head as usize) {
                down_shortcuts[self.downward_shortcut_offsets[head as usize]] = self.customized_graph.outgoing.to_shortcut(edge_id as usize);
                down_shortcuts[self.downward_shortcut_offsets[head as usize]].set_cache(profile_graph.take_cache(ShortcutId::Outgoing(edge_id)));
            }
        }

        for ((head, edge_id), _) in self.customized_graph.downward_bounds_graph().neighbor_iter(self.to) {
            if self.backward_tree_mask.get(head as usize) {
                up_shortcuts[self.upward_shortcut_offsets[head as usize]] = self.customized_graph.incoming.to_shortcut(edge_id as usize);
                up_shortcuts[self.upward_shortcut_offsets[head as usize]].set_cache(profile_graph.take_cache(ShortcutId::Incoming(edge_id)));
            }
        }

        let mut profile_graph = ProfileGraphWrapper {
            profile_graph,
            down_shortcuts: &mut down_shortcuts[..],
            up_shortcuts: &mut up_shortcuts[..],
        };

        let mut st_shortcut = Shortcut::new_finished(&[], tentative_distance, false);

        for (offset, &node) in self.forward_tree_path[1..].iter().chain(std::iter::once(&self.to)).enumerate() {
            let mut shortcut = Shortcut::new(None, &self.customized_graph.original_graph);
            std::mem::swap(&mut shortcut, profile_graph.down_shortcuts.get_mut(offset).unwrap_or(&mut st_shortcut));
            let upper_bound = self.forward.node_data(node).upper_bound;
            for label in self
                .forward
                .node_data(node)
                .labels
                .iter()
                // lazy label filtering
                .filter(|label| !upper_bound.fuzzy_lt(label.lower_bound))
            {
                if label.parent != self.from {
                    shortcut.merge(
                        (
                            (self.downward_shortcut_offsets[label.parent as usize] + self.customized_graph.incoming.head().len()) as EdgeId,
                            label.shortcut_id,
                        ),
                        &profile_graph,
                        &mut self.buffers,
                    );
                }
            }
            if offset < profile_graph.down_shortcuts.len() {
                shortcut.finalize_bounds(&profile_graph);
            }
            std::mem::swap(&mut shortcut, profile_graph.down_shortcuts.get_mut(offset).unwrap_or(&mut st_shortcut));
        }

        for (offset, &node) in self.backward_tree_path[1..].iter().chain(std::iter::once(&self.from)).enumerate() {
            let mut shortcut = Shortcut::new(None, &self.customized_graph.original_graph);
            std::mem::swap(&mut shortcut, profile_graph.up_shortcuts.get_mut(offset).unwrap_or(&mut st_shortcut));
            let upper_bound = self.backward.node_data(node).upper_bound;
            for label in self
                .backward
                .node_data(node)
                .labels
                .iter()
                // lazy label filtering
                .filter(|label| !upper_bound.fuzzy_lt(label.lower_bound))
            {
                if label.parent != self.to {
                    shortcut.merge(
                        (
                            label.shortcut_id,
                            (self.upward_shortcut_offsets[label.parent as usize] + self.customized_graph.outgoing.head().len()) as EdgeId,
                        ),
                        &profile_graph,
                        &mut self.buffers,
                    );
                }
            }

            if offset < profile_graph.up_shortcuts.len() {
                shortcut.finalize_bounds(&profile_graph);
            }
            std::mem::swap(&mut shortcut, profile_graph.up_shortcuts.get_mut(offset).unwrap_or(&mut st_shortcut));
        }

        for &(node, _) in &self.meeting_nodes {
            if node == self.from || node == self.to {
                continue;
            }
            st_shortcut.merge(
                (
                    (self.downward_shortcut_offsets[node as usize] + self.customized_graph.incoming.head().len()) as EdgeId,
                    (self.upward_shortcut_offsets[node as usize] + self.customized_graph.outgoing.head().len()) as EdgeId,
                ),
                &profile_graph,
                &mut self.buffers,
            )
        }

        // TODO path switch points

        while let Some(node) = self.backward_tree_path.pop() {
            if self.backward_tree_mask.get(node as usize) {
                for label in self.backward.node_data(node).labels.iter() {
                    profile_graph.profile_graph.clear(ShortcutId::Incoming(label.shortcut_id));
                }
            }
        }

        while let Some(node) = self.forward_tree_path.pop() {
            if self.forward_tree_mask.get(node as usize) {
                for label in self.forward.node_data(node).labels.iter() {
                    profile_graph.profile_graph.clear(ShortcutId::Outgoing(label.shortcut_id));
                }
            }
        }

        st_shortcut
    }
}
