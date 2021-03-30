//! CATCHUp query algorithm.

use super::*;

pub use crate::algo::customizable_contraction_hierarchy::ftd_cch::customize;

use floating_td_stepped_elimination_tree::{QueryProgress, *};

use crate::algo::customizable_contraction_hierarchy::*;
use crate::datastr::clearlist_vector::ClearlistVector;
use crate::datastr::graph::floating_time_dependent::PeriodicPiecewiseLinearFunction;
use crate::datastr::graph::floating_time_dependent::*;
use crate::datastr::index_heap::{IndexdMinHeap, Indexing};
use crate::datastr::rank_select_map::{BitVec, FastClearBitVec};
#[cfg(feature = "tdcch-query-detailed-timing")]
use crate::report::benchmark::Timer;
use time::Duration;

/// Query server struct for CATCHUp.
/// Implements the common query trait.
#[derive(Debug)]
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

    // Distances for Dijkstra/A* phase
    distances: ClearlistVector<Option<Box<[TTFPoint]>>>,
    // Distances estimates to target used as A* potentials
    lower_bounds_to_target: ClearlistVector<FlWeight>,
    // Distances estimates to target used as A* potentials
    bounds: ClearlistVector<(FlWeight, FlWeight)>,
    // Priority queue for Dijkstra/A* phase
    closest_node_priority_queue: IndexdMinHeap<State<FlWeight>>,
    // Bitset to mark all (upward) edges in the search space
    relevant_upward: FastClearBitVec,
    // Bitset to mark all (upward) edges in the search space
    relevant_original: FastClearBitVec,

    from: NodeId,
    to: NodeId,
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
            distances: ClearlistVector::new(n, None),
            bounds: ClearlistVector::new(n, (FlWeight::INFINITY, FlWeight::INFINITY)),
            lower_bounds_to_target: ClearlistVector::new(n, FlWeight::INFINITY),
            forward_tree_mask: BitVec::new(n),
            backward_tree_mask: BitVec::new(n),
            closest_node_priority_queue: IndexdMinHeap::new(n),
            relevant_upward: FastClearBitVec::new(m),
            relevant_original: FastClearBitVec::new(m_orig),
            from: 0,
            to: 0,
        }
    }

    #[allow(clippy::collapsible_if)]
    #[allow(clippy::cognitive_complexity)]
    pub fn distance(&mut self, from_node: NodeId, to_node: NodeId) -> Option<Box<[TTFPoint]>> {
        report!("algo", "Floating TDCCH Profile Query");

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let timer = Timer::new();

        self.from = self.cch_graph.node_order().rank(from_node);
        self.to = self.cch_graph.node_order().rank(to_node);

        let n = self.customized_graph.original_graph.num_nodes();

        // initialize
        let mut tentative_distance = (FlWeight::INFINITY, FlWeight::INFINITY);
        self.distances.reset();
        self.distances[self.from as usize] = Some(Box::new([TTFPoint {
            at: Timestamp::ZERO,
            val: FlWeight::ZERO,
        }]));
        self.bounds.reset();
        self.bounds[self.from as usize] = (FlWeight::ZERO, FlWeight::ZERO);
        self.lower_bounds_to_target.reset();
        self.meeting_nodes.clear();
        self.forward.initialize_query(self.from);
        self.backward.initialize_query(self.to);
        self.closest_node_priority_queue.clear();
        self.relevant_upward.clear();
        self.relevant_original.clear();

        self.forward_tree_path.clear();
        self.backward_tree_path.clear();

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let init_time = timer.get_passed();

        // stats
        let mut nodes_in_elimination_tree_search_space = 0;
        let mut relaxed_elimination_tree_arcs = 0;
        let mut relaxed_shortcut_arcs = 0;
        let mut relaxed_original_arcs = 0;
        let mut num_settled_nodes = 0;

        // elimination tree corridor query
        while self.forward.peek_next().is_some() || self.backward.peek_next().is_some() {
            // advance the direction which currently is at the lower rank
            if self.forward.peek_next().unwrap_or(n as NodeId) <= self.backward.peek_next().unwrap_or(n as NodeId) {
                // while we're here, clean up the forward_tree_mask
                // this is fine because we only check the flag for nodes on the tree path
                // we only set this to true when the node is guaranteed to be in the corridor.
                self.forward_tree_mask.unset_all_around(self.forward.peek_next().unwrap() as usize);

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
                if self.forward.node_data(self.forward.peek_next().unwrap()).lower_bound > tentative_distance.1
                    || (cfg!(feature = "tdcch-stall-on-demand") && stall())
                {
                    self.forward.skip_next();
                } else if let QueryProgress::Progress(node) = self.forward.next_step() {
                    if cfg!(feature = "detailed-stats") {
                        nodes_in_elimination_tree_search_space += 1;
                    }
                    if cfg!(feature = "detailed-stats") {
                        relaxed_elimination_tree_arcs += self.customized_graph.outgoing.degree(node);
                    }

                    // push node to tree path so we can efficiently walk back down later
                    self.forward_tree_path.push(node);

                    let lower_bound = self.forward.node_data(node).lower_bound + self.backward.node_data(node).lower_bound;
                    let upper_bound = self.forward.node_data(node).upper_bound + self.backward.node_data(node).upper_bound;

                    // we use this distance later for some pruning
                    // so here, we already store upper_bound + epsilon as a distance
                    // this allows for pruning but guarantees that we will later improve it with a real distance, even if its exactly upper_bound
                    self.bounds[node as usize].1 = min(self.bounds[node as usize].1, upper_bound + FlWeight::new(EPSILON));

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
                // while we're here, clean up the forward_tree_mask
                // this is fine because we only check the flag for nodes on the tree path
                // we only set this to true when the node is guaranteed to be in the corridor.
                self.backward_tree_mask.unset_all_around(self.backward.peek_next().unwrap() as usize);

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
        let tentative_latest_arrival = tentative_upper_bound;

        // all meeting nodes are in the corridor
        for &(node, _) in self
            .meeting_nodes
            .iter()
            .filter(|(_, lower_bound)| !tentative_upper_bound.fuzzy_lt(*lower_bound) && lower_bound.fuzzy_lt(FlWeight::INFINITY))
        {
            self.forward_tree_mask.set(node as usize);
            self.backward_tree_mask.set(node as usize);
        }

        // for all nodes on the path from root to target
        while let Some(node) = self.backward_tree_path.pop() {
            // if the node is in the corridor
            if self.backward_tree_mask.get(node as usize) {
                // set lower bound to target for the A* potentials later on
                self.lower_bounds_to_target[node as usize] = self.backward.node_data(node).lower_bound;
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
                }
            }
        }

        // for all nodes on the path from root to origin
        while let Some(node) = self.forward_tree_path.pop() {
            // if the node is in the corridor
            if self.forward_tree_mask.get(node as usize) {
                // for all nodes that were also in the backward search space,
                // (the ones where forward and backward path overlap)
                // we already know a lower bound to target.
                // we are going to propagate this downward the corridor towards the source and all nodes on the way
                let reverse_lower = self.lower_bounds_to_target[node as usize];
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
                    // update (relax) parent lower bound to target
                    self.lower_bounds_to_target[label.parent as usize] = min(
                        self.lower_bounds_to_target[label.parent as usize],
                        reverse_lower + label.lower_bound - self.forward.node_data(label.parent).lower_bound,
                    );
                    // mark parent as in corridor
                    self.forward_tree_mask.set(label.parent as usize);
                    // mark edge as in search space
                    self.relevant_upward.set(label.shortcut_id as usize);
                }
            }
        }

        // now we have the corridor so we proceed to the Dijkstra/A* phase

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let forward_select_time = timer.get_passed();

        let relevant_upward = &mut self.relevant_upward;
        debug_assert!(
            tentative_distance.0.fuzzy_eq(self.lower_bounds_to_target[self.from as usize]),
            "{:?}",
            dbg_each!(tentative_distance, self.lower_bounds_to_target[self.from as usize])
        );
        debug_assert!(
            FlWeight::ZERO.fuzzy_eq(self.lower_bounds_to_target[self.to as usize]),
            "{:?}",
            dbg_each!(self.lower_bounds_to_target[self.to as usize])
        );
        let lower_bounds_to_target = &mut self.lower_bounds_to_target;
        let relevant_original = &mut self.relevant_original;

        self.closest_node_priority_queue.push(State {
            key: tentative_distance.0,
            node: self.from,
        });

        let mut relax_timer = Timer::new();
        let mut unpack_time = Duration::zero();
        let mut link_merge_time = Duration::zero();

        // while there is a node in the queue
        while let Some(State { node, .. }) = self.closest_node_priority_queue.pop() {
            if cfg!(feature = "detailed-stats") {
                num_settled_nodes += 1;
            }

            debug_assert!(!self.bounds[node as usize].1.fuzzy_lt(self.bounds[node as usize].1));
            let lower_bound_from_source = self.bounds[node as usize].0;

            if self.bounds[self.to as usize].1.fuzzy_lt(lower_bound_from_source) {
                break;
            }

            relax_timer.restart();

            // for each outgoing (forward) edge
            for ((target, shortcut_id), (shortcut_lower_bound, _shortcut_upper_bound)) in self.customized_graph.upward_bounds_graph().neighbor_iter(node) {
                // if its in the search space
                // and can improve the distance of the target according to the bounds
                if relevant_upward.get(shortcut_id as usize)
                    && !min(tentative_latest_arrival, self.bounds[target as usize].1).fuzzy_lt(lower_bound_from_source + shortcut_lower_bound)
                {
                    if cfg!(feature = "detailed-stats") {
                        relaxed_shortcut_arcs += 1;
                    }

                    let lower_bound_target = lower_bounds_to_target[target as usize];

                    self.customized_graph.outgoing.add_first_original_arcs_to_searchspace(
                        shortcut_id,
                        lower_bound_target,
                        self.customized_graph,
                        lower_bounds_to_target,
                        relevant_original,
                        &mut |edge_id| relevant_upward.set(edge_id as usize),
                    );
                }
            }

            // for most nodes we relax down edges implicitly through the evaluate_next_segment_at method.
            // but when we have a node in the downward elimination tree corridor we also need to relax downward shortcuts
            if self.backward_tree_mask.get(node as usize) {
                let upper_bound = self.backward.node_data(node).upper_bound;

                // for all labels
                for label in self
                    .backward
                    .node_data(node)
                    .labels
                    .iter()
                    .filter(|label| !upper_bound.fuzzy_lt(label.lower_bound))
                {
                    // check by bounds if we need the edge
                    if !min(tentative_latest_arrival, self.bounds[label.parent as usize].1)
                        .fuzzy_lt(lower_bound_from_source + self.customized_graph.incoming.bounds()[label.shortcut_id as usize].0)
                    {
                        if cfg!(feature = "detailed-stats") {
                            relaxed_shortcut_arcs += 1;
                        }

                        let lower_bound_target = lower_bounds_to_target[label.parent as usize];
                        // if so do the same crazy relaxation as for upward edges
                        self.customized_graph.incoming.add_first_original_arcs_to_searchspace(
                            label.shortcut_id,
                            lower_bound_target,
                            self.customized_graph,
                            lower_bounds_to_target,
                            relevant_original,
                            &mut |edge_id| relevant_upward.set(edge_id as usize),
                        );
                    }
                }
            }

            unpack_time = unpack_time + relax_timer.get_passed();
            relax_timer.restart();

            for (head, edge_id) in self
                .customized_graph
                .original_graph
                .neighbor_and_edge_id_iter(self.cch_graph.node_order().node(node))
                .filter(|&(_, edge)| relevant_original.get(edge as usize))
            {
                if cfg!(feature = "detailed-stats") {
                    relaxed_original_arcs += 1;
                }

                let head = self.cch_graph.node_order().rank(head);
                let lower = if cfg!(feature = "tdcch-query-astar") {
                    lower_bounds_to_target[head as usize]
                } else {
                    FlWeight::ZERO
                };

                let current_better = |bounds: &mut ClearlistVector<(FlWeight, FlWeight)>, queue: &mut IndexdMinHeap<State<FlWeight>>| {
                    let next = State {
                        key: bounds[head as usize].0 + lower,
                        node: head,
                    };
                    if let Some(label) = queue.get(next.as_index()) {
                        if next < *label {
                            queue.decrease_key(next);
                        }
                    }
                };

                let update = |update: Box<[TTFPoint]>,
                              update_bounds: (FlWeight, FlWeight),
                              distances: &mut ClearlistVector<Option<Box<[TTFPoint]>>>,
                              bounds: &mut ClearlistVector<(FlWeight, FlWeight)>,
                              queue: &mut IndexdMinHeap<State<FlWeight>>| {
                    bounds[head as usize].0 = min(bounds[head as usize].0, update_bounds.0);
                    bounds[head as usize].1 = min(bounds[head as usize].1, update_bounds.1);
                    let next = State {
                        key: update_bounds.0 + lower,
                        node: head,
                    };
                    distances[head as usize] = Some(update);
                    if queue.contains_index(next.as_index()) {
                        queue.decrease_key(next);
                    } else {
                        queue.push(next);
                    }
                };

                let edge_ttf = self.customized_graph.original_graph.travel_time_function(edge_id);

                if self.bounds[head as usize].1.fuzzy_lt(lower_bound_from_source + edge_ttf.lower_bound())
                    || self.bounds[self.to as usize]
                        .1
                        .fuzzy_lt(lower_bound_from_source + edge_ttf.lower_bound() + lower)
                {
                    current_better(&mut self.bounds, &mut self.closest_node_priority_queue);
                    continue;
                }

                let linked = PeriodicPiecewiseLinearFunction::new(self.distances[node as usize].as_ref().unwrap()).link(&edge_ttf);
                let linked_ttf = PeriodicPiecewiseLinearFunction::new(&linked[..]);
                let linked_lower_bound = linked_ttf.lower_bound();
                let linked_upper_bound = linked_ttf.upper_bound();

                if self.bounds[head as usize].1.fuzzy_lt(linked_lower_bound) {
                    current_better(&mut self.bounds, &mut self.closest_node_priority_queue);
                    continue;
                } else if linked_upper_bound.fuzzy_lt(self.bounds[head as usize].0) {
                    update(
                        linked.into(),
                        (linked_lower_bound, linked_upper_bound),
                        &mut self.distances,
                        &mut self.bounds,
                        &mut self.closest_node_priority_queue,
                    );
                    continue;
                }

                if let Some(current_ttf) = self.distances[head as usize].as_ref() {
                    let (merged_raw, intersections) = PeriodicPiecewiseLinearFunction::new(current_ttf).merge(&linked_ttf, &mut Vec::new());

                    match &intersections[..] {
                        &[(_, true)] => {
                            current_better(&mut self.bounds, &mut self.closest_node_priority_queue);
                        }
                        &[(_, false)] => {
                            update(
                                linked.into(),
                                (linked_lower_bound, linked_upper_bound),
                                &mut self.distances,
                                &mut self.bounds,
                                &mut self.closest_node_priority_queue,
                            );
                        }
                        _ => {
                            let merged = PeriodicPiecewiseLinearFunction::new(&merged_raw);
                            let merged_lower_bound = merged.lower_bound();
                            let merged_upper_bound = merged.upper_bound();
                            update(
                                merged_raw,
                                (merged_lower_bound, merged_upper_bound),
                                &mut self.distances,
                                &mut self.bounds,
                                &mut self.closest_node_priority_queue,
                            );
                        }
                    };
                } else {
                    update(
                        linked.into(),
                        (linked_lower_bound, linked_upper_bound),
                        &mut self.distances,
                        &mut self.bounds,
                        &mut self.closest_node_priority_queue,
                    );
                }
            }

            link_merge_time = link_merge_time + relax_timer.get_passed();
        }

        eprintln!(
            "unpack: {}ms, link/merge: {}ms",
            unpack_time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0,
            link_merge_time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0
        );

        if cfg!(feature = "detailed-stats") {
            report!("num_relaxed_shortcut_arcs", relaxed_shortcut_arcs);
            report!("num_relaxed_original_arcs", relaxed_original_arcs);
            report!("num_settled_nodes", num_settled_nodes);
        }

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let relax_time = timer.get_passed();

        #[cfg(feature = "tdcch-query-detailed-timing")]
        {
            eprintln!(
                "elimination tree: {} {:?}%",
                elimination_tree_time - init_time,
                100 * ((elimination_tree_time - init_time).num_nanoseconds().unwrap()) / relax_time.num_nanoseconds().unwrap()
            );
            eprintln!(
                "fw select: {} {:?}%",
                forward_select_time - elimination_tree_time,
                100 * ((forward_select_time - elimination_tree_time).num_nanoseconds().unwrap()) / relax_time.num_nanoseconds().unwrap()
            );
            eprintln!(
                "relax: {} {:?}%",
                relax_time - forward_select_time,
                100 * ((relax_time - forward_select_time).num_nanoseconds().unwrap()) / relax_time.num_nanoseconds().unwrap()
            );
        }

        self.distances[self.to as usize].take()
    }
}
