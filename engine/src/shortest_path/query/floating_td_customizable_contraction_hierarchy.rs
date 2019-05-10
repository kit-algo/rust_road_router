use std::cmp::*;
use crate::report::*;
use crate::util::*;

use super::*;
use super::floating_td_stepped_elimination_tree::{*, QueryProgress};

use crate::graph::floating_time_dependent::*;
use crate::shortest_path::customizable_contraction_hierarchy::cch_graph::CCHGraph;
use crate::shortest_path::timestamped_vector::TimestampedVector;
use crate::rank_select_map::{BitVec, FastClearBitVec};
#[cfg(feature = "tdcch-query-detailed-timing")]
use crate::benchmark::Timer;
use crate::index_heap::{IndexdMinHeap, Indexing};

#[derive(Copy, Clone, Eq, PartialEq, Debug, Ord, PartialOrd)]
pub struct State {
    pub distance: Timestamp,
    pub node: NodeId,
}

impl Indexing for State {
    fn as_index(&self) -> usize {
        self.node as usize
    }
}

#[derive(Debug)]
pub struct Server<'a> {
    forward: FloatingTDSteppedEliminationTree<'a, 'a>,
    backward: FloatingTDSteppedEliminationTree<'a, 'a>,
    cch_graph: &'a CCHGraph,
    customized_graph: &'a CustomizedGraph<'a>,
    meeting_nodes: Vec<(NodeId, FlWeight)>,
    forward_tree_path: Vec<NodeId>,
    backward_tree_path: Vec<NodeId>,
    distances: TimestampedVector<Timestamp>,
    lower_bounds_to_target: TimestampedVector<FlWeight>,
    parents: Vec<(NodeId, EdgeId)>,
    backward_tree_mask: BitVec,
    forward_tree_mask: BitVec,
    closest_node_priority_queue: IndexdMinHeap<State>,
    relevant_upward: FastClearBitVec,
    from: NodeId,
    to: NodeId,
}

impl<'a> Server<'a> {
    pub fn new(cch_graph: &'a CCHGraph, customized_graph: &'a CustomizedGraph<'a>) -> Self {
        let n = customized_graph.original_graph.num_nodes();
        let m = cch_graph.num_arcs();
        Self {
            forward: FloatingTDSteppedEliminationTree::new(customized_graph.upward_bounds_graph(), cch_graph.elimination_tree()),
            backward: FloatingTDSteppedEliminationTree::new(customized_graph.downward_bounds_graph(), cch_graph.elimination_tree()),
            cch_graph,
            meeting_nodes: Vec::new(),
            customized_graph,
            forward_tree_path: Vec::new(),
            backward_tree_path: Vec::new(),
            distances: TimestampedVector::new(n, Timestamp::NEVER),
            lower_bounds_to_target: TimestampedVector::new(n, FlWeight::INFINITY),
            parents: vec![(std::u32::MAX, std::u32::MAX); n],
            forward_tree_mask: BitVec::new(n),
            backward_tree_mask: BitVec::new(n),
            closest_node_priority_queue: IndexdMinHeap::new(n),
            relevant_upward: FastClearBitVec::new(m),
            from: 0,
            to: 0
        }
    }

    #[allow(clippy::collapsible_if)]
    #[allow(clippy::cognitive_complexity)]
    pub fn distance(&mut self, from_node: NodeId, to_node: NodeId, departure_time: Timestamp) -> Option<FlWeight> {
        report!("algo", "Floating TDCCH Query");

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let timer = Timer::new();

        self.from = self.cch_graph.node_order().rank(from_node);
        self.to = self.cch_graph.node_order().rank(to_node);

        let n = self.customized_graph.original_graph.num_nodes();

        // initialize
        let mut tentative_distance = (FlWeight::INFINITY, FlWeight::INFINITY);
        self.distances.reset();
        self.distances.set(self.from as usize, departure_time);
        self.lower_bounds_to_target.reset();
        self.meeting_nodes.clear();
        self.forward.initialize_query(self.from);
        self.backward.initialize_query(self.to);
        self.closest_node_priority_queue.clear();
        self.relevant_upward.clear();

        self.forward_tree_path.clear();
        self.backward_tree_path.clear();

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let init_time = timer.get_passed();

        let mut nodes_in_elimination_tree_search_space = 0;
        let mut relaxed_elimination_tree_arcs = 0;
        let mut relaxed_shortcut_arcs = 0;
        let mut num_settled_nodes = 0;

        while self.forward.peek_next().is_some() || self.backward.peek_next().is_some() {
            if self.forward.peek_next().unwrap_or(n as NodeId) <= self.backward.peek_next().unwrap_or(n as NodeId) {
                let stall = || {
                    for ((target, _), (_, shortcut_upper_bound)) in self.customized_graph.downward_bounds_graph().neighbor_iter(self.forward.peek_next().unwrap()) {
                        if self.forward.node_data(target).upper_bound + shortcut_upper_bound < self.forward.node_data(self.forward.peek_next().unwrap()).lower_bound {
                            return true;
                        }
                    }
                    false
                };

                if self.forward.node_data(self.forward.peek_next().unwrap()).lower_bound > tentative_distance.1 || (cfg!(tdcch_stall_on_demand) && stall()) {
                    self.forward.skip_next();
                } else if let QueryProgress::Progress(node) = self.forward.next_step() {
                    if cfg!(feature = "detailed-stats") { nodes_in_elimination_tree_search_space += 1; }
                    if cfg!(feature = "detailed-stats") { relaxed_elimination_tree_arcs += self.customized_graph.outgoing.degree(node); }

                    self.forward_tree_mask.unset_all_around(node as usize);
                    self.forward_tree_path.push(node);

                    let lower_bound = self.forward.node_data(node).lower_bound + self.backward.node_data(node).lower_bound;
                    let upper_bound = self.forward.node_data(node).upper_bound + self.backward.node_data(node).upper_bound;

                    self.distances[node as usize] = min(self.distances[node as usize], departure_time + upper_bound + FlWeight::new(EPSILON));

                    if lower_bound < tentative_distance.1 {
                        tentative_distance.0 = min(tentative_distance.0, lower_bound);
                        tentative_distance.1 = min(tentative_distance.1, upper_bound);
                        debug_assert!(tentative_distance.0 <= tentative_distance.1);
                        self.meeting_nodes.push((node, lower_bound));
                    }
                } else {
                    unreachable!("inconsistent elimination tree state");
                }
            } else {
                if self.backward.node_data(self.backward.peek_next().unwrap()).lower_bound > tentative_distance.1 {
                    self.backward.skip_next();
                } else if let QueryProgress::Progress(node) = self.backward.next_step() {
                    if cfg!(feature = "detailed-stats") { nodes_in_elimination_tree_search_space += 1; }
                    if cfg!(feature = "detailed-stats") { relaxed_elimination_tree_arcs += self.customized_graph.incoming.degree(node); }

                    self.backward_tree_mask.unset_all_around(node as usize);
                    self.backward_tree_path.push(node);

                    let lower_bound = self.forward.node_data(node).lower_bound + self.backward.node_data(node).lower_bound;
                    if lower_bound < tentative_distance.1 {
                        tentative_distance.0 = min(tentative_distance.0, lower_bound);
                        tentative_distance.1 = min(tentative_distance.1, self.forward.node_data(node).upper_bound + self.backward.node_data(node).upper_bound);
                        debug_assert!(tentative_distance.0 <= tentative_distance.1);
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

        let tentative_upper_bound = tentative_distance.1;
        let tentative_latest_arrival = departure_time + tentative_upper_bound;

        for &(node, _) in self.meeting_nodes.iter().filter(|(_, lower_bound)| *lower_bound <= tentative_upper_bound) {
            self.forward_tree_mask.set(node as usize);
            self.backward_tree_mask.set(node as usize);
        }

        while let Some(node) = self.backward_tree_path.pop() {
            if self.backward_tree_mask.get(node as usize) {
                self.lower_bounds_to_target[node as usize] = self.backward.node_data(node).lower_bound;
                let upper_bound = self.backward.node_data(node).upper_bound;

                for label in self.backward.node_data(node).labels.iter().filter(|label| label.lower_bound <= upper_bound) {
                    self.backward_tree_mask.set(label.parent as usize);
                }
            }
        }

        while let Some(node) = self.forward_tree_path.pop() {
            if self.forward_tree_mask.get(node as usize) {
                let reverse_lower = self.lower_bounds_to_target[node as usize];
                let upper_bound = self.forward.node_data(node).upper_bound;

                for label in self.forward.node_data(node).labels.iter().filter(|label| label.lower_bound <= upper_bound) {
                    debug_assert!(self.customized_graph.outgoing.bounds()[label.shortcut_id as usize].0.fuzzy_eq(label.lower_bound - self.forward.node_data(label.parent).lower_bound));
                    self.lower_bounds_to_target[label.parent as usize] = min(self.lower_bounds_to_target[label.parent as usize], reverse_lower + label.lower_bound - self.forward.node_data(label.parent).lower_bound);
                    self.forward_tree_mask.set(label.parent as usize);
                    self.relevant_upward.set(label.shortcut_id as usize);
                }
            }
        }


        #[cfg(feature = "tdcch-query-detailed-timing")]
        let forward_select_time = timer.get_passed();

        let relevant_upward = &mut self.relevant_upward;
        debug_assert!(tentative_distance.0.fuzzy_eq(self.lower_bounds_to_target[self.from as usize]));
        debug_assert!(FlWeight::zero().fuzzy_eq(self.lower_bounds_to_target[self.to as usize]));
        let lower_bounds_to_target = &mut self.lower_bounds_to_target;

        self.closest_node_priority_queue.push(State { distance: departure_time + tentative_distance.0, node: self.from });

        while let Some(State { node, .. }) = self.closest_node_priority_queue.pop() {
            if cfg!(feature = "detailed-stats") { num_settled_nodes += 1; }

            let distance = self.distances[node as usize];

            if node == self.to { break }

            for ((target, shortcut_id), (shortcut_lower_bound, _shortcut_upper_bound)) in self.customized_graph.upward_bounds_graph().neighbor_iter(node) {
                if relevant_upward.get(shortcut_id as usize) && !min(tentative_latest_arrival, self.distances[target as usize]).fuzzy_lt(distance + shortcut_lower_bound) {
                    if cfg!(feature = "detailed-stats") { relaxed_shortcut_arcs += 1; }

                    let lower_bound_target = lower_bounds_to_target[target as usize];
                    let (time, next_on_path, evaled_edge_id) = self.customized_graph.outgoing.evaluate_next_segment_at::<True, _>(shortcut_id, distance, lower_bound_target, self.customized_graph, lower_bounds_to_target,
                        &mut |edge_id| relevant_upward.set(edge_id as usize)).unwrap();
                    let lower = if cfg!(feature = "tdcch-query-astar") { lower_bounds_to_target[next_on_path as usize] } else { FlWeight::zero() };

                    let next_ea = distance + time;
                    let next = State { distance: next_ea + lower, node: next_on_path };

                    if next_ea < self.distances[next.node as usize] {
                        self.distances.set(next.node as usize, next_ea);
                        self.parents[next.node as usize] = (node, evaled_edge_id);
                    if self.closest_node_priority_queue.contains_index(next.as_index()) {
                            self.closest_node_priority_queue.decrease_key(next);
                        } else {
                            self.closest_node_priority_queue.push(next);
                        }
                    } else if let Some(label) = self.closest_node_priority_queue.get(next.as_index()) {
                        if next < *label {
                            self.closest_node_priority_queue.decrease_key(next);
                        }
                    }
                }
            }

            if self.backward_tree_mask.get(node as usize) {
                let upper_bound = self.backward.node_data(node).upper_bound;

                for label in self.backward.node_data(node).labels.iter().filter(|label| label.lower_bound <= upper_bound) {
                    if !min(tentative_latest_arrival, self.distances[label.parent as usize]).fuzzy_lt(distance + self.customized_graph.incoming.bounds()[label.shortcut_id as usize].0) {
                        if cfg!(feature = "detailed-stats") { relaxed_shortcut_arcs += 1; }

                        let lower_bound_target = lower_bounds_to_target[label.parent as usize];
                        let (time, next_on_path, evaled_edge_id) = self.customized_graph.incoming.evaluate_next_segment_at::<False, _>(label.shortcut_id, distance, lower_bound_target, self.customized_graph, lower_bounds_to_target,
                            &mut |edge_id| relevant_upward.set(edge_id as usize)).unwrap();
                        let lower = if cfg!(feature = "tdcch-query-astar") { lower_bounds_to_target[next_on_path as usize] } else { FlWeight::zero() };

                        let next_ea = distance + time;
                        let next = State { distance: next_ea + lower, node: next_on_path };

                        if next_ea < self.distances[next.node as usize] {
                            self.distances.set(next.node as usize, next_ea);
                            self.parents[next.node as usize] = (node, evaled_edge_id);
                            if self.closest_node_priority_queue.contains_index(next.as_index()) {
                                self.closest_node_priority_queue.decrease_key(next);
                            } else {
                                self.closest_node_priority_queue.push(next);
                            }
                        } else if let Some(label) = self.closest_node_priority_queue.get(next.as_index()) {
                            if next < *label {
                                self.closest_node_priority_queue.decrease_key(next);
                            }
                        }
                    }
                }
            }
        }

        if cfg!(feature = "detailed-stats") {
            report!("num_relaxed_shortcut_arcs", relaxed_shortcut_arcs);
            report!("num_settled_nodes", num_settled_nodes);
        }

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let relax_time = timer.get_passed();

        #[cfg(feature = "tdcch-query-detailed-timing")]
        {
            eprintln!("elimination tree: {} {:?}%", elimination_tree_time - init_time, 100 * ((elimination_tree_time - init_time).num_nanoseconds().unwrap()) / relax_time.num_nanoseconds().unwrap());
            eprintln!("fw select: {} {:?}%", forward_select_time - elimination_tree_time, 100 * ((forward_select_time - elimination_tree_time).num_nanoseconds().unwrap()) / relax_time.num_nanoseconds().unwrap());
            eprintln!("relax: {} {:?}%", relax_time - forward_select_time, 100 * ((relax_time - forward_select_time).num_nanoseconds().unwrap()) / relax_time.num_nanoseconds().unwrap());
        }

        if self.distances[self.to as usize] < Timestamp::NEVER {
            Some(self.distances[self.to as usize] - departure_time)
        } else {
            None
        }
    }

    pub fn path(&self) -> Vec<(NodeId, Timestamp)> {
        let mut path = Vec::new();
        path.push((self.to, self.distances[self.to as usize]));

        while let Some((rank, t_prev)) = path.pop() {
            debug_assert_eq!(t_prev, self.distances[rank as usize]);
            if rank == self.from {
                path.push((rank, t_prev));
                break;
            }

            let (parent, shortcut_id) = self.parents[rank as usize];
            let t_parent = self.distances[parent as usize];

            let mut shortcut_path = Vec::new();
            if parent > rank {
                self.customized_graph.incoming.unpack_at(shortcut_id, t_parent, &self.customized_graph, &mut shortcut_path);
            } else {
                self.customized_graph.outgoing.unpack_at(shortcut_id, t_parent, &self.customized_graph, &mut shortcut_path);
            };

            for (edge, arrival) in shortcut_path.into_iter().rev() {
                path.push((self.cch_graph.node_order().rank(self.customized_graph.original_graph.head()[edge as usize]), arrival));
            }

            path.push((parent, t_parent));
        }

        path.reverse();

        for (rank, _) in &mut path {
            *rank = self.cch_graph.node_order().node(*rank);
        }

        path
    }
}
