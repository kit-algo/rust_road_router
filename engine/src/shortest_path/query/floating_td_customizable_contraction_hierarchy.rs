use std::cmp::*;
use crate::report::*;

use super::*;
use super::floating_td_stepped_elimination_tree::{*, QueryProgress};

use crate::graph::floating_time_dependent::*;
use crate::shortest_path::customizable_contraction_hierarchy::cch_graph::CCHGraph;
use crate::shortest_path::timestamped_vector::TimestampedVector;
use crate::rank_select_map::BitVec;
#[cfg(feature = "tdcch-query-detailed-timing")]
use crate::benchmark::Timer;

#[derive(Debug)]
pub struct Server<'a> {
    forward: FloatingTDSteppedEliminationTree<'a, 'a>,
    backward: FloatingTDSteppedEliminationTree<'a, 'a>,
    cch_graph: &'a CCHGraph,
    customized_graph: &'a CustomizedGraph<'a>,
    tentative_distance: (FlWeight, FlWeight),
    meeting_nodes: Vec<(NodeId, FlWeight)>,
    forward_tree_path: Vec<NodeId>,
    backward_tree_path: Vec<NodeId>,
    shortcut_queue: Vec<(EdgeId, NodeId, NodeId)>,
    distances: TimestampedVector<Timestamp>,
    parents: Vec<(NodeId, EdgeId)>,
    backward_tree_mask: BitVec,
    forward_tree_mask: BitVec,
    from: NodeId,
    to: NodeId,
}

impl<'a> Server<'a> {
    pub fn new(cch_graph: &'a CCHGraph, customized_graph: &'a CustomizedGraph<'a>) -> Self {
        let n = customized_graph.original_graph.num_nodes();
        Self {
            forward: FloatingTDSteppedEliminationTree::new(customized_graph.upward_bounds_graph(), cch_graph.elimination_tree()),
            backward: FloatingTDSteppedEliminationTree::new(customized_graph.downward_bounds_graph(), cch_graph.elimination_tree()),
            cch_graph,
            meeting_nodes: Vec::new(),
            tentative_distance: (FlWeight::INFINITY, FlWeight::INFINITY),
            customized_graph,
            forward_tree_path: Vec::new(),
            backward_tree_path: Vec::new(),
            shortcut_queue: Vec::new(),
            distances: TimestampedVector::new(n, Timestamp::NEVER),
            parents: vec![(std::u32::MAX, std::u32::MAX); n],
            forward_tree_mask: BitVec::new(n),
            backward_tree_mask: BitVec::new(n),
            from: 0,
            to: 0
        }
    }

    #[allow(clippy::collapsible_if)]
    #[allow(clippy::cyclomatic_complexity)]
    pub fn distance(&mut self, from_node: NodeId, to_node: NodeId, departure_time: Timestamp) -> Option<FlWeight> {
        report!("algo", "Floating TDCCH Query");

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let timer = Timer::new();

        self.from = self.cch_graph.node_order().rank(from_node);
        self.to = self.cch_graph.node_order().rank(to_node);

        let n = self.customized_graph.original_graph.num_nodes();

        // initialize
        self.tentative_distance = (FlWeight::INFINITY, FlWeight::INFINITY);
        self.distances.reset();
        self.distances.set(self.from as usize, departure_time);
        self.meeting_nodes.clear();
        self.forward.initialize_query(self.from);
        self.backward.initialize_query(self.to);

        self.forward_tree_path.clear();
        self.backward_tree_path.clear();
        self.shortcut_queue.clear();

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let init_time = timer.get_passed();

        let mut nodes_in_elimination_tree_search_space = 0;
        let mut relaxed_elimination_tree_arcs = 0;
        let mut relaxed_shortcut_arcs = 0;

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

                if self.forward.node_data(self.forward.peek_next().unwrap()).lower_bound > self.tentative_distance.1 || (cfg!(tdcch_stall_on_demand) && stall()) {
                    self.forward.skip_next();
                } else if let QueryProgress::Progress(node) = self.forward.next_step() {
                    nodes_in_elimination_tree_search_space += 1;
                    relaxed_elimination_tree_arcs += self.cch_graph.degree(node);

                    self.forward_tree_mask.unset_all_around(node as usize);
                    self.forward_tree_path.push(node);

                    let lower_bound = self.forward.node_data(node).lower_bound + self.backward.node_data(node).lower_bound;
                    let upper_bound = self.forward.node_data(node).upper_bound + self.backward.node_data(node).upper_bound;

                    self.distances[node as usize] = min(self.distances[node as usize], departure_time + upper_bound + FlWeight::new(EPSILON));

                    if lower_bound < self.tentative_distance.1 {
                        self.tentative_distance.0 = min(self.tentative_distance.0, lower_bound);
                        self.tentative_distance.1 = min(self.tentative_distance.1, upper_bound);
                        debug_assert!(self.tentative_distance.0 <= self.tentative_distance.1);
                        self.meeting_nodes.push((node, lower_bound));
                    }
                } else {
                    debug_assert!(false, "inconsistent elimination tree state");
                }
            } else {
                if self.backward.node_data(self.backward.peek_next().unwrap()).lower_bound > self.tentative_distance.1 {
                    self.backward.skip_next();
                } else if let QueryProgress::Progress(node) = self.backward.next_step() {
                    nodes_in_elimination_tree_search_space += 1;
                    relaxed_elimination_tree_arcs += self.cch_graph.degree(node);

                    self.backward_tree_mask.unset_all_around(node as usize);
                    self.backward_tree_path.push(node);

                    let lower_bound = self.forward.node_data(node).lower_bound + self.backward.node_data(node).lower_bound;
                    if lower_bound < self.tentative_distance.1 {
                        self.tentative_distance.0 = min(self.tentative_distance.0, lower_bound);
                        self.tentative_distance.1 = min(self.tentative_distance.1, self.forward.node_data(node).upper_bound + self.backward.node_data(node).upper_bound);
                        debug_assert!(self.tentative_distance.0 <= self.tentative_distance.1);
                        self.meeting_nodes.push((node, lower_bound));
                    }
                } else {
                    debug_assert!(false, "inconsistent elimination tree state");
                }
            }
        }

        report!("num_nodes_in_elimination_tree_search_space", nodes_in_elimination_tree_search_space);
        report!("num_relaxed_elimination_tree_arcs", relaxed_elimination_tree_arcs);
        report!("num_meeting_nodes", self.meeting_nodes.len());

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let elimination_tree_time = timer.get_passed();

        let tentative_upper_bound = self.tentative_distance.1;
        let tentative_latest_arrival = departure_time + tentative_upper_bound;

        for &(node, _) in self.meeting_nodes.iter().filter(|(_, lower_bound)| *lower_bound <= tentative_upper_bound) {
            self.forward_tree_mask.set(node as usize);
            self.backward_tree_mask.set(node as usize);
        }

        while let Some(node) = self.forward_tree_path.pop() {
            if self.forward_tree_mask.get(node as usize) {
                let upper_bound = self.forward.node_data(node).upper_bound;

                for label in self.forward.node_data(node).labels.iter().filter(|label| label.lower_bound <= upper_bound) {
                    self.forward_tree_mask.set(label.parent as usize);
                    self.shortcut_queue.push((label.shortcut_id, label.parent, node));
                }
            }
        }

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let forward_select_time = timer.get_passed();

        // TODO kill double refs
        let customized_graph = &self.customized_graph;
        let cch_graph = &self.cch_graph;
        let shortcut_queue = &mut self.shortcut_queue;
        let distances = &mut self.distances;
        let parents = &mut self.parents;

        for &(shortcut_id, tail, head) in shortcut_queue.iter().rev() {
            let t_cur = distances[tail as usize];
            let pruning_bound = min(tentative_latest_arrival, distances[head as usize]);
            debug_assert!(customized_graph.outgoing.bounds()[shortcut_id as usize].0 < FlWeight::INFINITY);
            debug_assert!(customized_graph.outgoing.bounds()[shortcut_id as usize].1 < FlWeight::INFINITY);
            if !pruning_bound.fuzzy_lt(t_cur + customized_graph.outgoing.bounds()[shortcut_id as usize].0) {
                relaxed_shortcut_arcs += 1;

                let mut parent = tail;
                let mut parent_shortcut_id = shortcut_id;
                let t_next = t_cur + customized_graph.outgoing.evaluate(shortcut_id, t_cur, customized_graph, &mut |up, shortcut_id, t| {

                    if up {
                        debug_assert!(customized_graph.outgoing.bounds()[shortcut_id as usize].0 < FlWeight::INFINITY);
                        debug_assert!(customized_graph.outgoing.bounds()[shortcut_id as usize].1 < FlWeight::INFINITY);
                        let tail = cch_graph.edge_id_to_tail(shortcut_id);
                        let res = if t <= distances[tail as usize] {
                            distances[tail as usize] = t;
                            debug_assert!(distances[parent as usize] < Timestamp::NEVER);
                            if tail != parent {
                                parents[tail as usize] = (parent, parent_shortcut_id);
                            }

                            let head = cch_graph.head()[shortcut_id as usize];
                            if distances[head as usize].fuzzy_lt(t + customized_graph.outgoing.bounds()[shortcut_id as usize].0) {
                                false
                            } else {
                                relaxed_shortcut_arcs += 1;
                                true
                            }
                        } else {
                            false
                        };
                        parent = tail;
                        parent_shortcut_id = shortcut_id;
                        res
                    } else {
                        debug_assert!(customized_graph.incoming.bounds()[shortcut_id as usize].0 < FlWeight::INFINITY);
                        debug_assert!(customized_graph.incoming.bounds()[shortcut_id as usize].1 < FlWeight::INFINITY);
                        let head = cch_graph.head()[shortcut_id as usize];
                        let res = if t <= distances[head as usize] {
                            distances[head as usize] = t;
                            debug_assert!(distances[parent as usize] < Timestamp::NEVER);
                            if head != parent {
                                parents[head as usize] = (parent, parent_shortcut_id);
                            }

                            let tail = cch_graph.edge_id_to_tail(shortcut_id);
                            if distances[tail as usize].fuzzy_lt(t + customized_graph.incoming.bounds()[shortcut_id as usize].0) {
                                false
                            } else {
                                relaxed_shortcut_arcs += 1;
                                true
                            }
                        } else {
                            false
                        };
                        parent = head;
                        parent_shortcut_id = shortcut_id;
                        res
                    }
                });
                if t_next < distances[head as usize] {
                    distances[head as usize] = t_next;
                    parents[head as usize] = (parent, parent_shortcut_id);
                    debug_assert!(distances[parent as usize] < Timestamp::NEVER);
                }
            }
        }

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let forward_relax_time = timer.get_passed();

        while let Some(node) = self.backward_tree_path.pop() {
            if self.backward_tree_mask.get(node as usize) {
                let upper_bound = self.backward.node_data(node).upper_bound;
                for label in self.backward.node_data(node).labels.iter().filter(|label| label.lower_bound <= upper_bound) {
                    self.backward_tree_mask.set(label.parent as usize);
                    debug_assert!(customized_graph.incoming.bounds()[label.shortcut_id as usize].0 < FlWeight::INFINITY);
                    debug_assert!(customized_graph.incoming.bounds()[label.shortcut_id as usize].1 < FlWeight::INFINITY);
                    let tail = node;
                    let head = label.parent;
                    let t_cur = distances[tail as usize];
                    let pruning_bound = min(tentative_latest_arrival, distances[head as usize]);
                    if !pruning_bound.fuzzy_lt(t_cur + customized_graph.incoming.bounds()[label.shortcut_id as usize].0) {
                        relaxed_shortcut_arcs += 1;

                        let mut parent = tail;
                        let mut parent_shortcut_id = label.shortcut_id;
                        let t_next = t_cur + customized_graph.incoming.evaluate(label.shortcut_id, t_cur, customized_graph, &mut |up, shortcut_id, t| {

                            if up {
                                debug_assert!(customized_graph.outgoing.bounds()[shortcut_id as usize].0 < FlWeight::INFINITY);
                                debug_assert!(customized_graph.outgoing.bounds()[shortcut_id as usize].1 < FlWeight::INFINITY);
                                let tail = cch_graph.edge_id_to_tail(shortcut_id);
                                let res = if t <= distances[tail as usize] {
                                    distances[tail as usize] = t;
                                    debug_assert!(distances[parent as usize] < Timestamp::NEVER);
                                    if tail != parent {
                                        parents[tail as usize] = (parent, parent_shortcut_id);
                                    }

                                    let head = cch_graph.head()[shortcut_id as usize];
                                    if distances[head as usize].fuzzy_lt(t + customized_graph.outgoing.bounds()[shortcut_id as usize].0) {
                                        false
                                    } else {
                                        relaxed_shortcut_arcs += 1;
                                        true
                                    }
                                } else {
                                    false
                                };
                                parent = tail;
                                parent_shortcut_id = shortcut_id;
                                res
                            } else {
                                debug_assert!(customized_graph.incoming.bounds()[shortcut_id as usize].0 < FlWeight::INFINITY);
                                debug_assert!(customized_graph.incoming.bounds()[shortcut_id as usize].1 < FlWeight::INFINITY);
                                let head = cch_graph.head()[shortcut_id as usize];
                                let res = if t <= distances[head as usize] {
                                    distances[head as usize] = t;
                                    debug_assert!(distances[parent as usize] < Timestamp::NEVER);
                                    if head != parent {
                                        parents[head as usize] = (parent, parent_shortcut_id);
                                    }

                                    let tail = cch_graph.edge_id_to_tail(shortcut_id);
                                    if distances[tail as usize].fuzzy_lt(t + customized_graph.incoming.bounds()[shortcut_id as usize].0) {
                                        false
                                    } else {
                                        relaxed_shortcut_arcs += 1;
                                        true
                                    }
                                } else {
                                    false
                                };
                                parent = head;
                                parent_shortcut_id = shortcut_id;
                                res
                            }
                        });
                        if t_next < distances[head as usize] {
                            distances[head as usize] = t_next;
                            parents[head as usize] = (parent, parent_shortcut_id);
                            debug_assert!(distances[parent as usize] < Timestamp::NEVER);
                        }
                    }
                }
            }
        }

        report!("num_relaxed_shortcut_arcs", relaxed_shortcut_arcs);

        #[cfg(feature = "tdcch-query-detailed-timing")]
        let backward_relax_time = timer.get_passed();

        #[cfg(feature = "tdcch-query-detailed-timing")]
        {
            eprintln!("elimination tree: {} {:?}%", elimination_tree_time - init_time, 100 * ((elimination_tree_time - init_time).num_nanoseconds().unwrap()) / backward_relax_time.num_nanoseconds().unwrap());
            eprintln!("fw select: {} {:?}%", forward_select_time - elimination_tree_time, 100 * ((forward_select_time - elimination_tree_time).num_nanoseconds().unwrap()) / backward_relax_time.num_nanoseconds().unwrap());
            eprintln!("fw relax: {} {:?}%", forward_relax_time - forward_select_time, 100 * ((forward_relax_time - forward_select_time).num_nanoseconds().unwrap()) / backward_relax_time.num_nanoseconds().unwrap());
            eprintln!("bw relax: {} {:?}%", backward_relax_time - forward_relax_time, 100 * ((backward_relax_time - forward_relax_time).num_nanoseconds().unwrap()) / backward_relax_time.num_nanoseconds().unwrap());
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
