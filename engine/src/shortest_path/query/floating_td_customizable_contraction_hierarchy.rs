use std::cmp::*;

use super::*;
use super::floating_td_stepped_elimination_tree::{*, QueryProgress};

use crate::graph::floating_time_dependent::*;
use crate::shortest_path::customizable_contraction_hierarchy::cch_graph::CCHGraph;
use crate::shortest_path::timestamped_vector::TimestampedVector;
use crate::rank_select_map::BitVec;

#[derive(Debug)]
pub struct Server<'a> {
    forward: FloatingTDSteppedEliminationTree<'a, 'a>,
    backward: FloatingTDSteppedEliminationTree<'a, 'a>,
    cch_graph: &'a CCHGraph,
    shortcut_graph: &'a ShortcutGraph<'a>,
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
    pub fn new(cch_graph: &'a CCHGraph, shortcut_graph: &'a ShortcutGraph<'a>) -> Self {
        let n = shortcut_graph.original_graph().num_nodes();
        Self {
            forward: FloatingTDSteppedEliminationTree::new(shortcut_graph.upward_graph(), cch_graph.elimination_tree()),
            backward: FloatingTDSteppedEliminationTree::new(shortcut_graph.downward_graph(), cch_graph.elimination_tree()),
            cch_graph,
            meeting_nodes: Vec::new(),
            tentative_distance: (FlWeight::new(f64::from(INFINITY)), FlWeight::new(f64::from(INFINITY))),
            shortcut_graph,
            forward_tree_path: Vec::new(),
            backward_tree_path: Vec::new(),
            shortcut_queue: Vec::new(),
            distances: TimestampedVector::new(n, Timestamp::new(f64::from(INFINITY))),
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
        self.from = self.cch_graph.node_order().rank(from_node);
        self.to = self.cch_graph.node_order().rank(to_node);

        let n = self.shortcut_graph.original_graph().num_nodes();

        // initialize
        self.tentative_distance = (FlWeight::new(f64::from(INFINITY)), FlWeight::new(f64::from(INFINITY)));
        self.distances.reset();
        self.distances.set(self.from as usize, departure_time);
        self.meeting_nodes.clear();
        self.forward.initialize_query(self.from);
        self.backward.initialize_query(self.to);

        self.forward_tree_path.clear();
        self.backward_tree_path.clear();
        self.shortcut_queue.clear();

        while self.forward.peek_next().is_some() || self.backward.peek_next().is_some() {
            if self.forward.peek_next().unwrap_or(n as NodeId) <= self.backward.peek_next().unwrap_or(n as NodeId) {
                let stall = || {
                    for ((target, _), shortcut) in self.shortcut_graph.downward_graph().neighbor_iter(self.forward.peek_next().unwrap()) {
                        if self.forward.node_data(target).upper_bound + shortcut.upper_bound < self.forward.node_data(self.forward.peek_next().unwrap()).lower_bound {
                            return true;
                        }
                    }
                    false
                };

                if self.forward.node_data(self.forward.peek_next().unwrap()).lower_bound > self.tentative_distance.1 || (cfg!(tdcch_stall_on_demand) && stall()) {
                    self.forward.skip_next();
                } else if let QueryProgress::Progress(node) = self.forward.next_step() {
                    self.forward_tree_mask.unset_all_around(node as usize);
                    self.forward_tree_path.push(node);

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
            } else {
                if self.backward.node_data(self.backward.peek_next().unwrap()).lower_bound > self.tentative_distance.1 {
                    self.backward.skip_next();
                } else if let QueryProgress::Progress(node) = self.backward.next_step() {
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

        let tentative_upper_bound = self.tentative_distance.1;

        self.meeting_nodes.retain(|&(_, lower_bound)| lower_bound <= tentative_upper_bound);

        for &(node, _) in &self.meeting_nodes {
            self.forward_tree_mask.set(node as usize);
            self.backward_tree_mask.set(node as usize);
        }

        while let Some(node) = self.forward_tree_path.pop() {
            if self.forward_tree_mask.get(node as usize) {
                for label in &self.forward.node_data(node).labels {
                    self.forward_tree_mask.set(label.parent as usize);
                    self.shortcut_queue.push((label.shortcut_id, label.parent, node));
                }
            }
        }

        let shortcut_graph = &self.shortcut_graph;
        let cch_graph = &self.cch_graph;
        let shortcut_queue = &mut self.shortcut_queue;
        let distances = &mut self.distances;
        let parents = &mut self.parents;

        for &(shortcut_id, tail, head) in shortcut_queue.iter().rev() {
            let t_cur = distances[tail as usize];
            if t_cur < distances[head as usize] {
                let mut parent = tail;
                let mut parent_shortcut_id = shortcut_id;
                let t_next = t_cur + shortcut_graph.get_outgoing(shortcut_id).evaluate(t_cur, shortcut_graph, &mut |up, shortcut_id, t| {
                    if up {
                        let tail = cch_graph.edge_id_to_tail(shortcut_id);
                        let res = if t <= distances[tail as usize] {
                            distances[tail as usize] = t;
                            debug_assert_ne!(distances[parent as usize], Timestamp::new(f64::from(INFINITY)));
                            if tail != parent {
                                parents[tail as usize] = (parent, parent_shortcut_id);
                            }
                            true
                        } else {
                            false
                        };
                        parent = tail;
                        parent_shortcut_id = shortcut_id;
                        res
                    } else {
                        let head = cch_graph.head(shortcut_id);
                        let res = if t <= distances[head as usize] {
                            distances[head as usize] = t;
                            debug_assert_ne!(distances[parent as usize], Timestamp::new(f64::from(INFINITY)));
                            if head != parent {
                                parents[head as usize] = (parent, parent_shortcut_id);
                            }
                            true
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
                    debug_assert_ne!(distances[parent as usize], Timestamp::new(f64::from(INFINITY)));
                }
            }
        }

        while let Some(node) = self.backward_tree_path.pop() {
            if self.backward_tree_mask.get(node as usize) {
                for label in &self.backward.node_data(node).labels {
                    self.backward_tree_mask.set(label.parent as usize);
                    let tail = node;
                    let head = label.parent;
                    let t_cur = distances[tail as usize];
                    if t_cur < distances[head as usize] {
                        let mut parent = tail;
                        let mut parent_shortcut_id = label.shortcut_id;
                        let t_next = t_cur + shortcut_graph.get_incoming(label.shortcut_id).evaluate(t_cur, shortcut_graph, &mut |up, shortcut_id, t| {
                            if up {
                                let tail = cch_graph.edge_id_to_tail(shortcut_id);
                                let res = if t <= distances[tail as usize] {
                                    distances[tail as usize] = t;
                                    debug_assert_ne!(distances[parent as usize], Timestamp::new(f64::from(INFINITY)));
                                    if tail != parent {
                                        parents[tail as usize] = (parent, parent_shortcut_id);
                                    }
                                    true
                                } else {
                                    false
                                };
                                parent = tail;
                                parent_shortcut_id = shortcut_id;
                                res
                            } else {
                                let head = cch_graph.head(shortcut_id);
                                let res = if t <= distances[head as usize] {
                                    distances[head as usize] = t;
                                    debug_assert_ne!(distances[parent as usize], Timestamp::new(f64::from(INFINITY)));
                                    if head != parent {
                                        parents[head as usize] = (parent, parent_shortcut_id);
                                    }
                                    true
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
                            debug_assert_ne!(distances[parent as usize], Timestamp::new(f64::from(INFINITY)));
                        }
                    }
                }
            }
        }

        if self.distances[self.to as usize] < Timestamp::new(f64::from(INFINITY)) {
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
                self.shortcut_graph.get_incoming(shortcut_id).unpack_at(t_parent, &self.shortcut_graph, &mut shortcut_path);
            } else {
                self.shortcut_graph.get_outgoing(shortcut_id).unpack_at(t_parent, &self.shortcut_graph, &mut shortcut_path);
            };

            for (edge, arrival) in shortcut_path.into_iter().rev() {
                path.push((self.cch_graph.node_order().rank(self.shortcut_graph.original_graph().head()[edge as usize]), arrival));
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
