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
    shortcut_queue: Vec<EdgeId>,
    distances: TimestampedVector<Timestamp>,
}

impl<'a> Server<'a> {
    pub fn new(cch_graph: &'a CCHGraph, shortcut_graph: &'a ShortcutGraph<'a>) -> Self {
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
            distances: TimestampedVector::new(shortcut_graph.original_graph().num_nodes(), Timestamp::new(f64::from(INFINITY))),
        }
    }

    #[allow(clippy::collapsible_if)]
    pub fn distance(&mut self, from: NodeId, to: NodeId, departure_time: Timestamp) -> Option<FlWeight> {
        let from = self.cch_graph.node_order().rank(from);
        let to = self.cch_graph.node_order().rank(to);

        let n = self.shortcut_graph.original_graph().num_nodes();

        // initialize
        self.tentative_distance = (FlWeight::new(f64::from(INFINITY)), FlWeight::new(f64::from(INFINITY)));
        self.distances.reset();
        self.distances.set(from as usize, departure_time);
        self.meeting_nodes.clear();
        self.forward.initialize_query(from);
        self.backward.initialize_query(to);

        // TODO get rid of reinit
        self.forward_tree_path.clear();
        self.backward_tree_path.clear();
        self.shortcut_queue.clear();
        let mut forward_tree_mask = BitVec::new(n);
        let mut backward_tree_mask = BitVec::new(n);

        while self.forward.peek_next().is_some() || self.backward.peek_next().is_some() {
            if self.forward.peek_next().unwrap_or(n as NodeId) <= self.backward.peek_next().unwrap_or(n as NodeId) {
                if self.forward.node_data(self.forward.peek_next().unwrap()).lower_bound > self.tentative_distance.1 {
                    self.forward.skip_next();
                } else if let QueryProgress::Progress(node) = self.forward.next_step() {
                    self.forward_tree_path.push(node);

                    let lower_bound = self.forward.node_data(node).lower_bound + self.backward.node_data(node).lower_bound;
                    if lower_bound < self.tentative_distance.1 {
                        self.tentative_distance.0 = min(self.tentative_distance.0, lower_bound);
                        self.tentative_distance.1 = min(self.tentative_distance.1, self.forward.node_data(node).upper_bound + self.backward.node_data(node).upper_bound);
                        debug_assert!(self.tentative_distance.0 <= self.tentative_distance.1);
                        self.meeting_nodes.push((node, lower_bound));
                    }
                } else {
                    panic!("wtf");
                }
            } else {
                if self.backward.node_data(self.backward.peek_next().unwrap()).lower_bound > self.tentative_distance.1 {
                    self.backward.skip_next();
                } else if let QueryProgress::Progress(node) = self.backward.next_step() {
                    self.backward_tree_path.push(node);

                    let lower_bound = self.forward.node_data(node).lower_bound + self.backward.node_data(node).lower_bound;
                    if lower_bound < self.tentative_distance.1 {
                        self.tentative_distance.0 = min(self.tentative_distance.0, lower_bound);
                        self.tentative_distance.1 = min(self.tentative_distance.1, self.forward.node_data(node).upper_bound + self.backward.node_data(node).upper_bound);
                        debug_assert!(self.tentative_distance.0 <= self.tentative_distance.1);
                        self.meeting_nodes.push((node, lower_bound));
                    }
                } else {
                    panic!("wtf");
                }
            }
        }

        let tentative_upper_bound = self.tentative_distance.1;

        self.meeting_nodes.retain(|&(_, lower_bound)| lower_bound <= tentative_upper_bound);

        for &(node, _) in &self.meeting_nodes {
            forward_tree_mask.set(node as usize);
            backward_tree_mask.set(node as usize);
        }

        while let Some(node) = self.forward_tree_path.pop() {
            if forward_tree_mask.get(node as usize) {
                for label in &self.forward.node_data(node).labels {
                    forward_tree_mask.set(label.parent as usize);
                    self.shortcut_queue.push(label.shortcut_id);
                }
            }
        }

        let shortcut_graph = &self.shortcut_graph;
        let cch_graph = &self.cch_graph;
        let shortcut_queue = &mut self.shortcut_queue;
        let distances = &mut self.distances;

        for &shortcut_id in shortcut_queue.iter().rev() {
            let tail = cch_graph.edge_id_to_tail(shortcut_id);
            let head = cch_graph.head(shortcut_id);
            let t_cur = distances[tail as usize];
            let t_next = t_cur + shortcut_graph.get_outgoing(shortcut_id).evaluate(t_cur, shortcut_graph, &mut |up, shortcut_id, t| {
                // true
                if up {
                    let tail = cch_graph.edge_id_to_tail(shortcut_id);
                    if t <= distances[tail as usize] {
                        distances[tail as usize] = t;
                        true
                    } else {
                        false
                    }
                } else {
                    let head = cch_graph.head(shortcut_id);
                    if t <= distances[head as usize] {
                        distances[head as usize] = t;
                        true
                    } else {
                        false
                    }
                }
            });
            if t_next < distances[head as usize] {
                distances[head as usize] = t_next;
            }
        }

        while let Some(node) = self.backward_tree_path.pop() {
            if backward_tree_mask.get(node as usize) {
                for label in &self.backward.node_data(node).labels {
                    backward_tree_mask.set(label.parent as usize);
                    let tail = node;
                    let head = label.parent;
                    let t_cur = distances[tail as usize];
                    let t_next = t_cur + shortcut_graph.get_incoming(label.shortcut_id).evaluate(t_cur, shortcut_graph, &mut |up, shortcut_id, t| {
                        // true
                        if up {
                            let tail = cch_graph.edge_id_to_tail(shortcut_id);
                            if t <= distances[tail as usize] {
                                distances[tail as usize] = t;
                                true
                            } else {
                                false
                            }
                        } else {
                            let head = cch_graph.head(shortcut_id);
                            if t <= distances[head as usize] {
                                distances[head as usize] = t;
                                true
                            } else {
                                false
                            }
                        }
                    });
                    if t_next < distances[head as usize] {
                        distances[head as usize] = t_next;
                    }
                }
            }
        }

        if self.distances[to as usize] < Timestamp::new(f64::from(INFINITY)) {
            Some(self.distances[to as usize] - departure_time)
        } else {
            None
        }
    }
}
