use std::collections::LinkedList;
use graph::time_dependent::*;
use shortest_path::td_stepped_dijkstra::QueryProgress as OtherQueryProgress;
use std::cmp::*;
use shortest_path::td_stepped_dijkstra::TDSteppedDijkstra;
use super::*;
use super::td_stepped_elimination_tree::*;
use ::shortest_path::customizable_contraction_hierarchy::cch_graph::CCHGraph;
use self::td_stepped_elimination_tree::QueryProgress;
use rank_select_map::BitVec;
use benchmark::report_time;
use math::RangeExtensions;

#[derive(Debug)]
pub struct Server<'a> {
    forward: TDSteppedEliminationTreeWithDeparture<'a, 'a>,
    backward: TDSteppedEliminationTree<'a, 'a>,
    td_dijkstra: TDSteppedDijkstra,
    cch_graph: &'a CCHGraph,
    shortcut_graph: &'a ShortcutGraph<'a>,
    tentative_distance: (Weight, Weight),
    meeting_nodes: Vec<(NodeId, Weight)>,
    forward_tree_path: Vec<NodeId>,
    backward_tree_path: Vec<NodeId>,
}

impl<'a> Server<'a> {
    pub fn new(cch_graph: &'a CCHGraph, shortcut_graph: &'a ShortcutGraph<'a>) -> Self {
        Self {
            forward: TDSteppedEliminationTreeWithDeparture::new(shortcut_graph.upward_graph(), cch_graph.elimination_tree()),
            backward: TDSteppedEliminationTree::new(shortcut_graph.downward_graph(), cch_graph.elimination_tree()),
            td_dijkstra: TDSteppedDijkstra::new(shortcut_graph.original_graph().clone()), // TODO fix clone
            cch_graph,
            meeting_nodes: Vec::new(),
            tentative_distance: (INFINITY, INFINITY),
            shortcut_graph,
            forward_tree_path: Vec::new(),
            backward_tree_path: Vec::new(),
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId, departure_time: Timestamp) -> Option<Weight> {
        // initialize
        self.tentative_distance = (INFINITY, INFINITY);
        self.meeting_nodes.clear();
        self.forward.initialize_query(self.cch_graph.node_order().rank(from), departure_time);
        self.backward.initialize_query(self.cch_graph.node_order().rank(to));

        // TODO get rid of reinit
        self.forward_tree_path.clear();
        self.backward_tree_path.clear();
        let mut forward_tree_mask = BitVec::new(self.shortcut_graph.original_graph().num_nodes());
        let mut backward_tree_mask = BitVec::new(self.shortcut_graph.original_graph().num_nodes());

        report_time("cch elimination tree", || {
            // forward up
            while let QueryProgress::Progress(node) = self.forward.next_step() {
                self.forward_tree_path.push(node);
            }

            // backward up
            while let QueryProgress::Progress(node) = self.backward.next_step() {
                self.backward_tree_path.push(node);
                let lower_bound = self.forward.node_data(node).lower_bound + self.backward.node_data(node).lower_bound;
                if lower_bound < self.tentative_distance.1 {
                    self.tentative_distance.0 = min(self.tentative_distance.0, lower_bound);
                    self.tentative_distance.1 = min(self.tentative_distance.1, self.forward.node_data(node).upper_bound + self.backward.node_data(node).upper_bound);
                    debug_assert!(self.tentative_distance.0 <= self.tentative_distance.1);
                    self.meeting_nodes.push((node, lower_bound));
                }
            }
        });

        let tentative_upper_bound = self.tentative_distance.1;
        let mut original_edges = BitVec::new(self.shortcut_graph.original_graph().num_arcs()); // TODO get rid of reinit
        let mut shortcuts = BitVec::new(2 * self.cch_graph.num_arcs() * NUM_WINDOWS); // TODO get rid of reinit

        report_time("unpacking", || {
            self.meeting_nodes.retain(|&(_, lower_bound)| lower_bound <= tentative_upper_bound);

            for &(node, _) in &self.meeting_nodes {
                forward_tree_mask.set(node as usize);
                backward_tree_mask.set(node as usize);
            }

            let mut needs_unpacking = |shortcut_id, window| {
                let index = match shortcut_id {
                    ShortcutId::Outgoing(id) => (2 * id) * NUM_WINDOWS as u32 + window as u32,
                    ShortcutId::Incmoing(id) => (2 * id + 1) * NUM_WINDOWS as u32 + window as u32,
                };
                let res = !shortcuts.get(index as usize);
                shortcuts.set(index as usize);
                res
            };

            while let Some(node) = self.forward_tree_path.pop() {
                if forward_tree_mask.get(node as usize) {
                    for label in &self.forward.node_data(node).labels {
                        let parent = &self.forward.node_data(label.parent);
                        let (first, mut second) = WrappingRange::new(parent.lower_bound % period() .. parent.upper_bound % period()).monotonize().split(period());
                        second.start -= period();
                        second.end -= period();
                        Shortcut::unpack(ShortcutId::Outgoing(label.shortcut_id), &first, self.shortcut_graph, &mut needs_unpacking, &mut |edge_id| original_edges.set(edge_id as usize));
                        Shortcut::unpack(ShortcutId::Outgoing(label.shortcut_id), &second, self.shortcut_graph, &mut needs_unpacking, &mut |edge_id| original_edges.set(edge_id as usize));
                        forward_tree_mask.set(label.parent as usize);
                    }
                }
            }

            while let Some(node) = self.backward_tree_path.pop() {
                if backward_tree_mask.get(node as usize) {
                    let forward_node_data = self.forward.node_data_mut();
                    let current_node_lower = forward_node_data[node as usize].lower_bound;
                    let current_node_upper = forward_node_data[node as usize].upper_bound;
                    let (first, mut second) = WrappingRange::new(current_node_lower % period() .. current_node_upper % period()).monotonize().split(period());
                    second.start -= period();
                    second.end -= period();

                    for label in &self.backward.node_data(node).labels {
                        let shortcut = self.shortcut_graph.get_incoming(label.shortcut_id);

                        let (lower, upper) = match (shortcut.bounds_for(&first), shortcut.bounds_for(&second)) {
                            (Some((first_min, first_max)), Some((second_min, second_max))) =>
                                (current_node_lower + min(first_min, second_min), current_node_upper + max(first_max, second_max)),
                            (Some((first_min, first_max)), None) =>
                                (current_node_lower + first_min, current_node_upper + first_max),
                            (None, Some((second_min, second_max))) =>
                                (current_node_lower + second_min, current_node_upper + second_max),
                            (None, None) =>
                                panic!("weird")
                        };

                        if lower < forward_node_data[label.parent as usize].upper_bound {
                            forward_node_data[label.parent as usize].lower_bound = min(forward_node_data[label.parent as usize].lower_bound, lower);
                            forward_node_data[label.parent as usize].upper_bound = min(forward_node_data[label.parent as usize].upper_bound, upper);

                            Shortcut::unpack(ShortcutId::Incmoing(label.shortcut_id), &first, self.shortcut_graph, &mut needs_unpacking, &mut |edge_id| original_edges.set(edge_id as usize));
                            Shortcut::unpack(ShortcutId::Incmoing(label.shortcut_id), &second, self.shortcut_graph, &mut needs_unpacking, &mut |edge_id| original_edges.set(edge_id as usize));

                            backward_tree_mask.set(label.parent as usize);
                        }
                    }
                }
            }
        });

        report_time("TD Dijkstra", || {
            self.td_dijkstra.initialize_query(TDQuery { from, to, departure_time });
            loop {
                match self.td_dijkstra.next_step(|edge_id| original_edges.get(edge_id as usize)) {
                    OtherQueryProgress::Progress(_) => continue,
                    OtherQueryProgress::Done(result) => return result
                }
            }
        })
    }


    pub fn path(&self) -> LinkedList<(NodeId, Weight)> {
        let mut path = LinkedList::new();
        path.push_front((self.td_dijkstra.query().to, self.td_dijkstra.tentative_distance(self.td_dijkstra.query().to) - self.td_dijkstra.query().departure_time));

        while path.front().unwrap().0 != self.td_dijkstra.query().from {
            let next = self.td_dijkstra.predecessor(path.front().unwrap().0);
            let t = self.td_dijkstra.tentative_distance(next) - self.td_dijkstra.query().departure_time;
            path.push_front((next, t));
        }

        path
    }
}
