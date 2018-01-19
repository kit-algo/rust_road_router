use super::*;
use super::stepped_elimination_tree::SteppedEliminationTree;
use ::shortest_path::customizable_contraction_hierarchy::cch_graph::CCHGraph;
use std::collections::LinkedList;
use ::in_range_option::InRangeOption;

#[derive(Debug)]
pub struct Server<'a> {
    forward: SteppedEliminationTree<'a, FirstOutGraph<&'a[EdgeId], &'a[NodeId], Vec<Weight>>>,
    backward: SteppedEliminationTree<'a, FirstOutGraph<&'a[EdgeId], &'a[NodeId], Vec<Weight>>>,
    cch_graph: &'a CCHGraph,
    tentative_distance: Weight,
    meeting_node: NodeId,
    upward_shortcut_expansions: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>,
    downward_shortcut_expansions: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>
}

impl<'a> Server<'a> {
    pub fn new<Graph>(cch_graph: &'a CCHGraph, metric: &Graph) -> Server<'a> where
        Graph: for<'b> LinkIterGraph<'b> + RandomLinkAccessGraph
    {
        let (upward, downward, upward_shortcut_expansions, downward_shortcut_expansions) = cch_graph.customize(metric);
        let forward = SteppedEliminationTree::new(upward, cch_graph.elimination_tree());
        let backward = SteppedEliminationTree::new(downward, cch_graph.elimination_tree());

        Server {
            forward,
            backward,
            cch_graph,
            tentative_distance: INFINITY,
            meeting_node: 0,
            upward_shortcut_expansions,
            downward_shortcut_expansions,
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        let from = self.cch_graph.node_order().rank(from);
        let to = self.cch_graph.node_order().rank(to);

        // initialize
        self.tentative_distance = INFINITY;
        self.meeting_node = 0;
        self.forward.initialize_query(from);
        self.backward.initialize_query(to);

        while self.forward.next().is_some() {
            self.forward.next_step();
        }

        while let QueryProgress::Progress(State { distance, node }) = self.backward.next_step() {
            if distance + self.forward.tentative_distance(node) < self.tentative_distance {
                self.tentative_distance = distance + self.forward.tentative_distance(node);
                self.meeting_node = node;
            }
        }

        match self.tentative_distance {
            INFINITY => None,
            dist => Some(dist)
        }
    }

    pub fn path(&self) -> LinkedList<NodeId> {
        let mut forwad_path = LinkedList::new();
        forwad_path.push_front(self.meeting_node);

        while *forwad_path.front().unwrap() != self.forward.origin() {
            let current = forwad_path.pop_front().unwrap();
            let next = self.forward.predecessor(current);
            let edge_id = self.forward.graph().edge_index(next, current).unwrap();

            let mut expanded_path = self.expand_shortcut(edge_id, Direction::Up);

            debug_assert_eq!(*expanded_path.back().unwrap(), current);
            debug_assert_eq!(*expanded_path.front().unwrap(), next);
            debug_assert_ne!(*expanded_path.back().unwrap(), *forwad_path.front().unwrap_or(&(self.forward.graph().num_nodes() as NodeId)));

            expanded_path.append(&mut forwad_path);
            forwad_path = expanded_path;
        }

        let mut backward_path = LinkedList::new();
        backward_path.push_back(self.meeting_node);

        while *backward_path.back().unwrap() != self.backward.origin() {
            let current = backward_path.pop_back().unwrap();
            let next = self.backward.predecessor(current);
            let edge_id = self.backward.graph().edge_index(next, current).unwrap();

            let mut expanded_path = self.expand_shortcut(edge_id, Direction::Down);

            debug_assert_eq!(*expanded_path.front().unwrap(), current);
            debug_assert_eq!(*expanded_path.back().unwrap(), next);
            debug_assert_ne!(*expanded_path.front().unwrap(), *backward_path.back().unwrap_or(&(self.forward.graph().num_nodes() as NodeId)));

            backward_path.append(&mut expanded_path);
        }

        forwad_path.pop_back();
        forwad_path.append(&mut backward_path);
        for node in forwad_path.iter_mut() {
            *node = self.cch_graph.node_order().node(*node);
        }
        forwad_path
    }

    fn expand_shortcut(&self, edge_id: EdgeId, direction: Direction) -> LinkedList<NodeId> {
        let mut list = LinkedList::new();

        let (expansion, graph) = match direction {
            Direction::Up => (&self.upward_shortcut_expansions, self.forward.graph()),
            Direction::Down => (&self.downward_shortcut_expansions, self.backward.graph()),
        };

        let lower = self.cch_graph.edge_id_to_tail(edge_id);
        let higher = graph.link(edge_id).node;

        let (tail, head) = match direction {
            Direction::Up => (lower, higher),
            Direction::Down => (higher, lower),
        };

        if expansion[edge_id as usize].0.value().is_some() {
            let middle = self.cch_graph.edge_id_to_tail(expansion[edge_id as usize].0.value().unwrap());
            debug_assert_eq!(self.cch_graph.edge_id_to_tail(expansion[edge_id as usize].1.value().unwrap()), middle);
            debug_assert_eq!(self.backward.graph().link(expansion[edge_id as usize].0.value().unwrap()).node, tail);
            debug_assert_eq!(self.forward.graph().link(expansion[edge_id as usize].1.value().unwrap()).node, head);

            let mut down = self.expand_shortcut(expansion[edge_id as usize].0.value().unwrap(), Direction::Down);
            let mut up = self.expand_shortcut(expansion[edge_id as usize].1.value().unwrap(), Direction::Up);

            debug_assert_eq!(*down.front().unwrap(), tail);
            debug_assert_eq!(*up.back().unwrap(), head);
            debug_assert_eq!(*down.back().unwrap(), *up.front().unwrap());

            list.append(&mut down);
            list.pop_back();
            list.append(&mut up);
        } else {
            list.push_back(higher);
            match direction {
                Direction::Up => list.push_front(lower),
                Direction::Down => list.push_back(lower),
            };
        }

        list
    }
}

#[derive(Debug)]
enum Direction {
    Up,
    Down,
}
