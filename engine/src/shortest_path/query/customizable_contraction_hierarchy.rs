use super::*;
use super::stepped_elimination_tree::SteppedEliminationTree;
use ::shortest_path::customizable_contraction_hierarchy::cch_graph::CCHGraph;
use super::node_order::NodeOrder;
use std::collections::LinkedList;
use ::inrange_option::InrangeOption;

#[derive(Debug)]
pub struct Server<'a> {
    forward: SteppedEliminationTree<'a, FirstOutGraph<&'a[EdgeId], &'a[NodeId], Vec<Weight>>>,
    backward: SteppedEliminationTree<'a, FirstOutGraph<&'a[EdgeId], &'a[NodeId], Vec<Weight>>>,
    node_order: &'a NodeOrder,
    tentative_distance: Weight,
    meeting_node: NodeId,
    upward_shortcut_expansions: Vec<(InrangeOption<EdgeId>, InrangeOption<EdgeId>)>,
    downward_shortcut_expansions: Vec<(InrangeOption<EdgeId>, InrangeOption<EdgeId>)>
}

impl<'a> Server<'a> {
    pub fn new(cch_graph: &'a CCHGraph, metric: &OwnedGraph) -> Server<'a> {
        let (upward, downward, upward_shortcut_expansions, downward_shortcut_expansions) = cch_graph.customize(metric);
        let forward = SteppedEliminationTree::new(upward, cch_graph.elimination_tree());
        let backward = SteppedEliminationTree::new(downward, cch_graph.elimination_tree());

        Server {
            forward,
            backward,
            node_order: cch_graph.node_order(),
            tentative_distance: INFINITY,
            meeting_node: 0,
            upward_shortcut_expansions,
            downward_shortcut_expansions,
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        let from = self.node_order.rank(from);
        let to = self.node_order.rank(to);

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
            let next = self.forward.predecessor(*forwad_path.front().unwrap());
            let edge_id = self.forward.graph().edge_index(next, *forwad_path.front().unwrap()).unwrap();
            forwad_path.pop_front();
            let mut expanded_path = self.expand_shortcut(edge_id, Direction::Up);
            expanded_path.append(&mut forwad_path);
            forwad_path = expanded_path;
            forwad_path.push_front(next);
        }

        let mut backward_path = LinkedList::new();
        backward_path.push_back(self.meeting_node);

        while *backward_path.back().unwrap() != self.backward.origin() {
            let next = self.backward.predecessor(*backward_path.back().unwrap());
            let edge_id = self.backward.graph().edge_index(next, *backward_path.back().unwrap()).unwrap();
            backward_path.pop_back();
            backward_path.append(&mut self.expand_shortcut(edge_id, Direction::Down));
            backward_path.push_back(next);
        }

        forwad_path.pop_back();
        forwad_path.append(&mut backward_path);
        forwad_path
    }

    fn expand_shortcut(&self, edge_id: EdgeId, direction: Direction) -> LinkedList<NodeId> {
        let mut list = LinkedList::new();

        let (expansion, graph) = match direction {
            Direction::Up => (&self.upward_shortcut_expansions, self.forward.graph()),
            Direction::Down => (&self.downward_shortcut_expansions, self.backward.graph()),
        };

        if expansion[edge_id as usize].0.value().is_some() {
            list.append(&mut self.expand_shortcut(expansion[edge_id as usize].0.value().unwrap(), Direction::Down));
            list.append(&mut self.expand_shortcut(expansion[edge_id as usize].1.value().unwrap(), Direction::Up));
        } else {
            list.push_back(graph.link(edge_id).node);
        }

        list
    }
}

#[derive(Debug)]
enum Direction {
    Up,
    Down,
}
