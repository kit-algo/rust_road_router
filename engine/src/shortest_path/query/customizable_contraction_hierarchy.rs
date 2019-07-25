use super::*;
use super::stepped_elimination_tree::SteppedEliminationTree;
use crate::shortest_path::customizable_contraction_hierarchy::CCHGraph;

#[derive(Debug)]
pub struct Server<'a> {
    forward: SteppedEliminationTree<'a, FirstOutGraph<&'a[EdgeId], &'a[NodeId], Vec<Weight>>>,
    backward: SteppedEliminationTree<'a, FirstOutGraph<&'a[EdgeId], &'a[NodeId], Vec<Weight>>>,
    cch_graph: &'a CCHGraph,
    tentative_distance: Weight,
    meeting_node: NodeId,
}

impl<'a> Server<'a> {
    pub fn new<Graph>(cch_graph: &'a CCHGraph, metric: &Graph) -> Server<'a> where
        Graph: for<'b> LinkIterGraph<'b> + RandomLinkAccessGraph
    {
        let (upward, downward) = cch_graph.customize(metric);
        let forward = SteppedEliminationTree::new(upward, cch_graph.elimination_tree());
        let backward = SteppedEliminationTree::new(downward, cch_graph.elimination_tree());

        Server {
            forward,
            backward,
            cch_graph,
            tentative_distance: INFINITY,
            meeting_node: 0,
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

    pub fn path(&mut self) -> Vec<NodeId> {
        self.forward.unpack_path(self.meeting_node, true, self.cch_graph, self.backward.graph().weight());
        self.backward.unpack_path(self.meeting_node, true, self.cch_graph, self.forward.graph().weight());

        let mut path = Vec::new();
        path.push(self.meeting_node);

        while *path.last().unwrap() != self.forward.origin() {
            path.push(self.forward.predecessor(*path.last().unwrap()));
        }

        path.reverse();

        while *path.last().unwrap() != self.backward.origin() {
            path.push(self.backward.predecessor(*path.last().unwrap()));
        }

        path
    }
}
