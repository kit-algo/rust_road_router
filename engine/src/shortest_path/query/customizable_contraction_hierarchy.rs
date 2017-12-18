use super::*;
use super::stepped_elimination_tree::SteppedEliminationTree;
use ::shortest_path::customizable_contraction_hierarchy::cch_graph::CCHGraph;
use super::node_order::NodeOrder;

#[derive(Debug)]
pub struct Server {
    forward: SteppedEliminationTree<Graph>,
    backward: SteppedEliminationTree<Graph>,
    node_order: NodeOrder,
    tentative_distance: Weight,
    meeting_node: NodeId
}

impl Server {
    pub fn new(cch_graph: CCHGraph) -> Server {
        Server {
            forward: SteppedEliminationTree::new(cch_graph.upward, cch_graph.elimination_tree.clone()),
            backward: SteppedEliminationTree::new(cch_graph.downward, cch_graph.elimination_tree),
            node_order: cch_graph.node_order,
            tentative_distance: INFINITY,
            meeting_node: 0
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
}
