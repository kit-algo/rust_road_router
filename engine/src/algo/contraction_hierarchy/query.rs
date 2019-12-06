//! Contraction Hierarchy query server.
//!
//! Actually not much more than a bidirectional dijkstra with a different stopping criterion.
//! And more complicated path unpacking.
//! This works because the augmented graph was split into an upward and an downward part.
//! This implicitly makes sure, that both searches only go to higher ranked nodes.

use super::*;

#[derive(Debug)]
pub struct Server {
    forward_dijkstra: SteppedDijkstra<OwnedGraph>,
    backward_dijkstra: SteppedDijkstra<OwnedGraph>,
    tentative_distance: Weight,
    meeting_node: NodeId,
    shortcut_middle_nodes: Option<(Vec<NodeId>, Vec<NodeId>)>,
    order: NodeOrder,
}

impl Server {
    pub fn new(ch: ContractionHierarchy, order: NodeOrder) -> Server {
        Server {
            forward_dijkstra: SteppedDijkstra::new(ch.forward),
            backward_dijkstra: SteppedDijkstra::new(ch.backward),
            tentative_distance: INFINITY,
            meeting_node: 0,
            shortcut_middle_nodes: ch.middle_nodes,
            order,
        }
    }

    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        let from = self.order.rank(from);
        let to = self.order.rank(to);

        // initialize
        self.tentative_distance = INFINITY;

        self.forward_dijkstra.initialize_query(Query { from, to });
        self.backward_dijkstra.initialize_query(Query { from: to, to: from });

        let mut forward_progress = 0;
        let mut backward_progress = 0;
        let mut forward_done = false;
        let mut backward_done = false;

        // compare tentative distance to both directions progress individually rather than the sum!
        while (self.tentative_distance > forward_progress || self.tentative_distance > backward_progress) && !(forward_done && backward_done) {
            if backward_done || (forward_progress <= backward_progress && !forward_done) {
                match self.forward_dijkstra.next_step() {
                    QueryProgress::Settled(State { distance, node }) => {
                        forward_progress = distance;
                        if distance + self.backward_dijkstra.tentative_distance(node) < self.tentative_distance {
                            self.tentative_distance = distance + self.backward_dijkstra.tentative_distance(node);
                            self.meeting_node = node;
                        }
                    }
                    QueryProgress::Done(Some(distance)) => {
                        forward_done = true;
                        forward_progress = distance;
                        if distance < self.tentative_distance {
                            self.tentative_distance = distance;
                            self.meeting_node = to;
                        }
                    }
                    QueryProgress::Done(None) => forward_done = true,
                }
            } else {
                match self.backward_dijkstra.next_step() {
                    QueryProgress::Settled(State { distance, node }) => {
                        backward_progress = distance;
                        if distance + self.forward_dijkstra.tentative_distance(node) < self.tentative_distance {
                            self.tentative_distance = distance + self.forward_dijkstra.tentative_distance(node);
                            self.meeting_node = node;
                        }
                    }
                    QueryProgress::Done(Some(distance)) => {
                        backward_done = true;
                        backward_progress = distance;
                        if distance < self.tentative_distance {
                            self.tentative_distance = distance;
                            self.meeting_node = from;
                        }
                    }
                    QueryProgress::Done(None) => backward_done = true,
                }
            }
        }

        match self.tentative_distance {
            INFINITY => None,
            dist => Some(dist),
        }
    }

    // Ugly path unpacking.
    // Should probably switch to something similar as in CCH query, which utilizes the parent pointers in the dijkstra struct.
    fn path(&self) -> Vec<NodeId> {
        use std::collections::LinkedList;

        let &(ref forward_middle_nodes, ref backward_middle_nodes) = self.shortcut_middle_nodes.as_ref().unwrap();
        let mut forwad_path = LinkedList::new();
        forwad_path.push_front(self.meeting_node);

        while *forwad_path.front().unwrap() != self.forward_dijkstra.query().from {
            let next = self.forward_dijkstra.predecessor(*forwad_path.front().unwrap());
            let mut shortcut_stack = vec![next];

            while let Some(node) = shortcut_stack.pop() {
                let next = self.forward_dijkstra.predecessor(*forwad_path.front().unwrap());
                let middle = forward_middle_nodes[self.forward_dijkstra.graph().edge_index(node, next).unwrap() as usize];
                if middle < self.forward_dijkstra.graph().num_nodes() as NodeId {
                    shortcut_stack.push(node);
                    shortcut_stack.push(middle);
                } else {
                    forwad_path.push_front(node);
                }
            }
        }

        let mut backward_path = LinkedList::new();
        backward_path.push_back(self.meeting_node);

        while *backward_path.back().unwrap() != self.backward_dijkstra.query().from {
            let next = self.backward_dijkstra.predecessor(*backward_path.back().unwrap());
            let mut shortcut_stack = vec![next];

            while let Some(node) = shortcut_stack.pop() {
                let next = self.backward_dijkstra.predecessor(*backward_path.back().unwrap());
                let middle = backward_middle_nodes[self.backward_dijkstra.graph().edge_index(node, next).unwrap() as usize];
                if middle < self.backward_dijkstra.graph().num_nodes() as NodeId {
                    shortcut_stack.push(node);
                    shortcut_stack.push(middle);
                } else {
                    backward_path.push_back(node);
                }
            }
        }

        forwad_path.pop_back();
        forwad_path.append(&mut backward_path);

        for node in &mut forwad_path {
            *node = self.order.node(*node);
        }

        forwad_path.into_iter().collect()
    }
}

pub struct PathServerWrapper<'s>(&'s Server);

impl<'s> PathServer for PathServerWrapper<'s> {
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0)
    }
}

impl<'s> QueryServer<'s> for Server {
    type P = PathServerWrapper<'s>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query.from, query.to)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self)))
    }
}
