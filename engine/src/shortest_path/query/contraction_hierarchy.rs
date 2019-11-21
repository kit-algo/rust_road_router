use super::*;

#[derive(Debug)]
pub struct Server {
    forward_dijkstra: SteppedDijkstra<OwnedGraph>,
    backward_dijkstra: SteppedDijkstra<OwnedGraph>,
    tentative_distance: Weight,
    meeting_node: NodeId,
    shortcut_middle_nodes: Option<(Vec<NodeId>, Vec<NodeId>)>,
}

impl Server {
    pub fn new(((up, down), shortcut_middle_nodes): ((OwnedGraph, OwnedGraph), Option<(Vec<NodeId>, Vec<NodeId>)>)) -> Server {
        Server {
            forward_dijkstra: SteppedDijkstra::new(up),
            backward_dijkstra: SteppedDijkstra::new(down),
            tentative_distance: INFINITY,
            meeting_node: 0,
            shortcut_middle_nodes,
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        // initialize
        self.tentative_distance = INFINITY;

        self.forward_dijkstra.initialize_query(Query { from, to });
        self.backward_dijkstra.initialize_query(Query { from: to, to: from });

        let mut forward_progress = 0;
        let mut backward_progress = 0;
        let mut forward_done = false;
        let mut backward_done = false;

        while (self.tentative_distance > forward_progress || self.tentative_distance > backward_progress) && !(forward_done && backward_done) {
            if backward_done || (forward_progress <= backward_progress && !forward_done) {
                match self.forward_dijkstra.next_step() {
                    QueryProgress::Progress(State { distance, node }) => {
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
                    QueryProgress::Progress(State { distance, node }) => {
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

    pub fn is_in_searchspace(&self, node: NodeId) -> bool {
        self.forward_dijkstra.tentative_distance(node) < INFINITY || self.backward_dijkstra.tentative_distance(node) < INFINITY
    }

    pub fn path(&self) -> Vec<NodeId> {
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
        forwad_path.into_iter().collect()
    }
}
