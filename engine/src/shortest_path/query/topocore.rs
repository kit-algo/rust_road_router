use crate::shortest_path::node_order::NodeOrder;
use super::*;

#[derive(Debug)]
pub struct Server {
    forward_dijkstra: SteppedDijkstra<OwnedGraph>,
    backward_dijkstra: SteppedDijkstra<OwnedGraph>,
    order: NodeOrder,
    core_size: NodeId,
    meeting_node: NodeId,
}

impl Server {
    pub fn new(forward: OwnedGraph, backward: OwnedGraph, order: NodeOrder, core_size: NodeId) -> Server {

        Server {
            forward_dijkstra: SteppedDijkstra::new(forward),
            backward_dijkstra: SteppedDijkstra::new(backward),
            core_size,
            meeting_node: 0,
            order,
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        let from = self.order.rank(from);
        let to = self.order.rank(to);
        // initialize
        let mut tentative_distance = INFINITY;
        let mut forward_done = false;
        let mut backward_done = false;
        let mut forward_non_core_count = 0;
        let mut backward_non_core_count = 0;

        // node ids were reordered so that core nodes are at the "front" of the order, that is come first
        // so we can check if a node is in core by checking if its id is smaller than the number of nodes in the core
        let in_core = { let core_size = self.core_size; move |node| {
            node < core_size
        } };

        if !in_core(from) {
            forward_non_core_count += 1;
        }
        if !in_core(to) {
            backward_non_core_count += 1;
        }

        self.forward_dijkstra.initialize_query(Query { from, to });
        self.backward_dijkstra.initialize_query(Query { from: to, to: from });

        let mut forward_progress = 0;
        let mut backward_progress = 0;

        while !(forward_done && backward_done) {
        // while !(forward_done && backward_done) && forward_progress + backward_progress < self.tentative_distance {
            if !forward_done && (backward_done || forward_progress <= backward_progress) {
                match self.forward_dijkstra.next_step_with_queue_insert_callback(|node| if !in_core(node) { forward_non_core_count += 1; }) {
                    QueryProgress::Progress(State { distance, node }) => {
                        dbg!("forward", distance, node);
                        if !dbg!(in_core(node)) { forward_non_core_count -= 1; }

                        forward_progress = distance;
                        if distance + self.backward_dijkstra.tentative_distance(node) < tentative_distance {
                            tentative_distance = distance + self.backward_dijkstra.tentative_distance(node);
                            self.meeting_node = node;
                        }
                    },
                    QueryProgress::Done(result) => {
                        if let Some(distance) = result {
                            if !in_core(to) { forward_non_core_count -= 1; }

                            if distance < tentative_distance {
                                tentative_distance = distance;
                                self.meeting_node = to;
                            }
                        }
                        forward_done = true;
                    }
                }
            } else {
                match self.backward_dijkstra.next_step_with_queue_insert_callback(|node| if !in_core(node) { backward_non_core_count += 1; }) {
                    QueryProgress::Progress(State { distance, node }) => {
                        dbg!("backward", distance, node);
                        if !dbg!(in_core(node)) { backward_non_core_count -= 1; }

                        backward_progress = distance;
                        if distance + self.forward_dijkstra.tentative_distance(node) < tentative_distance {
                            tentative_distance = distance + self.forward_dijkstra.tentative_distance(node);
                            self.meeting_node = node;
                        }
                    },
                    QueryProgress::Done(result) => {
                        if let Some(distance) = result {
                            if !in_core(from) { backward_non_core_count -= 1; }

                            backward_progress = distance;
                            if distance < tentative_distance {
                                tentative_distance = distance;
                                self.meeting_node = from;
                            }
                        }
                        backward_done = true;
                    }
                }
            }
        }

        match tentative_distance {
            INFINITY => None,
            dist => Some(dist)
        }
    }

    pub fn is_in_searchspace(&self, node: NodeId) -> bool {
        self.forward_dijkstra.tentative_distance(node) < INFINITY
            || self.backward_dijkstra.tentative_distance(node) < INFINITY
    }

    pub fn path(&self) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(self.meeting_node);

        while *path.last().unwrap() != self.forward_dijkstra.query().from {
            let next = self.forward_dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();

        while *path.last().unwrap() != self.backward_dijkstra.query().from {
            let next = self.backward_dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        for node in &mut path {
            *node = self.order.node(*node);
        }

        path
    }
}
