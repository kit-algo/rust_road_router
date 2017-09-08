use std::cmp::min;
use super::*;

#[derive(Debug)]
pub struct Server {
    forward_dijkstra: SteppedDijkstra<Graph>,
    backward_dijkstra: SteppedDijkstra<Graph>,
    tentative_distance: Weight
}

impl Server {
    pub fn new((up, down): (Graph, Graph)) -> Server {
        Server {
            forward_dijkstra: SteppedDijkstra::new(up),
            backward_dijkstra: SteppedDijkstra::new(down),
            tentative_distance: INFINITY
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

        while self.tentative_distance > forward_progress && self.tentative_distance > backward_progress && !(forward_done && backward_done) {
            if backward_done || (forward_progress <= backward_progress && !forward_done) {
                match self.forward_dijkstra.next_step() {
                    QueryProgress::Progress(State { distance, node }) => {
                        forward_progress = distance;
                        self.tentative_distance = min(distance + self.backward_dijkstra.tentative_distance(node), self.tentative_distance);
                    },
                    QueryProgress::Done(Some(distance)) => {
                        forward_done = true;
                        forward_progress = distance;
                        self.tentative_distance = min(distance, self.tentative_distance);
                    },
                    QueryProgress::Done(None) => forward_done = true
                }
            } else {
                match self.backward_dijkstra.next_step() {
                    QueryProgress::Progress(State { distance, node }) => {
                        backward_progress = distance;
                        self.tentative_distance = min(distance + self.forward_dijkstra.tentative_distance(node), self.tentative_distance);
                    },
                    QueryProgress::Done(Some(distance)) => {
                        backward_done = true;
                        backward_progress = distance;
                        self.tentative_distance = min(distance, self.tentative_distance);
                    },
                    QueryProgress::Done(None) => backward_done = true
                }
            }
        }

        match self.tentative_distance {
            INFINITY => None,
            dist => Some(dist)
        }
    }

    pub fn is_edge_in_searchspace(&self, from: NodeId, to: NodeId) -> bool {
        (self.forward_dijkstra.tentative_distance(from) < INFINITY && self.forward_dijkstra.tentative_distance(to) < INFINITY)
            || (self.backward_dijkstra.tentative_distance(from) < INFINITY && self.backward_dijkstra.tentative_distance(to) < INFINITY)
    }
}
