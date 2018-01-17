use std::collections::LinkedList;
use super::*;

#[derive(Debug)]
pub struct Server<G: for<'a> LinkIterGraph<'a>, H: for<'a> LinkIterGraph<'a>> {
    pub forward_dijkstra: SteppedDijkstra<G>,
    pub backward_dijkstra: SteppedDijkstra<H>,
    pub tentative_distance: Weight,
    pub maximum_distance: Weight,
    pub meeting_node: NodeId
}

impl<G: for<'a> LinkIterGraph<'a>> Server<G, OwnedGraph> {
    pub fn new(graph: G) -> Server<G, OwnedGraph> {
        let reversed = graph.reverse();

        Server {
            forward_dijkstra: SteppedDijkstra::new(graph),
            backward_dijkstra: SteppedDijkstra::new(reversed),
            tentative_distance: INFINITY,
            maximum_distance: INFINITY,
            meeting_node: 0
        }
    }
}

impl<G: for<'a> LinkIterGraph<'a>, H: for<'a> LinkIterGraph<'a>> Server<G, H> {
    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        // initialize
        self.tentative_distance = INFINITY;

        self.forward_dijkstra.initialize_query(Query { from, to });
        self.backward_dijkstra.initialize_query(Query { from: to, to: from });

        let mut forward_progress = 0;
        let mut backward_progress = 0;

        while forward_progress + backward_progress < self.tentative_distance &&
            forward_progress + backward_progress < self.maximum_distance {
            if forward_progress <= backward_progress {
                match self.forward_dijkstra.next_step() {
                    QueryProgress::Progress(State { distance, node }) => {
                        forward_progress = distance;
                        if distance + self.backward_dijkstra.tentative_distance(node) < self.tentative_distance {
                            self.tentative_distance = distance + self.backward_dijkstra.tentative_distance(node);
                            self.meeting_node = node;
                        }
                    },
                    QueryProgress::Done(result) => {
                        self.meeting_node = to;
                        return result
                    }
                }
            } else {
                match self.backward_dijkstra.next_step() {
                    QueryProgress::Progress(State { distance, node }) => {
                        backward_progress = distance;
                        if distance + self.forward_dijkstra.tentative_distance(node) < self.tentative_distance {
                            self.tentative_distance = distance + self.forward_dijkstra.tentative_distance(node);
                            self.meeting_node = node;
                        }
                    },
                    QueryProgress::Done(result) => {
                        self.meeting_node = from;
                        return result
                    }
                }
            }
        }

        match self.tentative_distance {
            INFINITY => None,
            dist => Some(dist)
        }
    }

    pub fn is_in_searchspace(&self, node: NodeId) -> bool {
        self.forward_dijkstra.tentative_distance(node) < INFINITY
            || self.backward_dijkstra.tentative_distance(node) < INFINITY
    }

    pub fn path(&self) -> LinkedList<NodeId> {
        let mut forwad_path = LinkedList::new();
        forwad_path.push_front(self.meeting_node);

        while *forwad_path.front().unwrap() != self.forward_dijkstra.query().from {
            let next = self.forward_dijkstra.predecessor(*forwad_path.front().unwrap());
            forwad_path.push_front(next);
        }

        let mut backward_path = LinkedList::new();
        backward_path.push_back(self.meeting_node);

        while *backward_path.back().unwrap() != self.backward_dijkstra.query().from {
            let next = self.backward_dijkstra.predecessor(*backward_path.back().unwrap());
            backward_path.push_back(next);
        }

        forwad_path.pop_back();
        forwad_path.append(&mut backward_path);
        forwad_path
    }
}
