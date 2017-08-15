use std::cmp::min;
use super::*;

#[derive(Debug)]
pub struct Server<G: DijkstrableGraph, H: DijkstrableGraph> {
    pub forward_dijkstra: SteppedDijkstra<G>,
    pub backward_dijkstra: SteppedDijkstra<H>,
    pub tentative_distance: Weight,
    pub maximum_distance: Weight
}

impl<G: DijkstrableGraph, H: DijkstrableGraph> Server<G, H> {
    pub fn new(graph: Graph) -> Server<Graph, Graph> {
        let reversed = graph.reverse();

        Server {
            forward_dijkstra: SteppedDijkstra::new(graph),
            backward_dijkstra: SteppedDijkstra::new(reversed),
            tentative_distance: INFINITY,
            maximum_distance: INFINITY
        }
    }

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
                        self.tentative_distance = min(distance + self.backward_dijkstra.tentative_distance(node), self.tentative_distance);
                    },
                    QueryProgress::Done(result) => return result
                }
            } else {
                match self.backward_dijkstra.next_step() {
                    QueryProgress::Progress(State { distance, node }) => {
                        backward_progress = distance;
                        self.tentative_distance = min(distance + self.forward_dijkstra.tentative_distance(node), self.tentative_distance);
                    },
                    QueryProgress::Done(result) => return result
                }
            }
        }

        match self.tentative_distance {
            INFINITY => None,
            dist => Some(dist)
        }
    }
}
