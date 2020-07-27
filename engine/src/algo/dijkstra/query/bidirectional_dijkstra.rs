use super::*;
use crate::algo::dijkstra::generic_dijkstra::*;

pub struct Server<G: for<'a> LinkIterGraph<'a>, H: for<'a> LinkIterGraph<'a>> {
    pub forward_dijkstra: StandardDijkstra<G>,
    pub backward_dijkstra: StandardDijkstra<H>,
    pub tentative_distance: Weight,
    pub meeting_node: NodeId,
}

impl<G: for<'a> LinkIterGraph<'a>> Server<G, OwnedGraph> {
    pub fn new(graph: G) -> Server<G, OwnedGraph> {
        let reversed = OwnedGraph::reversed(&graph);

        Server {
            forward_dijkstra: StandardDijkstra::new(graph),
            backward_dijkstra: StandardDijkstra::new(reversed),
            tentative_distance: INFINITY,
            meeting_node: 0,
        }
    }
}

impl<G: for<'a> LinkIterGraph<'a>, H: for<'a> LinkIterGraph<'a>> Server<G, H> {
    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        self.distance_with_cap(from, to, INFINITY)
    }

    pub fn distance_with_cap(&mut self, from: NodeId, to: NodeId, maximum_distance: Weight) -> Option<Weight> {
        // initialize
        self.tentative_distance = INFINITY;

        self.forward_dijkstra.initialize_query(Query { from, to });
        self.backward_dijkstra.initialize_query(Query { from: to, to: from });

        let mut forward_progress = 0;
        let mut backward_progress = 0;

        while forward_progress + backward_progress < self.tentative_distance && forward_progress + backward_progress < maximum_distance {
            if forward_progress <= backward_progress {
                if let Some(node) = self.forward_dijkstra.next() {
                    let distance = *self.forward_dijkstra.tentative_distance(node);
                    if node == to {
                        self.meeting_node = to;
                        return Some(distance);
                    }
                    forward_progress = distance;
                    if distance + self.backward_dijkstra.tentative_distance(node) < self.tentative_distance {
                        self.tentative_distance = distance + self.backward_dijkstra.tentative_distance(node);
                        self.meeting_node = node;
                    }
                } else {
                    return None;
                }
            } else {
                if let Some(node) = self.backward_dijkstra.next() {
                    let distance = *self.backward_dijkstra.tentative_distance(node);
                    if node == from {
                        self.meeting_node = from;
                        return Some(distance);
                    }
                    backward_progress = distance;
                    if distance + self.forward_dijkstra.tentative_distance(node) < self.tentative_distance {
                        self.tentative_distance = distance + self.forward_dijkstra.tentative_distance(node);
                        self.meeting_node = node;
                    }
                } else {
                    return None;
                }
            }
        }

        match self.tentative_distance {
            INFINITY => None,
            dist => Some(dist),
        }
    }

    fn path(&self, query: Query) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(self.meeting_node);

        while *path.last().unwrap() != query.from {
            let next = self.forward_dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();

        while *path.last().unwrap() != query.to {
            let next = self.backward_dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path
    }
}

pub struct PathServerWrapper<'s, G: for<'a> LinkIterGraph<'a>, H: for<'a> LinkIterGraph<'a>>(&'s Server<G, H>, Query);

impl<'s, G: for<'a> LinkIterGraph<'a>, H: for<'a> LinkIterGraph<'a>> PathServer for PathServerWrapper<'s, G, H> {
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<'s, G: 's + for<'a> LinkIterGraph<'a>, H: 's + for<'a> LinkIterGraph<'a>> QueryServer<'s> for Server<G, H> {
    type P = PathServerWrapper<'s, G, H>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query.from, query.to)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}
