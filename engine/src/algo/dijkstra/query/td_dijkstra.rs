use super::*;
use crate::algo::dijkstra::generic_dijkstra::*;
use crate::datastr::graph::time_dependent::*;
use crate::report::*;

pub struct Server {
    dijkstra: GenericDijkstra<Weight, TDDijkstraOps, TDGraph>,
}

impl Server {
    pub fn new(graph: TDGraph) -> Server {
        Server {
            dijkstra: GenericDijkstra::new(graph),
        }
    }

    fn distance(&mut self, from: NodeId, to: NodeId, departure: Timestamp) -> Option<Weight> {
        report!("algo", "TD Dijkstra Query");
        self.dijkstra.initialize_query(TDQuery { from, to, departure });

        while let Some(node) = self.dijkstra.next() {
            if node == to {
                return Some(*self.dijkstra.tentative_distance(node) - departure);
            }
        }

        None
    }

    fn path(&self, query: TDQuery<Weight>) -> Vec<(NodeId, Weight)> {
        let mut path = Vec::new();
        path.push((query.to, self.dijkstra.tentative_distance(query.to) - query.departure));

        while path.last().unwrap().0 != query.from {
            let next = self.dijkstra.predecessor(path.last().unwrap().0);
            let t = self.dijkstra.tentative_distance(next) - query.departure;
            path.push((next, t));
        }

        path.reverse();

        path
    }
}

pub struct PathServerWrapper<'s>(&'s Server, TDQuery<Weight>);

impl<'s> PathServer for PathServerWrapper<'s> {
    type NodeInfo = (NodeId, Weight);

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<'s> TDQueryServer<'s, Timestamp, Weight> for Server {
    type P = PathServerWrapper<'s>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query.from, query.to, query.departure)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}

pub struct TDDijkstraOps();

impl DijkstraOps<Weight, TDGraph> for TDDijkstraOps {
    type LinkResult = Weight;
    type Arc = (NodeId, EdgeId);

    #[inline(always)]
    fn link(&mut self, graph: &TDGraph, label: &Weight, link: &Self::Arc) -> Self::LinkResult {
        label + graph.travel_time_function(link.1).eval(*label)
    }

    #[inline(always)]
    fn merge(&mut self, label: &mut Weight, linked: Self::LinkResult) -> bool {
        if linked < *label {
            *label = linked;
            return true;
        }
        false
    }
}

impl Default for TDDijkstraOps {
    fn default() -> Self {
        TDDijkstraOps {}
    }
}
