use super::*;
use crate::algo::dijkstra::generic_dijkstra::*;
use crate::datastr::graph::floating_time_dependent::*;
use crate::report::*;

pub struct Server {
    dijkstra: GenericDijkstra<TDGraph, FlTDDijkstraOps>,
}

impl Server {
    pub fn new(graph: TDGraph) -> Server {
        Server {
            dijkstra: GenericDijkstra::new(graph),
        }
    }

    pub fn ranks<F>(&mut self, from: NodeId, departure_time: Timestamp, mut callback: F)
    where
        F: (FnMut(NodeId, Timestamp, usize)),
    {
        self.dijkstra.initialize_query(TDQuery {
            from,
            to: self.dijkstra.graph().num_nodes() as NodeId,
            departure: departure_time,
        });

        let mut i: usize = 0;
        while let Some(node) = self.dijkstra.next() {
            i += 1;
            if (i & (i - 1)) == 0 {
                // power of two
                callback(node, *self.dijkstra.tentative_distance(node), i.trailing_zeros() as usize);
            }
        }
    }

    fn distance(&mut self, query: TDQuery<Timestamp>) -> Option<FlWeight> {
        report!("algo", "Floating TD-Dijkstra");
        self.dijkstra.initialize_query(query);

        while let Some(node) = self.dijkstra.next() {
            if node == query.to {
                return Some(*self.dijkstra.tentative_distance(node) - query.departure);
            }
        }

        None
    }

    fn path(&self, query: TDQuery<Timestamp>) -> Vec<(NodeId, Timestamp)> {
        let mut path = Vec::new();
        path.push((query.to, *self.dijkstra.tentative_distance(query.to)));

        while path.last().unwrap().0 != query.from {
            let next = self.dijkstra.predecessor(path.last().unwrap().0);
            let t = *self.dijkstra.tentative_distance(next);
            path.push((next, t));
        }

        path.reverse();
        path
    }
}

pub struct PathServerWrapper<'s>(&'s Server, TDQuery<Timestamp>);

impl<'s> PathServer for PathServerWrapper<'s> {
    type NodeInfo = (NodeId, Timestamp);

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<'s> TDQueryServer<'s, Timestamp, FlWeight> for Server {
    type P = PathServerWrapper<'s>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, FlWeight>> {
        self.distance(query)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}

#[derive(Debug, Clone, Copy)]
struct FlTDDijkstraOps();

impl DijkstraOps<TDGraph> for FlTDDijkstraOps {
    type Label = Timestamp;
    type LinkResult = Timestamp;
    type Arc = (NodeId, EdgeId);

    #[inline(always)]
    fn link(&mut self, graph: &TDGraph, label: &Timestamp, link: &Self::Arc) -> Self::LinkResult {
        *label + graph.travel_time_function(link.1).evaluate(*label)
    }

    #[inline(always)]
    fn merge(&mut self, label: &mut Timestamp, linked: Self::LinkResult) -> bool {
        if linked < *label {
            *label = linked;
            return true;
        }
        false
    }
}

impl Default for FlTDDijkstraOps {
    fn default() -> Self {
        FlTDDijkstraOps {}
    }
}

impl Label for Timestamp {
    type Key = Self;

    fn neutral() -> Self {
        Timestamp::NEVER
    }

    #[inline(always)]
    fn key(&self) -> Self::Key {
        *self
    }
}
