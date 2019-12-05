use super::*;
use crate::algo::dijkstra::floating_td_stepped_dijkstra::{FloatingTDSteppedDijkstra, QueryProgress, State};
use crate::datastr::graph::floating_time_dependent::*;
use crate::report::*;

#[derive(Debug)]
pub struct Server {
    dijkstra: FloatingTDSteppedDijkstra,
    query: Option<TDQuery<Timestamp>>,
}

impl Server {
    pub fn new(graph: TDGraph) -> Server {
        Server {
            dijkstra: FloatingTDSteppedDijkstra::new(graph),
            query: None,
        }
    }

    pub fn ranks<F>(&mut self, from: NodeId, departure_time: Timestamp, mut callback: F)
    where
        F: (FnMut(NodeId, Timestamp, usize)),
    {
        self.dijkstra.initialize_query(from, departure_time);

        let mut i: usize = 0;
        while let QueryProgress::Progress(State { distance, node }) = self.dijkstra.next_step(|_| true) {
            i += 1;
            if (i & (i - 1)) == 0 {
                // power of two
                callback(node, distance, i.trailing_zeros() as usize);
            }
        }
    }

    fn distance(&mut self, from: NodeId, to: NodeId, departure: Timestamp) -> Option<FlWeight> {
        report!("algo", "Floating TD-Dijkstra");
        self.query = Some(TDQuery { from, to, departure });
        self.dijkstra.initialize_query(from, departure);

        loop {
            match self.dijkstra.next_step(|_| true) {
                QueryProgress::Progress(State { distance, node }) => {
                    if node == to {
                        return Some(distance - departure);
                    }
                }
                QueryProgress::Done() => return None,
            }
        }
    }

    fn path(&self) -> Vec<(NodeId, Timestamp)> {
        let mut path = Vec::new();
        path.push((self.query.unwrap().to, self.dijkstra.tentative_distance(self.query.unwrap().to)));

        while path.last().unwrap().0 != self.query.unwrap().from {
            let next = self.dijkstra.predecessor(path.last().unwrap().0);
            let t = self.dijkstra.tentative_distance(next);
            path.push((next, t));
        }

        path.reverse();
        path
    }
}

pub struct PathServerWrapper<'s>(&'s Server);

impl<'s> PathServer for PathServerWrapper<'s> {
    type NodeInfo = (NodeId, Timestamp);

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0)
    }
}

impl<'s> TDQueryServer<'s, Timestamp, FlWeight> for Server {
    type P = PathServerWrapper<'s>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, FlWeight>> {
        self.distance(query.from, query.to, query.departure)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self)))
    }
}
