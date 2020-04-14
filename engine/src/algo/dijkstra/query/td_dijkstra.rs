use super::*;
use crate::algo::dijkstra::td_stepped_dijkstra::TDSteppedDijkstra;
use crate::datastr::graph::time_dependent::*;

#[derive(Debug)]
pub struct Server {
    dijkstra: TDSteppedDijkstra,
}

impl Server {
    pub fn new(graph: TDGraph) -> Server {
        Server {
            dijkstra: TDSteppedDijkstra::new(graph),
        }
    }

    fn distance(&mut self, from: NodeId, to: NodeId, departure: Timestamp) -> Option<Weight> {
        self.dijkstra.initialize_query(TDQuery { from, to, departure });

        loop {
            match self.dijkstra.next_step(|_| true, |_| Some(0)) {
                QueryProgress::Settled(_) => continue,
                QueryProgress::Done(result) => return result,
            }
        }
    }

    fn path(&self) -> Vec<(NodeId, Weight)> {
        let mut path = Vec::new();
        path.push((
            self.dijkstra.query().to,
            self.dijkstra.tentative_distance(self.dijkstra.query().to) - self.dijkstra.query().departure,
        ));

        while path.last().unwrap().0 != self.dijkstra.query().from {
            let next = self.dijkstra.predecessor(path.last().unwrap().0);
            let t = self.dijkstra.tentative_distance(next) - self.dijkstra.query().departure;
            path.push((next, t));
        }

        path.reverse();

        path
    }
}

pub struct PathServerWrapper<'s>(&'s Server);

impl<'s> PathServer for PathServerWrapper<'s> {
    type NodeInfo = (NodeId, Weight);

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0)
    }
}

impl<'s> TDQueryServer<'s, Timestamp, Weight> for Server {
    type P = PathServerWrapper<'s>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query.from, query.to, query.departure)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self)))
    }
}
