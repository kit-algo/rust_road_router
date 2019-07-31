use super::*;
use crate::shortest_path::td_stepped_dijkstra::TDSteppedDijkstra;
use crate::graph::time_dependent::*;
use super::td_stepped_dijkstra::QueryProgress;

use std::collections::LinkedList;

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

    pub fn distance(&mut self, from: NodeId, to: NodeId, departure_time: Timestamp) -> Option<Weight> {
        self.dijkstra.initialize_query(TDQuery { from, to, departure_time });

        loop {
            match self.dijkstra.next_step(|_| true, |_| 0) {
                QueryProgress::Progress(_) => continue,
                QueryProgress::Done(result) => return result
            }
        }
    }

    pub fn is_in_searchspace(&self, node: NodeId) -> bool {
        self.dijkstra.tentative_distance(node) < INFINITY
    }

    pub fn path(&self) -> LinkedList<(NodeId, Weight)> {
        let mut path = LinkedList::new();
        path.push_front((self.dijkstra.query().to, self.dijkstra.tentative_distance(self.dijkstra.query().to) - self.dijkstra.query().departure_time));

        while path.front().unwrap().0 != self.dijkstra.query().from {
            let next = self.dijkstra.predecessor(path.front().unwrap().0);
            let t = self.dijkstra.tentative_distance(next) - self.dijkstra.query().departure_time;
            path.push_front((next, t));
        }

        path
    }
}
