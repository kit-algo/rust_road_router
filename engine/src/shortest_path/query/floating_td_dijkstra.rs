use super::*;
use crate::shortest_path::floating_td_stepped_dijkstra::FloatingTDSteppedDijkstra;
use crate::graph::floating_time_dependent::*;
use super::floating_td_stepped_dijkstra::QueryProgress;

use std::collections::LinkedList;

#[derive(Debug)]
pub struct Server {
    dijkstra: FloatingTDSteppedDijkstra,
}

impl Server {
    pub fn new(graph: TDGraph) -> Server {
        Server {
            dijkstra: FloatingTDSteppedDijkstra::new(graph),
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId, departure_time: Timestamp) -> Option<FlWeight> {
        self.dijkstra.initialize_query(FlTDQuery { from, to, departure_time });

        loop {
            match self.dijkstra.next_step(|_| true) {
                QueryProgress::Progress(_) => continue,
                QueryProgress::Done(result) => return result
            }
        }
    }

    pub fn is_in_searchspace(&self, node: NodeId) -> bool {
        self.dijkstra.tentative_distance(node) < Timestamp::new(f64::from(INFINITY))
    }

    pub fn path(&self) -> LinkedList<(NodeId, FlWeight)> {
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
