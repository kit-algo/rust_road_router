use super::*;
use crate::shortest_path::floating_td_stepped_dijkstra::FloatingTDSteppedDijkstra;
use crate::graph::floating_time_dependent::*;
use super::floating_td_stepped_dijkstra::{QueryProgress, State};

use std::collections::LinkedList;

#[derive(Debug)]
pub struct Server {
    dijkstra: FloatingTDSteppedDijkstra,
    query: Option<FlTDQuery>
}

impl Server {
    pub fn new(graph: TDGraph) -> Server {
        Server {
            dijkstra: FloatingTDSteppedDijkstra::new(graph),
            query: None
        }
    }

    pub fn ranks<F>(&mut self, from: NodeId, departure_time: Timestamp, mut callback: F)
        where F: (FnMut(NodeId, Timestamp, usize))
    {
        self.dijkstra.initialize_query(from, departure_time);

        let mut i: usize = 0;
        while let QueryProgress::Progress(State { distance, node }) = self.dijkstra.next_step(|_| true) {
            i += 1;
            if (i & (i - 1)) == 0 { // power of two
                callback(node, distance, i.trailing_zeros() as usize);
            }
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId, departure_time: Timestamp) -> Option<FlWeight> {
        self.query = Some(FlTDQuery { from, to, departure_time });
        self.dijkstra.initialize_query(from, departure_time);

        loop {
            match self.dijkstra.next_step(|_| true) {
                QueryProgress::Progress(State { distance, node }) => {
                    if node == to {
                        return Some(distance - departure_time)
                    }
                },
                QueryProgress::Done() => return None
            }
        }
    }

    pub fn is_in_searchspace(&self, node: NodeId) -> bool {
        self.dijkstra.tentative_distance(node) < Timestamp::new(f64::from(INFINITY))
    }

    pub fn path(&self) -> LinkedList<(NodeId, FlWeight)> {
        let mut path = LinkedList::new();
        path.push_front((self.query.unwrap().to, self.dijkstra.tentative_distance(self.query.unwrap().to) - self.query.unwrap().departure_time));

        while path.front().unwrap().0 != self.query.unwrap().from {
            let next = self.dijkstra.predecessor(path.front().unwrap().0);
            let t = self.dijkstra.tentative_distance(next) - self.query.unwrap().departure_time;
            path.push_front((next, t));
        }

        path
    }
}
