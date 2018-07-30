use super::*;
use shortest_path::td_stepped_dijkstra::TDSteppedDijkstra;
use graph::time_dependent::*;
use rank_select_map::BitVec;
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
        let mut active_edges = BitVec::new(self.dijkstra.graph().num_arcs());
        for i in 0..self.dijkstra.graph().num_arcs() {
            active_edges.set(i);
        }

        self.dijkstra.initialize_query(TDQuery { from, to, departure_time }, active_edges);

        loop {
            match self.dijkstra.next_step() {
                QueryProgress::Progress(_) => continue,
                QueryProgress::Done(result) => return result
            }
        }
    }

    pub fn is_in_searchspace(&self, node: NodeId) -> bool {
        self.dijkstra.tentative_distance(node) < INFINITY
    }

    pub fn path(&self) -> LinkedList<NodeId> {
        let mut path = LinkedList::new();
        path.push_front(self.dijkstra.query().to);

        while *path.front().unwrap() != self.dijkstra.query().from {
            let next = self.dijkstra.predecessor(*path.front().unwrap());
            path.push_front(next);
        }

        path
    }
}
