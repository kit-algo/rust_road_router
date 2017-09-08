use super::*;

use std::collections::LinkedList;

#[derive(Debug)]
pub struct Server {
    dijkstra: SteppedDijkstra<Graph>,
}

impl Server {
    pub fn new(graph: Graph) -> Server {
        Server {
            dijkstra: SteppedDijkstra::new(graph)
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        self.dijkstra.initialize_query(Query { from, to });

        loop {
            match self.dijkstra.next_step() {
                QueryProgress::Progress(_) => continue,
                QueryProgress::Done(result) => return result
            }
        }
    }

    pub fn is_edge_in_searchspace(&self, from: NodeId, to: NodeId) -> bool {
        self.dijkstra.tentative_distance(from) < INFINITY
            && self.dijkstra.tentative_distance(to) < INFINITY
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
