use super::*;

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
}
