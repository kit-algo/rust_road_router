use super::*;

#[derive(Debug)]
pub struct Server<Graph: for<'a> LinkIterGraph<'a>> {
    dijkstra: SteppedDijkstra<Graph>,
}

impl<Graph: for<'a> LinkIterGraph<'a>> Server<Graph> {
    pub fn new(graph: Graph) -> Server<Graph> {
        Server {
            dijkstra: SteppedDijkstra::new(graph),
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        self.dijkstra.initialize_query(Query { from, to });

        loop {
            match self.dijkstra.next_step() {
                QueryProgress::Progress(_) => continue,
                QueryProgress::Done(result) => return result,
            }
        }
    }

    pub fn is_in_searchspace(&self, node: NodeId) -> bool {
        self.dijkstra.tentative_distance(node) < INFINITY
    }

    pub fn path(&self) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(self.dijkstra.query().to);

        while *path.last().unwrap() != self.dijkstra.query().from {
            let next = self.dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();

        path
    }
}
