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

    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        self.dijkstra.initialize_query(Query { from, to });

        loop {
            match self.dijkstra.next_step() {
                QueryProgress::Progress(_) => continue,
                QueryProgress::Done(result) => return result,
            }
        }
    }

    fn path(&self) -> Vec<NodeId> {
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

pub struct PathServerWrapper<'s, G: for<'a> LinkIterGraph<'a>>(&'s Server<G>);

impl<'s, G: for<'a> LinkIterGraph<'a>> PathServer<'s> for PathServerWrapper<'s, G> {
    type NodeInfo = NodeId;

    fn path(&'s mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0)
    }
}

impl<'s, G: 's + for<'a> LinkIterGraph<'a>> QueryServer<'s> for Server<G> {
    type P = PathServerWrapper<'s, G>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query.from, query.to).map(move |distance| QueryResult {
            distance,
            path_server: PathServerWrapper(self),
        })
    }
}
