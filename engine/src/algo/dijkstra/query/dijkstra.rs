use super::*;
use crate::report::*;
use generic_dijkstra::GenericDijkstra;

pub struct Server<Graph> {
    dijkstra: GenericDijkstra<Link, Graph, Weight, generic_dijkstra::DefaultOps>,
    // dijkstra: SteppedDijkstra<Graph>,
}

impl<Graph: for<'a> LinkIterGraph<'a>> Server<Graph> {
    pub fn new(graph: Graph) -> Server<Graph> {
        Server {
            dijkstra: GenericDijkstra::new(graph),
        }
    }

    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        report!("algo", "Dijkstra Query");

        self.dijkstra.initialize_query(Query { from, to });

        while let Some(_) = self.dijkstra.next_step() {}

        let dist = *self.dijkstra.tentative_distance(to);
        if dist < INFINITY {
            Some(dist)
        } else {
            None
        }
    }

    fn path(&self, query: Query) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(query.to);

        while *path.last().unwrap() != query.from {
            let next = self.dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();

        path
    }

    pub fn ranks<F>(&mut self, from: NodeId, mut callback: F)
    where
        F: (FnMut(NodeId, Weight, usize)),
    {
        self.dijkstra.initialize_query(Query {
            from,
            to: self.dijkstra.graph().num_nodes() as NodeId,
        });

        let mut i: usize = 0;
        while let Some(node) = self.dijkstra.next_step() {
            i += 1;
            if (i & (i - 1)) == 0 {
                // power of two
                callback(node, *self.dijkstra.tentative_distance(node), i.trailing_zeros() as usize);
            }
        }
    }

    pub fn one_to_all(&mut self, from: NodeId) -> ServerWrapper<Graph> {
        self.query(Query {
            from,
            to: self.dijkstra.graph().num_nodes() as NodeId,
        });
        ServerWrapper(self)
    }
}

pub struct PathServerWrapper<'s, G: for<'a> LinkIterGraph<'a>>(&'s Server<G>, Query);

impl<'s, G: for<'a> LinkIterGraph<'a>> PathServer for PathServerWrapper<'s, G> {
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<'s, G: for<'a> LinkIterGraph<'a>> PathServerWrapper<'s, G> {
    /// Print path with debug info as js to stdout.
    pub fn debug_path(&mut self, lat: &[f32], lng: &[f32]) {
        for node in self.path() {
            println!(
                "var marker = L.marker([{}, {}], {{ icon: blueIcon }}).addTo(map);",
                lat[node as usize], lng[node as usize]
            );
            let dist = self.0.dijkstra.tentative_distance(node);
            println!("marker.bindPopup(\"id: {}<br>distance: {}\");", node, dist / 1000);
        }
    }

    pub fn distance(&self, node: NodeId) -> Weight {
        *self.0.dijkstra.tentative_distance(node)
    }
}

pub struct ServerWrapper<'s, G: for<'a> LinkIterGraph<'a>>(&'s Server<G>);

impl<'s, G: for<'a> LinkIterGraph<'a>> ServerWrapper<'s, G> {
    pub fn distance(&self, node: NodeId) -> Weight {
        *self.0.dijkstra.tentative_distance(node)
    }
}

impl<'s, G: 's + for<'a> LinkIterGraph<'a>> QueryServer<'s> for Server<G> {
    type P = PathServerWrapper<'s, G>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query.from, query.to)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}
