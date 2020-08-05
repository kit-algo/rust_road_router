use super::*;
use crate::datastr::graph::time_dependent::Timestamp;
use crate::report::*;
use generic_dijkstra::*;

pub struct Server<Ops: DijkstraOps<Graph>, Graph> {
    dijkstra: GenericDijkstra<Ops, Graph>,
}

impl<Ops: DijkstraOps<Graph, Label = Weight>, Graph: for<'a> LinkIterable<'a, Ops::Arc>> Server<Ops, Graph> {
    pub fn new(graph: Graph) -> Self
    where
        Ops: Default,
    {
        Self {
            dijkstra: GenericDijkstra::new(graph),
        }
    }

    fn distance(&mut self, query: impl GenQuery<Weight>) -> Option<Weight> {
        report!("algo", "Dijkstra Query");
        let to = query.to();
        self.dijkstra.initialize_query(query);

        let mut num_queue_pops = 0;
        while let Some(node) = self.dijkstra.next() {
            num_queue_pops += 1;
            if node == to {
                return Some(*self.dijkstra.tentative_distance(node));
            }
        }

        report!("num_queue_pops", num_queue_pops);
        report!("num_queue_pushs", self.dijkstra.num_queue_pushs());
        report!("num_relaxed_arcs", self.dijkstra.num_relaxed_arcs());

        None
    }

    fn path(&self, query: impl GenQuery<Weight>) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(query.to());

        while *path.last().unwrap() != query.from() {
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
        while let Some(node) = self.dijkstra.next() {
            i += 1;
            if (i & (i - 1)) == 0 {
                // power of two
                callback(node, *self.dijkstra.tentative_distance(node), i.trailing_zeros() as usize);
            }
        }
    }

    pub fn one_to_all(&mut self, from: NodeId) -> ServerWrapper<Ops, Graph> {
        self.distance(Query {
            from,
            to: self.dijkstra.graph().num_nodes() as NodeId,
        });
        ServerWrapper(self)
    }
}

pub struct PathServerWrapper<'s, O: DijkstraOps<G>, G, Q>(&'s mut Server<O, G>, Q);

impl<'s, O, G, Q> PathServer for PathServerWrapper<'s, O, G, Q>
where
    O: DijkstraOps<G, Label = Weight>,
    G: for<'a> LinkIterable<'a, O::Arc>,
    Q: GenQuery<Weight> + Copy,
{
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<'s, O, G, Q> PathServerWrapper<'s, O, G, Q>
where
    O: DijkstraOps<G, Label = Weight>,
    G: for<'a> LinkIterable<'a, O::Arc>,
    Q: GenQuery<Weight> + Copy,
{
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

pub struct ServerWrapper<'s, O: DijkstraOps<G>, G>(&'s Server<O, G>);

impl<'s, O: DijkstraOps<G, Label = Weight>, G: for<'a> LinkIterable<'a, O::Arc>> ServerWrapper<'s, O, G> {
    pub fn distance(&self, node: NodeId) -> Weight {
        *self.0.dijkstra.tentative_distance(node)
    }
}

impl<'s, O: 's + DijkstraOps<G, Label = Weight>, G: 's + for<'a> LinkIterable<'a, O::Arc>> QueryServer<'s> for Server<O, G> {
    type P = PathServerWrapper<'s, O, G, Query>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}

impl<'s, O: 's + DijkstraOps<G, Label = Weight>, G: 's + for<'a> LinkIterable<'a, O::Arc>> TDQueryServer<'s, Timestamp, Weight> for Server<O, G> {
    type P = PathServerWrapper<'s, O, G, TDQuery<Timestamp>>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}
