use super::*;
use crate::datastr::graph::time_dependent::Timestamp;
use crate::report::*;
use ch_potentials::{Potential, ZeroPotential};
use generic_dijkstra::*;

pub struct Server<Ops: DijkstraOps<Graph>, Graph, P> {
    dijkstra: GenericDijkstra<Ops, Graph>,
    potential: P,
}

impl<Ops: DijkstraOps<Graph, Label = Weight>, Graph: for<'a> LinkIterable<'a, Ops::Arc>> Server<Ops, Graph, ZeroPotential> {
    pub fn new(graph: Graph) -> Self
    where
        Ops: Default,
    {
        Self {
            dijkstra: GenericDijkstra::new(graph),
            potential: ZeroPotential(),
        }
    }
}

impl<Ops, Graph, P> Server<Ops, Graph, P>
where
    Ops: DijkstraOps<Graph, Label = Weight>,
    Graph: for<'a> LinkIterable<'a, Ops::Arc>,
    P: Potential,
{
    pub fn with_potential(graph: Graph, potential: P) -> Self
    where
        Ops: Default,
    {
        Self {
            dijkstra: GenericDijkstra::new(graph),
            potential,
        }
    }

    fn distance(&mut self, query: impl GenQuery<Weight>) -> Option<Weight> {
        report!("algo", "Dijkstra Query");
        let to = query.to();
        self.dijkstra.initialize_query(query);
        self.potential.init(to);

        let dijkstra = &mut self.dijkstra;
        let potential = &mut self.potential;

        let mut result = None;
        let mut num_queue_pops = 0;
        while let Some(node) = dijkstra.next_step_with_potential(|node| potential.potential(node)) {
            num_queue_pops += 1;
            if node == to {
                result = Some(*dijkstra.tentative_distance(node));
                break;
            }
        }

        report!("num_queue_pops", num_queue_pops);
        report!("num_queue_pushs", self.dijkstra.num_queue_pushs());
        report!("num_relaxed_arcs", self.dijkstra.num_relaxed_arcs());

        result
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

    pub fn one_to_all(&mut self, from: NodeId) -> ServerWrapper<Ops, Graph, P> {
        self.distance(Query {
            from,
            to: self.dijkstra.graph().num_nodes() as NodeId,
        });
        ServerWrapper(self)
    }
}

pub struct PathServerWrapper<'s, O: DijkstraOps<G>, G, Q, P>(&'s mut Server<O, G, P>, Q);

impl<'s, O, G, Q, P> PathServer for PathServerWrapper<'s, O, G, Q, P>
where
    O: DijkstraOps<G, Label = Weight>,
    G: for<'a> LinkIterable<'a, O::Arc>,
    Q: GenQuery<Weight> + Copy,
    P: Potential,
{
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<'s, O, G, Q, P> PathServerWrapper<'s, O, G, Q, P>
where
    O: DijkstraOps<G, Label = Weight>,
    G: for<'a> LinkIterable<'a, O::Arc>,
    Q: GenQuery<Weight> + Copy,
    P: Potential,
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

pub struct ServerWrapper<'s, O: DijkstraOps<G>, G, P>(&'s Server<O, G, P>);

impl<'s, O: DijkstraOps<G, Label = Weight>, G: for<'a> LinkIterable<'a, O::Arc>, P: Potential> ServerWrapper<'s, O, G, P> {
    pub fn distance(&self, node: NodeId) -> Weight {
        *self.0.dijkstra.tentative_distance(node)
    }
}

impl<'s, O: 's + DijkstraOps<G, Label = Weight>, G: 's + for<'a> LinkIterable<'a, O::Arc>, P: Potential + 's> QueryServer<'s> for Server<O, G, P> {
    type P = PathServerWrapper<'s, O, G, Query, P>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}

impl<'s, O: 's + DijkstraOps<G, Label = Weight>, G: 's + for<'a> LinkIterable<'a, O::Arc>, P: Potential + 's> TDQueryServer<'s, Timestamp, Weight>
    for Server<O, G, P>
{
    type P = PathServerWrapper<'s, O, G, TDQuery<Timestamp>, P>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}
