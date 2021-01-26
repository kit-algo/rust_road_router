use super::*;
use crate::datastr::graph::time_dependent::Timestamp;
use crate::report::*;
use a_star::{Potential, ZeroPotential};
use generic_dijkstra::*;
use std::borrow::Borrow;

pub struct Server<Graph = OwnedGraph, Ops = DefaultOps, P = ZeroPotential, GraphBorrow = Graph>
where
    Ops: DijkstraOps<Graph>,
{
    dijkstra: GenericDijkstra<Graph, Ops, GraphBorrow>,
    potential: P,
}

impl<Graph, Ops, GraphBorrow> Server<Graph, Ops, ZeroPotential, GraphBorrow>
where
    Ops: DijkstraOps<Graph, Label = Weight>,
    Graph: for<'a> LinkIterable<'a, Ops::Arc>,
    GraphBorrow: Borrow<Graph>,
{
    pub fn new(graph: GraphBorrow) -> Self
    where
        Ops: Default,
    {
        Self {
            dijkstra: GenericDijkstra::new(graph),
            potential: ZeroPotential(),
        }
    }
}

impl<Graph, Ops, P, GraphBorrow> Server<Graph, Ops, P, GraphBorrow>
where
    Ops: DijkstraOps<Graph, Label = Weight>,
    Graph: for<'a> LinkIterable<'a, Ops::Arc>,
    P: Potential,
    GraphBorrow: Borrow<Graph>,
{
    pub fn with_potential(graph: GraphBorrow, potential: P) -> Self
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

    pub fn one_to_all(&mut self, from: NodeId) -> ServerWrapper<Graph, Ops, P, GraphBorrow> {
        self.distance(Query {
            from,
            to: self.dijkstra.graph().num_nodes() as NodeId,
        });
        ServerWrapper(self)
    }
}

pub struct PathServerWrapper<'s, Q, G = OwnedGraph, O = DefaultOps, P = ZeroPotential, B = G>(&'s mut Server<G, O, P, B>, Q)
where
    O: DijkstraOps<G>;

impl<'s, Q, G, O, P, B> PathServer for PathServerWrapper<'s, Q, G, O, P, B>
where
    O: DijkstraOps<G, Label = Weight>,
    G: for<'a> LinkIterable<'a, O::Arc>,
    Q: GenQuery<Weight> + Copy,
    P: Potential,
    B: Borrow<G>,
{
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<'s, Q, G, O, P, B> PathServerWrapper<'s, Q, G, O, P, B>
where
    O: DijkstraOps<G, Label = Weight>,
    G: for<'a> LinkIterable<'a, O::Arc>,
    Q: GenQuery<Weight> + Copy,
    P: Potential,
    B: Borrow<G>,
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

pub struct ServerWrapper<'s, G = OwnedGraph, O = DefaultOps, P = ZeroPotential, B = G>(&'s Server<G, O, P, B>)
where
    O: DijkstraOps<G>;

impl<'s, G: for<'a> LinkIterable<'a, O::Arc>, O: DijkstraOps<G, Label = Weight>, P: Potential, B: Borrow<G>> ServerWrapper<'s, G, O, P, B> {
    pub fn distance(&self, node: NodeId) -> Weight {
        *self.0.dijkstra.tentative_distance(node)
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.0.dijkstra.predecessor(node)
    }
}

impl<'s, G: 's + for<'a> LinkIterable<'a, O::Arc>, O: 's + DijkstraOps<G, Label = Weight>, P: Potential + 's, B: Borrow<G> + 's> QueryServer<'s>
    for Server<G, O, P, B>
{
    type P = PathServerWrapper<'s, Query, G, O, P, B>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}

impl<'s, G: 's + for<'a> LinkIterable<'a, O::Arc>, O: 's + DijkstraOps<G, Label = Weight>, P: Potential + 's, B: Borrow<G> + 's>
    TDQueryServer<'s, Timestamp, Weight> for Server<G, O, P, B>
{
    type P = PathServerWrapper<'s, TDQuery<Timestamp>, G, O, P, B>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}
