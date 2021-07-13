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
    graph: GraphBorrow,
    dijkstra: DijkstraData<Ops::Label>,
    potential: P,
}

impl<Graph, Ops, GraphBorrow> Server<Graph, Ops, ZeroPotential, GraphBorrow>
where
    Ops: DijkstraOps<Graph, Label = Weight>,
    Graph: LinkIterable<Ops::Arc>,
    GraphBorrow: Borrow<Graph>,
{
    pub fn new(graph: GraphBorrow) -> Self {
        Self {
            dijkstra: DijkstraData::new(graph.borrow().num_nodes()),
            graph,
            potential: ZeroPotential(),
        }
    }
}

impl<Graph, Ops, P, GraphBorrow> Server<Graph, Ops, P, GraphBorrow>
where
    Ops: DijkstraOps<Graph, Label = Weight> + Default,
    Graph: LinkIterable<Ops::Arc>,
    P: Potential,
    GraphBorrow: Borrow<Graph>,
{
    pub fn with_potential(graph: GraphBorrow, potential: P) -> Self {
        Self {
            dijkstra: DijkstraData::new(graph.borrow().num_nodes()),
            graph,
            potential,
        }
    }

    fn distance(&mut self, query: impl GenQuery<Weight>) -> Option<Weight> {
        report!("algo", "Dijkstra Query");
        let to = query.to();
        let mut ops = Ops::default();
        let mut dijkstra = DijkstraRun::query(self.graph.borrow(), &mut self.dijkstra, &mut ops, query);
        self.potential.init(to);

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
        report!("num_queue_pushs", dijkstra.num_queue_pushs());
        report!("num_relaxed_arcs", dijkstra.num_relaxed_arcs());

        result
    }

    fn path(&self, query: impl GenQuery<Weight>) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(query.to());

        while *path.last().unwrap() != query.from() {
            let next = self.dijkstra.predecessors[*path.last().unwrap() as usize];
            path.push(next);
        }

        path.reverse();

        path
    }

    pub fn ranks<F>(&mut self, from: NodeId, mut callback: F)
    where
        F: (FnMut(NodeId, Weight, usize)),
    {
        let mut ops = Ops::default();
        let mut dijkstra = DijkstraRun::query(
            self.graph.borrow(),
            &mut self.dijkstra,
            &mut ops,
            Query {
                from,
                to: self.graph.borrow().num_nodes() as NodeId,
            },
        );

        let mut i: usize = 0;
        while let Some(node) = dijkstra.next() {
            i += 1;
            if (i & (i - 1)) == 0 {
                // power of two
                callback(node, *dijkstra.tentative_distance(node), i.trailing_zeros() as usize);
            }
        }
    }

    pub fn one_to_all(&mut self, from: NodeId) -> ServerWrapper<Graph, Ops, P, GraphBorrow> {
        self.distance(Query {
            from,
            to: self.graph.borrow().num_nodes() as NodeId,
        });
        ServerWrapper(self)
    }
}

pub struct PathServerWrapper<'s, Q, G = OwnedGraph, O = DefaultOps, P = ZeroPotential, B = G>(&'s mut Server<G, O, P, B>, Q)
where
    O: DijkstraOps<G>;

impl<'s, Q, G, O, P, B> PathServer for PathServerWrapper<'s, Q, G, O, P, B>
where
    O: DijkstraOps<G, Label = Weight> + Default,
    G: LinkIterable<O::Arc>,
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
    O: DijkstraOps<G, Label = Weight> + Default,
    G: LinkIterable<O::Arc>,
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
            let dist = self.0.dijkstra.distances[node as usize];
            println!("marker.bindPopup(\"id: {}<br>distance: {}\");", node, dist / 1000);
        }
    }

    pub fn distance(&self, node: NodeId) -> Weight {
        self.0.dijkstra.distances[node as usize]
    }
}

pub struct ServerWrapper<'s, G = OwnedGraph, O = DefaultOps, P = ZeroPotential, B = G>(&'s Server<G, O, P, B>)
where
    O: DijkstraOps<G>;

impl<'s, G: LinkIterable<O::Arc>, O: DijkstraOps<G, Label = Weight>, P: Potential, B: Borrow<G>> ServerWrapper<'s, G, O, P, B> {
    pub fn distance(&self, node: NodeId) -> Weight {
        self.0.dijkstra.distances[node as usize]
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.0.dijkstra.predecessors[node as usize]
    }
}

impl<G: LinkIterable<O::Arc>, O: DijkstraOps<G, Label = Weight> + Default, P: Potential, B: Borrow<G>> QueryServer for Server<G, O, P, B> {
    type P<'s>
    where
        Self: 's,
    = PathServerWrapper<'s, Query, G, O, P, B>;

    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query), PathServerWrapper(self, query))
    }
}

impl<G: LinkIterable<O::Arc>, O: DijkstraOps<G, Label = Weight> + Default, P: Potential, B: Borrow<G>> TDQueryServer<Timestamp, Weight> for Server<G, O, P, B> {
    type P<'s>
    where
        Self: 's,
    = PathServerWrapper<'s, TDQuery<Timestamp>, G, O, P, B>;

    fn td_query(&mut self, query: TDQuery<Timestamp>) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query), PathServerWrapper(self, query))
    }
}
