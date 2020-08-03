use super::*;

use crate::algo::dijkstra::gen_topo_dijkstra::*;
use crate::datastr::graph::time_dependent::*;

pub struct Server<P, Ops: DijkstraOps<Graph>, Graph> {
    forward_dijkstra: GenTopoDijkstra<Ops, Graph>,
    potential: P,

    #[cfg(feature = "chpot-visualize")]
    lat: &[f32],
    #[cfg(feature = "chpot-visualize")]
    lng: &[f32],
}

impl<P: Potential, Ops: DijkstraOps<Graph, Label = Timestamp>, Graph> Server<P, Ops, Graph>
where
    Graph: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, Ops::Arc>,
{
    pub fn new<G>(graph: G, potential: P, ops: Ops, #[cfg(feature = "chpot-visualize")] lat: &[f32], #[cfg(feature = "chpot-visualize")] lng: &[f32]) -> Self
    where
        G: for<'a> LinkIterable<'a, NodeId>,
        Graph: BuildPermutated<G>,
    {
        Self {
            forward_dijkstra: report_time_with_key("TDTopoDijkstra preprocessing", "topo_dijk_prepro", || GenTopoDijkstra::new_with_ops(graph, ops)),
            potential,

            #[cfg(feature = "chpot-visualize")]
            lat,
            #[cfg(feature = "chpot-visualize")]
            lng,
        }
    }

    fn distance(&mut self, query: impl GenQuery<Timestamp>) -> Option<Weight> {
        report!("algo", "CH Potentials TD Query");

        let to = query.to();
        let departure = query.initial_state();

        #[cfg(feature = "chpot-visualize")]
        {
            println!(
                "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
                self.lat[from as usize], self.lng[from as usize]
            );
            println!(
                "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
                self.lat[to as usize], self.lng[to as usize]
            );
        };
        let mut num_queue_pops = 0;

        self.forward_dijkstra.initialize_query(query);
        self.potential.init(to);
        let forward_dijkstra = &mut self.forward_dijkstra;
        let potential = &mut self.potential;

        while let Some(node) = forward_dijkstra.next_step_with_potential(|node| {
            if cfg!(feature = "chpot-only-topo") {
                Some(0)
            } else {
                potential.potential(node)
            }
        }) {
            num_queue_pops += 1;
            #[cfg(feature = "chpot-visualize")]
            {
                let node_id = self.order.node(_node) as usize;
                println!(
                    "var marker = L.marker([{}, {}], {{ icon: blueIcon }}).addTo(map);",
                    self.lat[node_id], self.lng[node_id]
                );
                println!(
                    "marker.bindPopup(\"id: {}<br>distance: {}<br>potential: {}\");",
                    node_id,
                    distance,
                    potential.potential(_node)
                );
            };

            if node == to
                || forward_dijkstra
                    .queue()
                    .peek()
                    .map(|e| e.key >= *forward_dijkstra.tentative_distance(to))
                    .unwrap_or(false)
            {
                break;
            }
        }
        report!("num_queue_pops", num_queue_pops);
        report!("num_queue_pushs", forward_dijkstra.num_queue_pushs());
        report!("num_pot_evals", potential.num_pot_evals());
        report!("num_relaxed_arcs", forward_dijkstra.num_relaxed_arcs());
        let dist = *forward_dijkstra.tentative_distance(to);
        if dist < INFINITY {
            Some(dist - departure)
        } else {
            None
        }
    }

    fn path(&self, query: impl GenQuery<Timestamp>) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(query.to());

        while *path.last().unwrap() != query.from() {
            let next = self.forward_dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();

        path
    }
}

pub struct PathServerWrapper<'s, P, O: DijkstraOps<G>, G, Q>(&'s mut Server<P, O, G>, Q);

impl<'s, P, O, G, Q> PathServer for PathServerWrapper<'s, P, O, G, Q>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc>,
    Q: GenQuery<Timestamp> + Copy,
{
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<'s, P, O, G, Q> PathServerWrapper<'s, P, O, G, Q>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc>,
    Q: GenQuery<Timestamp> + Copy,
{
    /// Print path with debug info as js to stdout.
    pub fn debug_path(&mut self, lat: &[f32], lng: &[f32]) {
        for node in self.path() {
            println!(
                "var marker = L.marker([{}, {}], {{ icon: blackIcon }}).addTo(map);",
                lat[node as usize], lng[node as usize]
            );
            let dist = *self.0.forward_dijkstra.tentative_distance(node);
            let pot = self.lower_bound(node).unwrap_or(INFINITY);
            println!(
                "marker.bindPopup(\"id: {}<br>distance: {}<br>lower_bound: {}<br>sum: {}\");",
                node,
                dist / 1000,
                pot / 1000,
                (pot + dist) / 1000
            );
        }
    }

    pub fn lower_bound(&mut self, node: NodeId) -> Option<Weight> {
        self.0.potential.potential(node)
    }
}

impl<'s, P: 's, O: 's, G: 's> TDQueryServer<'s, Timestamp, Weight> for Server<P, O, G>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc>,
{
    type P = PathServerWrapper<'s, P, O, G, TDQuery<Timestamp>>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}

impl<'s, P: 's, O: 's, G: 's> QueryServer<'s> for Server<P, O, G>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, O::Arc>,
{
    type P = PathServerWrapper<'s, P, O, G, Query>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}
