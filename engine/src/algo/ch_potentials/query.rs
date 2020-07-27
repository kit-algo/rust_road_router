use super::*;
use crate::{algo::dijkstra::gen_topo_dijkstra::*, report::*};

pub struct Server<P, G> {
    forward_dijkstra: StandardTopoDijkstra<G>,
    potential: P,

    #[cfg(feature = "chpot_visualize")]
    lat: &[f32],
    #[cfg(feature = "chpot_visualize")]
    lng: &[f32],
}

impl<P, G> Server<P, G>
where
    P: Potential,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, Link>,
{
    pub fn new<Graph>(graph: Graph, potential: P, #[cfg(feature = "chpot_visualize")] lat: &[f32], #[cfg(feature = "chpot_visualize")] lng: &[f32]) -> Self
    where
        Graph: for<'a> LinkIterable<'a, NodeId>,
        G: BuildPermutated<Graph>,
    {
        Server {
            forward_dijkstra: report_time_with_key("TopoDijkstra preprocessing", "topo_dijk_prepro", || GenTopoDijkstra::new(graph)),
            potential,

            #[cfg(feature = "chpot_visualize")]
            lat,
            #[cfg(feature = "chpot_visualize")]
            lng,
        }
    }

    fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        report!("algo", "CH Potentials Query");

        #[cfg(feature = "chpot_visualize")]
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

        self.forward_dijkstra.initialize_query(Query { from, to });
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
            #[cfg(feature = "chpot_visualize")]
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
            Some(dist)
        } else {
            None
        }
    }

    fn path(&self, query: Query) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(query.to);

        while *path.last().unwrap() != query.from {
            let next = self.forward_dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();

        path
    }
}

pub struct PathServerWrapper<'s, P, G>(&'s mut Server<P, G>, Query);

impl<'s, P, G> PathServer for PathServerWrapper<'s, P, G>
where
    P: Potential,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, Link>,
{
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<'s, P, G> PathServerWrapper<'s, P, G>
where
    P: Potential,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, Link>,
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

impl<'s, P: 's, G: 's> QueryServer<'s> for Server<P, G>
where
    P: Potential,
    G: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, Link>,
{
    type P = PathServerWrapper<'s, P, G>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query.from, query.to)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self, query)))
    }
}
