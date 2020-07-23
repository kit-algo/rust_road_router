use super::*;
use crate::{algo::dijkstra::topo_dijkstra::TopoDijkstra, report::*};

#[derive(Debug)]
pub struct Server<P> {
    forward_dijkstra: TopoDijkstra,
    potential: P,

    #[cfg(feature = "chpot_visualize")]
    lat: &[f32],
    #[cfg(feature = "chpot_visualize")]
    lng: &[f32],
}

impl<P: Potential> Server<P> {
    pub fn new<Graph>(graph: Graph, potential: P, #[cfg(feature = "chpot_visualize")] lat: &[f32], #[cfg(feature = "chpot_visualize")] lng: &[f32]) -> Self
    where
        Graph: for<'b> LinkIterGraph<'b> + for<'b> LinkIterable<'b, NodeId> + RandomLinkAccessGraph + Sync,
    {
        Server {
            forward_dijkstra: report_time_with_key("TopoDijkstra preprocessing", "topo_dijk_prepro", || TopoDijkstra::new(graph)),
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
        let mut num_queue_pushs = 0;
        let mut prev_queue_size = 1;

        self.forward_dijkstra.initialize_query(Query { from, to });
        self.potential.init(to);
        let forward_dijkstra = &mut self.forward_dijkstra;
        let potential = &mut self.potential;

        loop {
            match forward_dijkstra.next_step_with_potential(|node| {
                if cfg!(feature = "chpot-only-topo") {
                    Some(0)
                } else {
                    potential.potential(node)
                }
            }) {
                QueryProgress::Settled(State { node: _node, .. }) => {
                    num_queue_pops += 1;
                    num_queue_pushs += forward_dijkstra.queue().len() + 1 - prev_queue_size;
                    prev_queue_size = forward_dijkstra.queue().len();
                    #[cfg(feature = "chpot-print-node-order")]
                    {
                        println!("{}", _node);
                    }
                    #[cfg(feature = "chpot_visualize")]
                    {
                        let node_id = _node as usize;
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
                }
                QueryProgress::Done(result) => {
                    report!("num_queue_pops", num_queue_pops);
                    report!("num_queue_pushs", num_queue_pushs);
                    report!("num_pot_evals", potential.num_pot_evals());
                    report!("num_relaxed_arcs", self.forward_dijkstra.num_relaxed_arcs());
                    return result;
                }
            }
        }
    }

    fn path(&self) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(self.forward_dijkstra.query().to);

        while *path.last().unwrap() != self.forward_dijkstra.query().from {
            let next = self.forward_dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();

        path
    }
}

pub struct PathServerWrapper<'s, P>(&'s mut Server<P>);

impl<'s, P: Potential> PathServer for PathServerWrapper<'s, P> {
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0)
    }
}

impl<'s, P: Potential> PathServerWrapper<'s, P> {
    /// Print path with debug info as js to stdout.
    pub fn debug_path(&mut self, lat: &[f32], lng: &[f32]) {
        for node in self.path() {
            println!(
                "var marker = L.marker([{}, {}], {{ icon: blackIcon }}).addTo(map);",
                lat[node as usize], lng[node as usize]
            );
            let dist = self.0.forward_dijkstra.tentative_distance(node);
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

impl<'s, P: Potential + 's> QueryServer<'s> for Server<P> {
    type P = PathServerWrapper<'s, P>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query.from, query.to)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self)))
    }
}
