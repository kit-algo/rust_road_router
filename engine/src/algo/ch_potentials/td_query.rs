use super::*;

use crate::algo::dijkstra::td_topo_dijkstra::TDTopoDijkstra;
use crate::datastr::graph::time_dependent::*;
use crate::report::*;

#[derive(Debug)]
pub struct Server<P> {
    forward_dijkstra: TDTopoDijkstra,
    potential: P,

    #[cfg(feature = "chpot_visualize")]
    lat: &[f32],
    #[cfg(feature = "chpot_visualize")]
    lng: &[f32],
}

impl<P: Potential> Server<P> {
    pub fn new(graph: TDGraph, potential: P, #[cfg(feature = "chpot_visualize")] lat: &[f32], #[cfg(feature = "chpot_visualize")] lng: &[f32]) -> Self {
        Self {
            forward_dijkstra: report_time_with_key("TDTopoDijkstra preprocessing", "topo_dijk_prepro", || TDTopoDijkstra::new(graph)),
            potential,

            #[cfg(feature = "chpot_visualize")]
            lat,
            #[cfg(feature = "chpot_visualize")]
            lng,
        }
    }

    fn distance(&mut self, from: NodeId, to: NodeId, departure: Timestamp) -> Option<Weight> {
        report!("algo", "CH Potentials TD Query");

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

        self.forward_dijkstra.initialize_query(TDQuery { from, to, departure });
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
                }
                QueryProgress::Done(result) => {
                    report!("num_queue_pops", num_queue_pops);
                    report!("num_queue_pushs", num_queue_pushs);
                    report!("num_pot_evals", potential.num_pot_evals());
                    report!("num_relaxed_arcs", forward_dijkstra.num_relaxed_arcs());
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

    pub fn lower_bound(&mut self, node: NodeId) -> Option<Weight> {
        self.potential.potential(node)
    }
}

pub struct PathServerWrapper<'s, P>(&'s Server<P>);

impl<'s, P: Potential> PathServer for PathServerWrapper<'s, P> {
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0)
    }
}

impl<'s, P: Potential + 's> TDQueryServer<'s, Timestamp, Weight> for Server<P> {
    type P = PathServerWrapper<'s, P>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query.from, query.to, query.departure)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self)))
    }
}
