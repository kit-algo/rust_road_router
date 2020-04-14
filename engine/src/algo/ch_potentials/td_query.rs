use super::*;
use crate::algo::customizable_contraction_hierarchy::{query::stepped_elimination_tree::SteppedEliminationTree, *};
use crate::algo::dijkstra::td_topo_dijkstra::TDTopoDijkstra;
use crate::datastr::graph::time_dependent::*;
use crate::datastr::timestamped_vector::TimestampedVector;
use crate::util::in_range_option::InRangeOption;

#[derive(Debug)]
pub struct Server<'a> {
    forward_dijkstra: TDTopoDijkstra,
    stack: Vec<NodeId>,

    cch: &'a CCH,

    potentials: TimestampedVector<InRangeOption<Weight>>,
    forward_cch_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<Weight>>,
    backward_elimination_tree: SteppedEliminationTree<'a, FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<Weight>>>,

    #[cfg(feature = "chpot_visualize")]
    lat: &[f32],
    #[cfg(feature = "chpot_visualize")]
    lng: &[f32],
}

impl<'a> Server<'a> {
    pub fn new(graph: TDGraph, cch: &'a CCH, #[cfg(feature = "chpot_visualize")] lat: &[f32], #[cfg(feature = "chpot_visualize")] lng: &[f32]) -> Server<'a> {
        let lower_bound = (0..graph.num_arcs() as EdgeId)
            .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
            .collect::<Vec<Weight>>();
        let customized = customize(cch, &FirstOutGraph::new(graph.first_out(), graph.head(), lower_bound));
        let (forward_up_graph, backward_up_graph) = customized.into_ch_graphs();
        let backward_elimination_tree = SteppedEliminationTree::new(backward_up_graph, cch.elimination_tree());

        Server {
            forward_dijkstra: TDTopoDijkstra::new(graph),
            stack: Vec::new(),
            cch,
            forward_cch_graph: forward_up_graph,
            backward_elimination_tree,
            potentials: TimestampedVector::new(cch.num_nodes(), InRangeOption::new(None)),

            #[cfg(feature = "chpot_visualize")]
            lat,
            #[cfg(feature = "chpot_visualize")]
            lng,
        }
    }

    fn distance(&mut self, from: NodeId, to: NodeId, departure: Timestamp) -> Option<Weight> {
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

        self.potentials.reset();
        self.backward_elimination_tree.initialize_query(self.cch.node_order().rank(to));
        while self.backward_elimination_tree.next().is_some() {
            self.backward_elimination_tree.next_step();
        }

        self.forward_dijkstra.initialize_query(TDQuery { from, to, departure });
        let forward_dijkstra = &mut self.forward_dijkstra;
        let stack = &mut self.stack;

        let cch = &self.cch;

        let potentials = &mut self.potentials;
        let backward_elimination_tree = &self.backward_elimination_tree;
        let forward_cch_graph = &self.forward_cch_graph;

        loop {
            match forward_dijkstra.next_step_with_potential(|node| {
                if cfg!(feature = "chpot-only-topo") {
                    Some(0)
                } else {
                    Self::potential(potentials, forward_cch_graph, backward_elimination_tree, cch.node_order().rank(node), stack)
                }
            }) {
                QueryProgress::Settled(State { node: _node, .. }) => {
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
                            Self::potential(
                                potentials,
                                forward_elimination_tree,
                                backward_elimination_tree,
                                cch.node_order().rank(order.node(_node)),
                                stack
                            )
                        );
                    };
                }
                QueryProgress::Done(result) => return result,
            }
        }
    }

    fn potential(
        potentials: &mut TimestampedVector<InRangeOption<Weight>>,
        forward: &FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>,
        backward: &SteppedEliminationTree<'_, FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>>,
        node: NodeId,
        stack: &mut Vec<NodeId>,
    ) -> Option<Weight> {
        let mut cur_node = node;
        while potentials[cur_node as usize].value().is_none() {
            stack.push(cur_node);
            if let Some(parent) = backward.parent(cur_node).value() {
                cur_node = parent;
            } else {
                break;
            }
        }

        while let Some(node) = stack.pop() {
            let min_by_up = forward
                .neighbor_iter(node)
                .map(|edge| edge.weight + potentials[edge.node as usize].value().unwrap())
                .min()
                .unwrap_or(INFINITY);

            potentials[node as usize] = InRangeOption::new(Some(std::cmp::min(backward.tentative_distance(node), min_by_up)));
        }

        let dist = potentials[node as usize].value().unwrap();
        if dist < INFINITY {
            Some(dist)
        } else {
            None
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

pub struct PathServerWrapper<'s, 'a>(&'s Server<'a>);

impl<'s, 'a> PathServer for PathServerWrapper<'s, 'a> {
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0)
    }
}

impl<'s, 'a: 's> TDQueryServer<'s, Timestamp, Weight> for Server<'a> {
    type P = PathServerWrapper<'s, 'a>;

    fn query(&'s mut self, query: TDQuery<Timestamp>) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query.from, query.to, query.departure)
            .map(move |distance| QueryResult::new(distance, PathServerWrapper(self)))
    }
}
