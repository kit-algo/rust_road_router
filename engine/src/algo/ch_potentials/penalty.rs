use super::*;

use crate::algo::a_star::*;
use crate::algo::dijkstra::gen_topo_dijkstra::*;
use crate::algo::topocore::*;
use crate::datastr::rank_select_map::*;

pub struct Penalty<P> {
    virtual_topocore: VirtualTopocore,
    shortest_path_penalized: query::SkipLowDegServer<VirtualTopocoreGraph<OwnedGraph>, DefaultOps, PotentialForPermutated<P>>,
    alternative_graph_dijkstra: query::SkipLowDegServer<AlternativeGraph<VirtualTopocoreGraph<OwnedGraph>>>,
    reversed: ReversedGraphWithEdgeIds,
    edge_penelized: BitVec,
    edges_to_reset: Vec<EdgeId>,
}

impl<P: Potential> Penalty<P> {
    pub fn new<G>(graph: &G, potential: P) -> Self
    where
        G: for<'a> LinkIterable<'a, NodeId>,
        OwnedGraph: BuildPermutated<G>,
    {
        let (main_graph, _, virtual_topocore) = VirtualTopocoreGraph::new_topo_dijkstra_graphs(graph);
        let m = main_graph.num_arcs();

        let reversed = ReversedGraphWithEdgeIds::reversed(&main_graph);
        Self {
            shortest_path_penalized: query::SkipLowDegServer::new(
                main_graph.clone(),
                PotentialForPermutated {
                    order: virtual_topocore.order.clone(),
                    potential,
                },
                DefaultOps::default(),
            ),
            alternative_graph_dijkstra: query::SkipLowDegServer::new(
                AlternativeGraph {
                    graph: main_graph,
                    contained_edges: FastClearBitVec::new(m),
                },
                ZeroPotential(),
                DefaultOps::default(),
            ),
            virtual_topocore,
            reversed,
            edge_penelized: BitVec::new(m),
            edges_to_reset: Vec::new(),
        }
    }

    pub fn alternatives(&mut self, mut query: Query) -> Option<()> {
        query.permutate(&self.virtual_topocore.order);
        let core_from = self.virtual_topocore.bridge_node(query.from()).unwrap_or(query.from());
        let core_to = self.virtual_topocore.bridge_node(query.to()).unwrap_or(query.to());
        let query = Query { from: core_from, to: core_to };
        self.alternative_graph_dijkstra.graph_mut().clear();

        if let Some(mut result) = QueryServer::query(&mut self.shortest_path_penalized, query) {
            let base_dist = result.distance;
            let rejoin_penalty = ((base_dist as f64).sqrt() as Weight) / 2;
            let mut path = result.path();
            let shortest_path_penalized = &mut self.shortest_path_penalized;
            let alternative_graph_dijkstra = &mut self.alternative_graph_dijkstra;
            let mut path_edges: Vec<_> = path
                .array_windows::<2>()
                .map(|&[tail, head]| shortest_path_penalized.graph().graph.edge_index(tail, head).unwrap())
                .collect();

            loop {
                for &edge in &path_edges {
                    let weight = &mut shortest_path_penalized.graph_mut().graph.weights_mut()[edge as usize];
                    *weight = ((*weight as f64) * 1.04) as Weight;
                    if self.edge_penelized.get(edge as usize) {
                        self.edge_penelized.set(edge as usize);
                        self.edges_to_reset.push(edge);
                    }
                }
                for &[tail, head] in path.array_windows::<2>() {
                    for (rev_head, edge) in LinkIterable::<(NodeId, EdgeId)>::link_iter(&self.reversed, head) {
                        if rev_head != tail {
                            let weight = &mut shortest_path_penalized.graph_mut().graph.weights_mut()[edge as usize];
                            *weight += rejoin_penalty;
                            if self.edge_penelized.get(edge as usize) {
                                self.edge_penelized.set(edge as usize);
                                self.edges_to_reset.push(edge);
                            }
                        }
                    }
                }

                let mut result = QueryServer::query(shortest_path_penalized, query).unwrap();
                // TODO refactor get path edges
                path = result.path();
                path_edges = path
                    .array_windows::<2>()
                    .map(|&[tail, head]| alternative_graph_dijkstra.graph().graph.graph.edge_index(tail, head).unwrap())
                    .collect();
                let path_orig_len: Weight = path_edges
                    .iter()
                    .map(|&e| alternative_graph_dijkstra.graph().graph.graph.weight()[e as usize])
                    .sum();

                let mut feasable = false;
                let new_parts: Vec<_> = path_edges
                    .split(|&edge| alternative_graph_dijkstra.graph().contained_edges.get(edge as usize))
                    .filter(|part| part.is_empty())
                    .collect();
                for new_part in new_parts {
                    let part_start = result
                        .data()
                        .predecessor(alternative_graph_dijkstra.graph().graph.graph.head()[new_part[0] as usize]);
                    let part_end = alternative_graph_dijkstra.graph().graph.graph.head()[*new_part.last().unwrap() as usize];
                    let part_dist: Weight = new_part
                        .iter()
                        .map(|&e| alternative_graph_dijkstra.graph().graph.graph.weight()[e as usize])
                        .sum();

                    if part_dist * 10 > base_dist {
                        if part_dist * 10
                            <= QueryServer::query(
                                alternative_graph_dijkstra,
                                Query {
                                    from: part_start,
                                    to: part_end,
                                },
                            )
                            .map(|r| r.distance * 11)
                            .unwrap_or(INFINITY)
                        {
                            feasable = true;
                            break;
                        }
                    }
                }

                if feasable {
                    alternative_graph_dijkstra.graph_mut().add_edges(&path_edges);
                }

                if path_orig_len * 10 > base_dist * 11 {
                    break;
                }
            }

            for edge in self.edges_to_reset.drain(..) {
                let e = edge as usize;
                self.edge_penelized.unset_all_around(e as usize);
                shortest_path_penalized.graph_mut().graph.weights_mut()[e as usize] = alternative_graph_dijkstra.graph().graph.graph.weight()[e as usize];
            }

            // XBDV

            Some(())
        } else {
            None
        }
    }
}

struct AlternativeGraph<G> {
    graph: G,
    contained_edges: FastClearBitVec,
}

impl<G> AlternativeGraph<G> {
    fn add_edges(&mut self, edges: &[EdgeId]) {
        for &edge in edges {
            self.contained_edges.set(edge as usize);
        }
    }

    fn clear(&mut self) {
        self.contained_edges.clear()
    }
}

impl<G: Graph> Graph for AlternativeGraph<G> {
    fn degree(&self, node: NodeId) -> usize {
        self.graph.degree(node)
    }
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }
    fn num_arcs(&self) -> usize {
        self.graph.num_arcs()
    }
}

impl<'a, G: for<'b> LinkIterable<'b, L>, L> LinkIterable<'a, L> for AlternativeGraph<G> {
    type Iter = <G as LinkIterable<'a, L>>::Iter;

    #[inline(always)]
    fn link_iter(&'a self, node: NodeId) -> Self::Iter {
        self.graph.link_iter(node)
    }
}

impl<G: SymmetricDegreeGraph> SymmetricDegreeGraph for AlternativeGraph<G> {
    fn symmetric_degree(&self, node: NodeId) -> SymmetricDeg {
        self.graph.symmetric_degree(node)
    }
}
