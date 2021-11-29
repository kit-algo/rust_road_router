use super::*;

use crate::algo::a_star::*;
use crate::algo::dijkstra::gen_topo_dijkstra::*;
use crate::algo::topocore::*;
use crate::datastr::rank_select_map::*;

pub struct Penalty<P> {
    virtual_topocore: VirtualTopocore,
    // shortest_path_penalized: query::SkipLowDegServer<VirtualTopocoreGraph<OwnedGraph>, DefaultOpsWithLinkPath, RecyclingPotential<PotentialForPermutated<P>>, true, true>,
    // shortest_path_penalized: query::BiDirCoreServer<
    //     SymmetricBiDirPotential<RecyclingPotential<PotentialForPermutated<P>>, RecyclingPotential<PotentialForPermutated<P>>>,
    //     AlternatingDirs,
    // >,
    shortest_path_penalized: query::MultiThreadedBiDirSkipLowDegServer<BidirPenaltyPot<P>>,
    alternative_graph_dijkstra: query::SkipLowDegServer<AlternativeGraph<VirtualTopocoreGraph<OwnedGraph>>, DefaultOps, ZeroPotential, true, true>,
    reversed: ReversedGraphWithEdgeIds,
    tail: Vec<NodeId>,
    times_penalized: Vec<u8>,
    nodes_to_reset: Vec<NodeId>,
    reverse_dijkstra_data: DijkstraData<Weight>,
}

type PenaltyPot<P> = RecyclingPotential<PotentialForPermutated<P>>;
type BidirPenaltyPot<P> = SymmetricBiDirPotential<PenaltyPot<P>, PenaltyPot<P>>;

impl<P: Potential + Send + Clone> Penalty<P> {
    pub fn new<G>(graph: &G, forward_potential: P, backward_potential: P) -> Self
    where
        G: LinkIterable<NodeIdT>,
        OwnedGraph: BuildPermutated<G>,
    {
        let (main_graph, _, virtual_topocore) = VirtualTopocoreGraph::new_topo_dijkstra_graphs(graph);
        let n = main_graph.num_nodes();
        let m = main_graph.num_arcs();

        let reversed = ReversedGraphWithEdgeIds::reversed(&main_graph);
        let mut edge_id = 0;
        let mut tail = vec![0; m];
        for node in 0..main_graph.num_nodes() {
            for _ in LinkIterable::<NodeIdT>::link_iter(&main_graph, node as NodeId) {
                tail[edge_id] = node as NodeId;
                edge_id += 1;
            }
        }
        Self {
            // shortest_path_penalized: query::SkipLowDegServer::new(
            // shortest_path_penalized: query::BiDirCoreServer::new(
            shortest_path_penalized: query::MultiThreadedBiDirSkipLowDegServer::new(
                main_graph.clone(),
                SymmetricBiDirPotential::new(
                    RecyclingPotential::new(PotentialForPermutated {
                        order: virtual_topocore.order.clone(),
                        potential: forward_potential,
                    }),
                    RecyclingPotential::new(PotentialForPermutated {
                        order: virtual_topocore.order.clone(),
                        potential: backward_potential,
                    }),
                ),
                // DefaultOpsWithLinkPath::default(),
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
            times_penalized: vec![0; n],
            nodes_to_reset: Vec::new(),
            tail,
            reverse_dijkstra_data: DijkstraData::new(n),
        }
    }

    pub fn alternatives(&mut self, mut query: Query) -> Option<()> {
        query.permutate(&self.virtual_topocore.order);
        let core_from = self.virtual_topocore.bridge_node(query.from()).unwrap_or(query.from);
        let core_to = self.virtual_topocore.bridge_node(query.to()).unwrap_or(query.to);
        let query = Query { from: core_from, to: core_to };
        self.alternative_graph_dijkstra.graph_mut().clear();

        if let Some(mut result) = silent_report_time_with_key("initial_query", || self.shortest_path_penalized.query(query)).found() {
            let base_dist = result.distance();
            report!("base_dist", base_dist);

            let max_orig_dist = (base_dist as f64 * 1.25) as Weight;
            let rejoin_penalty = base_dist / 100;

            let max_penalized_dist = (base_dist as f64 * 1.25 * 1.1) as Weight + 2 * rejoin_penalty;
            let max_num_penalizations = 5;

            let mut path = result.node_path();
            let mut path_edges = result.edge_path();
            let shortest_path_penalized = &mut self.shortest_path_penalized;
            let alternative_graph_dijkstra = &mut self.alternative_graph_dijkstra;

            alternative_graph_dijkstra.graph_mut().add_edges(&path_edges);
            let path_orig_len: Weight = path_edges
                .iter()
                .map(|&EdgeIdT(e)| alternative_graph_dijkstra.graph().graph.graph.weight()[e as usize])
                .sum();
            debug_assert_eq!(base_dist, path_orig_len);

            let mut iterations_ctxt = push_collection_context("iterations".to_string());

            let mut i = 0;
            let mut s = 0;
            loop {
                i += 1;
                let _iteration_ctxt = iterations_ctxt.push_collection_item();
                report!("iteration", i);

                for &EdgeIdT(edge) in &path_edges {
                    if self.times_penalized[self.tail[edge as usize] as usize] < max_num_penalizations
                        || self.times_penalized[alternative_graph_dijkstra.graph().graph.graph.head()[edge as usize] as usize] < max_num_penalizations
                    {
                        let weight = shortest_path_penalized.graph().graph.weight()[edge as usize];
                        shortest_path_penalized.set_edge_weight(edge, weight * 11 / 10);
                    }
                }
                let dists: Vec<_> = std::iter::once(0)
                    .chain(path_edges.iter().scan(0, |state, &EdgeIdT(edge)| {
                        *state += shortest_path_penalized.graph().graph.weight()[edge as usize];
                        Some(*state)
                    }))
                    .collect();
                let total_penalized_dist = dists.last().unwrap();
                for (&[tail, head], &[tail_dist, head_dist]) in path.array_windows::<2>().zip(dists.array_windows::<2>()) {
                    if self.times_penalized[head as usize] < max_num_penalizations {
                        for (NodeIdT(rev_head), Reversed(EdgeIdT(edge))) in self.reversed.link_iter(head) {
                            if rev_head != tail {
                                let weight = shortest_path_penalized.graph().graph.weight()[edge as usize];
                                shortest_path_penalized.set_edge_weight(
                                    edge,
                                    weight + (rejoin_penalty as u64 * head_dist as u64 / *total_penalized_dist as u64) as Weight,
                                );
                            }
                        }
                    }

                    if self.times_penalized[tail as usize] < max_num_penalizations {
                        for (NodeIdT(edge_head), EdgeIdT(edge)) in
                            LinkIterable::<(NodeIdT, EdgeIdT)>::link_iter(&alternative_graph_dijkstra.graph().graph, tail)
                        {
                            if edge_head != head {
                                let weight = shortest_path_penalized.graph().graph.weight()[edge as usize];
                                shortest_path_penalized.set_edge_weight(
                                    edge,
                                    weight + (rejoin_penalty as u64 * tail_dist as u64 / *total_penalized_dist as u64) as Weight,
                                );
                            }
                        }
                    }
                }

                let mut any_penalized = false;
                for &node in &path {
                    if self.times_penalized[node as usize] == 0 {
                        self.nodes_to_reset.push(node);
                    }
                    if self.times_penalized[node as usize] < max_num_penalizations {
                        any_penalized = true;
                        self.times_penalized[node as usize] += 1;
                    }
                }

                if !any_penalized {
                    // dbg!("saturated");
                    break;
                }

                let (result, time) = measure(|| shortest_path_penalized.distance_with_cap(query, max_penalized_dist, Some(max_orig_dist)));
                // let (result, time) = measure(|| shortest_path_penalized.distance_with_cap(query, max_penalized_dist));
                report!("running_time_ms", time.as_secs_f64() * 1000.0);
                let mut result = if let Some(result) = result.found() {
                    result
                } else {
                    // dbg!("search pruned to death");
                    break;
                };
                let penalty_dist = result.distance();
                report!("penalty_dist", penalty_dist);

                path = result.node_path();
                path_edges = result.edge_path();
                let path_orig_len: Weight = path_edges
                    .iter()
                    .map(|&EdgeIdT(e)| alternative_graph_dijkstra.graph().graph.graph.weight()[e as usize])
                    .sum();
                report!("orig_dist", path_orig_len);
                debug_assert!(path_orig_len >= base_dist);

                let mut feasable = false;
                let new_parts: Vec<_> = path_edges
                    .split(|&EdgeIdT(edge)| alternative_graph_dijkstra.graph().contained_edges.get(edge as usize))
                    .filter(|part| !part.is_empty())
                    .collect();

                for new_part in new_parts {
                    let part_start = self.tail[new_part[0].0 as usize];
                    let part_end = alternative_graph_dijkstra.graph().graph.graph.head()[new_part.last().unwrap().0 as usize];

                    let part_dist: Weight = new_part
                        .iter()
                        .map(|&EdgeIdT(e)| alternative_graph_dijkstra.graph().graph.graph.weight()[e as usize])
                        .sum();

                    if part_dist * 20 > base_dist {
                        let _blocked = block_reporting();
                        if part_dist * 20
                            <= alternative_graph_dijkstra
                                .query(Query {
                                    from: part_start,
                                    to: part_end,
                                })
                                .distance()
                                .map(|d| d * 25)
                                .unwrap_or(INFINITY)
                        {
                            feasable = true;
                            alternative_graph_dijkstra.graph_mut().add_edges(new_part);
                            s += 1;
                        }
                    }
                }

                report!("feasable", feasable);

                if path_orig_len > max_orig_dist || penalty_dist > max_penalized_dist || s >= 10 {
                    // dbg!(path_orig_len > max_orig_dist);
                    // dbg!(penalty_dist > max_penalized_dist);
                    // dbg!(s >= 10);
                    break;
                }
            }

            drop(iterations_ctxt);

            report!("num_iterations", i);

            let mut ops = DefaultOps();
            let reversed_graph = ReversedAlternativeGraph {
                graph: &self.reversed,
                contained_edges: &alternative_graph_dijkstra.graph().contained_edges,
                weights: alternative_graph_dijkstra.graph().graph.graph.weight(),
            };
            for _ in DijkstraRun::query(&reversed_graph, &mut self.reverse_dijkstra_data, &mut ops, DijkstraInit::from(core_to)) {}
            let forward_dists = alternative_graph_dijkstra.one_to_all(core_from);
            let mut total_dist = 0.0;
            let mut avg_dist = 0;
            for e in forward_dists.graph().contained_edges.set_bits_iter() {
                let weight = forward_dists.graph().graph.graph.weight()[e];
                let head = forward_dists.graph().graph.graph.head()[e];
                let tail = self.tail[e];
                avg_dist += weight as u64;
                total_dist += weight as f64 / (forward_dists.distance(tail) + weight + self.reverse_dijkstra_data.distances[head as usize]) as f64;
            }
            let avg_dist = avg_dist as f64 / (base_dist as f64 * total_dist);
            let mut decision_edges = 0;
            for &node in &self.nodes_to_reset {
                decision_edges += LinkIterable::<NodeIdT>::link_iter(alternative_graph_dijkstra.graph(), node)
                    .count()
                    .saturating_sub(1);
            }
            report!("total_dist", total_dist);
            report!("avg_dist", avg_dist);
            report!("decision_edges", decision_edges);

            for node in self.nodes_to_reset.drain(..) {
                self.times_penalized[node as usize] = 0;
                for (_, EdgeIdT(edge)) in LinkIterable::<(NodeIdT, EdgeIdT)>::link_iter(&alternative_graph_dijkstra.graph().graph, node) {
                    let e = edge as usize;
                    shortest_path_penalized.set_edge_weight(edge, alternative_graph_dijkstra.graph().graph.graph.weight()[e as usize]);
                }
                for (_, Reversed(EdgeIdT(edge))) in self.reversed.link_iter(node) {
                    let e = edge as usize;
                    shortest_path_penalized.set_edge_weight(edge, alternative_graph_dijkstra.graph().graph.graph.weight()[e as usize]);
                }
            }

            Some(())
        } else {
            None
        }
    }

    pub fn potentials(&self) -> impl Iterator<Item = &PenaltyPot<P>> {
        // std::iter::once(self.shortest_path_penalized.potential())
        // let pots = self.shortest_path_penalized.potential();
        // vec![pots.forward(), pots.backward()].into_iter()
        let pots = self.shortest_path_penalized.potentials();
        vec![pots.0.forward(), pots.0.backward(), pots.1.forward(), pots.1.backward()].into_iter()
    }
}

struct AlternativeGraph<G> {
    graph: G,
    contained_edges: FastClearBitVec,
}

impl<G> AlternativeGraph<G> {
    fn add_edges(&mut self, edges: &[EdgeIdT]) {
        for &EdgeIdT(edge) in edges {
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

impl<G: LinkIterable<L> + EdgeIdGraph, L> LinkIterable<L> for AlternativeGraph<G> {
    type Iter<'a>
    where
        Self: 'a,
    = FilteredLinkIter<'a, <G as LinkIterable<L>>::Iter<'a>>;

    #[inline(always)]
    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        FilteredLinkIter {
            iter: self.graph.link_iter(node),
            edge_ids: self.graph.neighbor_edge_indices_usize(node),
            contained_edges: &self.contained_edges,
        }
    }
}

impl<G: SymmetricDegreeGraph> SymmetricDegreeGraph for AlternativeGraph<G> {
    fn symmetric_degree(&self, node: NodeId) -> SymmetricDeg {
        self.graph.symmetric_degree(node)
    }
}

struct FilteredLinkIter<'a, I> {
    iter: I,
    edge_ids: std::ops::Range<usize>,
    contained_edges: &'a FastClearBitVec,
}

impl<'a, L, I: Iterator<Item = L>> Iterator for FilteredLinkIter<'a, I> {
    type Item = L;

    fn next(&mut self) -> Option<Self::Item> {
        for e in self.edge_ids.by_ref() {
            let next = self.iter.next();
            if self.contained_edges.get(e) {
                return next;
            }
        }
        None
    }
}

struct ReversedAlternativeGraph<'a> {
    graph: &'a ReversedGraphWithEdgeIds,
    contained_edges: &'a FastClearBitVec,
    weights: &'a [Weight],
}

impl Graph for ReversedAlternativeGraph<'_> {
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }
    fn num_arcs(&self) -> usize {
        self.graph.num_arcs()
    }
    fn degree(&self, node: NodeId) -> usize {
        self.graph.degree(node)
    }
}

impl LinkIterable<Link> for ReversedAlternativeGraph<'_> {
    type Iter<'a>
    where
        Self: 'a,
    = impl Iterator<Item = Link> + 'a;

    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        self.graph.link_iter(node).filter_map(move |(NodeIdT(head), Reversed(EdgeIdT(edge_id)))| {
            if self.contained_edges.get(edge_id as usize) {
                Some(Link {
                    node: head,
                    weight: self.weights[edge_id as usize],
                })
            } else {
                None
            }
        })
    }
}

pub struct PenaltyIterative<'a> {
    virtual_topocore: VirtualTopocore,
    shortest_path_penalized: query::MultiThreadedBiDirSkipLowDegServer<BidirPenaltyPot<BorrowedCCHPot<'a>>>,
    alternative_graph_dijkstra: query::SkipLowDegServer<AlternativeGraph<VirtualTopocoreGraph<OwnedGraph>>, DefaultOps, ZeroPotential, true, true>,
    reversed: ReversedGraphWithEdgeIds,
    tail: Vec<NodeId>,
    times_penalized: Vec<u8>,
    nodes_to_reset: Vec<NodeId>,
}

impl<'a> PenaltyIterative<'a> {
    pub fn new<G>(graph: &'a G, cch_pot: &'a CCHPotData) -> Self
    where
        G: LinkIterable<NodeIdT>,
        OwnedGraph: BuildPermutated<G>,
    {
        let (main_graph, _, virtual_topocore) = VirtualTopocoreGraph::new_topo_dijkstra_graphs(graph);
        let n = main_graph.num_nodes();
        let m = main_graph.num_arcs();

        let reversed = ReversedGraphWithEdgeIds::reversed(&main_graph);
        let mut edge_id = 0;
        let mut tail = vec![0; m];
        for node in 0..main_graph.num_nodes() {
            for _ in LinkIterable::<NodeIdT>::link_iter(&main_graph, node as NodeId) {
                tail[edge_id] = node as NodeId;
                edge_id += 1;
            }
        }
        Self {
            shortest_path_penalized: query::MultiThreadedBiDirSkipLowDegServer::new(
                main_graph.clone(),
                SymmetricBiDirPotential::new(
                    RecyclingPotential::new(PotentialForPermutated {
                        order: virtual_topocore.order.clone(),
                        potential: cch_pot.forward_potential(),
                    }),
                    RecyclingPotential::new(PotentialForPermutated {
                        order: virtual_topocore.order.clone(),
                        potential: cch_pot.backward_potential(),
                    }),
                ),
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
            times_penalized: vec![0; n],
            nodes_to_reset: Vec::new(),
            tail,
        }
    }

    pub fn alternatives(&mut self, mut query: Query) -> Option<Vec<NodeId>> {
        query.permutate(&self.virtual_topocore.order);
        let core_from = self.virtual_topocore.bridge_node(query.from()).unwrap_or(query.from);
        let core_to = self.virtual_topocore.bridge_node(query.to()).unwrap_or(query.to);
        let query = Query { from: core_from, to: core_to };
        self.alternative_graph_dijkstra.graph_mut().clear();

        if let Some(mut result) = silent_report_time_with_key("initial_query", || self.shortest_path_penalized.query(query)).found() {
            let base_dist = result.distance();
            report!("base_dist", base_dist);

            let max_orig_dist = (base_dist as f64 * 1.25) as Weight;
            let rejoin_penalty = base_dist / 100;

            let max_penalized_dist = (base_dist as f64 * 1.25 * 1.1) as Weight + 2 * rejoin_penalty;
            let max_num_penalizations = 5;

            let mut path = result.node_path();
            let mut path_edges = result.edge_path();
            let shortest_path_penalized = &mut self.shortest_path_penalized;
            let alternative_graph_dijkstra = &mut self.alternative_graph_dijkstra;

            alternative_graph_dijkstra.graph_mut().add_edges(&path_edges);
            let path_orig_len: Weight = path_edges
                .iter()
                .map(|&EdgeIdT(e)| alternative_graph_dijkstra.graph().graph.graph.weight()[e as usize])
                .sum();
            debug_assert_eq!(base_dist, path_orig_len);

            let mut iterations_ctxt = push_collection_context("iterations".to_string());

            let mut i = 0;
            let mut alternative = loop {
                i += 1;
                let iteration_ctxt = iterations_ctxt.push_collection_item();
                report!("iteration", i);

                for &EdgeIdT(edge) in &path_edges {
                    if self.times_penalized[self.tail[edge as usize] as usize] < max_num_penalizations
                        || self.times_penalized[alternative_graph_dijkstra.graph().graph.graph.head()[edge as usize] as usize] < max_num_penalizations
                    {
                        let weight = shortest_path_penalized.graph().graph.weight()[edge as usize];
                        shortest_path_penalized.set_edge_weight(edge, weight * 11 / 10);
                    }
                }
                let dists: Vec<_> = std::iter::once(0)
                    .chain(path_edges.iter().scan(0, |state, &EdgeIdT(edge)| {
                        *state += shortest_path_penalized.graph().graph.weight()[edge as usize];
                        Some(*state)
                    }))
                    .collect();
                let total_penalized_dist = dists.last().unwrap();
                for (&[tail, head], &[tail_dist, head_dist]) in path.array_windows::<2>().zip(dists.array_windows::<2>()) {
                    if self.times_penalized[head as usize] < max_num_penalizations {
                        for (NodeIdT(rev_head), Reversed(EdgeIdT(edge))) in self.reversed.link_iter(head) {
                            if rev_head != tail {
                                let weight = shortest_path_penalized.graph().graph.weight()[edge as usize];
                                shortest_path_penalized.set_edge_weight(
                                    edge,
                                    weight + (rejoin_penalty as u64 * head_dist as u64 / *total_penalized_dist as u64) as Weight,
                                );
                            }
                        }
                    }

                    if self.times_penalized[tail as usize] < max_num_penalizations {
                        for (NodeIdT(edge_head), EdgeIdT(edge)) in
                            LinkIterable::<(NodeIdT, EdgeIdT)>::link_iter(&alternative_graph_dijkstra.graph().graph, tail)
                        {
                            if edge_head != head {
                                let weight = shortest_path_penalized.graph().graph.weight()[edge as usize];
                                shortest_path_penalized.set_edge_weight(
                                    edge,
                                    weight + (rejoin_penalty as u64 * tail_dist as u64 / *total_penalized_dist as u64) as Weight,
                                );
                            }
                        }
                    }
                }

                let mut any_penalized = false;
                for &node in &path {
                    if self.times_penalized[node as usize] == 0 {
                        self.nodes_to_reset.push(node);
                    }
                    if self.times_penalized[node as usize] < max_num_penalizations {
                        any_penalized = true;
                        self.times_penalized[node as usize] += 1;
                    }
                }

                if !any_penalized {
                    // dbg!("saturated");
                    drop(iteration_ctxt);
                    drop(iterations_ctxt);
                    break None;
                }

                let (result, time) = measure(|| shortest_path_penalized.distance_with_cap(query, max_penalized_dist, Some(max_orig_dist)));
                report!("running_time_ms", time.as_secs_f64() * 1000.0);
                let mut result = if let Some(result) = result.found() {
                    result
                } else {
                    // dbg!("search pruned to death");
                    drop(iteration_ctxt);
                    drop(iterations_ctxt);
                    break None;
                };
                let penalty_dist = result.distance();
                report!("penalty_dist", penalty_dist);

                path = result.node_path();
                path_edges = result.edge_path();
                let path_orig_len: Weight = path_edges
                    .iter()
                    .map(|&EdgeIdT(e)| alternative_graph_dijkstra.graph().graph.graph.weight()[e as usize])
                    .sum();
                report!("orig_dist", path_orig_len);
                debug_assert!(path_orig_len >= base_dist);

                if path_orig_len > max_orig_dist || penalty_dist > max_penalized_dist {
                    // dbg!(path_orig_len > max_orig_dist);
                    // dbg!(penalty_dist > max_penalized_dist);
                    drop(iteration_ctxt);
                    drop(iterations_ctxt);
                    break None;
                }

                let nonshared_length: Weight = path_edges
                    .iter()
                    .filter(|&&EdgeIdT(edge)| !alternative_graph_dijkstra.graph().contained_edges.get(edge as usize))
                    .map(|&EdgeIdT(e)| alternative_graph_dijkstra.graph().graph.graph.weight()[e as usize])
                    .sum();

                if nonshared_length > path_orig_len / 5 {
                    drop(iteration_ctxt);
                    drop(iterations_ctxt);
                    let num_differing_segments = path_edges
                        .split(|&EdgeIdT(edge)| alternative_graph_dijkstra.graph().contained_edges.get(edge as usize))
                        .filter(|part| !part.is_empty())
                        .count();
                    report!("sharing_percent", (path_orig_len - nonshared_length) * 100 / base_dist);
                    report!("num_differing_segments", num_differing_segments);
                    break Some(path);
                }
            };

            report!("num_iterations", i);
            report!("success", alternative.is_some());

            if let Some(alternative) = &mut alternative {
                for node in alternative.iter_mut() {
                    *node = self.virtual_topocore.order.node(*node);
                }
            }

            for node in self.nodes_to_reset.drain(..) {
                self.times_penalized[node as usize] = 0;
                for (_, EdgeIdT(edge)) in LinkIterable::<(NodeIdT, EdgeIdT)>::link_iter(&alternative_graph_dijkstra.graph().graph, node) {
                    let e = edge as usize;
                    shortest_path_penalized.set_edge_weight(edge, alternative_graph_dijkstra.graph().graph.graph.weight()[e as usize]);
                }
                for (_, Reversed(EdgeIdT(edge))) in self.reversed.link_iter(node) {
                    let e = edge as usize;
                    shortest_path_penalized.set_edge_weight(edge, alternative_graph_dijkstra.graph().graph.graph.weight()[e as usize]);
                }
            }

            alternative
        } else {
            None
        }
    }

    pub fn potentials(&self) -> impl Iterator<Item = &PenaltyPot<BorrowedCCHPot>> {
        let pots = self.shortest_path_penalized.potentials();
        vec![pots.0.forward(), pots.0.backward(), pots.1.forward(), pots.1.backward()].into_iter()
    }
}
