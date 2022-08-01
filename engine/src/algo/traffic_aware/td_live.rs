use super::*;
use crate::{
    algo::{ch_potentials::td_query::Server as TDTopoDijkServer, dijkstra::query::td_dijkstra::*, td_astar::*},
    datastr::graph::time_dependent::*,
};

pub struct TDTrafficAwareServer<'a, P> {
    ubs_checker: MinimalNonShortestSubPaths<'a>,
    dijkstra_data: DijkstraData<ForbiddenPathLabel, (), MultiCritNodeData<ForbiddenPathLabel>>,
    live_pot: P,
    live_graph: &'a PessimisticLiveTDGraph,
    dijkstra_ops: BlockedPathsDijkstra<PessimisticLiveTDDijkstraOps>,
}

impl<'a, P: TDPotential> TDTrafficAwareServer<'a, P> {
    pub fn new(smooth_graph: BorrowedGraph<'a>, live_graph: &'a PessimisticLiveTDGraph, smooth_cch_pot: &'a CCHPotData, live_pot: P) -> Self {
        let n = live_graph.num_nodes();
        let m = live_graph.num_arcs();
        Self {
            ubs_checker: MinimalNonShortestSubPaths::new(smooth_cch_pot, smooth_graph),
            dijkstra_data: DijkstraData::new(n),
            live_pot,
            live_graph,
            dijkstra_ops: BlockedPathsDijkstra::new(n, m, PessimisticLiveTDDijkstraOps()),
        }
    }

    pub fn query(&mut self, query: TDQuery<Timestamp>, epsilon: f64) -> Option<()> {
        let timer = Timer::new();
        report!("algo", "iterative_path_blocking");
        self.dijkstra_ops.reset(self.live_graph);

        self.live_pot.init(query.from, query.to, query.departure);
        let mut base_live_dist = None;
        let mut final_live_dist = INFINITY;

        let mut explore_time = std::time::Duration::ZERO;
        let mut ubs_time = std::time::Duration::ZERO;

        let mut i: usize = 0;
        let mut total_queue_pops = 0usize;
        let mut iterations_ctxt = push_collection_context("iterations");
        let result = loop {
            if TRAFFIC_MAX_QUERY_TIME.map(|m| timer.get_passed_ms() > m).unwrap_or(false) {
                break None;
            }

            let _it_ctxt = iterations_ctxt.push_collection_item();
            i += 1;
            report!("iteration", i);
            let live_pot = &mut self.live_pot;
            let mut dijk_run = MultiCritDijkstraRun::query(
                self.live_graph,
                &mut self.dijkstra_data,
                &mut self.dijkstra_ops,
                DijkstraInit {
                    source: NodeIdT(query.from),
                    initial_state: (
                        query.departure,
                        ActiveForbittenPaths::new(0),
                        (NodeIdT(query.from), ActiveForbittenPaths::new(0)),
                    ),
                },
                |node| live_pot.potential(node, None),
            );

            let mut num_queue_pops: usize = 0;
            let (_, time) = measure(|| {
                while let Some(node) = dijk_run.next_step_with_potential(|node| live_pot.potential(node, None)) {
                    num_queue_pops += 1;
                    total_queue_pops += 1;
                    if node == query.to {
                        break;
                    }
                }
            });

            report!("num_queue_pops", num_queue_pops);
            report!("exploration_time_ms", time.as_secs_f64() * 1000.0);
            explore_time += time;

            if self.dijkstra_data.distances[query.to as usize].popped().is_empty() {
                break None;
            }

            debug_assert_eq!(self.dijkstra_data.distances[query.to as usize].popped().len(), 1);
            final_live_dist = self.dijkstra_data.distances[query.to as usize].popped()[0].0 .0;
            base_live_dist.get_or_insert(final_live_dist);

            let mut path = vec![query.to];

            let mut label_idx = 0;
            while *path.last().unwrap() != query.from {
                let (dist, _, (NodeIdT(parent), parent_active_forb_paths)) =
                    &self.dijkstra_data.distances[*path.last().unwrap() as usize].popped()[label_idx].0;

                label_idx = self.dijkstra_data.distances[*parent as usize]
                    .popped()
                    .iter()
                    // why is_subset instead of ==
                    // because at node u, a better label (without some forbidden path [..., u, w])
                    // with same dist might get merged but when linking this new label along uv this will not cause a merge at v
                    // because its not on that forbidden path and thus both labels have the same bitsets.
                    .position(|NodeQueueLabelOrder(l)| {
                        let min_weight_edge = self
                            .live_graph
                            .edge_indices(*parent, *path.last().unwrap())
                            .min_by_key(|&EdgeIdT(edge_id)| self.live_graph.eval(edge_id, l.0))
                            .unwrap();

                        l.0 == dist - self.live_graph.eval(min_weight_edge.0, l.0) && l.1.is_subset(parent_active_forb_paths)
                    })
                    .unwrap();

                path.push(*parent);
            }

            path.reverse();
            report!("num_nodes_on_path", path.len());

            let (violating, time) = measure(|| self.ubs_checker.find_ubs_violating_subpaths(&path, epsilon));
            report!("ubs_time_ms", time.as_secs_f64() * 1000.0);
            ubs_time += time;

            if violating.is_empty() {
                break Some(());
            }

            for violating_range in violating {
                self.dijkstra_ops
                    .add_forbidden_path(&path[violating_range.start..=violating_range.end], self.live_graph);
            }
        };
        drop(iterations_ctxt);
        report!("failed", result.is_none());
        report!("num_iterations", i);
        report!("num_labels_pushed", self.dijkstra_ops.num_labels_pushed());
        report!("total_queue_pops", total_queue_pops);
        report!("num_forbidden_paths", self.dijkstra_ops.num_forbidden_paths());
        // report!(
        //     "num_forbidden_paths_with_shared_terminal_edges",
        //     self.dijkstra_ops.num_forbidden_paths_with_shared_terminal_edges
        // );
        report!(
            "length_increase_percent",
            (final_live_dist - base_live_dist.unwrap_or(INFINITY)) as f64 / base_live_dist.unwrap_or(INFINITY) as f64 * 100.0
        );
        report!("total_exploration_time_ms", explore_time.as_secs_f64() * 1000.0);
        report!("total_ubs_time_ms", ubs_time.as_secs_f64() * 1000.0);
        result
    }
}

pub struct HeuristicTDTrafficAwareServer<'a, P> {
    ubs_checker: MinimalNonShortestSubPaths<'a>,
    shortest_path:
        TDTopoDijkServer<PessimisticLiveTDGraph, BlockedDetoursDijkstra<PessimisticLiveTDDijkstraOps>, td_astar::TDRecyclingPotential<P>, true, true, true>,
}

impl<'a, P: td_astar::TDPotential> HeuristicTDTrafficAwareServer<'a, P> {
    pub fn new(smooth_graph: BorrowedGraph<'a>, live_graph: &'a PessimisticLiveTDGraph, smooth_cch_pot: &'a CCHPotData, live_cch_pot: P) -> Self {
        let n = live_graph.num_nodes();
        let _blocked = block_reporting();
        Self {
            ubs_checker: MinimalNonShortestSubPaths::new(smooth_cch_pot, smooth_graph),
            shortest_path: TDTopoDijkServer::new(
                live_graph,
                td_astar::TDRecyclingPotential::new(live_cch_pot),
                BlockedDetoursDijkstra::new(n, PessimisticLiveTDDijkstraOps()),
            ),
        }
    }

    pub fn query(&mut self, query: TDQuery<Timestamp>, epsilon: f64, mut path_cb: impl FnMut(&[NodeId])) -> Option<()> {
        let timer = Timer::new();
        report!("algo", "iterative_detour_blocking");
        self.shortest_path.ops().reset();

        let mut base_live_dist = None;
        let mut final_live_dist = INFINITY;

        let mut explore_time = std::time::Duration::ZERO;
        let mut ubs_time = std::time::Duration::ZERO;

        let mut i: usize = 0;
        let mut iterations_ctxt = push_collection_context("iterations");
        let result = loop {
            if TRAFFIC_MAX_QUERY_TIME.map(|m| timer.get_passed_ms() > m).unwrap_or(false) {
                break None;
            }

            let _it_ctxt = iterations_ctxt.push_collection_item();
            i += 1;
            report!("iteration", i);

            let (res, time) = measure(|| self.shortest_path.td_query(query));

            report!("exploration_time_ms", time.as_secs_f64() * 1000.0);
            explore_time += time;

            let mut res = if let Some(res) = res.found() {
                res
            } else {
                break None;
            };

            final_live_dist = res.distance();
            if base_live_dist.is_none() {
                base_live_dist = Some(res.distance());
            }
            let mut path = res.node_path();
            report!("num_nodes_on_path", path.len());

            let (violating, time) = measure(|| self.ubs_checker.find_ubs_violating_subpaths(&path, epsilon));
            report!("ubs_time_ms", time.as_secs_f64() * 1000.0);
            ubs_time += time;

            path_cb(&path);

            if violating.is_empty() {
                break Some(());
            }

            for node in &mut path {
                *node = self.shortest_path.order().rank(*node);
            }
            for violating_range in violating {
                self.shortest_path.ops().add_forbidden_path(&path[violating_range.start..=violating_range.end]);
            }
        };
        drop(iterations_ctxt);
        report!("failed", result.is_none());
        report!("num_iterations", i);
        report!("num_forbidden_paths", self.shortest_path.ops().num_forbidden_paths());
        if let Some(base_live_dist) = base_live_dist {
            report!(
                "length_increase_percent",
                (final_live_dist - base_live_dist) as f64 / base_live_dist as f64 * 100.0
            );
        }
        report!("total_exploration_time_ms", explore_time.as_secs_f64() * 1000.0);
        report!("total_ubs_time_ms", ubs_time.as_secs_f64() * 1000.0);
        result
    }
}

pub struct IterativePathFixing<'a, P> {
    ubs_checker: MinimalNonShortestSubPaths<'a>,
    shortest_path: TDTopoDijkServer<PessimisticLiveTDGraph, PessimisticLiveTDDijkstraOps, P, true, true, true>,
    live_graph: &'a PessimisticLiveTDGraph,
}

impl<'a, P: TDPotential> IterativePathFixing<'a, P> {
    pub fn new(smooth_graph: BorrowedGraph<'a>, live_graph: &'a PessimisticLiveTDGraph, smooth_cch_pot: &'a CCHPotData, live_cch_pot: P) -> Self {
        let _blocked = block_reporting();
        Self {
            ubs_checker: MinimalNonShortestSubPaths::new(smooth_cch_pot, smooth_graph),
            shortest_path: TDTopoDijkServer::new(live_graph, live_cch_pot, PessimisticLiveTDDijkstraOps()),
            live_graph,
        }
    }

    pub fn query(&mut self, query: TDQuery<Timestamp>, epsilon: f64) -> Option<()> {
        report!("algo", "iterative_path_fixing");

        let (res, time) = measure(|| {
            let _expl_ctxt = push_context("exploration");
            self.shortest_path.td_query(query)
        });
        report!("exploration_time_ms", time.as_secs_f64() * 1000.0);

        let mut res = res.found()?;

        let base_live_dist = Some(res.distance());
        let path = res.node_path();
        report!("num_nodes_on_path", path.len());

        let (fixed, time) = measure(|| self.ubs_checker.fix_violating_subpaths(&path, epsilon));
        report!("total_ubs_time_ms", time.as_secs_f64() * 1000.0);

        let final_path = match &fixed {
            Ok(None) => &path,
            Ok(Some(fixed)) => fixed,
            Err(fixed) => fixed,
        };

        let final_live_dist = path_dist_iter(final_path, query.departure, self.live_graph).last().unwrap();

        report!("failed", fixed.is_err());
        if let Some(base_live_dist) = base_live_dist {
            report!(
                "length_increase_percent",
                (final_live_dist - base_live_dist) as f64 / base_live_dist as f64 * 100.0
            );
        }
        Some(())
    }
}

pub struct SmoothPathBaseline<'a, P> {
    shortest_live_path: TDTopoDijkServer<PessimisticLiveTDGraph, PessimisticLiveTDDijkstraOps, P, true, true, true>,
    shortest_path_by_smooth: TopoDijkServer<OwnedGraph, DefaultOps, BorrowedCCHPot<'a>, true, true, true>,
    live_graph: &'a PessimisticLiveTDGraph,
}

impl<'a, P: TDPotential> SmoothPathBaseline<'a, P> {
    pub fn new(smooth_graph: BorrowedGraph<'a>, live_graph: &'a PessimisticLiveTDGraph, smooth_cch_pot: &'a CCHPotData, live_cch_pot: P) -> Self {
        let _blocked = block_reporting();
        Self {
            shortest_live_path: TDTopoDijkServer::new(live_graph, live_cch_pot, PessimisticLiveTDDijkstraOps()),
            shortest_path_by_smooth: TopoDijkServer::new(&smooth_graph, smooth_cch_pot.forward_potential(), DefaultOps()),
            live_graph,
        }
    }

    pub fn query(&mut self, query: TDQuery<Timestamp>) -> Option<()> {
        report!("algo", "smooth_path_baseline");

        let (res, time) = measure(|| {
            let _expl_ctxt = push_context("exploration");
            self.shortest_path_by_smooth.query(Query {
                from: query.from,
                to: query.to,
            })
        });
        report!("exploration_time_ms", time.as_secs_f64() * 1000.0);

        let mut res = res.found()?;

        let base_live_dist = without_reporting(|| self.shortest_live_path.td_query(query).distance());
        let path = res.node_path();
        report!("num_nodes_on_path", path.len());

        let final_live_dist = path_dist_iter(&path, query.departure, self.live_graph).last().unwrap();

        report!("failed", false);
        if let Some(base_live_dist) = base_live_dist {
            report!(
                "length_increase_percent",
                (final_live_dist - base_live_dist) as f64 / base_live_dist as f64 * 100.0
            );
        }
        Some(())
    }
}

pub fn path_dist_iter<'a>(path: &'a [NodeId], departure: Timestamp, graph: &'a PessimisticLiveTDGraph) -> impl Iterator<Item = Weight> + 'a {
    std::iter::once(departure).chain(path.windows(2).scan(0, move |state, node_pair| {
        let mut min_edge_weight = INFINITY;
        for EdgeIdT(edge_id) in graph.edge_indices(node_pair[0], node_pair[1]) {
            min_edge_weight = std::cmp::min(min_edge_weight, graph.eval(edge_id, *state));
        }
        debug_assert_ne!(min_edge_weight, INFINITY);

        *state += min_edge_weight;
        Some(*state)
    }))
}
