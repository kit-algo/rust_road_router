use super::*;

use crate::{
    algo::{
        a_star::*,
        ch_potentials::*,
        dijkstra::{generic_dijkstra::*, *},
        minimal_nonshortest_subpaths::*,
    },
    datastr::{graph::first_out_graph::BorrowedGraph, timestamped_vector::*},
    report::*,
    util::with_index,
};

#[derive(Clone, Debug, PartialEq)]
pub enum ActiveForbittenPaths {
    One(u128),
    More(Box<[u128]>),
}

impl ActiveForbittenPaths {
    fn new(size: usize) -> Self {
        if size <= 128 {
            Self::One(0)
        } else {
            Self::More(std::iter::repeat(0).take((size + 127) / 128).collect())
        }
    }

    fn get(&self, idx: usize) -> bool {
        match self {
            Self::One(active) => active & (1 << idx) != 0,
            Self::More(active) => (active[idx / 128] & (1 << (idx % 128))) != 0,
        }
    }

    fn set(&mut self, idx: usize) {
        match self {
            Self::One(active) => *active |= 1 << idx,
            Self::More(active) => active[idx / 128] |= 1 << (idx % 128),
        }
    }

    fn is_subset(&self, rhs: &Self) -> bool {
        match (self, rhs) {
            (&Self::One(self_active), &Self::One(rhs_active)) => !(!self_active | rhs_active) == 0,
            (&Self::One(self_active), Self::More(rhs_active)) => !(!self_active | rhs_active[0]) == 0,
            (Self::More(self_active), &Self::One(rhs_active)) => !(!self_active[0] | rhs_active) == 0 && self_active[1..].iter().all(|&a| a == 0),
            (Self::More(self_active), Self::More(rhs_active)) => self_active
                .iter()
                .zip(rhs_active.iter().chain(std::iter::repeat(&0)))
                .all(|(&s_a, &rhs_a)| !(!s_a | rhs_a) == 0),
        }
    }
}

impl Label for Vec<(Weight, ActiveForbittenPaths, (NodeIdT, ActiveForbittenPaths))> {
    type Key = Weight;
    fn neutral() -> Self {
        Vec::new()
    }
    fn key(&self) -> Self::Key {
        self.iter().map(|&(w, ..)| w).min().unwrap_or(INFINITY)
    }
}

pub struct BlockedPathsDijkstra {
    // Next Node, Next Node Local Offset
    forbidden_paths: Vec<Vec<(NodeIdT, u32)>>,
    // Global forbidden path id, offset on path, Tail node Local offset
    edge_forbidden_paths: Vec<Vec<(usize, usize, u32)>>,
    node_forbidden_path_counter: Vec<usize>,
    cycles: Vec<Vec<NodeId>>,
}

impl BlockedPathsDijkstra {
    pub fn new(n: usize, m: usize) -> Self {
        Self {
            forbidden_paths: Vec::new(),
            edge_forbidden_paths: vec![Vec::new(); m],
            node_forbidden_path_counter: vec![0; n],
            cycles: Vec::new(),
        }
    }

    pub fn add_forbidden_path(&mut self, path: &[NodeId], graph: &impl EdgeIdGraph) {
        if cfg!(debug_assertions) {
            let mut active_forbidden_paths = ActiveForbittenPaths::new(self.node_forbidden_path_counter[path[0] as usize]);

            for &[tail, head] in path.array_windows::<2>() {
                let mut head_active_forbidden_paths = ActiveForbittenPaths::new(self.node_forbidden_path_counter[head as usize]);
                if let Some(EdgeIdT(link_id)) = graph.edge_indices(tail, head).next() {
                    for &(global_id, current_edge_path_offset, tail_local_idx) in &self.edge_forbidden_paths[link_id as usize] {
                        if current_edge_path_offset == 1 || active_forbidden_paths.get(tail_local_idx as usize) {
                            if self.forbidden_paths[global_id].len() == current_edge_path_offset + 1 {
                                if head == *path.last().unwrap() {
                                    panic!("path already forbidden: {:#?}", path);
                                } else {
                                    dbg!("subpath already forbidden");
                                    return;
                                }
                            } else {
                                head_active_forbidden_paths.set(self.forbidden_paths[global_id][current_edge_path_offset].1 as usize);
                            }
                        }
                    }
                }
                active_forbidden_paths = head_active_forbidden_paths;
            }
        }

        let global_id = self.forbidden_paths.len();
        let mut forbidden_path = Vec::with_capacity(path.len());
        forbidden_path.push((NodeIdT(path[0]), 0));

        for (offset, &[tail, head]) in path.array_windows::<2>().enumerate() {
            for EdgeIdT(link_id) in graph.edge_indices(tail, head) {
                self.edge_forbidden_paths[link_id as usize].push((global_id, offset + 1, self.node_forbidden_path_counter[tail as usize] as u32));
            }
            forbidden_path.push((NodeIdT(head), self.node_forbidden_path_counter[head as usize] as u32));
        }
        for &node in &path[1..path.len() - 1] {
            self.node_forbidden_path_counter[node as usize] += 1;
        }

        self.forbidden_paths.push(forbidden_path);
    }

    pub fn reset(&mut self, graph: &impl EdgeIdGraph) {
        for path in self.forbidden_paths.drain(..) {
            for &[(NodeIdT(tail), _), (NodeIdT(head), _)] in path.array_windows::<2>() {
                for EdgeIdT(link_id) in graph.edge_indices(tail, head) {
                    self.edge_forbidden_paths[link_id as usize].clear();
                }
            }
            for (NodeIdT(node), _) in path {
                self.node_forbidden_path_counter[node as usize] = 0;
            }
        }
    }

    pub fn num_forbidden_paths(&self) -> usize {
        self.forbidden_paths.len()
    }

    pub fn block_cycles(&mut self, graph: &impl EdgeIdGraph) {
        let mut tmp = Vec::new();
        std::mem::swap(&mut tmp, &mut self.cycles);

        tmp.sort();
        tmp.dedup();

        for cycle in tmp.drain(..) {
            debug_assert_eq!(cycle.first(), cycle.last());
            debug_assert!(cycle.len() > 2);
            self.add_forbidden_path(&cycle, graph);
        }
        std::mem::swap(&mut tmp, &mut self.cycles);
    }
}

impl<G: EdgeIdGraph + EdgeRandomAccessGraph<Link>> ComplexDijkstraOps<G> for BlockedPathsDijkstra {
    type Label = Vec<ForbiddenPathLabel>;
    type Arc = (NodeIdT, EdgeIdT);
    type LinkResult = Vec<ForbiddenPathLabel>;
    type PredecessorLink = ();

    fn link(
        &mut self,
        #[allow(unused)] graph: &G,
        #[allow(unused)] labels: &TimestampedVector<Self::Label>,
        _parents: &[(NodeId, Self::PredecessorLink)],
        NodeIdT(tail): NodeIdT,
        key: Weight,
        label: &Self::Label,
        &(NodeIdT(head), EdgeIdT(link_id)): &Self::Arc,
    ) -> Self::LinkResult {
        let weight = graph.link(link_id).weight;
        let mut linked = Vec::new();

        // let fastest_existing_label = labels[head as usize].first();

        #[allow(unused)]
        for (i, l) in label.iter().enumerate() {
            if l.0 < key {
                continue;
            }
            let mut illegal = false;
            let mut head_active_forbidden_paths = ActiveForbittenPaths::new(self.node_forbidden_path_counter[head as usize]);
            let active_forbidden_paths = &l.1;

            for &(global_id, current_edge_path_offset, tail_local_idx) in &self.edge_forbidden_paths[link_id as usize] {
                if current_edge_path_offset == 1 || active_forbidden_paths.get(tail_local_idx as usize) {
                    if self.forbidden_paths[global_id].len() == current_edge_path_offset + 1 {
                        illegal = true;
                        break;
                    } else {
                        head_active_forbidden_paths.set(self.forbidden_paths[global_id][current_edge_path_offset].1 as usize);
                    }
                }
            }
            // if !illegal {
            //     if let Some(fastest_existing_label) = fastest_existing_label {
            //         if fastest_existing_label.0 < l.0 + weight && !fastest_existing_label.1.is_subset(&head_active_forbidden_paths) {
            //             let mut label_idx = i;
            //             let mut cur = tail;
            //             while labels[cur as usize][label_idx].0 > fastest_existing_label.0 {
            //                 let (dist, _, (NodeIdT(parent), parent_active_forb_paths)) = &labels[cur as usize][label_idx];

            //                 let min_weight_edge = graph
            //                     .edge_indices(*parent, cur)
            //                     .min_by_key(|&EdgeIdT(edge_id)| graph.link(edge_id).weight)
            //                     .unwrap();

            //                 if let Some(par_label_idx) = labels[*parent as usize]
            //                     .iter()
            //                     .position(|l| l.0 == dist - graph.link(min_weight_edge.0).weight && l.1.is_subset(&parent_active_forb_paths))
            //                 {
            //                     label_idx = par_label_idx;
            //                 } else {
            //                     illegal = true;
            //                     break;
            //                 }

            //                 cur = *parent;

            //                 if cur == head {
            //                     let mut cycle = Vec::new();
            //                     cycle.push(cur);
            //                     cycle.push(tail);

            //                     label_idx = i;
            //                     cur = tail;

            //                     while cur != head {
            //                         let (dist, _, (NodeIdT(parent), parent_active_forb_paths)) = &labels[cur as usize][label_idx];
            //                         cycle.push(*parent);

            //                         let min_weight_edge = graph
            //                             .edge_indices(*parent, cur)
            //                             .min_by_key(|&EdgeIdT(edge_id)| graph.link(edge_id).weight)
            //                             .unwrap();

            //                         label_idx = labels[*parent as usize]
            //                             .iter()
            //                             .position(|l| l.0 == dist - graph.link(min_weight_edge.0).weight && l.1.is_subset(&parent_active_forb_paths))
            //                             .unwrap();

            //                         cur = *parent;
            //                     }

            //                     cycle.reverse();

            //                     for &[tail, head] in cycle.array_windows::<2>() {
            //                         debug_assert!(graph.edge_indices(tail, head).next().is_some(), "{:#?}", (cycle, tail, head));
            //                     }

            //                     illegal = true;
            //                     self.cycles.push(cycle);
            //                     break;
            //                 }
            //             }
            //         }
            //     }
            // }
            if !illegal {
                linked.push((l.0 + weight, head_active_forbidden_paths, (NodeIdT(tail), l.1.clone())));
            }
        }

        linked
    }
    fn merge(&mut self, label: &mut Self::Label, mut linked: Self::LinkResult) -> Option<Weight> {
        linked.retain(|new_label| {
            let mut needed = true;

            for cur_label in label.iter() {
                if cur_label.0 > new_label.0 {
                    break;
                }

                if cur_label.1.is_subset(&new_label.1) {
                    needed = false;
                    break;
                }
            }

            needed
        });

        let updated = linked.first().map(|l| l.0);

        label.extend_from_slice(&linked);
        label.sort_unstable_by_key(|l| l.0);

        let mut cur_dist_start_idx = 0;
        let mut cur_dist = 0;
        let mut cur_idx = 0;
        while cur_idx < label.len() {
            if label[cur_idx].0 > cur_dist {
                cur_dist = label[cur_idx].0;
                cur_dist_start_idx = cur_idx;
            }
            let active_paths = label[cur_idx].1.clone(); // TODO optimize

            label.retain(with_index(|idx, l: &(Weight, ActiveForbittenPaths, (NodeIdT, ActiveForbittenPaths))| {
                idx < cur_dist_start_idx || idx == cur_idx || !active_paths.is_subset(&l.1)
            }));
            cur_idx += 1;
        }

        updated
    }
    fn predecessor_link(&self, _link: &Self::Arc) -> Self::PredecessorLink {}
}

pub struct BlockedDetoursDijkstra {
    node_forbidden_paths: Vec<Vec<Box<[NodeIdT]>>>,
    forbidden_paths_end_nodes: Vec<NodeIdT>,
}

impl BlockedDetoursDijkstra {
    pub fn new(n: usize) -> Self {
        Self {
            node_forbidden_paths: vec![Vec::new(); n],
            forbidden_paths_end_nodes: Vec::new(),
        }
    }

    pub fn add_forbidden_path(&mut self, path: &[NodeId]) {
        let (&last, rest) = path.split_last().unwrap();
        self.forbidden_paths_end_nodes.push(NodeIdT(last));
        self.node_forbidden_paths[last as usize].push(rest.iter().copied().map(NodeIdT).collect());
    }

    pub fn reset(&mut self) {
        for NodeIdT(node) in self.forbidden_paths_end_nodes.drain(..) {
            self.node_forbidden_paths[node as usize].clear();
        }
    }

    pub fn num_forbidden_paths(&self) -> usize {
        self.forbidden_paths_end_nodes.len()
    }
}

impl<G> ComplexDijkstraOps<G> for BlockedDetoursDijkstra {
    type Label = Weight;
    type Arc = Link;
    type LinkResult = Weight;
    type PredecessorLink = ();

    fn link(
        &mut self,
        _graph: &G,
        _labels: &TimestampedVector<Self::Label>,
        parents: &[(NodeId, Self::PredecessorLink)],
        NodeIdT(tail): NodeIdT,
        _key: Weight,
        label: &Self::Label,
        link: &Self::Arc,
    ) -> Self::LinkResult {
        let illegal = self.node_forbidden_paths[link.node as usize].iter().any(|forbidden_path| {
            let mut deviated = false;
            let mut cur = tail;
            for &NodeIdT(forbidden_parent) in forbidden_path.iter().rev() {
                if forbidden_parent == cur {
                    cur = parents[cur as usize].0;
                } else {
                    deviated = true;
                    break;
                }
            }
            !deviated
        });

        if illegal {
            INFINITY
        } else {
            label + link.weight
        }
    }
    fn merge(&mut self, label: &mut Self::Label, linked: Self::LinkResult) -> Option<Weight> {
        if linked < *label {
            *label = linked;
            return Some(*label);
        }
        None
    }
    fn predecessor_link(&self, _link: &Self::Arc) -> Self::PredecessorLink {}
}

pub struct TrafficAwareQuery(Query);

impl GenQuery<Vec<ForbiddenPathLabel>> for TrafficAwareQuery {
    fn new(from: NodeId, to: NodeId, _initial_state: Vec<ForbiddenPathLabel>) -> Self {
        TrafficAwareQuery(Query { from, to })
    }

    fn from(&self) -> NodeId {
        self.0.from
    }
    fn to(&self) -> NodeId {
        self.0.to
    }
    fn initial_state(&self) -> Vec<ForbiddenPathLabel> {
        vec![(0, ActiveForbittenPaths::new(0), (NodeIdT(self.0.from), ActiveForbittenPaths::new(0)))]
    }
    fn permutate(&mut self, order: &NodeOrder) {
        self.0.from = order.rank(self.0.from);
        self.0.to = order.rank(self.0.to);
    }
}

impl GenQuery<Weight> for TrafficAwareQuery {
    fn new(from: NodeId, to: NodeId, _initial_state: Weight) -> Self {
        TrafficAwareQuery(Query { from, to })
    }

    fn from(&self) -> NodeId {
        self.0.from
    }
    fn to(&self) -> NodeId {
        self.0.to
    }
    fn initial_state(&self) -> Weight {
        0
    }
    fn permutate(&mut self, order: &NodeOrder) {
        self.0.from = order.rank(self.0.from);
        self.0.to = order.rank(self.0.to);
    }
}

type ForbiddenPathLabel = (Weight, ActiveForbittenPaths, (NodeIdT, ActiveForbittenPaths));

pub struct TrafficAwareServer<'a> {
    ubs_checker: MinimalNonShortestSubPaths<'a>,
    dijkstra_data: DijkstraData<Vec<ForbiddenPathLabel>>,
    live_pot: BorrowedCCHPot<'a>,
    live_graph: BorrowedGraph<'a>,
    smooth_graph: BorrowedGraph<'a>,
    dijkstra_ops: BlockedPathsDijkstra,
}

impl<'a> TrafficAwareServer<'a> {
    pub fn new(smooth_graph: BorrowedGraph<'a>, live_graph: BorrowedGraph<'a>, smooth_cch_pot: &'a CCHPotData, live_cch_pot: &'a CCHPotData) -> Self {
        let n = smooth_graph.num_nodes();
        let m = smooth_graph.num_arcs();
        Self {
            ubs_checker: MinimalNonShortestSubPaths::new(smooth_cch_pot),
            dijkstra_data: DijkstraData::new(n),
            live_pot: live_cch_pot.forward_potential(),
            live_graph,
            smooth_graph,
            dijkstra_ops: BlockedPathsDijkstra::new(n, m),
        }
    }

    pub fn query(&mut self, query: Query, epsilon: f64) -> Option<()> {
        self.dijkstra_ops.reset(&self.smooth_graph);
        // self.dijkstra_ops.reset();

        self.live_pot.init(query.to);
        let base_live_dist = self.live_pot.potential(query.from)?;
        let mut final_live_dist = base_live_dist;

        let mut explore_time = time::Duration::zero();
        let mut ubs_time = time::Duration::zero();

        let mut i = 0;
        let mut iterations_ctxt = push_collection_context("iterations".to_string());
        let result = loop {
            if i > 200 {
                break None;
            }

            let _it_ctxt = iterations_ctxt.push_collection_item();
            i += 1;
            report!("iteration", i);
            let live_pot = &mut self.live_pot;
            let mut dijk_run = ComplexDijkstraRun::query(
                &self.live_graph,
                &mut self.dijkstra_data,
                &mut self.dijkstra_ops,
                TrafficAwareQuery(query),
                |node| live_pot.potential(node),
            );

            let mut num_queue_pops = 0;
            let mut num_labels_in_search_space = 0;
            let (_, time) = measure(|| {
                while let Some(node) = dijk_run.next_step_with_potential(|node| live_pot.potential(node)) {
                    num_queue_pops += 1;
                    num_labels_in_search_space += dijk_run.tentative_distance(node).len();
                    if node == query.to {
                        break;
                    }
                }
            });

            report!("num_pruned_cycles", self.dijkstra_ops.cycles.len());
            report!("num_queue_pops", num_queue_pops);
            report!("num_labels_in_search_space", num_labels_in_search_space);
            report!("exploration_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            explore_time = explore_time + time;

            if self.dijkstra_data.distances[query.to as usize].is_empty() {
                break None;
            }

            final_live_dist = self.dijkstra_data.distances[query.to as usize][0].0;

            // let mut cycle_pruning_broke_shortest_path = false;

            let mut path = vec![query.to];

            let mut label_idx = 0;
            while *path.last().unwrap() != query.from {
                let (dist, _, (NodeIdT(parent), parent_active_forb_paths)) = &self.dijkstra_data.distances[*path.last().unwrap() as usize][label_idx];

                let min_weight_edge = self
                    .live_graph
                    .edge_indices(*parent, *path.last().unwrap())
                    .min_by_key(|&EdgeIdT(edge_id)| self.live_graph.link(edge_id).weight)
                    .unwrap();

                if let Some(parent_label_idx) = self.dijkstra_data.distances[*parent as usize]
                    .iter()
                    // why is_subset instead of ==
                    // because at node u, a better label (without some forbidden path [..., u, w])
                    // with same dist might get merged but when linking this new label along uv this will not cause a merge at v
                    // because its not on that forbidden path and thus both labels have the same bitsets.
                    .position(|l| l.0 == dist - self.live_graph.link(min_weight_edge.0).weight && l.1.is_subset(parent_active_forb_paths))
                {
                    label_idx = parent_label_idx;
                } else {
                    // how can this legally happen?
                    // only with instant cycle pruning
                    // forbidden path [s,u,t]
                    // cycle to avoid [u,v,u]
                    // relaxing path [s,...,v,u,t] first because A* stuff
                    // then [s,u,v,u] -> if the distance from s to u (and then v) is shorter than the other path
                    // then we will dominate the [s,...,v] label at v and throw it away.
                    // but we will keep the label [s,...,v,u] at u because the cycle pruning removes the other one.
                    // then we will have a broken path
                    // cycle_pruning_broke_shortest_path = true;
                    break;
                }

                path.push(*parent);
            }

            path.reverse();
            report!("num_nodes_on_path", path.len());

            let (violating, time) = measure(|| self.ubs_checker.find_ubs_violating_subpaths(&path, &self.smooth_graph, epsilon));
            report!("ubs_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            ubs_time = ubs_time + time;

            if violating.is_empty() {
                // if violating.is_empty() && self.dijkstra_ops.cycles.is_empty() {
                // if violating.is_empty() && !cycle_pruning_broke_shortest_path {
                break Some(());
            }

            for violating_range in violating {
                self.dijkstra_ops
                    .add_forbidden_path(&path[violating_range.start..=violating_range.end], &self.smooth_graph);
            }
            // if cycle_pruning_broke_shortest_path {
            // self.dijkstra_ops.block_cycles(&self.smooth_graph);
            // } else {
            //     self.dijkstra_ops.cycles.clear();
            // }
        };
        drop(iterations_ctxt);
        report!("failed", result.is_none());
        report!("num_iterations", i);
        report!("num_forbidden_paths", self.dijkstra_ops.num_forbidden_paths());
        report!(
            "length_increase_percent",
            (final_live_dist - base_live_dist) as f64 / base_live_dist as f64 * 100.0
        );
        report!("total_exploration_time_ms", explore_time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        report!("total_ubs_time_ms", ubs_time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        result
    }
}

pub struct HeuristicTrafficAwareServer<'a> {
    ubs_checker: MinimalNonShortestSubPaths<'a>,
    dijkstra_data: DijkstraData<Weight>,
    live_pot: BorrowedCCHPot<'a>,
    live_graph: BorrowedGraph<'a>,
    smooth_graph: BorrowedGraph<'a>,
    dijkstra_ops: BlockedDetoursDijkstra,
}

impl<'a> HeuristicTrafficAwareServer<'a> {
    pub fn new(smooth_graph: BorrowedGraph<'a>, live_graph: BorrowedGraph<'a>, smooth_cch_pot: &'a CCHPotData, live_cch_pot: &'a CCHPotData) -> Self {
        let n = smooth_graph.num_nodes();
        Self {
            ubs_checker: MinimalNonShortestSubPaths::new(smooth_cch_pot),
            dijkstra_data: DijkstraData::new(n),
            live_pot: live_cch_pot.forward_potential(),
            live_graph,
            smooth_graph,
            dijkstra_ops: BlockedDetoursDijkstra::new(n),
        }
    }

    pub fn query(&mut self, query: Query, epsilon: f64) -> Option<()> {
        self.dijkstra_ops.reset();

        self.live_pot.init(query.to);
        let base_live_dist = self.live_pot.potential(query.from)?;
        let mut final_live_dist = base_live_dist;

        let mut explore_time = time::Duration::zero();
        let mut ubs_time = time::Duration::zero();

        let mut i = 0;
        let mut iterations_ctxt = push_collection_context("iterations".to_string());
        let result = loop {
            if i > 200 {
                break None;
            }

            let _it_ctxt = iterations_ctxt.push_collection_item();
            i += 1;
            report!("iteration", i);
            let live_pot = &mut self.live_pot;
            let mut dijk_run = ComplexDijkstraRun::query(
                &self.live_graph,
                &mut self.dijkstra_data,
                &mut self.dijkstra_ops,
                TrafficAwareQuery(query),
                |node| live_pot.potential(node),
            );

            let mut num_queue_pops = 0;
            let (_, time) = measure(|| {
                while let Some(node) = dijk_run.next_step_with_potential(|node| live_pot.potential(node)) {
                    num_queue_pops += 1;
                    if node == query.to {
                        break;
                    }
                }
            });

            report!("num_queue_pops", num_queue_pops);
            report!("exploration_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            explore_time = explore_time + time;

            if self.dijkstra_data.distances[query.to as usize] == INFINITY {
                break None;
            }

            final_live_dist = self.dijkstra_data.distances[query.to as usize];
            let path = self.dijkstra_data.node_path(query.from, query.to);
            report!("num_nodes_on_path", path.len());

            let (violating, time) = measure(|| self.ubs_checker.find_ubs_violating_subpaths(&path, &self.smooth_graph, epsilon));
            report!("ubs_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            ubs_time = ubs_time + time;

            if violating.is_empty() {
                break Some(());
            }

            for violating_range in violating {
                self.dijkstra_ops.add_forbidden_path(&path[violating_range.start..=violating_range.end]);
            }
        };
        drop(iterations_ctxt);
        report!("failed", result.is_none());
        report!("num_iterations", i);
        report!("num_forbidden_paths", self.dijkstra_ops.num_forbidden_paths());
        report!(
            "length_increase_percent",
            (final_live_dist - base_live_dist) as f64 / base_live_dist as f64 * 100.0
        );
        report!("total_exploration_time_ms", explore_time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        report!("total_ubs_time_ms", ubs_time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linking_forbidden_paths() {
        let graph = FirstOutGraph::new(&[0, 1, 2], &[1, 0], &[1, 1]);
        let mut ops = BlockedPathsDijkstra::new(2, 2);
        let dd = DijkstraData::new(2);
        ops.add_forbidden_path(&[0, 1], &graph);

        assert_eq!(
            ops.link(
                &graph,
                &dd.distances,
                &dd.predecessors,
                NodeIdT(0),
                0,
                &vec![(0, ActiveForbittenPaths::One(0), (NodeIdT(0), ActiveForbittenPaths::One(0)))],
                &(NodeIdT(1), EdgeIdT(0)),
            ),
            vec![]
        );

        let mut ops = BlockedPathsDijkstra::new(2, 2);
        ops.add_forbidden_path(&[0, 1, 0], &graph);
        assert_eq!(
            ops.link(
                &graph,
                &dd.distances,
                &dd.predecessors,
                NodeIdT(0),
                0,
                &vec![(0, ActiveForbittenPaths::One(0), (NodeIdT(0), ActiveForbittenPaths::One(0)))],
                &(NodeIdT(1), EdgeIdT(0)),
            ),
            vec![(1, ActiveForbittenPaths::One(1), (NodeIdT(0), ActiveForbittenPaths::One(0)))]
        );
        assert_eq!(
            ops.link(
                &graph,
                &dd.distances,
                &dd.predecessors,
                NodeIdT(1),
                0,
                &vec![(1, ActiveForbittenPaths::One(1), (NodeIdT(0), ActiveForbittenPaths::One(0)))],
                &(NodeIdT(0), EdgeIdT(1))
            ),
            vec![]
        );
    }

    #[test]
    fn test_merging() {
        let mut ops = BlockedPathsDijkstra::new(0, 0);
        let mut current = vec![(10, ActiveForbittenPaths::One(1), (NodeIdT(1), ActiveForbittenPaths::One(0)))];
        let improved = ComplexDijkstraOps::<OwnedGraph>::merge(
            &mut ops,
            &mut current,
            vec![(10, ActiveForbittenPaths::One(0), (NodeIdT(2), ActiveForbittenPaths::One(0)))],
        );
        assert!(improved.is_some());
        assert_eq!(current, vec![(10, ActiveForbittenPaths::One(0), (NodeIdT(2), ActiveForbittenPaths::One(0)))]);
    }
}
