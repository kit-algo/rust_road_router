use super::*;

use crate::{
    algo::{a_star::*, ch_potentials::*, customizable_contraction_hierarchy::*, dijkstra::*},
    datastr::rank_select_map::BitVec,
    report::*,
    util::{in_range_option::InRangeOption, with_index},
};
use std::{collections::BTreeMap, ops::Range};

pub struct UBSChecker<'a> {
    target_pot: CCHPotentialWithPathUnpacking<'a>,
    source_pot: CCHPotentialWithPathUnpacking<'a>,
    path_parent_cache: Vec<InRangeOption<NodeId>>,
    path_ranks: Vec<InRangeOption<usize>>,
}

impl UBSChecker<'_> {
    pub fn find_ubs_violating_subpaths<G>(&mut self, path: &[NodeId], graph: &G, epsilon: f64) -> Vec<Range<usize>>
    where
        G: EdgeIdGraph + EdgeRandomAccessGraph<Link>,
    {
        let mut dists = Vec::with_capacity(path.len());
        dists.push(0);
        for node_pair in path.windows(2) {
            let mut min_edge_weight = INFINITY;
            for EdgeIdT(edge_id) in graph.edge_indices(node_pair[0], node_pair[1]) {
                min_edge_weight = std::cmp::min(min_edge_weight, graph.link(edge_id).weight);
            }
            dists.push(dists.last().unwrap() + min_edge_weight);
        }
        self.find_ubs_violating_subpaths_tree(path, &dists, epsilon)
    }

    #[allow(dead_code)]
    fn find_ubs_violating_subpaths_naive(&mut self, path: &[NodeId], dists: &[Weight], epsilon: f64) -> Vec<Range<usize>> {
        let mut violating = Vec::new();
        for (end_rank, end) in path.iter().copied().enumerate() {
            self.target_pot.init(end);
            for (start_rank, start) in path[..end_rank].iter().copied().enumerate().rev() {
                let path_dist = dists[end_rank] - dists[start_rank];
                let shortest_dist = self.target_pot.potential(start).unwrap();
                if path_dist - shortest_dist > (shortest_dist as f64 * epsilon) as Weight {
                    violating.push(start_rank..end_rank);
                    break;
                }
            }
        }
        Self::filter_covered(&mut violating);
        violating
    }

    fn filter_covered(ranges: &mut Vec<Range<usize>>) {
        ranges.sort_unstable_by(|lhs, rhs| {
            let cmp_start = lhs.start.cmp(&rhs.start);
            if let std::cmp::Ordering::Equal = cmp_start {
                rhs.end.cmp(&lhs.end)
            } else {
                cmp_start
            }
        });

        let mut covered = BitVec::new(ranges.len());
        let mut active = BTreeMap::new();

        for (i, range) in ranges.iter().enumerate() {
            active = active.split_off(&range.start);

            let active_and_covered = active.split_off(&range.end);
            for (_, entry) in active_and_covered {
                for idx in entry {
                    covered.set(idx);
                }
            }

            let ending_at_current_end = active.entry(range.end).or_insert(Vec::new());
            ending_at_current_end.push(i);
        }

        ranges.retain(with_index(|index, _| !covered.get(index)));
    }

    fn find_ubs_violating_subpaths_tree(&mut self, path: &[NodeId], dists: &[Weight], epsilon: f64) -> Vec<Range<usize>> {
        for rank in &self.path_ranks {
            debug_assert!(rank.value().is_none());
        }
        let mut violating = Vec::new();
        let path_ranks = &mut self.path_ranks;

        for (rank, &node) in path.iter().enumerate() {
            if let Some(prev_rank) = path_ranks[node as usize].value() {
                eprintln!("Found Cycle of len: {}", rank - prev_rank);
                for &node in path {
                    self.path_ranks[node as usize] = InRangeOption::new(None);
                }
                return vec![prev_rank..rank];
            }
            path_ranks[node as usize] = InRangeOption::new(Some(rank));
        }

        let full_path = path;
        let mut path = path;

        let mut i = 0;
        while !path.is_empty() {
            i += 1;
            let &start = path.first().unwrap();
            let &end = path.last().unwrap();
            self.target_pot.init(end);
            self.source_pot.init(start);
            for &node in path {
                self.target_pot.potential(node);
                self.target_pot.unpack_path(NodeIdT(node));
                self.source_pot.potential(node);
                self.source_pot.unpack_path(NodeIdT(node));
            }
            let cch_order = self.target_pot.cch().node_order();

            // sp tree to target
            for &node in path {
                path_parent(
                    cch_order.rank(node),
                    self.target_pot.target_shortest_path_tree(),
                    &mut self.path_parent_cache,
                    |cch_rank| {
                        path_ranks[cch_order.node(cch_rank) as usize].value().map_or(false, |path_rank| {
                            path_rank >= path_ranks[start as usize].value().unwrap() && path_rank <= path_ranks[end as usize].value().unwrap()
                        })
                    },
                );
            }

            let mut target_earliest_deviation_rank = None;
            for &[node, next_on_path] in path.array_windows::<2>() {
                let path_dist = dists[path_ranks[end as usize].value().unwrap()] - dists[path_ranks[node as usize].value().unwrap() as usize];
                let shortest_dist = self.target_pot.potential(node).unwrap();
                let next_on_path = self.target_pot.cch().node_order().rank(next_on_path);
                let node = self.target_pot.cch().node_order().rank(node);
                if self.target_pot.target_shortest_path_tree()[node as usize] != next_on_path && shortest_dist != path_dist {
                    let path_parent = self.path_parent_cache[node as usize].value().unwrap();
                    let path_parent_rank = path_ranks[self.target_pot.cch().node_order().node(path_parent) as usize].value().unwrap();
                    if let Some(target_earliest_deviation_rank_inner) = target_earliest_deviation_rank {
                        target_earliest_deviation_rank = Some(std::cmp::max(target_earliest_deviation_rank_inner, path_parent_rank));
                    } else {
                        target_earliest_deviation_rank = Some(path_parent_rank);
                    }
                }
            }

            for &node in path {
                reset_path_parent_cache(
                    self.target_pot.cch().node_order().rank(node),
                    self.target_pot.target_shortest_path_tree(),
                    &mut self.path_parent_cache,
                );
            }
            for pp in &self.path_parent_cache {
                debug_assert_eq!(pp.value(), None);
            }

            if target_earliest_deviation_rank.is_none() {
                break;
            }

            // sp tree from source
            for &node in path {
                let cch_order = self.target_pot.cch().node_order();
                path_parent(
                    self.target_pot.cch().node_order().rank(node),
                    self.source_pot.target_shortest_path_tree(),
                    &mut self.path_parent_cache,
                    |cch_rank| {
                        path_ranks[cch_order.node(cch_rank) as usize].value().map_or(false, |path_rank| {
                            path_rank >= path_ranks[start as usize].value().unwrap() && path_rank <= path_ranks[end as usize].value().unwrap()
                        })
                    },
                );
            }

            let mut source_earliest_deviation_rank = None;
            for &[prev_on_path, node] in path.array_windows::<2>() {
                let path_dist = dists[path_ranks[node as usize].value().unwrap() as usize] - dists[path_ranks[start as usize].value().unwrap()];
                let shortest_dist = self.source_pot.potential(node).unwrap();
                let prev_on_path = self.target_pot.cch().node_order().rank(prev_on_path);
                let node = self.target_pot.cch().node_order().rank(node);
                if self.source_pot.target_shortest_path_tree()[node as usize] != prev_on_path && shortest_dist != path_dist {
                    let path_parent = self.path_parent_cache[node as usize].value().unwrap();
                    let path_parent_rank = path_ranks[self.target_pot.cch().node_order().node(path_parent) as usize].value().unwrap();
                    if let Some(source_earliest_deviation_rank_inner) = source_earliest_deviation_rank {
                        source_earliest_deviation_rank = Some(std::cmp::min(source_earliest_deviation_rank_inner, path_parent_rank));
                    } else {
                        source_earliest_deviation_rank = Some(path_parent_rank);
                    }
                }
            }

            for &node in path {
                reset_path_parent_cache(
                    self.target_pot.cch().node_order().rank(node),
                    self.source_pot.target_shortest_path_tree(),
                    &mut self.path_parent_cache,
                );
            }
            for pp in &self.path_parent_cache {
                debug_assert_eq!(pp.value(), None);
            }

            let target_earliest_deviation_rank = target_earliest_deviation_rank.unwrap();
            let target_earliest_deviation_node = full_path[target_earliest_deviation_rank];
            let source_earliest_deviation_rank = source_earliest_deviation_rank.unwrap();
            let source_earliest_deviation_node = full_path[source_earliest_deviation_rank];

            let target_path_base_dist = dists[path_ranks[end as usize].value().unwrap()] - dists[target_earliest_deviation_rank];
            debug_assert_eq!(target_path_base_dist, self.target_pot.potential(target_earliest_deviation_node).unwrap());
            let source_path_base_dist = dists[source_earliest_deviation_rank] - dists[path_ranks[start as usize].value().unwrap()];
            debug_assert_eq!(source_path_base_dist, self.source_pot.potential(source_earliest_deviation_node).unwrap());

            for &node in &full_path[source_earliest_deviation_rank..=target_earliest_deviation_rank] {
                let path_dist = dists[path_ranks[node as usize].value().unwrap()] - dists[path_ranks[start as usize].value().unwrap()] - source_path_base_dist;
                let shortest_dist = self.source_pot.potential(node).unwrap() - source_path_base_dist;
                if path_dist - shortest_dist > (shortest_dist as f64 * epsilon) as Weight {
                    violating.push(source_earliest_deviation_rank..path_ranks[node as usize].value().unwrap());
                    break;
                }
            }
            for &node in full_path[source_earliest_deviation_rank..=target_earliest_deviation_rank].iter().rev() {
                let path_dist = dists[path_ranks[end as usize].value().unwrap()] - dists[path_ranks[node as usize].value().unwrap()] - target_path_base_dist;
                let shortest_dist = self.target_pot.potential(node).unwrap() - target_path_base_dist;
                if path_dist - shortest_dist > (shortest_dist as f64 * epsilon) as Weight {
                    violating.push(path_ranks[node as usize].value().unwrap()..target_earliest_deviation_rank);
                    break;
                }
            }

            path = &full_path[source_earliest_deviation_rank + 1..target_earliest_deviation_rank];
        }
        report!("num_ubs_tree_iterations", i);

        for &node in full_path {
            self.path_ranks[node as usize] = InRangeOption::new(None);
        }

        Self::filter_covered(&mut violating);
        violating
    }
}

#[derive(Clone, Copy, Debug, PartialEq)]
pub struct ActiveForbittenPaths(u32);

impl ActiveForbittenPaths {
    fn is_subset(&self, rhs: &Self) -> bool {
        !(!self.0 | rhs.0) == 0
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
    // Outgoing Edge, Next Node Local Offset
    forbidden_paths: Vec<Vec<(NodeIdT, u8)>>,
    // Global forbidden path id, Node offset on path
    node_forbidden_paths: Vec<Vec<(usize, usize)>>,
    forbidden_paths_start_nodes: Vec<NodeIdT>,
}

impl BlockedPathsDijkstra {
    pub fn new(n: usize) -> Self {
        Self {
            forbidden_paths: Vec::new(),
            node_forbidden_paths: vec![Vec::new(); n],
            forbidden_paths_start_nodes: Vec::new(),
        }
    }

    pub fn add_forbidden_path(&mut self, path: &[NodeId]) {
        self.forbidden_paths_start_nodes.push(NodeIdT(path[0]));

        let global_id = self.forbidden_paths.len();
        let mut forbidden_path = Vec::with_capacity(path.len() - 1);

        for (node_path_index, &[tail, head]) in path.array_windows::<2>().enumerate() {
            self.node_forbidden_paths[tail as usize].push((global_id, node_path_index));
            let node_forbidden_path_index = self.node_forbidden_paths[head as usize].len();
            assert!(node_forbidden_path_index < 32, "{:#?}", (&self.node_forbidden_paths[head as usize], tail, head));
            forbidden_path.push((NodeIdT(head), node_forbidden_path_index as u8));
        }

        self.forbidden_paths.push(forbidden_path);
    }

    pub fn reset(&mut self) {
        for NodeIdT(node) in self.forbidden_paths_start_nodes.drain(..) {
            self.node_forbidden_paths[node as usize].clear();
        }
        for path in self.forbidden_paths.drain(..) {
            for (NodeIdT(node), _) in path {
                self.node_forbidden_paths[node as usize].clear();
            }
        }
    }
}

impl<G> DijkstraOps<G> for BlockedPathsDijkstra {
    type Label = Vec<(Weight, ActiveForbittenPaths, (NodeIdT, ActiveForbittenPaths))>;
    type Arc = Link;
    type LinkResult = Vec<(Weight, ActiveForbittenPaths, (NodeIdT, ActiveForbittenPaths))>;
    type PredecessorLink = ();

    fn link(&mut self, _graph: &G, NodeIdT(tail): NodeIdT, label: &Self::Label, link: &Self::Arc) -> Self::LinkResult {
        let mut linked = Vec::new();

        for l in label {
            let mut illegal = false;
            let mut head_active_forbidden_paths = 0;
            let active_forbidden_paths = l.1 .0;

            for (local_idx, &(global_id, current_node_path_offset)) in self.node_forbidden_paths[tail as usize].iter().enumerate() {
                if current_node_path_offset == 0 || active_forbidden_paths & (1 << local_idx) != 0 {
                    if self.forbidden_paths[global_id][current_node_path_offset].0 .0 == link.node {
                        if self.forbidden_paths[global_id].len() == current_node_path_offset + 1 {
                            illegal = true;
                            break;
                        } else {
                            head_active_forbidden_paths |= 1 << self.forbidden_paths[global_id][current_node_path_offset].1
                        }
                    }
                }
            }
            if !illegal {
                linked.push((l.0 + link.weight, ActiveForbittenPaths(head_active_forbidden_paths), (NodeIdT(tail), l.1)));
            }
        }

        linked
    }
    fn merge(&mut self, label: &mut Self::Label, mut linked: Self::LinkResult) -> bool {
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

        let updated = !linked.is_empty();

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
            let active_paths = label[cur_idx].1;

            label.retain(with_index(|idx, l: &(Weight, ActiveForbittenPaths, (NodeIdT, ActiveForbittenPaths))| {
                idx < cur_dist_start_idx || idx == cur_idx || !active_paths.is_subset(&l.1)
            }));
            cur_idx += 1;
        }

        updated
    }
    fn predecessor_link(&self, _link: &Self::Arc) -> Self::PredecessorLink {
        ()
    }
}

pub struct TrafficAwareQuery(Query);

impl GenQuery<Vec<(Weight, ActiveForbittenPaths, (NodeIdT, ActiveForbittenPaths))>> for TrafficAwareQuery {
    fn new(from: NodeId, to: NodeId, _initial_state: Vec<(Weight, ActiveForbittenPaths, (NodeIdT, ActiveForbittenPaths))>) -> Self {
        TrafficAwareQuery(Query { from, to })
    }

    fn from(&self) -> NodeId {
        self.0.from
    }
    fn to(&self) -> NodeId {
        self.0.to
    }
    fn initial_state(&self) -> Vec<(Weight, ActiveForbittenPaths, (NodeIdT, ActiveForbittenPaths))> {
        vec![(0, ActiveForbittenPaths(0), (NodeIdT(self.0.from), ActiveForbittenPaths(0)))]
    }
    fn permutate(&mut self, order: &NodeOrder) {
        self.0.from = order.rank(self.0.from);
        self.0.to = order.rank(self.0.to);
    }
}

pub struct TrafficAwareServer<'a> {
    ubs_checker: UBSChecker<'a>,
    dijkstra_data: DijkstraData<Vec<(Weight, ActiveForbittenPaths, (NodeIdT, ActiveForbittenPaths))>>,
    live_pot: CCHPotential<'a, FirstOutGraph<&'a [EdgeId], &'a [NodeId], &'a [Weight]>, FirstOutGraph<&'a [EdgeId], &'a [NodeId], &'a [Weight]>>,
    live_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], &'a [Weight]>,
    smooth_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], &'a [Weight]>,
    dijkstra_ops: BlockedPathsDijkstra,
}

impl<'a> TrafficAwareServer<'a> {
    pub fn new(
        smooth_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], &'a [Weight]>,
        live_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], &'a [Weight]>,
        smooth_cch_pot: &'a CCHPotData,
        live_cch_pot: &'a CCHPotData,
    ) -> Self {
        let n = smooth_graph.num_nodes();
        Self {
            ubs_checker: UBSChecker {
                target_pot: smooth_cch_pot.forward_path_potential(),
                source_pot: smooth_cch_pot.backward_path_potential(),
                path_parent_cache: vec![InRangeOption::new(None); n],
                path_ranks: vec![InRangeOption::new(None); n],
            },
            dijkstra_data: DijkstraData::new(n),
            live_pot: live_cch_pot.forward_potential(),
            live_graph,
            smooth_graph,
            dijkstra_ops: BlockedPathsDijkstra::new(n),
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
            let _it_ctxt = iterations_ctxt.push_collection_item();
            i += 1;
            report!("iteration", i);
            let mut dijk_run = DijkstraRun::query(&self.live_graph, &mut self.dijkstra_data, &mut self.dijkstra_ops, TrafficAwareQuery(query));

            let mut num_queue_pops = 0;
            let mut num_labels_in_search_space = 0;
            let live_pot = &mut self.live_pot;
            let (_, time) = measure(|| {
                while let Some(node) = dijk_run.next_step_with_potential(|node| live_pot.potential(node)) {
                    num_queue_pops += 1;
                    num_labels_in_search_space += dijk_run.tentative_distance(node).len();
                    if node == query.to {
                        break;
                    }
                }
            });
            report!("num_queue_pops", num_queue_pops);
            report!("num_labels_in_search_space", num_labels_in_search_space);
            report!("exploration_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            explore_time = explore_time + time;

            if self.dijkstra_data.distances[query.to as usize].is_empty() {
                break None;
            }

            final_live_dist = self.dijkstra_data.distances[query.to as usize][0].0;

            let mut path = Vec::new();
            path.push(query.to);

            let mut label_idx = 0;
            while *path.last().unwrap() != query.from {
                let (dist, _, (NodeIdT(parent), parent_active_forb_paths)) = self.dijkstra_data.distances[*path.last().unwrap() as usize][label_idx];

                let min_weight_edge = self
                    .live_graph
                    .edge_indices(parent, *path.last().unwrap())
                    .min_by_key(|&EdgeIdT(edge_id)| self.live_graph.link(edge_id).weight)
                    .unwrap();

                label_idx = self.dijkstra_data.distances[parent as usize]
                    .iter()
                    .position(|l| l.0 == dist - self.live_graph.link(min_weight_edge.0).weight && l.1.is_subset(&parent_active_forb_paths))
                    .unwrap();

                path.push(parent);
            }

            path.reverse();
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
        report!("num_iterations", i);
        report!("num_forbidden_paths", self.dijkstra_ops.forbidden_paths.len());
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
    fn test_filter_covered() {
        let mut ranges = vec![0..1];
        UBSChecker::filter_covered(&mut ranges);
        assert_eq!(vec![0..1], ranges);

        let mut ranges = vec![1..3, 1..2, 2..3, 0..3];
        UBSChecker::filter_covered(&mut ranges);
        assert_eq!(vec![1..2, 2..3], ranges);

        let mut ranges = vec![0..1, 0..2];
        UBSChecker::filter_covered(&mut ranges);
        assert_eq!(vec![0..1], ranges);

        let mut ranges = vec![1..2, 0..2];
        UBSChecker::filter_covered(&mut ranges);
        assert_eq!(vec![1..2], ranges);

        let mut ranges = vec![0..1, 1..2];
        UBSChecker::filter_covered(&mut ranges);
        assert_eq!(vec![0..1, 1..2], ranges);
    }

    #[test]
    fn test_linking_forbidden_paths() {
        let graph = FirstOutGraph::new(&[0, 0, 0], &[], &[]);
        let mut ops = BlockedPathsDijkstra::new(2);
        ops.add_forbidden_path(&[0, 1]);

        assert_eq!(
            ops.link(
                &graph,
                NodeIdT(0),
                &vec![(0, ActiveForbittenPaths(0), (NodeIdT(0), ActiveForbittenPaths(0)))],
                &Link { node: 1, weight: 1 },
            ),
            vec![]
        );

        let mut ops = BlockedPathsDijkstra::new(2);
        ops.add_forbidden_path(&[0, 1, 0]);
        assert_eq!(
            ops.link(
                &graph,
                NodeIdT(0),
                &vec![(0, ActiveForbittenPaths(0), (NodeIdT(0), ActiveForbittenPaths(0)))],
                &Link { node: 1, weight: 1 },
            ),
            vec![(1, ActiveForbittenPaths(1), (NodeIdT(0), ActiveForbittenPaths(0)))]
        );
        assert_eq!(
            ops.link(
                &graph,
                NodeIdT(1),
                &vec![(1, ActiveForbittenPaths(1), (NodeIdT(0), ActiveForbittenPaths(0)))],
                &Link { node: 0, weight: 1 }
            ),
            vec![]
        );
    }

    #[test]
    fn test_merging() {
        let mut ops = BlockedPathsDijkstra::new(0);
        let mut current = vec![(10, ActiveForbittenPaths(1), (NodeIdT(1), ActiveForbittenPaths(0)))];
        let improved = DijkstraOps::<OwnedGraph>::merge(
            &mut ops,
            &mut current,
            vec![(10, ActiveForbittenPaths(0), (NodeIdT(2), ActiveForbittenPaths(0)))],
        );
        assert!(improved);
        assert_eq!(current, vec![(10, ActiveForbittenPaths(0), (NodeIdT(2), ActiveForbittenPaths(0)))]);
    }
}
