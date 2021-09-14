use super::*;

use crate::{
    algo::{a_star::*, ch_potentials::*, customizable_contraction_hierarchy::*, dijkstra::*},
    datastr::rank_select_map::BitVec,
    report::*,
    util::{in_range_option::InRangeOption, with_index},
};
use std::{collections::BTreeMap, ops::Range};

pub struct UBSChecker<'a> {
    forward_pot: CCHPotentialWithPathUnpacking<'a>,
    backward_pot: CCHPotentialWithPathUnpacking<'a>,
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

    fn find_ubs_violating_subpaths_naive(&mut self, path: &[NodeId], dists: &[Weight], epsilon: f64) -> Vec<Range<usize>> {
        let mut violating = Vec::new();
        for (end_rank, end) in path.iter().copied().enumerate() {
            self.forward_pot.init(end);
            for (start_rank, start) in path[..end_rank].iter().copied().enumerate().rev() {
                let path_dist = dists[end_rank] - dists[start_rank];
                let shortest_dist = self.forward_pot.potential(start).unwrap();
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
        let mut violating = Vec::new();
        let path_ranks = &mut self.path_ranks;

        for (rank, &node) in path.iter().enumerate() {
            if let Some(prev_rank) = path_ranks[node as usize].value() {
                eprintln!("Found Cycle");
                return vec![prev_rank..rank];
            }
            path_ranks[node as usize] = InRangeOption::new(Some(rank));
        }

        let full_path = path;
        let mut path = path;

        while !path.is_empty() {
            let &start = path.first().unwrap();
            let &end = path.last().unwrap();
            self.forward_pot.init(end);
            self.backward_pot.init(start);
            for &node in path {
                self.forward_pot.potential(node);
                self.forward_pot.unpack_path(NodeIdT(node));
                self.backward_pot.potential(node);
                self.backward_pot.unpack_path(NodeIdT(node));
            }
            let cch_order = self.forward_pot.cch().node_order();

            for &node in path {
                path_parent(
                    cch_order.rank(node),
                    self.forward_pot.target_shortest_path_tree(),
                    &mut self.path_parent_cache,
                    |cch_rank| {
                        path_ranks[cch_order.node(cch_rank) as usize].value().map_or(false, |path_rank| {
                            path_rank >= path_ranks[start as usize].value().unwrap() && path_rank <= path_ranks[end as usize].value().unwrap()
                        })
                    },
                );
            }

            let mut fw_earliest_deviation_rank = None;
            for &node in path.iter() {
                let node = cch_order.rank(node);
                if self.forward_pot.target_shortest_path_tree()[node as usize] != self.path_parent_cache[node as usize].value().unwrap() {
                    let path_parent = self.path_parent_cache[node as usize].value().unwrap();
                    let path_parent_rank = path_ranks[cch_order.node(path_parent) as usize].value().unwrap();
                    if let Some(fw_earliest_deviation_rank_inner) = fw_earliest_deviation_rank {
                        fw_earliest_deviation_rank = Some(std::cmp::max(fw_earliest_deviation_rank_inner, path_parent_rank));
                    } else {
                        fw_earliest_deviation_rank = Some(path_parent_rank);
                    }
                }
            }

            for &node in path {
                reset_path_parent_cache(cch_order.rank(node), self.forward_pot.target_shortest_path_tree(), &mut self.path_parent_cache);
            }
            for pp in &self.path_parent_cache {
                debug_assert_eq!(pp.value(), None);
            }

            if fw_earliest_deviation_rank.is_none() {
                break;
            }

            for &node in path {
                path_parent(
                    cch_order.rank(node),
                    self.backward_pot.target_shortest_path_tree(),
                    &mut self.path_parent_cache,
                    |cch_rank| {
                        path_ranks[cch_order.node(cch_rank) as usize].value().map_or(false, |path_rank| {
                            path_rank >= path_ranks[start as usize].value().unwrap() && path_rank <= path_ranks[end as usize].value().unwrap()
                        })
                    },
                );
            }

            let mut bw_earliest_deviation_rank = None;
            for &node in path.iter() {
                let node = cch_order.rank(node);
                if self.backward_pot.target_shortest_path_tree()[node as usize] != self.path_parent_cache[node as usize].value().unwrap() {
                    let path_parent = self.path_parent_cache[node as usize].value().unwrap();
                    let path_parent_rank = path_ranks[cch_order.node(path_parent) as usize].value().unwrap();
                    if let Some(bw_earliest_deviation_rank_inner) = bw_earliest_deviation_rank {
                        bw_earliest_deviation_rank = Some(std::cmp::min(bw_earliest_deviation_rank_inner, path_parent_rank));
                    } else {
                        bw_earliest_deviation_rank = Some(path_parent_rank);
                    }
                }
            }

            for &node in path {
                reset_path_parent_cache(cch_order.rank(node), self.backward_pot.target_shortest_path_tree(), &mut self.path_parent_cache);
            }
            for pp in &self.path_parent_cache {
                debug_assert_eq!(pp.value(), None);
            }

            let fw_earliest_deviation_rank = fw_earliest_deviation_rank.unwrap();
            let fw_earliest_deviation_node = full_path[fw_earliest_deviation_rank];
            let bw_earliest_deviation_rank = bw_earliest_deviation_rank.unwrap();
            let bw_earliest_deviation_node = full_path[bw_earliest_deviation_rank];
            let fw_path_base_dist = dists[path_ranks[end as usize].value().unwrap()] - dists[fw_earliest_deviation_rank];
            debug_assert_eq!(fw_path_base_dist, self.forward_pot.potential(fw_earliest_deviation_node).unwrap());
            let bw_path_base_dist = dists[bw_earliest_deviation_rank] - dists[path_ranks[start as usize].value().unwrap()];
            debug_assert_eq!(bw_path_base_dist, self.backward_pot.potential(bw_earliest_deviation_node).unwrap());

            for &node in &full_path[bw_earliest_deviation_rank..=fw_earliest_deviation_rank] {
                let path_dist = dists[path_ranks[node as usize].value().unwrap()] - dists[path_ranks[start as usize].value().unwrap()] - bw_path_base_dist;
                let shortest_dist = self.backward_pot.potential(node).unwrap() - bw_path_base_dist;
                if path_dist - shortest_dist > (shortest_dist as f64 * epsilon) as Weight {
                    violating.push(bw_earliest_deviation_rank..path_ranks[node as usize].value().unwrap());
                    break;
                }
            }
            for &node in full_path[bw_earliest_deviation_rank..=fw_earliest_deviation_rank].iter().rev() {
                let path_dist = dists[path_ranks[end as usize].value().unwrap()] - dists[path_ranks[node as usize].value().unwrap()] - fw_path_base_dist;
                let shortest_dist = self.forward_pot.potential(node).unwrap() - fw_path_base_dist;
                if path_dist - shortest_dist > (shortest_dist as f64 * epsilon) as Weight {
                    violating.push(path_ranks[node as usize].value().unwrap()..fw_earliest_deviation_rank);
                    break;
                }
            }

            path = &full_path[bw_earliest_deviation_rank + 1..fw_earliest_deviation_rank];
        }

        for &node in path {
            self.path_ranks[node as usize] = InRangeOption::new(None);
        }

        Self::filter_covered(&mut violating);
        violating
    }
}

#[derive(Clone, Copy, Debug)]
pub struct ActiveForbittenPaths(u32);

impl ActiveForbittenPaths {
    fn is_subset(&self, rhs: &Self) -> bool {
        !(!self.0 | rhs.0) == 0
    }
}

impl Label for Vec<(Weight, ActiveForbittenPaths, NodeIdT)> {
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
            assert!(node_forbidden_path_index < 256);
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
    type Label = Vec<(Weight, ActiveForbittenPaths, NodeIdT)>;
    type Arc = Link;
    type LinkResult = Vec<(Weight, ActiveForbittenPaths, NodeIdT)>;
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
                linked.push((l.0 + link.weight, ActiveForbittenPaths(head_active_forbidden_paths), NodeIdT(tail)));
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

            label.retain(with_index(|idx, l: &(Weight, ActiveForbittenPaths, NodeIdT)| {
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

impl GenQuery<Vec<(Weight, ActiveForbittenPaths, NodeIdT)>> for TrafficAwareQuery {
    fn new(from: NodeId, to: NodeId, _initial_state: Vec<(Weight, ActiveForbittenPaths, NodeIdT)>) -> Self {
        TrafficAwareQuery(Query { from, to })
    }

    fn from(&self) -> NodeId {
        self.0.from
    }
    fn to(&self) -> NodeId {
        self.0.to
    }
    fn initial_state(&self) -> Vec<(Weight, ActiveForbittenPaths, NodeIdT)> {
        vec![(0, ActiveForbittenPaths(0), NodeIdT(self.0.from))]
    }
    fn permutate(&mut self, order: &NodeOrder) {
        self.0.from = order.rank(self.0.from);
        self.0.to = order.rank(self.0.to);
    }
}

pub struct TrafficAwareServer<'a> {
    ubs_checker: UBSChecker<'a>,
    dijkstra_data: DijkstraData<Vec<(Weight, ActiveForbittenPaths, NodeIdT)>>,
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
                forward_pot: smooth_cch_pot.forward_path_potential(),
                backward_pot: smooth_cch_pot.backward_path_potential(),
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

        let mut i = 0;
        let result = loop {
            i += 1;
            let mut dijk_run = DijkstraRun::query(&self.live_graph, &mut self.dijkstra_data, &mut self.dijkstra_ops, TrafficAwareQuery(query));

            let live_pot = &mut self.live_pot;
            while let Some(node) = dijk_run.next_step_with_potential(|node| live_pot.potential(node)) {
                if node == query.to {
                    break;
                }
            }
            if self.dijkstra_data.distances[query.to as usize].is_empty() {
                break None;
            }

            let mut path = Vec::new();
            path.push(query.to);

            let mut label_idx = 0;
            while *path.last().unwrap() != query.from {
                let (dist, _, NodeIdT(parent)) = self.dijkstra_data.distances[*path.last().unwrap() as usize][label_idx];

                let min_weight_edge = self
                    .live_graph
                    .edge_indices(parent, *path.last().unwrap())
                    .min_by_key(|&EdgeIdT(edge_id)| self.live_graph.link(edge_id).weight)
                    .unwrap();

                label_idx = self.dijkstra_data.distances[parent as usize]
                    .iter()
                    .position(|l| l.0 == dist - self.live_graph.link(min_weight_edge.0).weight)
                    .unwrap();

                path.push(parent);
            }

            path.reverse();

            let violating = self.ubs_checker.find_ubs_violating_subpaths(&path, &self.smooth_graph, epsilon);

            if violating.is_empty() {
                break Some(());
            }

            for violating_range in violating {
                self.dijkstra_ops.add_forbidden_path(&path[violating_range.start..=violating_range.end]);
            }
        };
        report!("num_iterations", i);
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
}
