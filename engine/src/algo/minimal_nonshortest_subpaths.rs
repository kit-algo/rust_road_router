use super::*;

use crate::{
    algo::{a_star::*, ch_potentials::*, customizable_contraction_hierarchy::*},
    datastr::{graph::first_out_graph::BorrowedGraph, rank_select_map::BitVec},
    report::*,
    util::{in_range_option::InRangeOption, with_index},
};
use std::{collections::BTreeMap, ops::Range};

pub struct MinimalNonShortestSubPaths<'a> {
    target_pot: CCHPotentialWithPathUnpacking<'a>,
    source_pot: CCHPotentialWithPathUnpacking<'a>,
    path_parent_cache: Vec<InRangeOption<NodeId>>,
    path_ranks: Vec<InRangeOption<usize>>,
    smooth_graph: BorrowedGraph<'a>,
}

impl<'a> MinimalNonShortestSubPaths<'a> {
    pub fn new(smooth_cch_pot: &'a CCHPotData, smooth_graph: BorrowedGraph<'a>) -> Self {
        let n = smooth_cch_pot.num_nodes();
        Self {
            target_pot: smooth_cch_pot.forward_path_potential(),
            source_pot: smooth_cch_pot.backward_path_potential(),
            path_parent_cache: vec![InRangeOption::NONE; n],
            path_ranks: vec![InRangeOption::NONE; n],
            smooth_graph,
        }
    }

    pub fn find_ubs_violating_subpaths_lazy_rphast_naive(&mut self, path: &[NodeId], epsilon: f64) -> Vec<Range<usize>> {
        let dists = self.path_dists(path);
        let path: Vec<_> = path.iter().map(|&node| self.target_pot.cch().node_order().rank(node)).collect();
        let mut violating = Vec::new();

        let path_ranks = &mut self.path_ranks;

        for (rank, &node) in path.iter().enumerate() {
            if let Some(prev_rank) = path_ranks[node as usize].value() {
                // eprintln!("Found Cycle of len: {}", rank - prev_rank);
                violating.push(prev_rank..rank)
            }
            path_ranks[node as usize] = InRangeOption::some(rank);
        }

        for &node in &path {
            self.path_ranks[node as usize] = InRangeOption::NONE;
        }
        if !violating.is_empty() {
            return violating;
        }

        for (u_idx, &u) in path[..path.len() - 1].iter().enumerate() {
            self.source_pot.init(u);
            for (v_idx, &v) in path.iter().enumerate().skip(u_idx + 1) {
                let shortest_dist = self.source_pot.potential(v).unwrap();
                let path_dist = dists[v_idx] - dists[u_idx];

                debug_assert!(path_dist >= shortest_dist, "{} {}", path_dist, shortest_dist);
                debug_assert!(shortest_dist > 0);
                if path_dist - shortest_dist > (shortest_dist as f64 * epsilon) as Weight {
                    violating.push(u_idx..v_idx);
                    break;
                }
            }
        }

        filter_covered(&mut violating);
        violating
    }

    pub fn find_ubs_violating_subpaths(&mut self, orig_path: &[NodeId], epsilon: f64) -> Vec<Range<usize>> {
        let path: Vec<_> = orig_path.iter().map(|&node| self.target_pot.cch().node_order().rank(node)).collect();
        let path = &path[..];

        for rank in &self.path_ranks {
            debug_assert!(rank.value().is_none());
        }
        let mut violating = Vec::new();
        let path_ranks = &mut self.path_ranks;

        for (rank, &node) in path.iter().enumerate() {
            if let Some(prev_rank) = path_ranks[node as usize].value() {
                // eprintln!("Found Cycle of len: {}", rank - prev_rank);
                violating.push(prev_rank..rank)
            }
            path_ranks[node as usize] = InRangeOption::some(rank);
        }

        if !violating.is_empty() {
            for &node in path {
                self.path_ranks[node as usize] = InRangeOption::NONE;
            }
            return violating;
        }

        let dists = self.path_dists(orig_path);

        self.tree_find_minimal_nonshortest(path, &dists, |range, path_dist, shortest_dist| {
            debug_assert!(path_dist >= shortest_dist);
            debug_assert!(shortest_dist > 0);
            if path_dist - shortest_dist > (shortest_dist as f64 * epsilon) as Weight {
                violating.push(range);
                true
            } else {
                false
            }
        });

        for &node in path {
            self.path_ranks[node as usize] = InRangeOption::NONE;
        }

        filter_covered(&mut violating);
        violating
    }

    pub fn fix_violating_subpaths(&mut self, orig_path: &[NodeId], epsilon: f64) -> Result<Option<Vec<NodeId>>, Vec<NodeId>> {
        let mut result = None;

        let mut iterations_ctxt = push_collection_context("iterations".to_string());
        let mut i = 0;
        while let Some(fixed) = {
            let _blocked = block_reporting();
            self.fix_violating_subpaths_int(result.as_deref().unwrap_or(orig_path), epsilon)
        } {
            let _it = iterations_ctxt.push_collection_item();
            i += 1;
            if i > 250 {
                report!("num_iterations", i);
                return Err(fixed);
            }
            report!("iteration", i);
            result = Some(fixed);
            drop(_it);
        }
        drop(iterations_ctxt);
        report!("num_iterations", i);
        Ok(result)
    }

    fn fix_violating_subpaths_int(&mut self, orig_path: &[NodeId], epsilon: f64) -> Option<Vec<NodeId>> {
        let mut violating = self.find_ubs_violating_subpaths(orig_path, epsilon);
        if violating.is_empty() {
            return None;
        }
        violating.sort_unstable_by_key(|r| r.start);

        let mut covered_until = 0;
        violating.retain(|r| {
            if r.start >= covered_until {
                covered_until = r.end;
                true
            } else {
                false
            }
        });
        report!("num_fixed_segments", violating.len());

        let order = self.source_pot.cch().node_order();
        let mut fixed_path = Vec::new();
        let mut pushed_until = 0;

        for r in violating {
            fixed_path.extend_from_slice(&orig_path[pushed_until..=r.start]);

            let segment_end = order.rank(orig_path[r.end]);
            let segment_start = order.rank(orig_path[r.start]);
            self.target_pot.init(segment_end);
            self.target_pot.unpack_path(NodeIdT(segment_start));

            while *fixed_path.last().unwrap() != orig_path[r.end] {
                let last = *fixed_path.last().unwrap();
                let parent = order.node(self.target_pot.target_shortest_path_tree()[order.rank(last) as usize]);
                fixed_path.push(parent);
            }

            pushed_until = r.end + 1;
        }
        fixed_path.extend_from_slice(&orig_path[pushed_until..]);

        Some(fixed_path)
    }

    pub fn local_optimality(&mut self, orig_path: &[NodeId]) -> f64 {
        let path: Vec<_> = orig_path.iter().map(|&node| self.target_pot.cch().node_order().rank(node)).collect();
        let path = &path[..];

        for rank in &self.path_ranks {
            debug_assert!(rank.value().is_none());
        }
        let path_ranks = &mut self.path_ranks;

        for (rank, &node) in path.iter().enumerate() {
            if let Some(prev_rank) = path_ranks[node as usize].value() {
                panic!("Found Cycle of len: {}", rank - prev_rank);
            }
            path_ranks[node as usize] = InRangeOption::some(rank);
        }

        let dists = self.path_dists(orig_path);
        let base_dist = *dists.last().unwrap();
        let mut local_optimality = base_dist;

        self.tree_find_minimal_nonshortest(path, &dists, |_range, path_dist, shortest_dist| {
            if path_dist != shortest_dist {
                local_optimality = std::cmp::min(local_optimality, path_dist);
                true
            } else {
                false
            }
        });

        for &node in path {
            self.path_ranks[node as usize] = InRangeOption::NONE;
        }

        local_optimality as f64 / base_dist as f64
    }

    pub fn suboptimal_stats(&mut self, orig_path: &[NodeId]) -> (f64, f64) {
        let path: Vec<_> = orig_path.iter().map(|&node| self.target_pot.cch().node_order().rank(node)).collect();
        let path = &path[..];

        for rank in &self.path_ranks {
            debug_assert!(rank.value().is_none());
        }
        let path_ranks = &mut self.path_ranks;

        for (rank, &node) in path.iter().enumerate() {
            if let Some(prev_rank) = path_ranks[node as usize].value() {
                panic!("Found Cycle of len: {}", rank - prev_rank);
            }
            path_ranks[node as usize] = InRangeOption::some(rank);
        }

        let dists = self.path_dists(orig_path);
        let base_dist = *dists.last().unwrap();
        let mut local_optimality = base_dist;
        let mut ubs = 0.0;

        self.tree_find_minimal_nonshortest(path, &dists, |_range, path_dist, shortest_dist| {
            debug_assert_ne!(shortest_dist, 0);
            if path_dist != shortest_dist {
                local_optimality = std::cmp::min(local_optimality, path_dist);
                let new_ubs = (path_dist - shortest_dist) as f64 / (shortest_dist as f64);
                if new_ubs > ubs {
                    ubs = new_ubs;
                }
            }
            false
        });

        for &node in path {
            self.path_ranks[node as usize] = InRangeOption::NONE;
        }

        (local_optimality as f64 / base_dist as f64, ubs + 1.0)
    }

    fn tree_find_minimal_nonshortest(&mut self, path: &[NodeId], dists: &[Weight], mut found_violating: impl FnMut(Range<usize>, Weight, Weight) -> bool) {
        let path_ranks = &self.path_ranks;

        for (rank, &node) in path.iter().enumerate() {
            debug_assert_eq!(rank, path_ranks[node as usize].value().unwrap() as usize);
        }

        let full_path = path;
        let mut path = path;

        let mut i = 0;
        while !path.is_empty() {
            i += 1;
            let &start = path.first().unwrap();
            let &end = path.last().unwrap();

            self.target_pot.init(end);
            let mut target_earliest_deviation_rank = None;
            let mut target_earliest_suboptimal_rank = path_ranks[start as usize].value().unwrap();

            for &[node, next_on_path] in path.array_windows::<2>() {
                let path_dist = dists[path_ranks[end as usize].value().unwrap()] - dists[path_ranks[node as usize].value().unwrap() as usize];
                let shortest_dist = self.target_pot.potential(node).unwrap();

                if path_dist == shortest_dist {
                    break;
                }
                target_earliest_suboptimal_rank = path_ranks[node as usize].value().unwrap() as usize;
                self.target_pot.unpack_path(NodeIdT(node));

                if self.target_pot.target_shortest_path_tree()[node as usize] != next_on_path && shortest_dist != path_dist {
                    path_parent(node, self.target_pot.target_shortest_path_tree(), &mut self.path_parent_cache, |node| {
                        path_ranks[node as usize].value().map_or(false, |path_rank| {
                            path_rank >= path_ranks[start as usize].value().unwrap() && path_rank <= path_ranks[end as usize].value().unwrap()
                        })
                    });

                    let path_parent = self.path_parent_cache[node as usize].value().unwrap();
                    let path_parent_rank = path_ranks[path_parent as usize].value().unwrap();
                    if let Some(target_earliest_deviation_rank_inner) = target_earliest_deviation_rank {
                        target_earliest_deviation_rank = Some(std::cmp::max(target_earliest_deviation_rank_inner, path_parent_rank));
                    } else {
                        target_earliest_deviation_rank = Some(path_parent_rank);
                    }
                }
            }

            for &node in path {
                reset_path_parent_cache(node, self.target_pot.target_shortest_path_tree(), &mut self.path_parent_cache);
            }
            for pp in &self.path_parent_cache {
                debug_assert_eq!(pp.value(), None);
            }

            if target_earliest_deviation_rank.is_none() {
                break;
            }

            // sp tree from source
            self.source_pot.init(start);
            let mut source_earliest_deviation_rank = None;
            let mut source_earliest_suboptimal_rank = path_ranks[end as usize].value().unwrap();

            for &[prev_on_path, node] in path.array_windows::<2>().rev() {
                let path_dist = dists[path_ranks[node as usize].value().unwrap() as usize] - dists[path_ranks[start as usize].value().unwrap()];
                let shortest_dist = self.source_pot.potential(node).unwrap();

                if path_dist == shortest_dist {
                    break;
                }
                source_earliest_suboptimal_rank = path_ranks[node as usize].value().unwrap() as usize;
                self.source_pot.unpack_path(NodeIdT(node));

                if self.source_pot.target_shortest_path_tree()[node as usize] != prev_on_path && shortest_dist != path_dist {
                    path_parent(node, self.source_pot.target_shortest_path_tree(), &mut self.path_parent_cache, |node| {
                        path_ranks[node as usize].value().map_or(false, |path_rank| {
                            path_rank >= path_ranks[start as usize].value().unwrap() && path_rank <= path_ranks[end as usize].value().unwrap()
                        })
                    });

                    let path_parent = self.path_parent_cache[node as usize].value().unwrap();
                    let path_parent_rank = path_ranks[path_parent as usize].value().unwrap();
                    if let Some(source_earliest_deviation_rank_inner) = source_earliest_deviation_rank {
                        source_earliest_deviation_rank = Some(std::cmp::min(source_earliest_deviation_rank_inner, path_parent_rank));
                    } else {
                        source_earliest_deviation_rank = Some(path_parent_rank);
                    }
                }
            }

            for &node in path {
                reset_path_parent_cache(node, self.source_pot.target_shortest_path_tree(), &mut self.path_parent_cache);
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

            for &node in &full_path[source_earliest_suboptimal_rank..=target_earliest_deviation_rank] {
                let path_dist = dists[path_ranks[node as usize].value().unwrap()] - dists[path_ranks[start as usize].value().unwrap()] - source_path_base_dist;
                let shortest_dist = self.source_pot.potential(node).unwrap() - source_path_base_dist;
                if found_violating(
                    source_earliest_deviation_rank..path_ranks[node as usize].value().unwrap(),
                    path_dist,
                    shortest_dist,
                ) {
                    break;
                }
            }
            for &node in full_path[source_earliest_deviation_rank..=target_earliest_suboptimal_rank].iter().rev() {
                let path_dist = dists[path_ranks[end as usize].value().unwrap()] - dists[path_ranks[node as usize].value().unwrap()] - target_path_base_dist;
                let shortest_dist = self.target_pot.potential(node).unwrap() - target_path_base_dist;
                if found_violating(
                    path_ranks[node as usize].value().unwrap()..target_earliest_deviation_rank,
                    path_dist,
                    shortest_dist,
                ) {
                    break;
                }
            }

            path = &full_path[source_earliest_deviation_rank + 1..target_earliest_deviation_rank];
        }
        report!("num_ubs_tree_iterations", i);
    }

    fn path_dists(&self, path: &[NodeId]) -> Vec<Weight> {
        path_dists(path, &self.smooth_graph)
    }
}

pub fn path_dists(path: &[NodeId], graph: &BorrowedGraph) -> Vec<Weight> {
    path_dist_iter(path, graph).collect()
}

pub fn path_dist_iter<'a>(path: &'a [NodeId], graph: &'a BorrowedGraph) -> impl Iterator<Item = Weight> + 'a {
    std::iter::once(0).chain(path.windows(2).scan(0, move |state, node_pair| {
        let mut min_edge_weight = INFINITY;
        for EdgeIdT(edge_id) in graph.edge_indices(node_pair[0], node_pair[1]) {
            min_edge_weight = std::cmp::min(min_edge_weight, graph.link(edge_id).weight);
        }
        debug_assert_ne!(min_edge_weight, INFINITY);

        *state += min_edge_weight;
        Some(*state)
    }))
}

pub fn filter_covered(ranges: &mut Vec<Range<usize>>) {
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

        let ending_at_current_end = active.entry(range.end).or_insert_with(Vec::new);
        ending_at_current_end.push(i);
    }

    ranges.retain(with_index(|index, _| !covered.get(index)));
}

pub fn path_parent(node: NodeId, predecessors: &[NodeId], path_parent_cache: &mut [InRangeOption<NodeId>], mut on_path: impl FnMut(NodeId) -> bool) -> NodeId {
    if let Some(path_parent) = path_parent_cache[node as usize].value() {
        return path_parent;
    }

    if on_path(predecessors[node as usize]) {
        path_parent_cache[node as usize] = InRangeOption::some(predecessors[node as usize]);
        return predecessors[node as usize];
    }

    let pp = path_parent(predecessors[node as usize], predecessors, path_parent_cache, on_path);
    path_parent_cache[node as usize] = InRangeOption::some(pp);
    pp
}

pub fn reset_path_parent_cache(node: NodeId, predecessors: &[NodeId], path_parent_cache: &mut [InRangeOption<NodeId>]) {
    if path_parent_cache[node as usize].value().is_some() {
        path_parent_cache[node as usize] = InRangeOption::NONE;
        reset_path_parent_cache(predecessors[node as usize], predecessors, path_parent_cache);
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_filter_covered() {
        let mut ranges = vec![0..1];
        filter_covered(&mut ranges);
        assert_eq!(vec![0..1], ranges);

        let mut ranges = vec![1..3, 1..2, 2..3, 0..3];
        filter_covered(&mut ranges);
        assert_eq!(vec![1..2, 2..3], ranges);

        let mut ranges = vec![0..1, 0..2];
        filter_covered(&mut ranges);
        assert_eq!(vec![0..1], ranges);

        let mut ranges = vec![1..2, 0..2];
        filter_covered(&mut ranges);
        assert_eq!(vec![1..2], ranges);

        let mut ranges = vec![0..1, 1..2];
        filter_covered(&mut ranges);
        assert_eq!(vec![0..1, 1..2], ranges);
    }
}

use crate::algo::rphast::*;

pub struct MinimalNonShortestSubPathsSSERphast<'a> {
    rphast: RPHAST<BorrowedGraph<'a>, BorrowedGraph<'a>>,
    rphast_query: SSERPHASTQuery,
    path_ranks: Vec<InRangeOption<usize>>,
    smooth_graph: BorrowedGraph<'a>,
}

impl<'a> MinimalNonShortestSubPathsSSERphast<'a> {
    pub fn new(smooth_cch_pot: &'a CCHPotData, smooth_graph: BorrowedGraph<'a>) -> Self {
        let n = smooth_cch_pot.num_nodes();
        let rphast = RPHAST::new(
            // flipped for better cache efficiency when reading results
            smooth_cch_pot.customized().backward_graph(),
            smooth_cch_pot.customized().forward_graph(),
            smooth_cch_pot.customized().cch().node_order().clone(),
        );
        Self {
            rphast_query: SSERPHASTQuery::new(&rphast),
            rphast,
            path_ranks: vec![InRangeOption::NONE; n],
            smooth_graph,
        }
    }

    pub fn find_ubs_violating_subpaths_sse_rphast(&mut self, path: &[NodeId], epsilon: f64) -> Vec<Range<usize>> {
        let mut violating = Vec::new();
        let path_ranks = &mut self.path_ranks;

        for (rank, &node) in path.iter().enumerate() {
            if let Some(prev_rank) = path_ranks[node as usize].value() {
                // eprintln!("Found Cycle of len: {}", rank - prev_rank);
                violating.push(prev_rank..rank)
            }
            path_ranks[node as usize] = InRangeOption::some(rank);
        }

        for &node in path {
            self.path_ranks[node as usize] = InRangeOption::NONE;
        }
        if !violating.is_empty() {
            return violating;
        }

        let dists = path_dists(path, &self.smooth_graph);

        self.rphast.select(path);
        let distances = self.rphast_query.query(path, &self.rphast);

        for (u_idx, &u) in path[..path.len() - 1].iter().enumerate() {
            for (v_idx, &_) in path.iter().enumerate().skip(u_idx + 1) {
                let shortest_dist = distances.distances(u)[v_idx];
                let path_dist = dists[v_idx] - dists[u_idx];

                debug_assert!(path_dist >= shortest_dist, "{} {}", path_dist, shortest_dist);
                debug_assert!(shortest_dist > 0);
                if path_dist - shortest_dist > (shortest_dist as f64 * epsilon) as Weight {
                    violating.push(u_idx..v_idx);
                    break;
                }
            }
        }

        filter_covered(&mut violating);
        violating
    }
}

use crate::algo::dijkstra::*;

pub struct MinimalNonShortestSubPathsDijkstra<'a> {
    forward_data: DijkstraData<Weight>,
    backward_data: DijkstraData<Weight>,
    forward_graph: BorrowedGraph<'a>,
    backward_graph: OwnedGraph,
    path_parent_cache: Vec<InRangeOption<NodeId>>,
    path_ranks: Vec<InRangeOption<usize>>,
}

impl<'a> MinimalNonShortestSubPathsDijkstra<'a> {
    pub fn new(graph: BorrowedGraph<'a>) -> Self {
        let n = graph.num_nodes();
        Self {
            forward_data: DijkstraData::new(n),
            backward_data: DijkstraData::new(n),
            backward_graph: OwnedGraph::reversed(&graph),
            forward_graph: graph,
            path_parent_cache: vec![InRangeOption::NONE; n],
            path_ranks: vec![InRangeOption::NONE; n],
        }
    }

    pub fn find_ubs_violating_subpaths(&mut self, path: &[NodeId], epsilon: f64) -> Vec<Range<usize>> {
        for rank in &self.path_ranks {
            debug_assert!(rank.value().is_none());
        }
        let mut violating = Vec::new();
        let path_ranks = &mut self.path_ranks;

        for (rank, &node) in path.iter().enumerate() {
            if let Some(prev_rank) = path_ranks[node as usize].value() {
                // eprintln!("Found Cycle of len: {}", rank - prev_rank);
                violating.push(prev_rank..rank)
            }
            path_ranks[node as usize] = InRangeOption::some(rank);
        }

        if !violating.is_empty() {
            for &node in path {
                self.path_ranks[node as usize] = InRangeOption::NONE;
            }
            return violating;
        }

        let dists = self.path_dists(path);

        self.tree_find_minimal_nonshortest(path, &dists, |range, path_dist, shortest_dist| {
            debug_assert!(path_dist >= shortest_dist);
            debug_assert!(shortest_dist > 0);
            if path_dist - shortest_dist > (shortest_dist as f64 * epsilon) as Weight {
                violating.push(range);
                true
            } else {
                false
            }
        });

        for &node in path {
            self.path_ranks[node as usize] = InRangeOption::NONE;
        }

        filter_covered(&mut violating);
        violating
    }

    fn tree_find_minimal_nonshortest(&mut self, path: &[NodeId], dists: &[Weight], mut found_violating: impl FnMut(Range<usize>, Weight, Weight) -> bool) {
        let path_ranks = &self.path_ranks;

        for (rank, &node) in path.iter().enumerate() {
            debug_assert_eq!(rank, path_ranks[node as usize].value().unwrap() as usize);
        }

        let full_path = path;
        let mut path = path;

        let mut i = 0;
        while !path.is_empty() {
            i += 1;
            let &start = path.first().unwrap();
            let &end = path.last().unwrap();

            let mut ops = DefaultOps();
            let mut remaining_path_nodes_counter = path.len();
            for node in DijkstraRun::query(&self.backward_graph, &mut self.backward_data, &mut ops, DijkstraInit::from(end)) {
                if let Some(node_path_rank) = path_ranks[node as usize].value() {
                    if node_path_rank >= path_ranks[start as usize].value().unwrap() && node_path_rank <= path_ranks[end as usize].value().unwrap() {
                        remaining_path_nodes_counter -= 1;
                        if remaining_path_nodes_counter == 0 {
                            break;
                        }
                    }
                }
            }

            let mut target_earliest_deviation_rank = None;
            let mut target_earliest_suboptimal_rank = path_ranks[start as usize].value().unwrap();

            for &[node, next_on_path] in path.array_windows::<2>() {
                let path_dist = dists[path_ranks[end as usize].value().unwrap()] - dists[path_ranks[node as usize].value().unwrap() as usize];
                let shortest_dist = self.backward_data.distances[node as usize];

                if path_dist == shortest_dist {
                    break;
                }
                target_earliest_suboptimal_rank = path_ranks[node as usize].value().unwrap() as usize;

                if self.backward_data.predecessors[node as usize].0 != next_on_path && shortest_dist != path_dist {
                    path_parent(node, self.backward_data.predecessors(), &mut self.path_parent_cache, |node| {
                        path_ranks[node as usize].value().map_or(false, |path_rank| {
                            path_rank >= path_ranks[start as usize].value().unwrap() && path_rank <= path_ranks[end as usize].value().unwrap()
                        })
                    });

                    let path_parent = self.path_parent_cache[node as usize].value().unwrap();
                    let path_parent_rank = path_ranks[path_parent as usize].value().unwrap();
                    if let Some(target_earliest_deviation_rank_inner) = target_earliest_deviation_rank {
                        target_earliest_deviation_rank = Some(std::cmp::max(target_earliest_deviation_rank_inner, path_parent_rank));
                    } else {
                        target_earliest_deviation_rank = Some(path_parent_rank);
                    }
                }
            }

            for &node in path {
                reset_path_parent_cache(node, self.backward_data.predecessors(), &mut self.path_parent_cache);
            }
            for pp in &self.path_parent_cache {
                debug_assert_eq!(pp.value(), None);
            }

            if target_earliest_deviation_rank.is_none() {
                break;
            }

            // sp tree from source
            let mut remaining_path_nodes_counter = path.len();
            for node in DijkstraRun::query(&self.forward_graph, &mut self.forward_data, &mut ops, DijkstraInit::from(start)) {
                if let Some(node_path_rank) = path_ranks[node as usize].value() {
                    if node_path_rank >= path_ranks[start as usize].value().unwrap() && node_path_rank <= path_ranks[end as usize].value().unwrap() {
                        remaining_path_nodes_counter -= 1;
                        if remaining_path_nodes_counter == 0 {
                            break;
                        }
                    }
                }
            }

            let mut source_earliest_deviation_rank = None;
            let mut source_earliest_suboptimal_rank = path_ranks[end as usize].value().unwrap();

            for &[prev_on_path, node] in path.array_windows::<2>().rev() {
                let path_dist = dists[path_ranks[node as usize].value().unwrap() as usize] - dists[path_ranks[start as usize].value().unwrap()];
                let shortest_dist = self.forward_data.distances[node as usize];

                if path_dist == shortest_dist {
                    break;
                }
                source_earliest_suboptimal_rank = path_ranks[node as usize].value().unwrap() as usize;

                if self.forward_data.predecessors[node as usize].0 != prev_on_path && shortest_dist != path_dist {
                    path_parent(node, self.forward_data.predecessors(), &mut self.path_parent_cache, |node| {
                        path_ranks[node as usize].value().map_or(false, |path_rank| {
                            path_rank >= path_ranks[start as usize].value().unwrap() && path_rank <= path_ranks[end as usize].value().unwrap()
                        })
                    });

                    let path_parent = self.path_parent_cache[node as usize].value().unwrap();
                    let path_parent_rank = path_ranks[path_parent as usize].value().unwrap();
                    if let Some(source_earliest_deviation_rank_inner) = source_earliest_deviation_rank {
                        source_earliest_deviation_rank = Some(std::cmp::min(source_earliest_deviation_rank_inner, path_parent_rank));
                    } else {
                        source_earliest_deviation_rank = Some(path_parent_rank);
                    }
                }
            }

            for &node in path {
                reset_path_parent_cache(node, self.forward_data.predecessors(), &mut self.path_parent_cache);
            }
            for pp in &self.path_parent_cache {
                debug_assert_eq!(pp.value(), None);
            }

            let target_earliest_deviation_rank = target_earliest_deviation_rank.unwrap();
            let target_earliest_deviation_node = full_path[target_earliest_deviation_rank];
            let source_earliest_deviation_rank = source_earliest_deviation_rank.unwrap();
            let source_earliest_deviation_node = full_path[source_earliest_deviation_rank];

            let target_path_base_dist = dists[path_ranks[end as usize].value().unwrap()] - dists[target_earliest_deviation_rank];
            debug_assert_eq!(target_path_base_dist, self.backward_data.distances[target_earliest_deviation_node as usize]);
            let source_path_base_dist = dists[source_earliest_deviation_rank] - dists[path_ranks[start as usize].value().unwrap()];
            debug_assert_eq!(source_path_base_dist, self.forward_data.distances[source_earliest_deviation_node as usize]);

            for &node in &full_path[source_earliest_suboptimal_rank..=target_earliest_deviation_rank] {
                let path_dist = dists[path_ranks[node as usize].value().unwrap()] - dists[path_ranks[start as usize].value().unwrap()] - source_path_base_dist;
                let shortest_dist = self.forward_data.distances[node as usize] - source_path_base_dist;
                if found_violating(
                    source_earliest_deviation_rank..path_ranks[node as usize].value().unwrap(),
                    path_dist,
                    shortest_dist,
                ) {
                    break;
                }
            }
            for &node in full_path[source_earliest_deviation_rank..=target_earliest_suboptimal_rank].iter().rev() {
                let path_dist = dists[path_ranks[end as usize].value().unwrap()] - dists[path_ranks[node as usize].value().unwrap()] - target_path_base_dist;
                let shortest_dist = self.backward_data.distances[node as usize] - target_path_base_dist;
                if found_violating(
                    path_ranks[node as usize].value().unwrap()..target_earliest_deviation_rank,
                    path_dist,
                    shortest_dist,
                ) {
                    break;
                }
            }

            path = &full_path[source_earliest_deviation_rank + 1..target_earliest_deviation_rank];
        }
        report!("num_ubs_tree_iterations", i);
    }

    fn path_dists(&self, path: &[NodeId]) -> Vec<Weight> {
        path_dists(path, &self.forward_graph)
    }
}
