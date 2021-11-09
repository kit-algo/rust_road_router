use super::*;

use crate::{
    algo::{a_star::*, ch_potentials::*, customizable_contraction_hierarchy::*},
    datastr::rank_select_map::BitVec,
    report::*,
    util::{in_range_option::InRangeOption, with_index},
};
use std::{collections::BTreeMap, ops::Range};

pub struct MinimalNonShortestSubPaths<'a> {
    target_pot: CCHPotentialWithPathUnpacking<'a>,
    source_pot: CCHPotentialWithPathUnpacking<'a>,
    path_parent_cache: Vec<InRangeOption<NodeId>>,
    path_ranks: Vec<InRangeOption<usize>>,
}

impl<'a> MinimalNonShortestSubPaths<'a> {
    pub fn new(smooth_cch_pot: &'a CCHPotData) -> Self {
        let n = smooth_cch_pot.num_nodes();
        Self {
            target_pot: smooth_cch_pot.forward_path_potential(),
            source_pot: smooth_cch_pot.backward_path_potential(),
            path_parent_cache: vec![InRangeOption::new(None); n],
            path_ranks: vec![InRangeOption::new(None); n],
        }
    }

    pub fn find_ubs_violating_subpaths<G>(&mut self, path: &[NodeId], graph: &G, epsilon: f64) -> Vec<Range<usize>>
    where
        G: EdgeIdGraph + EdgeRandomAccessGraph<Link>,
    {
        for rank in &self.path_ranks {
            debug_assert!(rank.value().is_none());
        }
        let mut violating = Vec::new();
        let path_ranks = &mut self.path_ranks;

        for (rank, &node) in path.iter().enumerate() {
            if let Some(prev_rank) = path_ranks[node as usize].value() {
                eprintln!("Found Cycle of len: {}", rank - prev_rank);
                violating.push(prev_rank..rank)
            }
            path_ranks[node as usize] = InRangeOption::new(Some(rank));
        }

        if !violating.is_empty() {
            for &node in path {
                self.path_ranks[node as usize] = InRangeOption::new(None);
            }
            return violating;
        }

        let dists = path_dists(path, graph);

        self.tree_find_minimal_nonshortest(path, &dists, |range, path_dist, shortest_dist| {
            debug_assert!(path_dist >= shortest_dist);
            if path_dist - shortest_dist > (shortest_dist as f64 * epsilon) as Weight {
                violating.push(range);
                true
            } else {
                false
            }
        });

        for &node in path {
            self.path_ranks[node as usize] = InRangeOption::new(None);
        }

        filter_covered(&mut violating);
        violating
    }

    pub fn local_optimality<G>(&mut self, path: &[NodeId], graph: &G) -> f64
    where
        G: EdgeIdGraph + EdgeRandomAccessGraph<Link>,
    {
        for rank in &self.path_ranks {
            debug_assert!(rank.value().is_none());
        }
        let path_ranks = &mut self.path_ranks;

        for (rank, &node) in path.iter().enumerate() {
            if let Some(prev_rank) = path_ranks[node as usize].value() {
                panic!("Found Cycle of len: {}", rank - prev_rank);
            }
            path_ranks[node as usize] = InRangeOption::new(Some(rank));
        }

        let dists = path_dists(path, graph);
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
            self.path_ranks[node as usize] = InRangeOption::new(None);
        }

        local_optimality as f64 / base_dist as f64
    }

    pub fn suboptimal_stats<G>(&mut self, path: &[NodeId], graph: &G) -> (f64, f64)
    where
        G: EdgeIdGraph + EdgeRandomAccessGraph<Link>,
    {
        for rank in &self.path_ranks {
            debug_assert!(rank.value().is_none());
        }
        let path_ranks = &mut self.path_ranks;

        for (rank, &node) in path.iter().enumerate() {
            if let Some(prev_rank) = path_ranks[node as usize].value() {
                panic!("Found Cycle of len: {}", rank - prev_rank);
            }
            path_ranks[node as usize] = InRangeOption::new(Some(rank));
        }

        let dists = path_dists(path, graph);
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
            self.path_ranks[node as usize] = InRangeOption::new(None);
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

                let next_on_path = self.target_pot.cch().node_order().rank(next_on_path);
                let node = self.target_pot.cch().node_order().rank(node);
                if self.target_pot.target_shortest_path_tree()[node as usize] != next_on_path && shortest_dist != path_dist {
                    let cch_order = self.target_pot.cch().node_order();
                    path_parent(node, self.target_pot.target_shortest_path_tree(), &mut self.path_parent_cache, |cch_rank| {
                        path_ranks[cch_order.node(cch_rank) as usize].value().map_or(false, |path_rank| {
                            path_rank >= path_ranks[start as usize].value().unwrap() && path_rank <= path_ranks[end as usize].value().unwrap()
                        })
                    });

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

                let prev_on_path = self.target_pot.cch().node_order().rank(prev_on_path);
                let node = self.target_pot.cch().node_order().rank(node);
                if self.source_pot.target_shortest_path_tree()[node as usize] != prev_on_path && shortest_dist != path_dist {
                    let cch_order = self.target_pot.cch().node_order();
                    path_parent(node, self.source_pot.target_shortest_path_tree(), &mut self.path_parent_cache, |cch_rank| {
                        path_ranks[cch_order.node(cch_rank) as usize].value().map_or(false, |path_rank| {
                            path_rank >= path_ranks[start as usize].value().unwrap() && path_rank <= path_ranks[end as usize].value().unwrap()
                        })
                    });

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

fn path_dists<G>(path: &[NodeId], graph: &G) -> Vec<Weight>
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
        debug_assert_ne!(min_edge_weight, INFINITY);
        dists.push(dists.last().unwrap() + min_edge_weight);
    }
    dists
}

pub fn path_parent(node: NodeId, predecessors: &[NodeId], path_parent_cache: &mut [InRangeOption<NodeId>], mut on_path: impl FnMut(NodeId) -> bool) -> NodeId {
    if let Some(path_parent) = path_parent_cache[node as usize].value() {
        return path_parent;
    }

    if on_path(predecessors[node as usize]) {
        path_parent_cache[node as usize] = InRangeOption::new(Some(predecessors[node as usize]));
        return predecessors[node as usize];
    }

    let pp = path_parent(predecessors[node as usize], predecessors, path_parent_cache, on_path);
    path_parent_cache[node as usize] = InRangeOption::new(Some(pp));
    pp
}

pub fn reset_path_parent_cache(node: NodeId, predecessors: &[NodeId], path_parent_cache: &mut [InRangeOption<NodeId>]) {
    if path_parent_cache[node as usize].value().is_some() {
        path_parent_cache[node as usize] = InRangeOption::new(None);
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
