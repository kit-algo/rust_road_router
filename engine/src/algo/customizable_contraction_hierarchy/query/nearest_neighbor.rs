use super::*;
use crate::{
    algo::{a_star::Potential, ch_potentials::*},
    datastr::index_heap::*,
};
use std::collections::binary_heap::BinaryHeap;

const TARGETS_PER_CELL_THRESHOLD: usize = 8;

pub struct SeparatorBasedNearestNeighbor<'a> {
    cch: &'a CCH,
    one_to_many: BorrowedCCHPot<'a>,
    targets: Vec<NodeId>,
    stack: Vec<(Weight, &'a SeparatorTree)>,
    closest_targets: BinaryHeap<(Weight, NodeId)>,
}

impl<'a> SeparatorBasedNearestNeighbor<'a> {
    pub fn new(cch: &'a CCH, one_to_many: BorrowedCCHPot<'a>) -> Self {
        SeparatorBasedNearestNeighbor {
            cch,
            one_to_many,
            targets: Vec::new(),
            stack: Vec::new(),
            closest_targets: BinaryHeap::new(),
        }
    }

    pub fn select_targets(&mut self, targets: &[NodeId]) -> SeparatorBasedNearestNeighborSelectedTargets<'_, 'a> {
        self.targets.clear();
        self.targets.extend(targets.iter().map(|&node| self.cch.node_order().rank(node)));
        self.targets.sort_unstable();

        SeparatorBasedNearestNeighborSelectedTargets(self)
    }

    fn query(&mut self, source: NodeId, k: usize) -> Vec<(Weight, NodeId)> {
        self.one_to_many.init(source);

        if k < self.targets.len() {
            self.stack.push((0, self.cch.separators()));
        } else {
            for &target in &self.targets {
                if let Some(dist) = self.one_to_many.potential_with_cch_rank(target) {
                    self.closest_targets.push((dist, target));
                }
            }
        }

        while let Some((min_dist, cell)) = self.stack.pop() {
            if self.closest_targets.len() >= k && min_dist >= self.closest_targets.peek().unwrap().0 {
                continue;
            }
            let mut sep_nodes_range = cell.nodes.separator_nodes_range();
            let cell_nodes_range = if sep_nodes_range.is_empty() {
                sep_nodes_range = cell.num_nodes as NodeId..cell.num_nodes as NodeId;
                0..cell.num_nodes as NodeId
            } else {
                sep_nodes_range.end - cell.num_nodes as NodeId..sep_nodes_range.end
            };

            let cell_targets_start_idx = match self.targets.binary_search(&cell_nodes_range.start) {
                Ok(idx) => idx,
                Err(idx) => idx,
            };
            let cell_targets_end_idx = match self.targets.binary_search(&cell_nodes_range.end) {
                Ok(idx) => idx,
                Err(idx) => idx,
            };
            let cell_targets_range = cell_targets_start_idx..cell_targets_end_idx;

            if cell_targets_range.len() < TARGETS_PER_CELL_THRESHOLD {
                for &target in &self.targets[cell_targets_range] {
                    if let Some(dist) = self.one_to_many.potential_with_cch_rank(target) {
                        if self.closest_targets.len() < k {
                            self.closest_targets.push((dist, target));
                        } else if dist < self.closest_targets.peek().unwrap().0 {
                            self.closest_targets.pop();
                            self.closest_targets.push((dist, target));
                        }
                    }
                }
            } else {
                for &target in self.targets[cell_targets_range].iter().rev() {
                    if target < sep_nodes_range.start {
                        break;
                    }
                    if let Some(dist) = self.one_to_many.potential_with_cch_rank(target) {
                        if self.closest_targets.len() < k {
                            self.closest_targets.push((dist, target));
                        } else if dist < self.closest_targets.peek().unwrap().0 {
                            self.closest_targets.pop();
                            self.closest_targets.push((dist, target));
                        }
                    }
                }

                let current_stack_len = self.stack.len();

                for child in &cell.children {
                    let top_cell_node = child.nodes.highest_ranked_node().unwrap();
                    let top_node_out_edges = self.cch.neighbor_edge_indices_usize(top_cell_node);
                    let mut lower_bound_dist = INFINITY;
                    for &boundary in &self.cch.head()[top_node_out_edges] {
                        lower_bound_dist = std::cmp::min(lower_bound_dist, self.one_to_many.potential_with_cch_rank(boundary).unwrap_or(INFINITY));
                    }
                    self.stack.push((lower_bound_dist, child));
                }

                self.stack[current_stack_len..].sort_unstable_by_key(|(d, _)| std::cmp::Reverse(*d));
            }
        }

        Vec::from_iter(self.closest_targets.drain().map(|(d, n)| (d, self.cch.node_order().node(n))))
    }
}

pub struct SeparatorBasedNearestNeighborSelectedTargets<'s, 'a: 's>(&'s mut SeparatorBasedNearestNeighbor<'a>);

impl<'s, 'a: 's> SeparatorBasedNearestNeighborSelectedTargets<'s, 'a> {
    pub fn query(&mut self, source: NodeId, k: usize) -> Vec<(Weight, NodeId)> {
        self.0.query(source, k)
    }
}

pub struct LazyRphastNearestNeighbor<'a> {
    one_to_many: BorrowedCCHPot<'a>,
    closest_targets: BinaryHeap<(Weight, NodeId)>,
}

impl<'a> LazyRphastNearestNeighbor<'a> {
    pub fn new(one_to_many: BorrowedCCHPot<'a>) -> Self {
        LazyRphastNearestNeighbor {
            one_to_many,
            closest_targets: BinaryHeap::new(),
        }
    }

    pub fn query(&mut self, source: NodeId, targets: &[NodeId], k: usize) -> Vec<(Weight, NodeId)> {
        self.one_to_many.init(source);

        for &target in targets {
            if let Some(dist) = self.one_to_many.potential(target) {
                if self.closest_targets.len() < k {
                    self.closest_targets.push((dist, target));
                } else if dist < self.closest_targets.peek().unwrap().0 {
                    self.closest_targets.pop();
                    self.closest_targets.push((dist, target));
                }
            }
        }

        Vec::from_iter(self.closest_targets.drain())
    }
}

use std::cmp::Reverse;

pub struct BCCHNearestNeighbor<'a> {
    customized: &'a CustomizedPerfect<'a, CCH>,
    selection_data: BucketCHSelectionData,
    closest_targets: IndexdMinHeap<Reverse<(Weight, NodeId)>>,
    fw_distances: Vec<Weight>,
    fw_parents: Vec<(NodeId, EdgeId)>,
}

impl Indexing for std::cmp::Reverse<(Weight, NodeId)> {
    fn as_index(&self) -> usize {
        self.0 .1 as usize
    }
}

impl<'a> BCCHNearestNeighbor<'a> {
    pub fn new(customized: &'a CustomizedPerfect<'a, CCH>) -> Self {
        let n = customized.cch().num_nodes();
        let m = customized.cch().num_arcs();
        Self {
            customized,
            selection_data: BucketCHSelectionData::new(n),
            closest_targets: IndexdMinHeap::new(n),
            fw_distances: vec![INFINITY; n],
            fw_parents: vec![(n as NodeId, m as EdgeId); n],
        }
    }

    pub fn select_targets<'s>(&'s mut self, targets: &'s [NodeId]) -> BCCHNearestNeighborSelectedTargets<'_, 'a> {
        let bw_graph = self.customized.backward_graph();
        let mut selection = BucketCHSelectionRun::query(
            &bw_graph,
            &mut self.selection_data,
            targets.iter().map(|&node| self.customized.cch().node_order().rank(node)),
        );
        while let Some(NodeIdT(node)) = selection.next() {
            selection.buckets_mut(node).sort_unstable_by_key(|&(_node, dist)| dist);
        }

        BCCHNearestNeighborSelectedTargets(self, targets)
    }

    fn query(&mut self, source: NodeId, targets: &[NodeId], k: usize) -> Vec<(Weight, NodeId)> {
        let source = self.customized.cch().node_order().rank(source);
        let fw_graph = self.customized.forward_graph();

        let mut fw_walk = EliminationTreeWalk::query_with_resetted(
            &fw_graph,
            self.customized.cch().elimination_tree(),
            &mut self.fw_distances,
            &mut self.fw_parents,
            source,
        );

        while let Some(node) = fw_walk.peek() {
            let fw_dist = fw_walk.tentative_distance(node);
            if self.closest_targets.len() < k || fw_dist < self.closest_targets.peek().map(|&Reverse((d, _))| d).unwrap_or(INFINITY) {
                for &(target, bw_dist) in self.selection_data.buckets(node).iter().take(k) {
                    let dist = fw_dist + bw_dist;
                    if let Some(&Reverse((old_dist, _))) = self.closest_targets.get(target as usize) {
                        if dist < old_dist {
                            self.closest_targets.increase_key(Reverse((dist, target)));
                        }
                    } else {
                        if self.closest_targets.len() < k {
                            self.closest_targets.push(Reverse((dist, target)));
                        } else if dist < self.closest_targets.peek().unwrap().0 .0 {
                            self.closest_targets.pop();
                            self.closest_targets.push(Reverse((dist, target)));
                        }
                    }
                }

                fw_walk.next();
            } else {
                fw_walk.skip_next();
            }

            fw_walk.reset_distance(node);
        }

        Vec::from_iter(
            self.closest_targets
                .drain()
                .map(|Reverse((dist, target_idx))| (dist, targets[target_idx as usize])),
        )
    }
}

pub struct BCCHNearestNeighborSelectedTargets<'s, 'a: 's>(&'s mut BCCHNearestNeighbor<'a>, &'s [NodeId]);

impl<'s, 'a: 's> BCCHNearestNeighborSelectedTargets<'s, 'a> {
    pub fn query(&mut self, source: NodeId, k: usize) -> Vec<(Weight, NodeId)> {
        self.0.query(source, self.1, k)
    }
}
