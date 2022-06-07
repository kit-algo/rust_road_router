use super::*;
use crate::algo::{a_star::Potential, ch_potentials::BorrowedCCHPot};
use std::collections::binary_heap::BinaryHeap;

const TARGETS_PER_CELL_THRESHOLD: usize = 8;

pub struct NearestNeighborServer<'a> {
    cch: &'a CCH,
    one_to_many: BorrowedCCHPot<'a>,
    targets: Vec<NodeId>,
    stack: Vec<(Weight, &'a SeparatorTree)>,
    closest_targets: BinaryHeap<(Weight, NodeId)>,
}

impl<'a> NearestNeighborServer<'a> {
    pub fn new(cch: &'a CCH, one_to_many: BorrowedCCHPot<'a>) -> Self {
        NearestNeighborServer {
            cch,
            one_to_many,
            targets: Vec::new(),
            stack: Vec::new(),
            closest_targets: BinaryHeap::new(),
        }
    }

    pub fn select_targets(&mut self, targets: &[NodeId]) -> NearestNeighborSelectedTargets<'_, 'a> {
        self.targets.clear();
        self.targets.extend(targets.iter().map(|&node| self.cch.node_order().rank(node)));
        self.targets.sort_unstable();

        NearestNeighborSelectedTargets(self)
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

        Vec::from_iter(self.closest_targets.drain())
    }
}

pub struct NearestNeighborSelectedTargets<'s, 'a: 's>(&'s mut NearestNeighborServer<'a>);

impl<'s, 'a: 's> NearestNeighborSelectedTargets<'s, 'a> {
    pub fn query(&mut self, source: NodeId, k: usize) -> Vec<(Weight, NodeId)> {
        self.0.query(source, k)
    }
}
