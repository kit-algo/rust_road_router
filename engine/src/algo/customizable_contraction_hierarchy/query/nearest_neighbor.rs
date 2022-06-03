use super::*;
use crate::algo::{a_star::Potential, ch_potentials::BorrowedCCHPot};

const TARGETS_PER_CELL_THRESHOLD: usize = 8;

pub struct NearestNeighborServer<'a> {
    cch: &'a CCH,
    one_to_many: BorrowedCCHPot<'a>,
    targets: Vec<NodeId>,
    num_targets_with_smaller_id: Vec<u32>,
    stack: Vec<(Weight, &'a SeparatorTree)>,
    closest_targets: std::collections::binary_heap::BinaryHeap<(Weight, NodeId)>,
}

impl NearestNeighborServer<'_> {
    pub fn query(&mut self, source: NodeId, targets: &[NodeId], k: usize) -> Vec<(Weight, NodeId)> {
        // TODO catch less targets than k

        let n = self.cch.num_nodes();
        // selection
        self.targets.clear();
        self.targets.extend(targets.iter().map(|&node| self.cch.node_order().rank(node)));
        self.targets.sort_unstable();
        let mut prev_target = 0;
        for (count, &target) in self.targets.iter().enumerate() {
            for node_target_counter in &mut self.num_targets_with_smaller_id[prev_target..=target as usize] {
                *node_target_counter = count as u32;
                prev_target = target as usize + 1;
            }
        }
        for node_target_counter in &mut self.num_targets_with_smaller_id[prev_target..=n] {
            *node_target_counter = targets.len() as u32;
        }

        // query
        self.one_to_many.init(source);
        self.stack.push((0, self.cch.separators()));

        while let Some((min_dist, cell)) = self.stack.pop() {
            if self.closest_targets.len() >= k && min_dist >= self.closest_targets.peek().unwrap().0 {
                continue;
            }
            let sep_nodes_range = cell.nodes.separator_nodes_range();
            let cell_nodes_range = sep_nodes_range.end - cell.num_nodes as NodeId..sep_nodes_range.end;

            let sep_targets_range = self.num_targets_with_smaller_id[sep_nodes_range.start as usize] as usize
                ..self.num_targets_with_smaller_id[sep_nodes_range.end as usize] as usize;
            let cell_targets_range = self.num_targets_with_smaller_id[cell_nodes_range.start as usize] as usize
                ..self.num_targets_with_smaller_id[cell_nodes_range.end as usize] as usize;

            if cell_targets_range.len() < TARGETS_PER_CELL_THRESHOLD {
                for &target in &self.targets[cell_targets_range] {
                    if let Some(dist) = self.one_to_many.potential(target) {
                        if self.closest_targets.len() < k {
                            self.closest_targets.push((dist, target));
                        } else if dist < self.closest_targets.peek().unwrap().0 {
                            self.closest_targets.pop();
                            self.closest_targets.push((dist, target));
                        }
                    }
                }
            } else {
                for &target in &self.targets[sep_targets_range] {
                    if let Some(dist) = self.one_to_many.potential(target) {
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
                        lower_bound_dist = std::cmp::min(lower_bound_dist, self.one_to_many.potential(boundary).unwrap_or(INFINITY));
                    }
                    self.stack.push((lower_bound_dist, child));
                }

                self.stack[current_stack_len..].sort_unstable_by_key(|(d, _)| std::cmp::Reverse(*d));
            }
        }

        Vec::from_iter(self.closest_targets.drain())
    }
}
