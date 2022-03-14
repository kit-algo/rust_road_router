use super::*;
use crate::datastr::graph::time_dependent::*;

pub fn customize_perfect_with_validity(
    cch: &CCH,
    upward_weights: &mut [Weight],
    downward_weights: &mut [Weight],
    fw_validity: &mut [Timestamp],
    bw_validity: &mut [Timestamp],
) {
    let n = cch.num_nodes();
    let customize_perfect = |nodes: Range<usize>, upward: *mut Weight, downward: *mut Weight, up_valid: *mut Timestamp, down_valid: *mut Timestamp| {
        PERFECT_WORKSPACE.with(|node_edge_ids| {
            let mut node_edge_ids = node_edge_ids.borrow_mut();

            // processing nodes in reverse order
            for current_node in nodes.rev() {
                let current_node = current_node as NodeId;
                // store mapping of head node to corresponding outgoing edge id
                for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                    node_edge_ids[node as usize] = InRangeOption::some(edge_id);
                }

                for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                    let shortcut_edge_ids = cch.neighbor_edge_indices(node);
                    for (target, shortcut_edge_id) in cch.neighbor_iter(node).zip(shortcut_edge_ids) {
                        if let Some(other_edge_id) = node_edge_ids[target as usize].value() {
                            // Here we have both an intermediate and an upper triangle
                            // depending on which edge we take as the base
                            // Relax all them.
                            unsafe {
                                let relax_weight = (*upward.add(edge_id as usize)) + (*upward.add(shortcut_edge_id as usize));
                                let relax_valid = std::cmp::max(*up_valid.add(edge_id as usize), *up_valid.add(shortcut_edge_id as usize));
                                let edge_weight = upward.add(other_edge_id as usize);
                                if relax_weight < *edge_weight {
                                    *edge_weight = relax_weight;
                                    *up_valid.add(other_edge_id as usize) = relax_valid;
                                }

                                let relax_weight = (*upward.add(other_edge_id as usize)) + (*downward.add(shortcut_edge_id as usize));
                                let relax_valid = std::cmp::max(*up_valid.add(other_edge_id as usize), *down_valid.add(shortcut_edge_id as usize));
                                let edge_weight = upward.add(edge_id as usize);
                                if relax_weight < *edge_weight {
                                    *edge_weight = relax_weight;
                                    *up_valid.add(edge_id as usize) = relax_valid;
                                }

                                let relax_weight = (*downward.add(edge_id as usize)) + (*downward.add(shortcut_edge_id as usize));
                                let relax_valid = std::cmp::max(*down_valid.add(edge_id as usize), *down_valid.add(shortcut_edge_id as usize));
                                let edge_weight = downward.add(other_edge_id as usize);
                                if relax_weight < *edge_weight {
                                    *edge_weight = relax_weight;
                                    *down_valid.add(other_edge_id as usize) = relax_valid;
                                }

                                let relax_weight = (*downward.add(other_edge_id as usize)) + (*upward.add(shortcut_edge_id as usize));
                                let relax_valid = std::cmp::max(*down_valid.add(other_edge_id as usize), *up_valid.add(shortcut_edge_id as usize));
                                let edge_weight = downward.add(edge_id as usize);
                                if relax_weight < *edge_weight {
                                    *edge_weight = relax_weight;
                                    *down_valid.add(edge_id as usize) = relax_valid;
                                }
                            }
                        }
                    }
                }

                // reset the mapping
                for node in cch.neighbor_iter(current_node) {
                    node_edge_ids[node as usize] = InRangeOption::NONE;
                }
            }
        });
    };

    let static_perfect_customization = SeperatorBasedPerfectParallelCustomization::new_with_aux(cch, customize_perfect, customize_perfect);

    report_time_with_key("CCH Perfect Customization", "perfect_customization_running_time_ms", || {
        static_perfect_customization.customize_with_aux(upward_weights, downward_weights, fw_validity, bw_validity, |cb| {
            PERFECT_WORKSPACE.set(&RefCell::new(vec![InRangeOption::NONE; n as usize]), cb);
        });
    });
}
