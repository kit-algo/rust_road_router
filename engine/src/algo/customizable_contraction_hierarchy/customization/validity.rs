use super::*;
use crate::datastr::graph::time_dependent::*;

scoped_thread_local!(static VALIDITY_UPWARD_WORKSPACE: RefCell<Vec<(Weight, Weight)>>);
scoped_thread_local!(static VALIDITY_DOWNWARD_WORKSPACE: RefCell<Vec<(Weight, Weight)>>);

pub struct CustomizedWithValidity {
    pub upward_weights: Vec<Weight>,
    pub downward_weights: Vec<Weight>,
    pub upward_valid: Vec<Timestamp>,
    pub downward_valid: Vec<Timestamp>,
}

pub fn customize_upper_bounds_with_limited_validity(cch: &CCH, metric: &PessimisticLiveTDGraph) -> CustomizedWithValidity {
    let m = cch.num_arcs();
    // buffers for the customized weights
    let mut upward_weights = vec![INFINITY; m];
    let mut downward_weights = vec![INFINITY; m];
    let mut upward_valid = vec![0; m];
    let mut downward_valid = vec![0; m];

    // respecting phase
    // copy metric weights to their respective edges in the CCH
    prepare_weights(cch, &mut upward_weights, &mut downward_weights, &mut upward_valid, &mut downward_valid, metric);

    customize_basic_with_validity(cch, &mut upward_weights, &mut downward_weights, &mut upward_valid, &mut downward_valid);
    customize_perfect_with_validity(cch, &mut upward_weights, &mut downward_weights, &mut upward_valid, &mut downward_valid);

    CustomizedWithValidity {
        upward_weights,
        downward_weights,
        upward_valid,
        downward_valid,
    }
}

fn prepare_weights(
    cch: &CCH,
    upward_weights: &mut [Weight],
    downward_weights: &mut [Weight],
    fw_validity: &mut [Timestamp],
    bw_validity: &mut [Timestamp],
    metric: &PessimisticLiveTDGraph,
) {
    report_time_with_key("CCH apply weights", "respecting_running_time_ms", || {
        upward_weights
            .par_iter_mut()
            .zip(fw_validity.par_iter_mut())
            .zip(cch.forward_cch_edge_to_orig_arc().par_iter())
            .for_each(|((weight, valid), orig_edges)| {
                for &EdgeIdT(edge) in orig_edges {
                    let upper = metric.upper_bound(edge);
                    if upper < *weight {
                        *weight = upper;
                        *valid = metric.live_ended_at(edge);
                    }
                }
            });
        downward_weights
            .par_iter_mut()
            .zip(bw_validity.par_iter_mut())
            .zip(cch.backward_cch_edge_to_orig_arc().par_iter())
            .for_each(|((weight, valid), orig_edges)| {
                for &EdgeIdT(edge) in orig_edges {
                    let upper = metric.upper_bound(edge);
                    if upper < *weight {
                        *weight = upper;
                        *valid = metric.live_ended_at(edge);
                    }
                }
            });
    });
}

fn customize_basic_with_validity(
    cch: &CCH,
    upward_weights: &mut [Weight],
    downward_weights: &mut [Weight],
    fw_validity: &mut [Timestamp],
    bw_validity: &mut [Timestamp],
) {
    let n = cch.num_nodes() as NodeId;

    let customize = |nodes: Range<usize>,
                     offset: usize,
                     _offset_down: usize,
                     upward_weights: &mut [Weight],
                     downward_weights: &mut [Weight],
                     fw_validity: &mut [Timestamp],
                     bw_validity: &mut [Timestamp]| {
        VALIDITY_UPWARD_WORKSPACE.with(|node_outgoing_weights| {
            let mut node_outgoing_weights = node_outgoing_weights.borrow_mut();

            VALIDITY_DOWNWARD_WORKSPACE.with(|node_incoming_weights| {
                let mut node_incoming_weights = node_incoming_weights.borrow_mut();

                // for all nodes that we should customize
                for current_node in nodes {
                    let current_node = current_node as NodeId;
                    let mut edges = cch.neighbor_edge_indices_usize(current_node);
                    edges.start -= offset;
                    edges.end -= offset;
                    // for all upward neighbors: store weight of edge in workspace at index `head`
                    for ((((node, &downward_weight), &upward_weight), &down_valid), &up_valid) in cch
                        .neighbor_iter(current_node)
                        .zip(&downward_weights[edges.clone()])
                        .zip(&upward_weights[edges.clone()])
                        .zip(&bw_validity[edges.clone()])
                        .zip(&fw_validity[edges.clone()])
                    {
                        node_incoming_weights[node as usize] = (downward_weight, down_valid);
                        node_outgoing_weights[node as usize] = (upward_weight, up_valid);
                    }

                    // for all downward edges of the current node
                    for (NodeIdT(low_node), Reversed(EdgeIdT(first_edge_id))) in cch.inverted.link_iter(current_node) {
                        let first_down_weight = downward_weights[first_edge_id as usize - offset];
                        let first_down_validity = bw_validity[first_edge_id as usize - offset];
                        let first_up_weight = upward_weights[first_edge_id as usize - offset];
                        let first_up_validity = fw_validity[first_edge_id as usize - offset];
                        let mut low_up_edges = cch.neighbor_edge_indices_usize(low_node);
                        low_up_edges.start -= offset;
                        low_up_edges.end -= offset;
                        // for all upward edges of the lower node
                        for ((((node, upward_weight), downward_weight), upward_validity), downward_validity) in cch
                            .neighbor_iter(low_node)
                            .rev() // reversed order, (from high to low), so we can terminate earlier
                            .zip(upward_weights[low_up_edges.clone()].iter().rev())
                            .zip(downward_weights[low_up_edges.clone()].iter().rev())
                            .zip(fw_validity[low_up_edges.clone()].iter().rev())
                            .zip(bw_validity[low_up_edges].iter().rev())
                        {
                            // since we go from high to low, once the ids are smaller than current node,
                            // we won't encounter any more lower triangles we need
                            if node <= current_node {
                                break;
                            }

                            // potential lower triangle of some outgoing edge of the current node

                            // we relax the weight value in the head -> weight mapping.
                            // Since we only copy the values back for outgoing edges of the current node,
                            // we don't care about all other values.
                            // So here, we just unconditionally relax the weight values, no matter if there is a
                            // corresponding edge or not. Checking if it is an edge and branching would actually be slower.

                            // the unsafes are safe, because the `head` nodes in the cch are all < n
                            // we might want to add an explicit validation for this
                            let relax = unsafe { node_outgoing_weights.get_unchecked_mut(node as usize) };
                            let triang_weight = upward_weight + first_down_weight;
                            let triang_validity = std::cmp::max(*upward_validity, first_down_validity);
                            if triang_weight < relax.0 {
                                *relax = (triang_weight, triang_validity)
                            }
                            let relax = unsafe { node_incoming_weights.get_unchecked_mut(node as usize) };
                            let triang_weight = downward_weight + first_up_weight;
                            let triang_validity = std::cmp::max(*downward_validity, first_up_validity);
                            if triang_weight < relax.0 {
                                *relax = (triang_weight, triang_validity)
                            }
                        }
                    }

                    // for all upward neighbors: copy weights from the mapping back to the edge
                    for ((((node, downward_weight), upward_weight), down_valid), up_valid) in cch
                        .neighbor_iter(current_node)
                        .zip(&mut downward_weights[edges.clone()])
                        .zip(&mut upward_weights[edges.clone()])
                        .zip(&mut bw_validity[edges.clone()])
                        .zip(&mut fw_validity[edges])
                    {
                        *downward_weight = node_incoming_weights[node as usize].0;
                        *upward_weight = node_outgoing_weights[node as usize].0;
                        *down_valid = node_incoming_weights[node as usize].1;
                        *up_valid = node_outgoing_weights[node as usize].1;
                    }
                }
            });
        });
    };

    // setup customization for parallization
    let customization = SeperatorBasedParallelCustomization::new_with_aux(cch, customize, customize);

    // execute customization
    report_time_with_key("CCH Customization", "basic_customization_running_time_ms", || {
        customization.customize_with_aux(upward_weights, downward_weights, fw_validity, bw_validity, |cb| {
            // create workspace vectors for the scope of the customization
            VALIDITY_UPWARD_WORKSPACE.set(&RefCell::new(vec![(INFINITY, 0); n as usize]), || {
                VALIDITY_DOWNWARD_WORKSPACE.set(&RefCell::new(vec![(INFINITY, 0); n as usize]), cb);
            });
            // everything will be dropped here
        });
    });
}

fn customize_perfect_with_validity(
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
