use super::*;

pub fn customize_directed_basic(cch: &DirectedCCH, mut upward_weights: Vec<Weight>, mut downward_weights: Vec<Weight>) -> CustomizedBasic<DirectedCCH> {
    let n = cch.num_nodes() as NodeId;
    let m_up = cch.forward_head().len() as EdgeId;
    let m_down = cch.backward_head().len() as EdgeId;
    report!("num_cch_fw_edges", m_up);
    report!("num_cch_bw_edges", m_down);

    // Main customization routine.
    // Executed by one thread for a consecutive range of nodes.
    // For these nodes all upward arcs will be customized
    // If `nodes` contains all nodes, than it means we perform the regular sequential customization.
    //
    // However, it is also possible to execute this routine just for subset of the nodes.
    // We use this for the separator based parallelization.
    // The graph will be split into smaller components using the separator decomposition.
    // These can be customized independently.
    // For this routine to work, the nodes of a component have to be a consecutive id range.
    // The weight slices have to contain the outgoing edges of the nodes and the edges that make up the lower triangles
    // of the edges we customize.
    // So i.e. if nodes are the nodes of a separator, than the weights have also to contain the edges of the components the separator split.
    // The offset parameter gives the edge id offset of the weight slices.
    let customize = |nodes: Range<usize>,
                     upward_offset: usize,
                     downward_offset: usize,
                     upward_weights: &mut [Weight],
                     downward_weights: &mut [Weight],
                     upward_unpack: &mut [(InRangeOption<EdgeId>, InRangeOption<EdgeId>)],
                     downward_unpack: &mut [(InRangeOption<EdgeId>, InRangeOption<EdgeId>)]| {
        UPWARD_WORKSPACE.with(|node_outgoing_weights| {
            let mut node_outgoing_weights = node_outgoing_weights.borrow_mut();

            DOWNWARD_WORKSPACE.with(|node_incoming_weights| {
                let mut node_incoming_weights = node_incoming_weights.borrow_mut();

                // for all nodes that we should customize
                for current_node in nodes {
                    let current_node = current_node as NodeId;
                    let mut forward_edges = cch.forward().range(current_node as usize);
                    forward_edges.start -= upward_offset;
                    forward_edges.end -= upward_offset;
                    let mut backward_edges = cch.backward().range(current_node as usize);
                    backward_edges.start -= downward_offset;
                    backward_edges.end -= downward_offset;
                    // for all upward neighbors: store weight of edge in workspace at index `head`
                    for (&node, &up_weight) in cch.forward()[current_node as usize].iter().zip(&upward_weights[forward_edges.clone()]) {
                        node_outgoing_weights[node as usize] = (up_weight, InRangeOption::NONE, InRangeOption::NONE);
                    }
                    for (&node, &down_weight) in cch.backward()[current_node as usize].iter().zip(&downward_weights[backward_edges.clone()]) {
                        node_incoming_weights[node as usize] = (down_weight, InRangeOption::NONE, InRangeOption::NONE);
                    }

                    // for all downward edges of the current node
                    for (NodeIdT(low_node), Reversed(EdgeIdT(first_edge_id))) in cch.backward_inverted.link_iter(current_node) {
                        let first_down_weight = downward_weights[first_edge_id as usize - downward_offset];
                        let mut low_up_edges = cch.forward().range(low_node as usize);
                        let low_up_edges_orig = low_up_edges.clone();
                        low_up_edges.start -= upward_offset;
                        low_up_edges.end -= upward_offset;
                        // for all upward edges of the lower node
                        for ((&node, upward_weight), second_edge_id) in cch.forward()[low_node as usize]
                            .iter()
                            .rev() // reversed order, (from high to low), so we can terminate earlier
                            .zip(upward_weights[low_up_edges].iter().rev())
                            .zip(low_up_edges_orig.rev())
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
                            if triang_weight < relax.0 {
                                *relax = (triang_weight, InRangeOption::some(first_edge_id), InRangeOption::some(second_edge_id as EdgeId))
                            }
                        }
                    }

                    // for all downward edges of the current node
                    for (NodeIdT(low_node), Reversed(EdgeIdT(first_edge_id))) in cch.forward_inverted.link_iter(current_node) {
                        let first_up_weight = upward_weights[first_edge_id as usize - upward_offset];
                        let mut low_up_edges = cch.backward().range(low_node as usize);
                        let low_up_edges_orig = low_up_edges.clone();
                        low_up_edges.start -= downward_offset;
                        low_up_edges.end -= downward_offset;
                        // for all upward edges of the lower node
                        for ((&node, downward_weight), second_edge_id) in cch.backward()[low_node as usize]
                            .iter()
                            .rev() // reversed order, (from high to low), so we can terminate earlier
                            .zip(downward_weights[low_up_edges].iter().rev())
                            .zip(low_up_edges_orig.rev())
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
                            let relax = unsafe { node_incoming_weights.get_unchecked_mut(node as usize) };
                            let triang_weight = downward_weight + first_up_weight;
                            if triang_weight < relax.0 {
                                *relax = (triang_weight, InRangeOption::some(second_edge_id as EdgeId), InRangeOption::some(first_edge_id))
                            }
                        }
                    }

                    // for all upward neighbors: copy weights from the mapping back to the edge
                    for ((&node, up_weight), upward_unpack) in cch.forward()[current_node as usize]
                        .iter()
                        .zip(&mut upward_weights[forward_edges.clone()])
                        .zip(&mut upward_unpack[forward_edges])
                    {
                        *up_weight = node_outgoing_weights[node as usize].0;
                        upward_unpack.0 = node_outgoing_weights[node as usize].1;
                        upward_unpack.1 = node_outgoing_weights[node as usize].2;
                    }
                    for ((&node, down_weight), downward_unpack) in cch.backward()[current_node as usize]
                        .iter()
                        .zip(&mut downward_weights[backward_edges.clone()])
                        .zip(&mut downward_unpack[backward_edges])
                    {
                        *down_weight = node_incoming_weights[node as usize].0;
                        downward_unpack.0 = node_incoming_weights[node as usize].1;
                        downward_unpack.1 = node_incoming_weights[node as usize].2;
                    }
                }
            });
        });
    };

    // setup customization for parallization
    let customization = SeperatorBasedParallelCustomization::new_with_aux(cch, customize, customize);

    let mut upward_unpack = vec![(InRangeOption::NONE, InRangeOption::NONE); m_up as usize];
    let mut downward_unpack = vec![(InRangeOption::NONE, InRangeOption::NONE); m_down as usize];

    // execute customization
    report_time_with_key("CCH Customization", "basic_customization_running_time_ms", || {
        customization.customize_with_aux(&mut upward_weights, &mut downward_weights, &mut upward_unpack, &mut downward_unpack, |cb| {
            // create workspace vectors for the scope of the customization
            UPWARD_WORKSPACE.set(&RefCell::new(vec![(INFINITY, InRangeOption::NONE, InRangeOption::NONE); n as usize]), || {
                DOWNWARD_WORKSPACE.set(&RefCell::new(vec![(INFINITY, InRangeOption::NONE, InRangeOption::NONE); n as usize]), cb);
            });
            // everything will be dropped here
        });
    });

    CustomizedBasic::new(cch, upward_weights, downward_weights, upward_unpack, downward_unpack)
}

pub fn customize_directed_perfect(mut customized: CustomizedBasic<DirectedCCH>) -> CustomizedPerfect<DirectedCCH> {
    let (upward_modified, downward_modified) = customize_directed_perfect_without_rebuild(&mut customized);
    rebuild_customized_perfect(customized, &upward_modified, &downward_modified)
}

scoped_thread_local!(static PERFECT_WORKSPACE: RefCell<Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)>>);

pub fn customize_directed_perfect_without_rebuild(customized: &mut CustomizedBasic<DirectedCCH>) -> (Vec<bool>, Vec<bool>) {
    let cch = customized.cch;
    let n = cch.num_nodes();
    let num_triangles = std::sync::atomic::AtomicU64::new(0);
    // Routine for perfect precustomization.
    // The interface is similar to the one for the basic customization, but we need access to nonconsecutive ranges of edges,
    // so we can't use slices. Thus, we just take a mutable pointer to the shortcut vecs.
    // The logic of the perfect customization based on separators guarantees, that we will never concurrently modify
    // the same shortcuts, but so far I haven't found a way to express that in safe rust.
    let customize_perfect = |nodes: Range<usize>, upward: *mut Weight, downward: *mut Weight, up_mod: *mut bool, down_mod: *mut bool| {
        let mut num_cell_triangles = 0;

        PERFECT_WORKSPACE.with(|node_edge_ids| {
            let mut node_edge_ids = node_edge_ids.borrow_mut();

            // processing nodes in reverse order
            for current_node in nodes.rev() {
                // store mapping of head node to corresponding outgoing edge id
                for (&node, edge_id) in cch.forward()[current_node].iter().zip(cch.forward().range(current_node)) {
                    node_edge_ids[node as usize].0 = InRangeOption::some(edge_id as EdgeId);
                }
                for (&node, edge_id) in cch.backward()[current_node].iter().zip(cch.backward().range(current_node)) {
                    node_edge_ids[node as usize].1 = InRangeOption::some(edge_id as EdgeId);
                }

                for (&node, edge_id) in cch.forward()[current_node].iter().zip(cch.forward().range(current_node)) {
                    for (&target, shortcut_edge_id) in cch.forward()[node as usize].iter().zip(cch.forward().range(node as usize)) {
                        if let Some(other_edge_id) = node_edge_ids[target as usize].0.value() {
                            unsafe {
                                let relax_weight = (*upward.add(edge_id as usize)) + (*upward.add(shortcut_edge_id as usize));
                                let edge_weight = upward.add(other_edge_id as usize);
                                if relax_weight < *edge_weight {
                                    *edge_weight = relax_weight;
                                    *up_mod.add(other_edge_id as usize) = true;
                                }
                            }
                            num_cell_triangles += 1;
                        }
                    }

                    for (&target, shortcut_edge_id) in cch.backward()[node as usize].iter().zip(cch.backward().range(node as usize)) {
                        if let Some(other_edge_id) = node_edge_ids[target as usize].0.value() {
                            unsafe {
                                let relax_weight = (*upward.add(other_edge_id as usize)) + (*downward.add(shortcut_edge_id as usize));
                                let edge_weight = upward.add(edge_id as usize);
                                if relax_weight < *edge_weight {
                                    *edge_weight = relax_weight;
                                    *up_mod.add(edge_id as usize) = true;
                                }
                            }
                            num_cell_triangles += 1;
                        }
                    }
                }

                for (&node, edge_id) in cch.backward()[current_node].iter().zip(cch.backward().range(current_node)) {
                    for (&target, shortcut_edge_id) in cch.forward()[node as usize].iter().zip(cch.forward().range(node as usize)) {
                        if let Some(other_edge_id) = node_edge_ids[target as usize].1.value() {
                            unsafe {
                                let relax_weight = (*downward.add(other_edge_id as usize)) + (*upward.add(shortcut_edge_id as usize));
                                let edge_weight = downward.add(edge_id as usize);
                                if relax_weight < *edge_weight {
                                    *edge_weight = relax_weight;
                                    *down_mod.add(edge_id as usize) = true;
                                }
                            }
                            num_cell_triangles += 1;
                        }
                    }

                    for (&target, shortcut_edge_id) in cch.backward()[node as usize].iter().zip(cch.backward().range(node as usize)) {
                        if let Some(other_edge_id) = node_edge_ids[target as usize].1.value() {
                            unsafe {
                                let relax_weight = (*downward.add(edge_id as usize)) + (*downward.add(shortcut_edge_id as usize));
                                let edge_weight = downward.add(other_edge_id as usize);
                                if relax_weight < *edge_weight {
                                    *edge_weight = relax_weight;
                                    *down_mod.add(other_edge_id as usize) = true;
                                }
                            }
                            num_cell_triangles += 1;
                        }
                    }
                }

                // reset the mapping
                for &node in &cch.forward()[current_node] {
                    node_edge_ids[node as usize].0 = InRangeOption::NONE;
                }
                for &node in &cch.backward()[current_node] {
                    node_edge_ids[node as usize].1 = InRangeOption::NONE;
                }
            }
        });

        num_triangles.fetch_add(num_cell_triangles, std::sync::atomic::Ordering::SeqCst);
    };

    let static_perfect_customization = SeperatorBasedPerfectParallelCustomization::new_with_aux(cch, customize_perfect, customize_perfect);

    let mut upward_modified = vec![false; customized.upward.len()];
    let mut downward_modified = vec![false; customized.downward.len()];

    report_time_with_key("CCH Perfect Customization", "perfect_customization_running_time_ms", || {
        static_perfect_customization.customize_with_aux(
            &mut customized.upward,
            &mut customized.downward,
            &mut upward_modified,
            &mut downward_modified,
            |cb| {
                PERFECT_WORKSPACE.set(&RefCell::new(vec![(InRangeOption::NONE, InRangeOption::NONE); n as usize]), cb);
            },
        );
    });

    report!(
        "num_directed_upper_and_intermediate_triangles",
        num_triangles.load(std::sync::atomic::Ordering::SeqCst)
    );

    (upward_modified, downward_modified)
}
