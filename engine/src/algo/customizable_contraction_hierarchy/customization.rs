use super::*;
use rayon::prelude::*;
use std::{cell::RefCell, cmp::min};

pub mod parallelization;
use parallelization::*;
pub mod directed;
pub mod ftd;
pub mod ftd_for_pot;
pub mod validity;

// One mapping of node id to weight for each thread during the scope of the customization.
scoped_thread_local!(static UPWARD_WORKSPACE: RefCell<Vec<(Weight, InRangeOption<EdgeId>, InRangeOption<EdgeId>)>>);
scoped_thread_local!(static DOWNWARD_WORKSPACE: RefCell<Vec<(Weight, InRangeOption<EdgeId>, InRangeOption<EdgeId>)>>);
scoped_thread_local!(static PERFECT_WORKSPACE: RefCell<Vec<InRangeOption<EdgeId>>>);

/// Execute second phase, that is metric dependent preprocessing.
/// `metric` has to have the same topology as the original graph used for first phase preprocessing.
/// The weights of `metric` should be the ones that the cch should be customized with.
pub fn customize<'c, Graph>(cch: &'c CCH, metric: &Graph) -> CustomizedBasic<'c, CCH>
where
    Graph: LinkIterGraph + EdgeRandomAccessGraph<Link> + Sync,
{
    let m = cch.num_arcs();
    // buffers for the customized weights
    let mut upward_weights = vec![INFINITY; m];
    let mut downward_weights = vec![INFINITY; m];

    // respecting phase
    // copy metric weights to their respective edges in the CCH
    prepare_weights(cch, &mut upward_weights, &mut downward_weights, metric);

    customize_basic(cch, upward_weights, downward_weights)
}

/// Same as [customize], except with a `DirectedCCH`
pub fn customize_directed<'c, Graph>(cch: &'c DirectedCCH, metric: &Graph) -> CustomizedBasic<'c, DirectedCCH>
where
    Graph: LinkIterGraph + EdgeRandomAccessGraph<Link> + Sync,
{
    // buffers for the customized weights
    let mut upward_weights = vec![INFINITY; cch.forward_head.len()];
    let mut downward_weights = vec![INFINITY; cch.backward_head.len()];

    // respecting phase
    // copy metric weights to their respective edges in the CCH
    prepare_weights(cch, &mut upward_weights, &mut downward_weights, metric);

    directed::customize_directed_basic(cch, upward_weights, downward_weights)
}

pub use directed::customize_directed_perfect;

/// Customize with zero metric.
/// Edges that have weight infinity after customization will have this weight
/// for every metric and can be removed.
pub fn always_infinity(cch: &CCH) -> CustomizedBasic<CCH> {
    let m = cch.num_arcs();
    // buffers for the customized weights
    let mut upward_weights = vec![INFINITY; m];
    let mut downward_weights = vec![INFINITY; m];

    prepare_zero_weights(cch, &mut upward_weights, &mut downward_weights);

    customize_basic(cch, upward_weights, downward_weights)
}

fn prepare_weights<Graph, C>(cch: &C, upward_weights: &mut [Weight], downward_weights: &mut [Weight], metric: &Graph)
where
    Graph: LinkIterGraph + EdgeRandomAccessGraph<Link> + Sync,
    C: CCHT,
{
    report_time_with_key("CCH apply weights", "respecting_running_time_ms", || {
        upward_weights
            .par_iter_mut()
            .zip(cch.forward_cch_edge_to_orig_arc().par_iter())
            .for_each(|(up_weight, up_arcs)| {
                for &EdgeIdT(up_arc) in up_arcs {
                    *up_weight = min(metric.link(up_arc).weight, *up_weight);
                }
            });
        downward_weights
            .par_iter_mut()
            .zip(cch.backward_cch_edge_to_orig_arc().par_iter())
            .for_each(|(down_weight, down_arcs)| {
                for &EdgeIdT(down_arc) in down_arcs {
                    *down_weight = min(metric.link(down_arc).weight, *down_weight);
                }
            });
    });
}

fn prepare_zero_weights(cch: &impl CCHT, upward_weights: &mut [Weight], downward_weights: &mut [Weight]) {
    report_time_with_key("CCH apply weights", "respecting_running_time_ms", || {
        upward_weights
            .par_iter_mut()
            .zip(cch.forward_cch_edge_to_orig_arc().par_iter())
            .for_each(|(up_weight, up_arcs)| {
                if !up_arcs.is_empty() {
                    *up_weight = 0;
                }
            });
        downward_weights
            .par_iter_mut()
            .zip(cch.backward_cch_edge_to_orig_arc().par_iter())
            .for_each(|(down_weight, down_arcs)| {
                if !down_arcs.is_empty() {
                    *down_weight = 0;
                }
            });
    });
}

fn customize_basic(cch: &CCH, mut upward_weights: Vec<Weight>, mut downward_weights: Vec<Weight>) -> CustomizedBasic<CCH> {
    let n = cch.num_nodes() as NodeId;
    let m = cch.num_arcs() as EdgeId;

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
                     offset: usize,
                     _offset_down: usize,
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
                    let mut edges = cch.neighbor_edge_indices_usize(current_node);
                    edges.start -= offset;
                    edges.end -= offset;
                    // for all upward neighbors: store weight of edge in workspace at index `head`
                    for ((node, &down_weight), &up_weight) in cch
                        .neighbor_iter(current_node)
                        .zip(&downward_weights[edges.clone()])
                        .zip(&upward_weights[edges.clone()])
                    {
                        node_incoming_weights[node as usize] = (down_weight, InRangeOption::NONE, InRangeOption::NONE);
                        node_outgoing_weights[node as usize] = (up_weight, InRangeOption::NONE, InRangeOption::NONE);
                    }

                    // for all downward edges of the current node
                    for (NodeIdT(low_node), Reversed(EdgeIdT(first_edge_id))) in cch.inverted.link_iter(current_node) {
                        let first_down_weight = downward_weights[first_edge_id as usize - offset];
                        let first_up_weight = upward_weights[first_edge_id as usize - offset];
                        let mut low_up_edges = cch.neighbor_edge_indices_usize(low_node);
                        let low_up_edges_orig = low_up_edges.clone();
                        low_up_edges.start -= offset;
                        low_up_edges.end -= offset;
                        // for all upward edges of the lower node
                        for (((node, upward_weight), downward_weight), second_edge_id) in cch
                            .neighbor_iter(low_node)
                            .rev() // reversed order, (from high to low), so we can terminate earlier
                            .zip(upward_weights[low_up_edges.clone()].iter().rev())
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
                            let relax = unsafe { node_outgoing_weights.get_unchecked_mut(node as usize) };
                            let triang_weight = upward_weight + first_down_weight;
                            if triang_weight < relax.0 {
                                *relax = (triang_weight, InRangeOption::some(first_edge_id), InRangeOption::some(second_edge_id as EdgeId))
                            }
                            let relax = unsafe { node_incoming_weights.get_unchecked_mut(node as usize) };
                            let triang_weight = downward_weight + first_up_weight;
                            if triang_weight < relax.0 {
                                *relax = (triang_weight, InRangeOption::some(second_edge_id as EdgeId), InRangeOption::some(first_edge_id))
                            }
                        }
                    }

                    // for all upward neighbors: copy weights from the mapping back to the edge
                    for ((((node, downward_weight), upward_weight), downward_unpack), upward_unpack) in cch
                        .neighbor_iter(current_node)
                        .zip(&mut downward_weights[edges.clone()])
                        .zip(&mut upward_weights[edges.clone()])
                        .zip(&mut downward_unpack[edges.clone()])
                        .zip(&mut upward_unpack[edges])
                    {
                        *downward_weight = node_incoming_weights[node as usize].0;
                        *upward_weight = node_outgoing_weights[node as usize].0;
                        downward_unpack.0 = node_incoming_weights[node as usize].1;
                        downward_unpack.1 = node_incoming_weights[node as usize].2;
                        upward_unpack.0 = node_outgoing_weights[node as usize].1;
                        upward_unpack.1 = node_outgoing_weights[node as usize].2;
                    }
                }
            });
        });
    };

    // setup customization for parallization
    let customization = SeperatorBasedParallelCustomization::new_with_aux(cch, customize, customize);

    let mut upward_unpack = vec![(InRangeOption::NONE, InRangeOption::NONE); m as usize];
    let mut downward_unpack = vec![(InRangeOption::NONE, InRangeOption::NONE); m as usize];

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

pub fn customize_perfect(mut customized: CustomizedBasic<CCH>) -> CustomizedPerfect<CCH> {
    let (upward_modified, downward_modified) = customize_perfect_without_rebuild(&mut customized);
    rebuild_customized_perfect(customized, &upward_modified, &downward_modified)
}

pub fn customize_perfect_without_rebuild(customized: &mut CustomizedBasic<CCH>) -> (Vec<bool>, Vec<bool>) {
    let cch = customized.cch;
    let n = cch.num_nodes();
    // Routine for perfect precustomization.
    // The interface is similar to the one for the basic customization, but we need access to nonconsecutive ranges of edges,
    // so we can't use slices. Thus, we just take a mutable pointer to the shortcut vecs.
    // The logic of the perfect customization based on separators guarantees, that we will never concurrently modify
    // the same shortcuts, but so far I haven't found a way to express that in safe rust.
    let customize_perfect = |nodes: Range<usize>, upward: *mut Weight, downward: *mut Weight, up_mod: *mut bool, down_mod: *mut bool| {
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
                                let edge_weight = upward.add(other_edge_id as usize);
                                if relax_weight < *edge_weight {
                                    *edge_weight = relax_weight;
                                    *up_mod.add(other_edge_id as usize) = true;
                                }

                                let relax_weight = (*upward.add(other_edge_id as usize)) + (*downward.add(shortcut_edge_id as usize));
                                let edge_weight = upward.add(edge_id as usize);
                                if relax_weight < *edge_weight {
                                    *edge_weight = relax_weight;
                                    *up_mod.add(edge_id as usize) = true;
                                }

                                let relax_weight = (*downward.add(edge_id as usize)) + (*downward.add(shortcut_edge_id as usize));
                                let edge_weight = downward.add(other_edge_id as usize);
                                if relax_weight < *edge_weight {
                                    *edge_weight = relax_weight;
                                    *down_mod.add(other_edge_id as usize) = true;
                                }

                                let relax_weight = (*downward.add(other_edge_id as usize)) + (*upward.add(shortcut_edge_id as usize));
                                let edge_weight = downward.add(edge_id as usize);
                                if relax_weight < *edge_weight {
                                    *edge_weight = relax_weight;
                                    *down_mod.add(edge_id as usize) = true;
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

    let mut upward_modified = vec![false; customized.upward.len()];
    let mut downward_modified = vec![false; customized.downward.len()];

    report_time_with_key("CCH Perfect Customization", "perfect_customization_running_time_ms", || {
        static_perfect_customization.customize_with_aux(
            &mut customized.upward,
            &mut customized.downward,
            &mut upward_modified,
            &mut downward_modified,
            |cb| {
                PERFECT_WORKSPACE.set(&RefCell::new(vec![InRangeOption::NONE; n as usize]), cb);
            },
        );
    });

    (upward_modified, downward_modified)
}

pub fn rebuild_customized_perfect<'c, C: CCHT + Sync>(
    customized: CustomizedBasic<'c, C>,
    upward_modified: &[bool],
    downward_modified: &[bool],
) -> CustomizedPerfect<'c, C> {
    let cch = customized.cch;
    let n = cch.num_cch_nodes();
    let m_fw = cch.forward_head().len();
    let m_bw = cch.backward_head().len();

    report_time_with_key("Build Perfect Customized Graph", "graph_build_running_time_ms", || {
        let forward = customized.forward_graph();
        let backward = customized.backward_graph();

        let mut forward_first_out: Vec<EdgeId> = Vec::new();
        let mut forward_head: Vec<NodeId> = Vec::new();
        let mut forward_tail: Vec<NodeId> = Vec::new();
        let mut forward_weight: Vec<Weight> = Vec::new();
        let mut forward_unpacking: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)> = Vec::new();

        let mut backward_first_out: Vec<EdgeId> = Vec::new();
        let mut backward_head: Vec<NodeId> = Vec::new();
        let mut backward_tail: Vec<NodeId> = Vec::new();
        let mut backward_weight: Vec<Weight> = Vec::new();
        let mut backward_unpacking: Vec<(InRangeOption<EdgeId>, InRangeOption<EdgeId>)> = Vec::new();

        let mut forward_id_mapping = Vec::new();
        let mut backward_id_mapping = Vec::new();

        if cfg!(feature = "cch-disable-par") {
            forward_first_out = Vec::with_capacity(cch.forward_first_out().len());
            forward_first_out.push(0);
            forward_head = Vec::with_capacity(m_fw);
            forward_tail = Vec::with_capacity(m_fw);
            forward_weight = Vec::with_capacity(m_fw);
            forward_unpacking = Vec::with_capacity(m_fw);

            backward_first_out = Vec::with_capacity(cch.backward_first_out().len());
            backward_first_out.push(0);
            backward_head = Vec::with_capacity(m_bw);
            backward_tail = Vec::with_capacity(m_bw);
            backward_weight = Vec::with_capacity(m_bw);
            backward_unpacking = Vec::with_capacity(m_bw);

            forward_id_mapping = vec![m_fw as EdgeId; m_fw];
            backward_id_mapping = vec![m_bw as EdgeId; m_bw];

            for node in 0..n as NodeId {
                let fw_edge_ids = cch.forward().range(node as usize);
                for (((link, &modified), unpack), edge_idx) in LinkIterable::<Link>::link_iter(&forward, node)
                    .zip(&upward_modified[fw_edge_ids.clone()])
                    .zip(&customized.up_unpacking[fw_edge_ids.clone()])
                    .zip(fw_edge_ids)
                {
                    if !modified {
                        debug_assert!(link.weight < INFINITY);
                        forward_id_mapping[edge_idx] = forward_head.len() as EdgeId;
                        forward_head.push(link.node);
                        forward_tail.push(node);
                        forward_weight.push(link.weight);
                        forward_unpacking.push((
                            InRangeOption::new(unpack.0.value().map(|idx| backward_id_mapping[idx as usize] as EdgeId)),
                            InRangeOption::new(unpack.1.value().map(|idx| forward_id_mapping[idx as usize] as EdgeId)),
                        ));
                    }
                }
                let bw_edge_ids = cch.forward().range(node as usize);
                for (((link, &modified), unpack), edge_idx) in LinkIterable::<Link>::link_iter(&backward, node)
                    .zip(&downward_modified[bw_edge_ids.clone()])
                    .zip(&customized.down_unpacking[bw_edge_ids.clone()])
                    .zip(bw_edge_ids)
                {
                    if !modified {
                        debug_assert!(link.weight < INFINITY);
                        backward_id_mapping[edge_idx] = backward_head.len() as EdgeId;
                        backward_head.push(link.node);
                        backward_tail.push(node);
                        backward_weight.push(link.weight);
                        backward_unpacking.push((
                            InRangeOption::new(unpack.0.value().map(|idx| backward_id_mapping[idx as usize] as EdgeId)),
                            InRangeOption::new(unpack.1.value().map(|idx| forward_id_mapping[idx as usize] as EdgeId)),
                        ));
                    }
                }
                forward_first_out.push(forward_head.len() as EdgeId);
                backward_first_out.push(backward_head.len() as EdgeId);
            }
        } else {
            let k = rayon::current_num_threads() * 4;

            rayon::scope(|s| {
                s.spawn(|_| forward_first_out = vec![0; cch.forward_first_out().len()]);
                s.spawn(|_| forward_head = vec![0; m_fw]);
                s.spawn(|_| forward_tail = vec![0; m_fw]);
                s.spawn(|_| forward_weight = vec![0; m_fw]);
                s.spawn(|_| forward_unpacking = vec![(InRangeOption::NONE, InRangeOption::NONE); m_fw]);
                s.spawn(|_| forward_id_mapping = vec![m_fw as EdgeId; m_fw]);
                s.spawn(|_| backward_first_out = vec![0; cch.backward_first_out().len()]);
                s.spawn(|_| backward_head = vec![0; m_bw]);
                s.spawn(|_| backward_tail = vec![0; m_bw]);
                s.spawn(|_| backward_weight = vec![0; m_bw]);
                s.spawn(|_| backward_unpacking = vec![(InRangeOption::NONE, InRangeOption::NONE); m_bw]);
                s.spawn(|_| backward_id_mapping = vec![m_bw as EdgeId; m_bw]);
            });

            let mut edges_of_each_thread = vec![(0, 0); k + 1];
            let mut local_edge_counts = &mut edges_of_each_thread[1..];
            let target_edges_per_thread = (m_fw + k - 1) / k;
            let first_node_of_chunk: Vec<_> = cch
                .forward_tail()
                .chunks(target_edges_per_thread)
                .map(|chunk| chunk[0] as usize)
                .chain(std::iter::once(n))
                .collect();

            // let nodes_per_thread = (n + k - 1) / k;
            // let first_node_of_chunk: Vec<_> = (0..=k).map(|i| min(n, i * nodes_per_thread)).collect();

            rayon::scope(|s| {
                let upward_modified = &upward_modified;
                let downward_modified = &downward_modified;

                for i in 0..k {
                    let (local_count, rest_counts) = local_edge_counts.split_first_mut().unwrap();
                    local_edge_counts = rest_counts;
                    let local_nodes = first_node_of_chunk[i]..first_node_of_chunk[i + 1];
                    let local_fw_edges = cch.forward_first_out()[local_nodes.start] as usize..cch.forward_first_out()[local_nodes.end] as usize;
                    let local_bw_edges = cch.backward_first_out()[local_nodes.start] as usize..cch.backward_first_out()[local_nodes.end] as usize;
                    s.spawn(move |_| {
                        local_count.0 = upward_modified[local_fw_edges].iter().filter(|&&m| !m).count();
                        local_count.1 = downward_modified[local_bw_edges].iter().filter(|&&m| !m).count();
                    });
                }
            });

            let mut prefixes = (0, 0);
            for (fw_count, bw_count) in &mut edges_of_each_thread {
                prefixes.0 += *fw_count;
                prefixes.1 += *bw_count;
                *fw_count = prefixes.0;
                *bw_count = prefixes.1;
            }

            rayon::scope(|s| {
                let mut forward_first_out = &mut forward_first_out[..];
                let mut forward_head = &mut forward_head[..];
                let mut forward_tail = &mut forward_tail[..];
                let mut forward_weight = &mut forward_weight[..];
                let mut forward_unpacking = &mut forward_unpacking[..];
                let mut forward_mapping = &mut forward_id_mapping[..];
                let mut backward_first_out = &mut backward_first_out[..];
                let mut backward_head = &mut backward_head[..];
                let mut backward_tail = &mut backward_tail[..];
                let mut backward_weight = &mut backward_weight[..];
                let mut backward_unpacking = &mut backward_unpacking[..];
                let mut backward_mapping = &mut backward_id_mapping[..];

                let forward = &forward;
                let backward = &backward;
                let upward_modified = &upward_modified;
                let downward_modified = &downward_modified;
                let up_unpacking = &customized.up_unpacking;
                let down_unpacking = &customized.down_unpacking;

                for i in 0..k {
                    let local_nodes = first_node_of_chunk[i]..first_node_of_chunk[i + 1];
                    debug_assert!(local_nodes.start <= local_nodes.end);
                    let num_cch_fw_edges_in_batch = cch.forward_first_out()[local_nodes.end] - cch.forward_first_out()[local_nodes.start];
                    let num_cch_bw_edges_in_batch = cch.backward_first_out()[local_nodes.end] - cch.backward_first_out()[local_nodes.start];
                    let num_fw_edges_before = edges_of_each_thread[i].0;
                    let num_bw_edges_before = edges_of_each_thread[i].1;

                    let (local_fw_fo, rest_fw_fo) = forward_first_out.split_at_mut(local_nodes.end - local_nodes.start);
                    forward_first_out = rest_fw_fo;
                    let (local_bw_fo, rest_bw_fo) = backward_first_out.split_at_mut(local_nodes.end - local_nodes.start);
                    backward_first_out = rest_bw_fo;
                    let (local_fw_head, rest_fw_head) = forward_head.split_at_mut(edges_of_each_thread[i + 1].0 - num_fw_edges_before);
                    forward_head = rest_fw_head;
                    let (local_fw_tail, rest_fw_tail) = forward_tail.split_at_mut(edges_of_each_thread[i + 1].0 - num_fw_edges_before);
                    forward_tail = rest_fw_tail;
                    let (local_fw_weight, rest_fw_weight) = forward_weight.split_at_mut(edges_of_each_thread[i + 1].0 - num_fw_edges_before);
                    forward_weight = rest_fw_weight;
                    let (local_fw_unpacking, rest_fw_unpacking) = forward_unpacking.split_at_mut(edges_of_each_thread[i + 1].0 - num_fw_edges_before);
                    forward_unpacking = rest_fw_unpacking;
                    let (local_fw_mapping, rest_fw_mapping) = forward_mapping.split_at_mut(num_cch_fw_edges_in_batch as usize);
                    forward_mapping = rest_fw_mapping;
                    let (local_bw_head, rest_bw_head) = backward_head.split_at_mut(edges_of_each_thread[i + 1].1 - num_bw_edges_before);
                    backward_head = rest_bw_head;
                    let (local_bw_tail, rest_bw_tail) = backward_tail.split_at_mut(edges_of_each_thread[i + 1].1 - num_bw_edges_before);
                    backward_tail = rest_bw_tail;
                    let (local_bw_weight, rest_bw_weight) = backward_weight.split_at_mut(edges_of_each_thread[i + 1].1 - num_bw_edges_before);
                    backward_weight = rest_bw_weight;
                    let (local_bw_unpacking, rest_bw_unpacking) = backward_unpacking.split_at_mut(edges_of_each_thread[i + 1].1 - num_bw_edges_before);
                    backward_unpacking = rest_bw_unpacking;
                    let (local_bw_mapping, rest_bw_mapping) = backward_mapping.split_at_mut(num_cch_bw_edges_in_batch as usize);
                    backward_mapping = rest_bw_mapping;

                    s.spawn(move |_| {
                        let mut fw_edge_count = 0;
                        let mut bw_edge_count = 0;
                        let fw_edge_offset = cch.forward_first_out()[local_nodes.start] as usize;
                        let bw_edge_offset = cch.backward_first_out()[local_nodes.start] as usize;
                        for (local_node_idx, node) in local_nodes.enumerate() {
                            local_fw_fo[local_node_idx] = (num_fw_edges_before + fw_edge_count) as EdgeId;
                            local_bw_fo[local_node_idx] = (num_bw_edges_before + bw_edge_count) as EdgeId;

                            let fw_edge_ids = cch.forward().range(node);
                            for (((link, &modified), unpack), edge_idx) in LinkIterable::<Link>::link_iter(forward, node as NodeId)
                                .zip(&upward_modified[fw_edge_ids.clone()])
                                .zip(&up_unpacking[fw_edge_ids.clone()])
                                .zip(fw_edge_ids)
                            {
                                if !modified {
                                    // debug_assert!(link.weight < INFINITY);
                                    local_fw_head[fw_edge_count] = link.node;
                                    local_fw_tail[fw_edge_count] = node as NodeId;
                                    local_fw_weight[fw_edge_count] = link.weight;
                                    local_fw_unpacking[fw_edge_count] = *unpack;
                                    local_fw_mapping[edge_idx - fw_edge_offset] = (num_fw_edges_before + fw_edge_count) as EdgeId;
                                    fw_edge_count += 1;
                                }
                            }
                            let bw_edge_ids = cch.backward().range(node);
                            for (((link, &modified), unpack), edge_idx) in LinkIterable::<Link>::link_iter(backward, node as NodeId)
                                .zip(&downward_modified[bw_edge_ids.clone()])
                                .zip(&down_unpacking[bw_edge_ids.clone()])
                                .zip(bw_edge_ids)
                            {
                                if !modified {
                                    // debug_assert!(link.weight < INFINITY);
                                    local_bw_head[bw_edge_count] = link.node;
                                    local_bw_tail[bw_edge_count] = node as NodeId;
                                    local_bw_weight[bw_edge_count] = link.weight;
                                    local_bw_unpacking[bw_edge_count] = *unpack;
                                    local_bw_mapping[edge_idx - bw_edge_offset] = (num_bw_edges_before + bw_edge_count) as EdgeId;
                                    bw_edge_count += 1;
                                }
                            }
                        }
                    });
                }
            });

            forward_head.truncate(edges_of_each_thread[k].0);
            backward_head.truncate(edges_of_each_thread[k].1);
            forward_weight.truncate(edges_of_each_thread[k].0);
            backward_weight.truncate(edges_of_each_thread[k].1);
            forward_tail.truncate(edges_of_each_thread[k].0);
            backward_tail.truncate(edges_of_each_thread[k].1);
            forward_unpacking.truncate(edges_of_each_thread[k].0);
            backward_unpacking.truncate(edges_of_each_thread[k].1);
            forward_first_out[n] = forward_head.len() as EdgeId;
            backward_first_out[n] = backward_head.len() as EdgeId;

            forward_unpacking.par_iter_mut().for_each(|(down, up)| {
                *down = InRangeOption::new(down.value().map(|idx| backward_id_mapping[idx as usize] as EdgeId));
                *up = InRangeOption::new(up.value().map(|idx| forward_id_mapping[idx as usize] as EdgeId));
            });
            backward_unpacking.par_iter_mut().for_each(|(down, up)| {
                *down = InRangeOption::new(down.value().map(|idx| backward_id_mapping[idx as usize] as EdgeId));
                *up = InRangeOption::new(up.value().map(|idx| forward_id_mapping[idx as usize] as EdgeId));
            });
        }

        CustomizedPerfect::new(
            cch,
            OwnedGraph::new(forward_first_out, forward_head, forward_weight),
            OwnedGraph::new(backward_first_out, backward_head, backward_weight),
            forward_unpacking,
            backward_unpacking,
            forward_tail,
            backward_tail,
        )
    })
}
