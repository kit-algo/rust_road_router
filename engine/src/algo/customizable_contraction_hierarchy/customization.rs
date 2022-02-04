use super::*;
use rayon::prelude::*;
use std::{cell::RefCell, cmp::min};

mod parallelization;
use parallelization::*;
pub mod ftd;

// One mapping of node id to weight for each thread during the scope of the customization.
scoped_thread_local!(static UPWARD_WORKSPACE: RefCell<Vec<Weight>>);
scoped_thread_local!(static DOWNWARD_WORKSPACE: RefCell<Vec<Weight>>);
scoped_thread_local!(static PERFECT_WORKSPACE: RefCell<Vec<InRangeOption<EdgeId>>>);

/// Execute second phase, that is metric dependent preprocessing.
/// `metric` has to have the same topology as the original graph used for first phase preprocessing.
/// The weights of `metric` should be the ones that the cch should be customized with.
pub fn customize<'c, Graph>(cch: &'c CCH, metric: &Graph) -> Customized<CCH, &'c CCH>
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
pub fn customize_directed<'c, Graph>(cch: &'c DirectedCCH, metric: &Graph) -> Customized<DirectedCCH, &'c DirectedCCH>
where
    Graph: LinkIterGraph + EdgeRandomAccessGraph<Link> + Sync,
{
    // buffers for the customized weights
    let mut upward_weights = vec![INFINITY; cch.forward_head.len()];
    let mut downward_weights = vec![INFINITY; cch.backward_head.len()];

    // respecting phase
    // copy metric weights to their respective edges in the CCH
    prepare_weights_directed(cch, &mut upward_weights, &mut downward_weights, metric);

    customize_directed_basic(cch, upward_weights, downward_weights)
}

/// Customize with zero metric.
/// Edges that have weight infinity after customization will have this weight
/// for every metric and can be removed.
pub fn always_infinity(cch: &CCH) -> Customized<CCH, &CCH> {
    let m = cch.num_arcs();
    // buffers for the customized weights
    let mut upward_weights = vec![INFINITY; m];
    let mut downward_weights = vec![INFINITY; m];

    prepare_zero_weights(cch, &mut upward_weights, &mut downward_weights);

    customize_basic(cch, upward_weights, downward_weights)
}

fn prepare_weights<Graph>(cch: &CCH, upward_weights: &mut [Weight], downward_weights: &mut [Weight], metric: &Graph)
where
    Graph: LinkIterGraph + EdgeRandomAccessGraph<Link> + Sync,
{
    report_time_with_key("CCH apply weights", "respecting_running_time_ms", || {
        upward_weights
            .par_iter_mut()
            .zip(cch.forward_cch_edge_to_orig_arc.par_iter())
            .for_each(|(up_weight, up_arcs)| {
                for &EdgeIdT(up_arc) in up_arcs {
                    *up_weight = min(metric.link(up_arc).weight, *up_weight);
                }
            });
        downward_weights
            .par_iter_mut()
            .zip(cch.backward_cch_edge_to_orig_arc.par_iter())
            .for_each(|(down_weight, down_arcs)| {
                for &EdgeIdT(down_arc) in down_arcs {
                    *down_weight = min(metric.link(down_arc).weight, *down_weight);
                }
            });
    });
}

fn prepare_weights_directed<Graph>(cch: &DirectedCCH, upward_weights: &mut [Weight], downward_weights: &mut [Weight], metric: &Graph)
where
    Graph: LinkIterGraph + EdgeRandomAccessGraph<Link> + Sync,
{
    report_time_with_key("CCH apply weights", "respecting_running_time_ms", || {
        upward_weights
            .par_iter_mut()
            .zip(cch.forward_cch_edge_to_orig_arc.par_iter())
            .for_each(|(up_weight, up_arcs)| {
                for &EdgeIdT(up_arc) in up_arcs {
                    *up_weight = min(metric.link(up_arc).weight, *up_weight);
                }
            });
        downward_weights
            .par_iter_mut()
            .zip(cch.backward_cch_edge_to_orig_arc.par_iter())
            .for_each(|(down_weight, down_arcs)| {
                for &EdgeIdT(down_arc) in down_arcs {
                    *down_weight = min(metric.link(down_arc).weight, *down_weight);
                }
            });
    });
}

fn prepare_zero_weights(cch: &CCH, upward_weights: &mut [Weight], downward_weights: &mut [Weight]) {
    report_time_with_key("CCH apply weights", "respecting_running_time_ms", || {
        upward_weights
            .par_iter_mut()
            .zip(cch.forward_cch_edge_to_orig_arc.par_iter())
            .for_each(|(up_weight, up_arcs)| {
                if !up_arcs.is_empty() {
                    *up_weight = 0;
                }
            });
        downward_weights
            .par_iter_mut()
            .zip(cch.backward_cch_edge_to_orig_arc.par_iter())
            .for_each(|(down_weight, down_arcs)| {
                if !down_arcs.is_empty() {
                    *down_weight = 0;
                }
            });
    });
}

fn customize_basic(cch: &CCH, mut upward_weights: Vec<Weight>, mut downward_weights: Vec<Weight>) -> Customized<CCH, &CCH> {
    let n = cch.num_nodes() as NodeId;

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
    let customize = |nodes: Range<usize>, offset: usize, upward_weights: &mut [Weight], downward_weights: &mut [Weight]| {
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
                        node_incoming_weights[node as usize] = down_weight;
                        node_outgoing_weights[node as usize] = up_weight;
                    }

                    // for all downward edges of the current node
                    for (NodeIdT(low_node), Reversed(EdgeIdT(first_edge_id))) in cch.inverted.link_iter(current_node) {
                        let first_down_weight = downward_weights[first_edge_id as usize - offset];
                        let first_up_weight = upward_weights[first_edge_id as usize - offset];
                        let mut low_up_edges = cch.neighbor_edge_indices_usize(low_node);
                        low_up_edges.start -= offset;
                        low_up_edges.end -= offset;
                        // for all upward edges of the lower node
                        for ((node, upward_weight), downward_weight) in cch
                            .neighbor_iter(low_node)
                            .rev() // reversed order, (from high to low), so we can terminate earlier
                            .zip(upward_weights[low_up_edges.clone()].iter().rev())
                            .zip(downward_weights[low_up_edges].iter().rev())
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
                            *relax = min(*relax, upward_weight + first_down_weight);
                            let relax = unsafe { node_incoming_weights.get_unchecked_mut(node as usize) };
                            *relax = min(*relax, downward_weight + first_up_weight);
                        }
                    }

                    // for all upward neighbors: copy weights from the mapping back to the edge
                    for (((node, downward_weight), upward_weight), _edge_id) in cch
                        .neighbor_iter(current_node)
                        .zip(&mut downward_weights[edges.clone()])
                        .zip(&mut upward_weights[edges.clone()])
                        .zip(edges)
                    {
                        *downward_weight = node_incoming_weights[node as usize];
                        *upward_weight = node_outgoing_weights[node as usize];
                    }
                }
            });
        });
    };

    // setup customization for parallization
    let customization = SeperatorBasedParallelCustomization::new_undirected(cch, customize, customize);

    // execute customization
    report_time_with_key("CCH Customization", "basic_customization_running_time_ms", || {
        customization.customize(&mut upward_weights, &mut downward_weights, |cb| {
            // create workspace vectors for the scope of the customization
            UPWARD_WORKSPACE.set(&RefCell::new(vec![INFINITY; n as usize]), || {
                DOWNWARD_WORKSPACE.set(&RefCell::new(vec![INFINITY; n as usize]), cb);
            });
            // everything will be dropped here
        });
    });

    Customized::new(cch, upward_weights, downward_weights)
}

pub fn customize_perfect(mut customized: Customized<CCH, &CCH>) -> Customized<DirectedCCH, DirectedCCH> {
    let cch = customized.cch;
    let n = cch.num_nodes();
    let m = cch.num_arcs();
    // Routine for perfect precustomization.
    // The interface is similar to the one for the basic customization, but we need access to nonconsecutive ranges of edges,
    // so we can't use slices. Thus, we just take a mutable pointer to the shortcut vecs.
    // The logic of the perfect customization based on separators guarantees, that we will never concurrently modify
    // the same shortcuts, but so far I haven't found a way to express that in safe rust.
    let customize_perfect = |nodes: Range<usize>, upward: *mut Weight, downward: *mut Weight| {
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
                                (*upward.add(other_edge_id as usize)) = min(
                                    *upward.add(other_edge_id as usize),
                                    (*upward.add(edge_id as usize)) + (*upward.add(shortcut_edge_id as usize)),
                                );

                                (*upward.add(edge_id as usize)) = min(
                                    *upward.add(edge_id as usize),
                                    (*upward.add(other_edge_id as usize)) + (*downward.add(shortcut_edge_id as usize)),
                                );

                                (*downward.add(other_edge_id as usize)) = min(
                                    *downward.add(other_edge_id as usize),
                                    (*downward.add(edge_id as usize)) + (*downward.add(shortcut_edge_id as usize)),
                                );

                                (*downward.add(edge_id as usize)) = min(
                                    *downward.add(edge_id as usize),
                                    (*downward.add(other_edge_id as usize)) + (*upward.add(shortcut_edge_id as usize)),
                                );
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

    let static_perfect_customization = SeperatorBasedPerfectParallelCustomization::new(cch, customize_perfect, customize_perfect);

    let upward = &mut customized.upward;
    let downward = &mut customized.downward;
    let upward_orig = upward.clone();
    let downward_orig = downward.clone();

    report_time_with_key("CCH Perfect Customization", "perfect_customization_running_time_ms", || {
        static_perfect_customization.customize(upward, downward, |cb| {
            PERFECT_WORKSPACE.set(&RefCell::new(vec![InRangeOption::NONE; n as usize]), cb);
        });
    });

    report_time_with_key("Build Perfect Customized Graph", "graph_build_running_time_ms", || {
        let forward = customized.forward_graph();
        let backward = customized.backward_graph();

        let mut timer = Timer::new();

        let mut forward_first_out: Vec<EdgeId>;
        let mut forward_head: Vec<NodeId>;
        let mut forward_weight: Vec<Weight>;

        let mut backward_first_out: Vec<EdgeId>;
        let mut backward_head: Vec<NodeId>;
        let mut backward_weight: Vec<Weight>;

        if cfg!(feature = "cch-disable-par") {
            forward_first_out = Vec::with_capacity(cch.first_out.len());
            forward_first_out.push(0);
            forward_head = Vec::with_capacity(m);
            forward_weight = Vec::with_capacity(m);

            backward_first_out = Vec::with_capacity(cch.first_out.len());
            backward_first_out.push(0);
            backward_head = Vec::with_capacity(m);
            backward_weight = Vec::with_capacity(m);

            eprintln!("alloc");
            timer.report_passed_ms();
            timer.restart();

            for node in 0..n as NodeId {
                let edge_ids = cch.neighbor_edge_indices_usize(node);
                for (link, &customized_weight) in LinkIterable::<Link>::link_iter(&forward, node).zip(&upward_orig[edge_ids.clone()]) {
                    if link.weight < INFINITY && link.weight >= customized_weight {
                        forward_head.push(link.node);
                        forward_weight.push(link.weight);
                    }
                }
                for (link, &customized_weight) in LinkIterable::<Link>::link_iter(&backward, node).zip(&downward_orig[edge_ids.clone()]) {
                    if link.weight < INFINITY && link.weight >= customized_weight {
                        backward_head.push(link.node);
                        backward_weight.push(link.weight);
                    }
                }
                forward_first_out.push(forward_head.len() as EdgeId);
                backward_first_out.push(backward_head.len() as EdgeId);
            }
        } else {
            let k = rayon::current_num_threads() * 16;

            forward_first_out = vec![0; cch.first_out.len()];
            forward_head = vec![0; m];
            forward_weight = vec![0; m];

            backward_first_out = vec![0; cch.first_out.len()];
            backward_head = vec![0; m];
            backward_weight = vec![0; m];

            eprintln!("alloc");
            timer.report_passed_ms();
            timer.restart();

            let mut edges_of_each_thread = vec![(0, 0); k + 1];
            let mut local_edge_counts = &mut edges_of_each_thread[1..];
            let nodes_per_thread = (n + k - 1) / k;

            rayon::scope(|s| {
                let forward = &forward;
                let backward = &backward;
                let upward_orig = &upward_orig;
                let downward_orig = &downward_orig;

                for i in 0..k {
                    let (local_count, rest_counts) = local_edge_counts.split_first_mut().unwrap();
                    local_edge_counts = rest_counts;
                    let local_nodes = i * nodes_per_thread..min((i + 1) * nodes_per_thread, n);
                    s.spawn(move |_| {
                        for node in local_nodes {
                            let edge_ids = cch.neighbor_edge_indices_usize(node as NodeId);
                            local_count.0 += LinkIterable::<Link>::link_iter(forward, node as NodeId)
                                .zip(&upward_orig[edge_ids.clone()])
                                .filter(|(link, &customized_weight)| link.weight < INFINITY && link.weight >= customized_weight)
                                .count();
                            local_count.1 += LinkIterable::<Link>::link_iter(backward, node as NodeId)
                                .zip(&downward_orig[edge_ids])
                                .filter(|(link, &customized_weight)| link.weight < INFINITY && link.weight >= customized_weight)
                                .count();
                        }
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
                let mut forward_weight = &mut forward_weight[..];
                let mut backward_first_out = &mut backward_first_out[..];
                let mut backward_head = &mut backward_head[..];
                let mut backward_weight = &mut backward_weight[..];

                let forward = &forward;
                let backward = &backward;
                let upward_orig = &upward_orig;
                let downward_orig = &downward_orig;

                for i in 0..k {
                    let local_nodes = i * nodes_per_thread..min((i + 1) * nodes_per_thread, n);
                    let num_fw_edges_before = edges_of_each_thread[i].0;
                    let num_bw_edges_before = edges_of_each_thread[i].1;

                    let (local_fw_fo, rest_fw_fo) = forward_first_out.split_at_mut(min(nodes_per_thread, forward_first_out.len()));
                    forward_first_out = rest_fw_fo;
                    let (local_bw_fo, rest_bw_fo) = backward_first_out.split_at_mut(min(nodes_per_thread, backward_first_out.len()));
                    backward_first_out = rest_bw_fo;
                    let (local_fw_head, rest_fw_head) = forward_head.split_at_mut(edges_of_each_thread[i + 1].0 - num_fw_edges_before);
                    forward_head = rest_fw_head;
                    let (local_fw_weight, rest_fw_weight) = forward_weight.split_at_mut(edges_of_each_thread[i + 1].0 - num_fw_edges_before);
                    forward_weight = rest_fw_weight;
                    let (local_bw_head, rest_bw_head) = backward_head.split_at_mut(edges_of_each_thread[i + 1].1 - num_bw_edges_before);
                    backward_head = rest_bw_head;
                    let (local_bw_weight, rest_bw_weight) = backward_weight.split_at_mut(edges_of_each_thread[i + 1].1 - num_bw_edges_before);
                    backward_weight = rest_bw_weight;

                    s.spawn(move |_| {
                        let mut fw_edge_count = 0;
                        let mut bw_edge_count = 0;
                        for (local_node_idx, node) in local_nodes.enumerate() {
                            local_fw_fo[local_node_idx] = (num_fw_edges_before + fw_edge_count) as EdgeId;
                            local_bw_fo[local_node_idx] = (num_bw_edges_before + bw_edge_count) as EdgeId;

                            let edge_ids = cch.neighbor_edge_indices_usize(node as NodeId);
                            for (link, &customized_weight) in LinkIterable::<Link>::link_iter(forward, node as NodeId).zip(&upward_orig[edge_ids.clone()]) {
                                if link.weight < INFINITY && link.weight >= customized_weight {
                                    local_fw_head[fw_edge_count] = link.node;
                                    local_fw_weight[fw_edge_count] = link.weight;
                                    fw_edge_count += 1;
                                }
                            }
                            for (link, &customized_weight) in LinkIterable::<Link>::link_iter(backward, node as NodeId).zip(&downward_orig[edge_ids.clone()]) {
                                if link.weight < INFINITY && link.weight >= customized_weight {
                                    local_bw_head[bw_edge_count] = link.node;
                                    local_bw_weight[bw_edge_count] = link.weight;
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
            forward_first_out[n] = forward_head.len() as EdgeId;
            backward_first_out[n] = backward_head.len() as EdgeId;
        }

        eprintln!("forward");
        timer.report_passed_ms();
        timer.restart();

        let forward_inverted = ReversedGraphWithEdgeIds::reversed(&UnweightedFirstOutGraph::new(&forward_first_out[..], &forward_head[..]));
        let backward_inverted = ReversedGraphWithEdgeIds::reversed(&UnweightedFirstOutGraph::new(&backward_first_out[..], &backward_head[..]));

        eprintln!("reversed");
        timer.report_passed_ms();

        Customized::new(
            DirectedCCH {
                forward_first_out,
                forward_head,
                backward_first_out,
                backward_head,
                node_order: cch.node_order.clone(),
                forward_cch_edge_to_orig_arc: Vecs::empty(),
                backward_cch_edge_to_orig_arc: Vecs::empty(),
                elimination_tree: cch.elimination_tree.clone(),
                forward_inverted,
                backward_inverted,
            },
            forward_weight,
            backward_weight,
        )
    })
}

fn customize_directed_basic(cch: &DirectedCCH, mut upward_weights: Vec<Weight>, mut downward_weights: Vec<Weight>) -> Customized<DirectedCCH, &DirectedCCH> {
    let n = cch.num_nodes() as NodeId;

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
    let customize = |nodes: Range<usize>, upward_offset: usize, downward_offset: usize, upward_weights: &mut [Weight], downward_weights: &mut [Weight]| {
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
                        node_outgoing_weights[node as usize] = up_weight;
                    }
                    for (&node, &down_weight) in cch.backward()[current_node as usize].iter().zip(&downward_weights[backward_edges.clone()]) {
                        node_incoming_weights[node as usize] = down_weight;
                    }

                    // for all downward edges of the current node
                    for (NodeIdT(low_node), Reversed(EdgeIdT(first_edge_id))) in cch.backward_inverted.link_iter(current_node) {
                        let first_down_weight = downward_weights[first_edge_id as usize - downward_offset];
                        let mut low_up_edges = cch.forward().range(low_node as usize);
                        low_up_edges.start -= upward_offset;
                        low_up_edges.end -= upward_offset;
                        // for all upward edges of the lower node
                        for (&node, upward_weight) in cch.forward()[low_node as usize]
                            .iter()
                            .rev() // reversed order, (from high to low), so we can terminate earlier
                            .zip(upward_weights[low_up_edges].iter().rev())
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
                            *relax = min(*relax, upward_weight + first_down_weight);
                        }
                    }

                    // for all downward edges of the current node
                    for (NodeIdT(low_node), Reversed(EdgeIdT(first_edge_id))) in cch.forward_inverted.link_iter(current_node) {
                        let first_up_weight = upward_weights[first_edge_id as usize - upward_offset];
                        let mut low_up_edges = cch.backward().range(low_node as usize);
                        low_up_edges.start -= downward_offset;
                        low_up_edges.end -= downward_offset;
                        // for all upward edges of the lower node
                        for (&node, downward_weight) in cch.backward()[low_node as usize]
                            .iter()
                            .rev() // reversed order, (from high to low), so we can terminate earlier
                            .zip(downward_weights[low_up_edges].iter().rev())
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
                            *relax = min(*relax, downward_weight + first_up_weight);
                        }
                    }

                    // for all upward neighbors: copy weights from the mapping back to the edge
                    for (&node, up_weight) in cch.forward()[current_node as usize].iter().zip(&mut upward_weights[forward_edges]) {
                        *up_weight = node_outgoing_weights[node as usize];
                    }
                    for (&node, down_weight) in cch.backward()[current_node as usize].iter().zip(&mut downward_weights[backward_edges]) {
                        *down_weight = node_incoming_weights[node as usize];
                    }
                }
            });
        });
    };

    // setup customization for parallization
    let customization = SeperatorBasedParallelCustomization::new(cch, customize, customize);

    // execute customization
    report_time_with_key("CCH Customization", "basic_customization_running_time_ms", || {
        customization.customize(&mut upward_weights, &mut downward_weights, |cb| {
            // create workspace vectors for the scope of the customization
            UPWARD_WORKSPACE.set(&RefCell::new(vec![INFINITY; n as usize]), || {
                DOWNWARD_WORKSPACE.set(&RefCell::new(vec![INFINITY; n as usize]), || cb());
            });
            // everything will be dropped here
        });
    });

    Customized::new(cch, upward_weights, downward_weights)
}
