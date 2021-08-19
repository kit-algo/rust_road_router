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
    report_time_with_key("CCH apply weights", "respecting", || {
        upward_weights
            .par_iter_mut()
            .zip(downward_weights.par_iter_mut())
            .zip(cch.cch_edge_to_orig_arc.par_iter())
            .for_each(|((up_weight, down_weight), (up_arcs, down_arcs))| {
                for &EdgeIdT(up_arc) in up_arcs {
                    *up_weight = min(metric.link(up_arc).weight, *up_weight);
                }
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
    report_time_with_key("CCH apply weights", "respecting", || {
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
    report_time_with_key("CCH apply weights", "respecting", || {
        upward_weights
            .par_iter_mut()
            .zip(downward_weights.par_iter_mut())
            .zip(cch.cch_edge_to_orig_arc.par_iter())
            .for_each(|((up_weight, down_weight), (up_arcs, down_arcs))| {
                if !up_arcs.is_empty() {
                    *up_weight = 0;
                }
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
    let customization = SeperatorBasedParallelCustomization::new(cch, customize, customize);

    // execute customization
    report_time_with_key("CCH Customization", "basic_customization", || {
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
    let customization = SeperatorBasedParallelDirectedCustomization::new(cch, customize, customize);

    // execute customization
    report_time_with_key("CCH Customization", "basic_customization", || {
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
