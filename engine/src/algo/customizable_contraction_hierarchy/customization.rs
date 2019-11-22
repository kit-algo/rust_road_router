use super::*;
use rayon::prelude::*;
use std::cell::RefCell;

mod parallelization;
use parallelization::*;
pub mod ftd;

scoped_thread_local!(static UPWARD_WORKSPACE: RefCell<Vec<Weight>>);
scoped_thread_local!(static DOWNWARD_WORKSPACE: RefCell<Vec<Weight>>);

pub fn customize<'c, Graph>(
    cch: &'c CCH,
    metric: &Graph,
) -> (
    FirstOutGraph<&'c [EdgeId], &'c [NodeId], Vec<Weight>>,
    FirstOutGraph<&'c [EdgeId], &'c [NodeId], Vec<Weight>>,
)
where
    Graph: for<'a> LinkIterGraph<'a> + RandomLinkAccessGraph + Sync,
{
    let n = cch.num_nodes() as NodeId;
    let m = cch.num_arcs();

    let customize = |nodes: Range<usize>, offset, upward_weights: &mut [Weight], downward_weights: &mut [Weight]| {
        UPWARD_WORKSPACE.with(|node_outgoing_weights| {
            let mut node_outgoing_weights = node_outgoing_weights.borrow_mut();

            DOWNWARD_WORKSPACE.with(|node_incoming_weights| {
                let mut node_incoming_weights = node_incoming_weights.borrow_mut();

                for current_node in nodes {
                    let current_node = current_node as NodeId;
                    let mut edges = cch.neighbor_edge_indices_usize(current_node);
                    edges.start -= offset;
                    edges.end -= offset;
                    for ((node, &down_weight), &up_weight) in cch
                        .neighbor_iter(current_node)
                        .zip(&downward_weights[edges.clone()])
                        .zip(&upward_weights[edges.clone()])
                    {
                        node_incoming_weights[node as usize] = down_weight;
                        node_outgoing_weights[node as usize] = up_weight;
                    }

                    for Link {
                        node: low_node,
                        weight: first_edge_id,
                    } in cch.inverted.neighbor_iter(current_node)
                    {
                        let first_down_weight = downward_weights[first_edge_id as usize - offset];
                        let first_up_weight = upward_weights[first_edge_id as usize - offset];
                        let mut low_up_edges = cch.neighbor_edge_indices_usize(low_node);
                        low_up_edges.start -= offset;
                        low_up_edges.end -= offset;
                        for ((node, upward_weight), downward_weight) in cch
                            .neighbor_iter(low_node)
                            .rev()
                            .zip(upward_weights[low_up_edges.clone()].iter().rev())
                            .zip(downward_weights[low_up_edges].iter().rev())
                        {
                            if node <= current_node {
                                break;
                            }

                            let relax = unsafe { node_outgoing_weights.get_unchecked_mut(node as usize) };
                            *relax = std::cmp::min(*relax, upward_weight + first_down_weight);
                            let relax = unsafe { node_incoming_weights.get_unchecked_mut(node as usize) };
                            *relax = std::cmp::min(*relax, downward_weight + first_up_weight);
                        }
                    }

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

    let customization = SeperatorBasedParallelCustomization::new(cch, customize, customize);

    let mut upward_weights = vec![INFINITY; m];
    let mut downward_weights = vec![INFINITY; m];

    report_time("CCH apply weights", || {
        upward_weights
            .par_iter_mut()
            .zip(downward_weights.par_iter_mut())
            .zip(cch.cch_edge_to_orig_arc.par_iter())
            .for_each(|((up_weight, down_weight), &(up_arc, down_arc))| {
                if let Some(up_arc) = up_arc.value() {
                    *up_weight = metric.link(up_arc).weight;
                }
                if let Some(down_arc) = down_arc.value() {
                    *down_weight = metric.link(down_arc).weight;
                }
            });
    });

    report_time("CCH Customization", || {
        customization.customize(&mut upward_weights, &mut downward_weights, |cb| {
            UPWARD_WORKSPACE.set(&RefCell::new(vec![INFINITY; n as usize]), || {
                DOWNWARD_WORKSPACE.set(&RefCell::new(vec![INFINITY; n as usize]), || cb());
            });
        });
    });

    let upward = FirstOutGraph::new(&cch.first_out[..], &cch.head[..], upward_weights);
    let downward = FirstOutGraph::new(&cch.first_out[..], &cch.head[..], downward_weights);
    (upward, downward)
}
