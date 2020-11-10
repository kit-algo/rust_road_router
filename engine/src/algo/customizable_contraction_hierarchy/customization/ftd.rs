//! CATCHUp Customization

use super::*;
use crate::report::*;
use floating_time_dependent::*;
use std::{
    cmp::{max, min, Ordering as Ord},
    sync::atomic::Ordering,
};

// Reusable buffers for main CATCHUp customization, to reduce allocations
scoped_thread_local!(static MERGE_BUFFERS: RefCell<MergeBuffers>);
// Workspaces for static CATCHUp precustomization - similar to regular static customization - see parent module
scoped_thread_local!(static UPWARD_WORKSPACE: RefCell<Vec<(FlWeight, FlWeight)>>);
scoped_thread_local!(static DOWNWARD_WORKSPACE: RefCell<Vec<(FlWeight, FlWeight)>>);
// Workspace for perfect static CATCHUp precustomization - here we just need one for both directions
// because we map to the edge id instead of the values.
scoped_thread_local!(static PERFECT_WORKSPACE: RefCell<Vec<InRangeOption<EdgeId>>>);

/// Run CATCHUp customization
pub fn customize<'a, 'b: 'a>(cch: &'a CCH, metric: &'b TDGraph) -> CustomizedGraph<'a> {
    let (upward, downward) = customize_internal(cch, metric);
    CustomizedGraph::new(metric, &cch.first_out, &cch.head, upward, downward)
}

pub fn customize_internal<'a, 'b: 'a>(cch: &'a CCH, metric: &'b TDGraph) -> (Vec<Shortcut>, Vec<Shortcut>) {
    report!("algo", "Floating TDCCH Customization");

    let n = (cch.first_out.len() - 1) as NodeId;
    let m = cch.head.len();

    // these will contain our customized shortcuts
    let mut upward: Vec<_> = std::iter::repeat_with(|| Shortcut::new(None, metric)).take(m).collect();
    let mut downward: Vec<_> = std::iter::repeat_with(|| Shortcut::new(None, metric)).take(m).collect();

    // start with respecting - set shortcuts to respective original edge.
    let subctxt = push_context("weight_applying".to_string());
    report_time("TD-CCH apply weights", || {
        upward
            .par_iter_mut()
            .zip(downward.par_iter_mut())
            .zip(cch.cch_edge_to_orig_arc.par_iter())
            .for_each(|((up_weight, down_weight), &(up_arc, down_arc))| {
                if let Some(up_arc) = up_arc.value() {
                    *up_weight = Shortcut::new(Some(up_arc), metric);
                }
                if let Some(down_arc) = down_arc.value() {
                    *down_weight = Shortcut::new(Some(down_arc), metric);
                }
            });
    });
    drop(subctxt);

    // This is the routine for basic static customization with just the upper and lower bounds.
    // It runs completely analogue the standard customization algorithm.
    let customize = |nodes: Range<usize>, offset, upward_weights: &mut [Shortcut], downward_weights: &mut [Shortcut]| {
        UPWARD_WORKSPACE.with(|node_outgoing_weights| {
            let mut node_outgoing_weights = node_outgoing_weights.borrow_mut();

            DOWNWARD_WORKSPACE.with(|node_incoming_weights| {
                let mut node_incoming_weights = node_incoming_weights.borrow_mut();

                for current_node in nodes {
                    let current_node = current_node as NodeId;
                    let mut edges = cch.neighbor_edge_indices_usize(current_node);
                    edges.start -= offset;
                    edges.end -= offset;
                    for ((node, down), up) in cch
                        .neighbor_iter(current_node)
                        .zip(&downward_weights[edges.clone()])
                        .zip(&upward_weights[edges.clone()])
                    {
                        node_incoming_weights[node as usize] = (down.lower_bound, down.upper_bound);
                        node_outgoing_weights[node as usize] = (up.lower_bound, up.upper_bound);
                    }

                    for Link {
                        node: low_node,
                        weight: first_edge_id,
                    } in LinkIterable::<Link>::link_iter(&cch.inverted, current_node)
                    {
                        let first_down_weight: &Shortcut = &downward_weights[first_edge_id as usize - offset];
                        let first_up_weight: &Shortcut = &upward_weights[first_edge_id as usize - offset];
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
                            relax.0 = std::cmp::min(relax.0, upward_weight.lower_bound + first_down_weight.lower_bound);
                            relax.1 = std::cmp::min(relax.1, upward_weight.upper_bound + first_down_weight.upper_bound);
                            let relax = unsafe { node_incoming_weights.get_unchecked_mut(node as usize) };
                            relax.0 = std::cmp::min(relax.0, downward_weight.lower_bound + first_up_weight.lower_bound);
                            relax.1 = std::cmp::min(relax.1, downward_weight.upper_bound + first_up_weight.upper_bound);
                        }
                    }

                    for (((node, down), up), _edge_id) in cch
                        .neighbor_iter(current_node)
                        .zip(&mut downward_weights[edges.clone()])
                        .zip(&mut upward_weights[edges.clone()])
                        .zip(edges)
                    {
                        down.lower_bound = node_incoming_weights[node as usize].0;
                        down.upper_bound = node_incoming_weights[node as usize].1;
                        up.lower_bound = node_outgoing_weights[node as usize].0;
                        up.upper_bound = node_outgoing_weights[node as usize].1;
                        down.update_is_constant();
                        up.update_is_constant();
                    }
                }
            });
        });
    };

    // Routine for CATCHUp perfect precustomization on the bounds.
    // The interface is similar to the one for the basic customization, but we need access to nonconsecutive ranges of edges,
    // so we can't use slices. Thus, we just take a mutable pointer to the shortcut vecs.
    // The logic of the perfect customization based on separators guarantees, that we will never concurrently modify
    // the same shortcuts, but so far I haven't found a way to express that in safe rust.
    let customize_perfect = |nodes: Range<usize>, upward: *mut Shortcut, downward: *mut Shortcut| {
        PERFECT_WORKSPACE.with(|node_edge_ids| {
            let mut node_edge_ids = node_edge_ids.borrow_mut();

            // processing nodes in reverse order
            for current_node in nodes.rev() {
                let current_node = current_node as NodeId;
                // store mapping of head node to corresponding outgoing edge id
                for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                    node_edge_ids[node as usize] = InRangeOption::new(Some(edge_id));
                }

                for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                    let shortcut_edge_ids = cch.neighbor_edge_indices(node);
                    for (target, shortcut_edge_id) in cch.neighbor_iter(node).zip(shortcut_edge_ids) {
                        if let Some(other_edge_id) = node_edge_ids[target as usize].value() {
                            // Here we have both an intermediate and an upper triangle
                            // depending on which edge we take as the base
                            // Relax all them.
                            unsafe {
                                (*upward.add(other_edge_id as usize)).upper_bound = min(
                                    (*upward.add(other_edge_id as usize)).upper_bound,
                                    (*upward.add(edge_id as usize)).upper_bound + (*upward.add(shortcut_edge_id as usize)).upper_bound,
                                );
                                (*upward.add(other_edge_id as usize)).lower_bound = min(
                                    (*upward.add(other_edge_id as usize)).lower_bound,
                                    (*upward.add(edge_id as usize)).lower_bound + (*upward.add(shortcut_edge_id as usize)).lower_bound,
                                );

                                (*upward.add(edge_id as usize)).upper_bound = min(
                                    (*upward.add(edge_id as usize)).upper_bound,
                                    (*upward.add(other_edge_id as usize)).upper_bound + (*downward.add(shortcut_edge_id as usize)).upper_bound,
                                );
                                (*upward.add(edge_id as usize)).lower_bound = min(
                                    (*upward.add(edge_id as usize)).lower_bound,
                                    (*upward.add(other_edge_id as usize)).lower_bound + (*downward.add(shortcut_edge_id as usize)).lower_bound,
                                );

                                (*downward.add(other_edge_id as usize)).upper_bound = min(
                                    (*downward.add(other_edge_id as usize)).upper_bound,
                                    (*downward.add(edge_id as usize)).upper_bound + (*downward.add(shortcut_edge_id as usize)).upper_bound,
                                );
                                (*downward.add(other_edge_id as usize)).lower_bound = min(
                                    (*downward.add(other_edge_id as usize)).lower_bound,
                                    (*downward.add(edge_id as usize)).lower_bound + (*downward.add(shortcut_edge_id as usize)).lower_bound,
                                );

                                (*downward.add(edge_id as usize)).upper_bound = min(
                                    (*downward.add(edge_id as usize)).upper_bound,
                                    (*downward.add(other_edge_id as usize)).upper_bound + (*upward.add(shortcut_edge_id as usize)).upper_bound,
                                );
                                (*downward.add(edge_id as usize)).lower_bound = min(
                                    (*downward.add(edge_id as usize)).lower_bound,
                                    (*downward.add(other_edge_id as usize)).lower_bound + (*upward.add(shortcut_edge_id as usize)).lower_bound,
                                );
                            }
                        }
                    }
                }

                // reset the mapping
                for node in cch.neighbor_iter(current_node) {
                    node_edge_ids[node as usize] = InRangeOption::new(None);
                }
            }
        });
    };

    // parallelize precusotmization
    let static_customization = SeperatorBasedParallelCustomization::new(cch, customize, customize);
    let static_perfect_customization = SeperatorBasedPerfectParallelCustomization::new(cch, customize_perfect, customize_perfect);

    // routine to disable shortcuts for which the perfect precustomization determined them to be irrelevant
    let disable_dominated = |(shortcut, &lower_bound): (&mut Shortcut, &FlWeight)| {
        // shortcut contains shortest path length, lower bound the length of the specific path represented by the shortcut (not necessarily the shortest)
        if shortcut.upper_bound.fuzzy_lt(lower_bound) {
            shortcut.required = false;
            shortcut.lower_bound = FlWeight::INFINITY;
            shortcut.upper_bound = FlWeight::INFINITY;
        } else {
            // reset shortcut lower bound from path to actual shortcut bound
            shortcut.lower_bound = lower_bound;
        }
    };

    if cfg!(feature = "tdcch-precustomization") {
        // execute CATCHUp precustomization
        let _subctxt = push_context("precustomization".to_string());
        report_time("TD-CCH Pre-Customization", || {
            static_customization.customize(&mut upward, &mut downward, |cb| {
                UPWARD_WORKSPACE.set(&RefCell::new(vec![(FlWeight::INFINITY, FlWeight::INFINITY); n as usize]), || {
                    DOWNWARD_WORKSPACE.set(&RefCell::new(vec![(FlWeight::INFINITY, FlWeight::INFINITY); n as usize]), || cb());
                });
            });

            let upward_preliminary_bounds: Vec<_> = upward.iter().map(|s| s.lower_bound).collect();
            let downward_preliminary_bounds: Vec<_> = downward.iter().map(|s| s.lower_bound).collect();

            static_perfect_customization.customize(&mut upward, &mut downward, |cb| {
                PERFECT_WORKSPACE.set(&RefCell::new(vec![InRangeOption::new(None); n as usize]), || cb());
            });

            upward.par_iter_mut().zip(upward_preliminary_bounds.par_iter()).for_each(disable_dominated);
            downward.par_iter_mut().zip(downward_preliminary_bounds.par_iter()).for_each(disable_dominated);
        });
    }

    // block for main CATCHUp customization
    {
        let subctxt = push_context("main".to_string());

        use std::sync::mpsc::{channel, RecvTimeoutError};
        use std::thread;

        let (tx, rx) = channel();
        let (events_tx, events_rx) = channel();

        // use separator based parallelization
        let customization = SeperatorBasedParallelCustomization::new(
            cch,
            // routines created in this function
            // we customize many cells in parallel - so iterate over triangles sequentially
            create_customization_fn(&cch, metric, SeqIter(&cch)),
            // the final separator can only be customized, once everything else is done, but it still takes up a significant amount of time
            // But we can still parallelize the processing of edges from one node within this separator.
            create_customization_fn(&cch, metric, ParIter(&cch)),
        );

        report_time("TD-CCH Customization", || {
            // spawn of a thread, which periodically reports the state of things
            thread::spawn(move || {
                let timer = Timer::new();

                let mut events = Vec::new();

                loop {
                    // this actually reports to stderr and stores the data in the `events` `Vec`
                    report!("at_s", timer.get_passed_ms() / 1000);
                    report!("nodes_customized", NODES_CUSTOMIZED.load(Ordering::Relaxed));
                    if cfg!(feature = "detailed-stats") {
                        report!("num_ipps_stored", IPP_COUNT.load(Ordering::Relaxed));
                        report!("num_shortcuts_active", ACTIVE_SHORTCUTS.load(Ordering::Relaxed));
                        report!("num_ipps_reduced_by_approx", SAVED_BY_APPROX.load(Ordering::Relaxed));
                        report!("num_ipps_considered_for_approx", CONSIDERED_FOR_APPROX.load(Ordering::Relaxed));
                        report!("num_shortcut_merge_points", PATH_SOURCES_COUNT.load(Ordering::Relaxed));
                        report!("num_performed_merges", ACTUALLY_MERGED.load(Ordering::Relaxed));
                        report!("num_performed_links", ACTUALLY_LINKED.load(Ordering::Relaxed));
                        report!("num_performed_unnecessary_links", UNNECESSARY_LINKED.load(Ordering::Relaxed));
                    }

                    if cfg!(feature = "detailed-stats") {
                        events.push((
                            timer.get_passed_ms() / 1000,
                            NODES_CUSTOMIZED.load(Ordering::Relaxed),
                            IPP_COUNT.load(Ordering::Relaxed),
                            ACTIVE_SHORTCUTS.load(Ordering::Relaxed),
                            SAVED_BY_APPROX.load(Ordering::Relaxed),
                            CONSIDERED_FOR_APPROX.load(Ordering::Relaxed),
                            PATH_SOURCES_COUNT.load(Ordering::Relaxed),
                            ACTUALLY_MERGED.load(Ordering::Relaxed),
                            ACTUALLY_LINKED.load(Ordering::Relaxed),
                            UNNECESSARY_LINKED.load(Ordering::Relaxed),
                        ));
                    } else {
                        events.push((timer.get_passed_ms() / 1000, NODES_CUSTOMIZED.load(Ordering::Relaxed), 0, 0, 0, 0, 0, 0, 0, 0));
                    }

                    if let Ok(()) | Err(RecvTimeoutError::Disconnected) = rx.recv_timeout(std::time::Duration::from_secs(3)) {
                        events_tx.send(events).unwrap();
                        break;
                    }
                }
            });

            // execute main customization
            customization.customize(&mut upward, &mut downward, |cb| {
                MERGE_BUFFERS.set(&RefCell::new(MergeBuffers::new()), || {
                    cb();
                });
            });
        });

        // shut down reporting thread
        tx.send(()).unwrap();

        // actual reporting for experiements
        // silent because we already wrote everything to stderr
        for events in events_rx {
            let mut events_ctxt = push_collection_context("events".to_string());

            for event in events {
                let _event = events_ctxt.push_collection_item();

                report_silent!("at_s", event.0);
                report_silent!("nodes_customized", event.1);
                if cfg!(feature = "detailed-stats") {
                    report_silent!("num_ipps_stored", event.2);
                    report_silent!("num_shortcuts_active", event.3);
                    report_silent!("num_ipps_reduced_by_approx", event.4);
                    report_silent!("num_ipps_considered_for_approx", event.5);
                    report_silent!("num_shortcut_merge_points", event.6);
                    report_silent!("num_performed_merges", event.7);
                    report_silent!("num_performed_links", event.8);
                    report_silent!("num_performed_unnecessary_links", event.9);
                }
            }
        }

        drop(subctxt);
    }

    if cfg!(feature = "detailed-stats") {
        report!("num_ipps_stored", IPP_COUNT.load(Ordering::Relaxed));
        report!("num_shortcuts_active", ACTIVE_SHORTCUTS.load(Ordering::Relaxed));
        report!("num_ipps_reduced_by_approx", SAVED_BY_APPROX.load(Ordering::Relaxed));
        report!("num_ipps_considered_for_approx", CONSIDERED_FOR_APPROX.load(Ordering::Relaxed));
        report!("num_shortcut_merge_points", PATH_SOURCES_COUNT.load(Ordering::Relaxed));
        report!("num_performed_merges", ACTUALLY_MERGED.load(Ordering::Relaxed));
        report!("num_performed_links", ACTUALLY_LINKED.load(Ordering::Relaxed));
        report!("num_performed_unnecessary_links", UNNECESSARY_LINKED.load(Ordering::Relaxed));
    }
    report!("approx", f64::from(APPROX));
    report!("approx_threshold", APPROX_THRESHOLD);

    if cfg!(feature = "tdcch-postcustomization") {
        // do perfect bound based customization again, because we now have better bounds and can get rid of some additional shortcuts
        let _subctxt = push_context("postcustomization".to_string());
        report_time("TD-CCH Post-Customization", || {
            let upward_preliminary_bounds: Vec<_> = upward.iter().map(|s| s.lower_bound).collect();
            let downward_preliminary_bounds: Vec<_> = downward.iter().map(|s| s.lower_bound).collect();

            static_perfect_customization.customize(&mut upward, &mut downward, |cb| {
                PERFECT_WORKSPACE.set(&RefCell::new(vec![InRangeOption::new(None); n as usize]), || cb());
            });

            upward.par_iter_mut().zip(upward_preliminary_bounds.par_iter()).for_each(disable_dominated);
            downward.par_iter_mut().zip(downward_preliminary_bounds.par_iter()).for_each(disable_dominated);

            for current_node in 0..n {
                let (upward_below, upward_above) = upward.split_at_mut(cch.first_out[current_node as usize] as usize);
                let upward_active = &mut upward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
                let (downward_below, downward_above) = downward.split_at_mut(cch.first_out[current_node as usize] as usize);
                let downward_active = &mut downward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
                let shortcut_graph = PartialShortcutGraph::new(metric, upward_below, downward_below, 0);

                for shortcut in &mut upward_active[..] {
                    shortcut.disable_if_unneccesary(&shortcut_graph);
                }

                for shortcut in &mut downward_active[..] {
                    shortcut.disable_if_unneccesary(&shortcut_graph);
                }
            }

            for current_node in (0..n).rev() {
                let (upward_below, upward_above) = upward.split_at_mut(cch.first_out[current_node as usize] as usize);
                let upward_active = &mut upward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
                let (downward_below, downward_above) = downward.split_at_mut(cch.first_out[current_node as usize] as usize);
                let downward_active = &mut downward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];

                for shortcut in &mut upward_active[..] {
                    shortcut.reenable_required(downward_below, upward_below);
                }

                for shortcut in &mut downward_active[..] {
                    shortcut.reenable_required(downward_below, upward_below);
                }
            }
        });
    }

    (upward, downward)
}

// Encapsulates the creation of the CATCHUp main customization lambdas
// The function signature gives us some additional control of lifetimes and stuff
fn create_customization_fn<'s, F: 's>(cch: &'s CCH, metric: &'s TDGraph, merge_iter: F) -> impl Fn(Range<usize>, usize, &mut [Shortcut], &mut [Shortcut]) + 's
where
    for<'p> F: ForEachIter<'p, 's, Shortcut>,
{
    move |nodes, edge_offset, upward: &mut [Shortcut], downward: &mut [Shortcut]| {
        // for all nodes we should currently process
        for current_node in nodes {
            let (upward_below, upward_above) = upward.split_at_mut(cch.first_out[current_node as usize] as usize - edge_offset);
            let upward_active = &mut upward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
            let (downward_below, downward_above) = downward.split_at_mut(cch.first_out[current_node as usize] as usize - edge_offset);
            let downward_active = &mut downward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
            let shortcut_graph = PartialShortcutGraph::new(metric, upward_below, downward_below, edge_offset);

            debug_assert_eq!(upward_active.len(), cch.degree(current_node as NodeId));
            debug_assert_eq!(downward_active.len(), cch.degree(current_node as NodeId));

            // for all outgoing edges - parallel or sequentially, depending on the type of `merge_iter`
            merge_iter.for_each(
                current_node as NodeId,
                upward_active,
                downward_active,
                |(((&node, _), upward_shortcut), downward_shortcut)| {
                    MERGE_BUFFERS.with(|buffers| {
                        let mut buffers = buffers.borrow_mut();

                        // here, we enumerate lower triangles the classic way, as described in the CCH journal
                        // because it is completely dominated by linking and merging.
                        // Also storing the triangles allows us to sort them and process shorter triangles first,
                        // which gives better bounds, which allows skipping unnecessary operations.
                        let mut triangles = Vec::new();

                        // downward edges from both endpoints of the current edge
                        let mut current_iter = LinkIterable::<Link>::link_iter(&cch.inverted, current_node as NodeId).peekable();
                        let mut other_iter = LinkIterable::<Link>::link_iter(&cch.inverted, node as NodeId).peekable();

                        while let (
                            Some(Link {
                                node: lower_from_current,
                                weight: edge_from_cur_id,
                            }),
                            Some(Link {
                                node: lower_from_other,
                                weight: edge_from_oth_id,
                            }),
                        ) = (current_iter.peek(), other_iter.peek())
                        {
                            debug_assert_eq!(cch.head()[*edge_from_cur_id as usize], current_node as NodeId);
                            debug_assert_eq!(cch.head()[*edge_from_oth_id as usize], node);
                            debug_assert_eq!(cch.edge_id_to_tail(*edge_from_cur_id), *lower_from_current);
                            debug_assert_eq!(cch.edge_id_to_tail(*edge_from_oth_id), *lower_from_other);

                            match lower_from_current.cmp(&lower_from_other) {
                                Ord::Less => current_iter.next(),
                                Ord::Greater => other_iter.next(),
                                Ord::Equal => {
                                    // lower triangle
                                    triangles.push((*edge_from_cur_id, *edge_from_oth_id));

                                    current_iter.next();
                                    other_iter.next()
                                }
                            };
                        }
                        if cfg!(feature = "tdcch-triangle-sorting") {
                            triangles.sort_by_key(|&(down, up)| shortcut_graph.get_incoming(down).lower_bound + shortcut_graph.get_outgoing(up).lower_bound);
                        }
                        for &edges in &triangles {
                            // main work happening here
                            upward_shortcut.merge(edges, &shortcut_graph, &mut buffers);
                        }
                        upward_shortcut.finalize_bounds(&shortcut_graph);

                        if cfg!(feature = "tdcch-triangle-sorting") {
                            triangles.sort_by_key(|&(up, down)| shortcut_graph.get_incoming(down).lower_bound + shortcut_graph.get_outgoing(up).lower_bound);
                        }
                        for &(up, down) in &triangles {
                            // an here
                            downward_shortcut.merge((down, up), &shortcut_graph, &mut buffers);
                        }
                        downward_shortcut.finalize_bounds(&shortcut_graph);
                    });
                },
            );

            // free up space - we will never need the explicit functions again during customization
            for Link { weight: edge_id, .. } in LinkIterable::<Link>::link_iter(&cch.inverted, current_node as NodeId) {
                upward[edge_id as usize - edge_offset].clear_plf();
                downward[edge_id as usize - edge_offset].clear_plf();
            }

            NODES_CUSTOMIZED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

trait ForEachIter<'s, 'c, S> {
    fn for_each(
        &self,
        current_node: NodeId,
        upward_active: &'s mut [S],
        downward_active: &'s mut [S],
        f: impl Send + Sync + Fn((((&'c NodeId, EdgeId), &'s mut S), &'s mut S)),
    );
}

struct SeqIter<'c>(&'c CCH);

impl<'s, 'c, S> ForEachIter<'s, 'c, S> for SeqIter<'c> {
    fn for_each(
        &self,
        current_node: NodeId,
        upward_active: &'s mut [S],
        downward_active: &'s mut [S],
        f: impl Send + Sync + Fn((((&'c NodeId, EdgeId), &'s mut S), &'s mut S)),
    ) {
        self.0.head[self.0.neighbor_edge_indices_usize(current_node)]
            .iter()
            .zip(self.0.neighbor_edge_indices(current_node))
            .zip(upward_active.iter_mut())
            .zip(downward_active.iter_mut())
            .for_each(f);
    }
}

struct ParIter<'c>(&'c CCH);

impl<'s, 'c, S: 's> ForEachIter<'s, 'c, S> for ParIter<'c>
where
    &'s mut [S]: IntoParallelIterator<Item = &'s mut S>,
    <&'s mut [S] as IntoParallelIterator>::Iter: IndexedParallelIterator,
{
    fn for_each(
        &self,
        current_node: NodeId,
        upward_active: &'s mut [S],
        downward_active: &'s mut [S],
        f: impl Send + Sync + Fn((((&'c NodeId, EdgeId), &'s mut S), &'s mut S)),
    ) {
        self.0.head[self.0.neighbor_edge_indices_usize(current_node)]
            .par_iter()
            .zip_eq(self.0.neighbor_edge_indices(current_node))
            .zip_eq(upward_active.par_iter_mut())
            .zip_eq(downward_active.par_iter_mut())
            .for_each(f);
    }
}

scoped_thread_local!(static UPWARD_WORKSPACE_LIVE: RefCell<Vec<PreLiveShortcut>>);
scoped_thread_local!(static DOWNWARD_WORKSPACE_LIVE: RefCell<Vec<PreLiveShortcut>>);

pub fn customize_live<'a, 'b: 'a>(cch: &'a CCH, metric: &'b LiveGraph) {
    report!("algo", "CATCHUp Live Customization");

    let n = (cch.first_out.len() - 1) as NodeId;
    let m = cch.head.len();
    let t_live = metric.t_live();

    // Perfect customization is ok, because upper bounds for paths are not used for pruning
    let subctxt = push_context("static".to_string());
    let (upward_pred, downward_pred) = customize_internal(cch, &metric.graph);
    drop(subctxt);
    let mut upward: Vec<_> = std::iter::repeat_with(|| LiveShortcut::new(None, metric)).take(m).collect();
    let mut downward: Vec<_> = std::iter::repeat_with(|| LiveShortcut::new(None, metric)).take(m).collect();

    // start with respecting - set shortcuts to respective original edge.
    let subctxt = push_context("weight_applying".to_string());
    report_time("CATCHUp Live respecting", || {
        upward
            .par_iter_mut()
            .zip(downward.par_iter_mut())
            .zip(cch.cch_edge_to_orig_arc.par_iter())
            .for_each(|((up_weight, down_weight), &(up_arc, down_arc))| {
                if let Some(up_arc) = up_arc.value() {
                    *up_weight = LiveShortcut::new(Some(up_arc), &metric);
                }
                if let Some(down_arc) = down_arc.value() {
                    *down_weight = LiveShortcut::new(Some(down_arc), &metric);
                }
            });
    });
    drop(subctxt);

    let mut pre_upward: Vec<_> = upward.par_iter().map(|s| s.to_pre_shortcut()).collect();
    let mut pre_downward: Vec<_> = downward.par_iter().map(|s| s.to_pre_shortcut()).collect();

    let customize = |nodes: Range<usize>, offset: usize, upward_weights: &mut [PreLiveShortcut], downward_weights: &mut [PreLiveShortcut]| {
        UPWARD_WORKSPACE_LIVE.with(|node_outgoing_weights| {
            let mut node_outgoing_weights = node_outgoing_weights.borrow_mut();

            DOWNWARD_WORKSPACE_LIVE.with(|node_incoming_weights| {
                let mut node_incoming_weights = node_incoming_weights.borrow_mut();

                for current_node in nodes {
                    let current_node = current_node as NodeId;
                    let mut edges = cch.neighbor_edge_indices_usize(current_node);
                    edges.start -= offset;
                    edges.end -= offset;
                    for ((node, down), up) in cch
                        .neighbor_iter(current_node)
                        .zip(&downward_weights[edges.clone()])
                        .zip(&upward_weights[edges.clone()])
                    {
                        node_incoming_weights[node as usize] = *down;
                        node_outgoing_weights[node as usize] = *up;
                    }

                    for Link {
                        node: low_node,
                        weight: first_edge_id,
                    } in LinkIterable::<Link>::link_iter(&cch.inverted, current_node)
                    {
                        let first_down_weight: &PreLiveShortcut = &downward_weights[first_edge_id as usize - offset];
                        let first_up_weight: &PreLiveShortcut = &upward_weights[first_edge_id as usize - offset];
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
                            relax.lower_bound = std::cmp::min(relax.lower_bound, upward_weight.lower_bound + first_down_weight.lower_bound);
                            relax.upper_bound = std::cmp::min(relax.upper_bound, upward_weight.upper_bound + first_down_weight.upper_bound);
                            let relax = unsafe { node_incoming_weights.get_unchecked_mut(node as usize) };
                            relax.lower_bound = std::cmp::min(relax.lower_bound, downward_weight.lower_bound + first_up_weight.lower_bound);
                            relax.upper_bound = std::cmp::min(relax.upper_bound, downward_weight.upper_bound + first_up_weight.upper_bound);
                        }
                    }

                    for Link {
                        node: low_node,
                        weight: first_edge_id,
                    } in LinkIterable::<Link>::link_iter(&cch.inverted, current_node)
                    {
                        let first_down_weight: &PreLiveShortcut = &downward_weights[first_edge_id as usize - offset];
                        let first_up_weight: &PreLiveShortcut = &upward_weights[first_edge_id as usize - offset];
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
                            if upward_weight.lower_bound + first_down_weight.lower_bound <= relax.upper_bound {
                                relax.live_until = std::cmp::max(
                                    relax.live_until,
                                    std::cmp::max(
                                        first_down_weight.live_until,
                                        upward_weight
                                            .live_until
                                            .map(|live_until| live_until - first_down_weight.lower_bound)
                                            .filter(|&live_until| live_until >= t_live),
                                    ),
                                )
                            }
                            let relax = unsafe { node_incoming_weights.get_unchecked_mut(node as usize) };
                            if downward_weight.lower_bound + first_up_weight.lower_bound <= relax.upper_bound {
                                relax.live_until = std::cmp::max(
                                    relax.live_until,
                                    std::cmp::max(
                                        downward_weight.live_until,
                                        first_up_weight
                                            .live_until
                                            .map(|live_until| live_until - downward_weight.lower_bound)
                                            .filter(|&live_until| live_until >= t_live),
                                    ),
                                )
                            }
                        }
                    }

                    for (((node, down), up), _edge_id) in cch
                        .neighbor_iter(current_node)
                        .zip(&mut downward_weights[edges.clone()])
                        .zip(&mut upward_weights[edges.clone()])
                        .zip(edges)
                    {
                        *down = node_incoming_weights[node as usize];
                        *up = node_outgoing_weights[node as usize];
                    }
                }
            });
        });
    };

    let static_customization = SeperatorBasedParallelCustomization::new(cch, customize, customize);

    let customize_perfect = |nodes: Range<usize>, upward: *mut PreLiveShortcut, downward: *mut PreLiveShortcut| {
        PERFECT_WORKSPACE.with(|node_edge_ids| {
            let mut node_edge_ids = node_edge_ids.borrow_mut();

            // processing nodes in reverse order
            for current_node in nodes.rev() {
                let current_node = current_node as NodeId;
                // store mapping of head node to corresponding outgoing edge id
                for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                    node_edge_ids[node as usize] = InRangeOption::new(Some(edge_id));
                }

                for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                    let shortcut_edge_ids = cch.neighbor_edge_indices(node);
                    for (target, shortcut_edge_id) in cch.neighbor_iter(node).zip(shortcut_edge_ids) {
                        if let Some(other_edge_id) = node_edge_ids[target as usize].value() {
                            // Here we have both an intermediate and an upper triangle
                            // depending on which edge we take as the base
                            // Relax all them.
                            unsafe {
                                (*upward.add(other_edge_id as usize)).upper_bound = min(
                                    (*upward.add(other_edge_id as usize)).upper_bound,
                                    (*upward.add(edge_id as usize)).upper_bound + (*upward.add(shortcut_edge_id as usize)).upper_bound,
                                );
                                (*upward.add(other_edge_id as usize)).lower_bound = min(
                                    (*upward.add(other_edge_id as usize)).lower_bound,
                                    (*upward.add(edge_id as usize)).lower_bound + (*upward.add(shortcut_edge_id as usize)).lower_bound,
                                );

                                (*upward.add(edge_id as usize)).upper_bound = min(
                                    (*upward.add(edge_id as usize)).upper_bound,
                                    (*upward.add(other_edge_id as usize)).upper_bound + (*downward.add(shortcut_edge_id as usize)).upper_bound,
                                );
                                (*upward.add(edge_id as usize)).lower_bound = min(
                                    (*upward.add(edge_id as usize)).lower_bound,
                                    (*upward.add(other_edge_id as usize)).lower_bound + (*downward.add(shortcut_edge_id as usize)).lower_bound,
                                );

                                (*downward.add(other_edge_id as usize)).upper_bound = min(
                                    (*downward.add(other_edge_id as usize)).upper_bound,
                                    (*downward.add(edge_id as usize)).upper_bound + (*downward.add(shortcut_edge_id as usize)).upper_bound,
                                );
                                (*downward.add(other_edge_id as usize)).lower_bound = min(
                                    (*downward.add(other_edge_id as usize)).lower_bound,
                                    (*downward.add(edge_id as usize)).lower_bound + (*downward.add(shortcut_edge_id as usize)).lower_bound,
                                );

                                (*downward.add(edge_id as usize)).upper_bound = min(
                                    (*downward.add(edge_id as usize)).upper_bound,
                                    (*downward.add(other_edge_id as usize)).upper_bound + (*upward.add(shortcut_edge_id as usize)).upper_bound,
                                );
                                (*downward.add(edge_id as usize)).lower_bound = min(
                                    (*downward.add(edge_id as usize)).lower_bound,
                                    (*downward.add(other_edge_id as usize)).lower_bound + (*upward.add(shortcut_edge_id as usize)).lower_bound,
                                );
                            }
                        }
                    }
                }

                for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                    let shortcut_edge_ids = cch.neighbor_edge_indices(node);
                    for (target, shortcut_edge_id) in cch.neighbor_iter(node).zip(shortcut_edge_ids) {
                        if let Some(other_edge_id) = node_edge_ids[target as usize].value() {
                            unsafe {
                                if !(*upward.add(shortcut_edge_id as usize))
                                    .upper_bound
                                    .fuzzy_lt((*downward.add(edge_id as usize)).lower_bound + (*upward.add(other_edge_id as usize)).lower_bound)
                                {
                                    (*downward.add(edge_id as usize)).unpack =
                                        max((*downward.add(edge_id as usize)).unpack, (*upward.add(shortcut_edge_id as usize)).live_until);
                                    (*upward.add(other_edge_id as usize)).unpack = max(
                                        (*upward.add(other_edge_id as usize)).unpack,
                                        (*upward.add(shortcut_edge_id as usize))
                                            .live_until
                                            .map(|live_until| live_until + (*downward.add(edge_id as usize)).upper_bound),
                                    );
                                }

                                if !(*downward.add(shortcut_edge_id as usize))
                                    .upper_bound
                                    .fuzzy_lt((*downward.add(other_edge_id as usize)).lower_bound + (*upward.add(edge_id as usize)).lower_bound)
                                {
                                    (*downward.add(other_edge_id as usize)).unpack = max(
                                        (*downward.add(other_edge_id as usize)).unpack,
                                        (*downward.add(shortcut_edge_id as usize)).live_until,
                                    );
                                    (*upward.add(edge_id as usize)).unpack = max(
                                        (*upward.add(edge_id as usize)).unpack,
                                        (*downward.add(shortcut_edge_id as usize))
                                            .live_until
                                            .map(|live_until| live_until + (*downward.add(other_edge_id as usize)).upper_bound),
                                    );
                                }
                            }
                        }
                    }
                }

                // reset the mapping
                for node in cch.neighbor_iter(current_node) {
                    node_edge_ids[node as usize] = InRangeOption::new(None);
                }
            }
        });
    };

    let static_perfect_customization = SeperatorBasedPerfectParallelCustomization::new(cch, customize_perfect, customize_perfect);

    let mark_for_unpacking = |nodes: Range<usize>, offset: usize, upward_weights: &mut [PreLiveShortcut], downward_weights: &mut [PreLiveShortcut]| {
        use floating_time_dependent::shortcut_source::ShortcutSource;
        let edges = cch.edge_indices_range_usize(nodes.start as NodeId..nodes.end as NodeId);
        for edge_idx in edges.rev() {
            if let Some(unpack) = upward_weights[edge_idx - offset].unpack {
                let start = upward_weights[edge_idx - offset].live_until.unwrap_or(t_live);
                if start.fuzzy_lt(unpack) {
                    for (_, source) in upward_pred[edge_idx].sources_for(start, unpack) {
                        if let ShortcutSource::Shortcut(down, up) = source.into() {
                            let down = down as usize - offset;
                            let up = up as usize - offset;
                            downward_weights[down].unpack = max(downward_weights[down].unpack, upward_weights[edge_idx - offset].unpack);
                            upward_weights[up].unpack = max(
                                upward_weights[up].unpack,
                                upward_weights[edge_idx - offset]
                                    .unpack
                                    .map(|unpack| unpack + downward_weights[down].upper_bound),
                            );
                        }
                    }
                }
            }

            if let Some(unpack) = downward_weights[edge_idx - offset].unpack {
                let start = downward_weights[edge_idx - offset].live_until.unwrap_or(t_live);
                if start.fuzzy_lt(unpack) {
                    for (_, source) in downward_pred[edge_idx].sources_for(start, unpack) {
                        if let ShortcutSource::Shortcut(down, up) = source.into() {
                            let down = down as usize - offset;
                            let up = up as usize - offset;
                            downward_weights[down].unpack = max(downward_weights[down].unpack, downward_weights[edge_idx - offset].unpack);
                            upward_weights[up].unpack = max(
                                upward_weights[up].unpack,
                                downward_weights[edge_idx - offset]
                                    .unpack
                                    .map(|unpack| unpack + downward_weights[down].upper_bound),
                            );
                        }
                    }
                }
            }
        }
    };

    let mark_for_unpacking = SeperatorBasedParallelCustomization::new_reverse(cch, mark_for_unpacking, mark_for_unpacking);

    // routine to disable shortcuts for which the perfect precustomization determined them to be irrelevant
    let disable_dominated = |(shortcut, &lower_bound): (&mut LiveShortcut, &FlWeight)| {
        // shortcut contains shortest path length, lower bound the length of the specific path represented by the shortcut (not necessarily the shortest)
        if shortcut.upper_bound.fuzzy_lt(lower_bound) {
            shortcut.required = false;
            shortcut.lower_bound = FlWeight::INFINITY;
            shortcut.upper_bound = FlWeight::INFINITY;
        } else {
            // reset shortcut lower bound from path to actual shortcut bound
            shortcut.lower_bound = lower_bound;
        }
    };

    {
        // execute CATCHUp precustomization
        let _subctxt = push_context("precustomization".to_string());
        report_time("CATCHUp Live Pre-Customization", || {
            static_customization.customize(&mut pre_upward, &mut pre_downward, |cb| {
                UPWARD_WORKSPACE_LIVE.set(&RefCell::new(vec![PreLiveShortcut::default(); n as usize]), || {
                    DOWNWARD_WORKSPACE_LIVE.set(&RefCell::new(vec![PreLiveShortcut::default(); n as usize]), || cb());
                });
            });

            let upward_preliminary_bounds: Vec<_> = upward.iter().map(|s| s.lower_bound).collect();
            let downward_preliminary_bounds: Vec<_> = downward.iter().map(|s| s.lower_bound).collect();

            static_perfect_customization.customize(&mut pre_upward, &mut pre_downward, |cb| {
                PERFECT_WORKSPACE.set(&RefCell::new(vec![InRangeOption::new(None); n as usize]), || cb());
            });

            mark_for_unpacking.customize(&mut pre_upward[..], &mut pre_downward[..], |_| {});

            upward.par_iter_mut().zip(pre_upward.par_iter()).for_each(|(s, p)| s.update_with(*p, t_live));
            downward
                .par_iter_mut()
                .zip(pre_downward.par_iter())
                .for_each(|(s, p)| s.update_with(*p, t_live));
            upward.par_iter_mut().zip(upward_preliminary_bounds.par_iter()).for_each(disable_dominated);
            downward.par_iter_mut().zip(downward_preliminary_bounds.par_iter()).for_each(disable_dominated);
        });
    }

    // block for main CATCHUp customization
    {
        let subctxt = push_context("main".to_string());

        use std::sync::mpsc::{channel, RecvTimeoutError};
        use std::thread;

        let (tx, rx) = channel();
        let (events_tx, events_rx) = channel();

        // use separator based parallelization
        let customization = SeperatorBasedParallelCustomization::new(
            cch,
            // routines created in this function
            // we customize many cells in parallel - so iterate over triangles sequentially
            create_live_customization_fn(&cch, metric, &upward_pred[..], &downward_pred[..], SeqIter(&cch)),
            // the final separator can only be customized, once everything else is done, but it still takes up a significant amount of time
            // But we can still parallelize the processing of edges from one node within this separator.
            create_live_customization_fn(&cch, metric, &upward_pred[..], &downward_pred[..], ParIter(&cch)),
        );

        report_time("CATCHUp Live Customization", || {
            // spawn of a thread, which periodically reports the state of things
            thread::spawn(move || {
                let timer = Timer::new();

                let mut events = Vec::new();

                loop {
                    // this actually reports to stderr and stores the data in the `events` `Vec`
                    report!("at_s", timer.get_passed_ms() / 1000);
                    report!("nodes_customized", NODES_CUSTOMIZED.load(Ordering::Relaxed));
                    if cfg!(feature = "detailed-stats") {
                        report!("num_ipps_stored", IPP_COUNT.load(Ordering::Relaxed));
                        report!("num_shortcuts_active", ACTIVE_SHORTCUTS.load(Ordering::Relaxed));
                        report!("num_ipps_reduced_by_approx", SAVED_BY_APPROX.load(Ordering::Relaxed));
                        report!("num_ipps_considered_for_approx", CONSIDERED_FOR_APPROX.load(Ordering::Relaxed));
                        report!("num_shortcut_merge_points", PATH_SOURCES_COUNT.load(Ordering::Relaxed));
                        report!("num_performed_merges", ACTUALLY_MERGED.load(Ordering::Relaxed));
                        report!("num_performed_links", ACTUALLY_LINKED.load(Ordering::Relaxed));
                        report!("num_performed_unnecessary_links", UNNECESSARY_LINKED.load(Ordering::Relaxed));
                    }

                    if cfg!(feature = "detailed-stats") {
                        events.push((
                            timer.get_passed_ms() / 1000,
                            NODES_CUSTOMIZED.load(Ordering::Relaxed),
                            IPP_COUNT.load(Ordering::Relaxed),
                            ACTIVE_SHORTCUTS.load(Ordering::Relaxed),
                            SAVED_BY_APPROX.load(Ordering::Relaxed),
                            CONSIDERED_FOR_APPROX.load(Ordering::Relaxed),
                            PATH_SOURCES_COUNT.load(Ordering::Relaxed),
                            ACTUALLY_MERGED.load(Ordering::Relaxed),
                            ACTUALLY_LINKED.load(Ordering::Relaxed),
                            UNNECESSARY_LINKED.load(Ordering::Relaxed),
                        ));
                    } else {
                        events.push((timer.get_passed_ms() / 1000, NODES_CUSTOMIZED.load(Ordering::Relaxed), 0, 0, 0, 0, 0, 0, 0, 0));
                    }

                    if let Ok(()) | Err(RecvTimeoutError::Disconnected) = rx.recv_timeout(std::time::Duration::from_secs(3)) {
                        events_tx.send(events).unwrap();
                        break;
                    }
                }
            });

            // execute main customization
            customization.customize(&mut upward, &mut downward, |cb| {
                MERGE_BUFFERS.set(&RefCell::new(MergeBuffers::new()), || {
                    cb();
                });
            });
        });

        // shut down reporting thread
        tx.send(()).unwrap();

        // actual reporting for experiements
        // silent because we already wrote everything to stderr
        for events in events_rx {
            let mut events_ctxt = push_collection_context("events".to_string());

            for event in events {
                let _event = events_ctxt.push_collection_item();

                report_silent!("at_s", event.0);
                report_silent!("nodes_customized", event.1);
                if cfg!(feature = "detailed-stats") {
                    report_silent!("num_ipps_stored", event.2);
                    report_silent!("num_shortcuts_active", event.3);
                    report_silent!("num_ipps_reduced_by_approx", event.4);
                    report_silent!("num_ipps_considered_for_approx", event.5);
                    report_silent!("num_shortcut_merge_points", event.6);
                    report_silent!("num_performed_merges", event.7);
                    report_silent!("num_performed_links", event.8);
                    report_silent!("num_performed_unnecessary_links", event.9);
                }
            }
        }

        drop(subctxt);
    }

    if cfg!(feature = "detailed-stats") {
        report!("num_ipps_stored", IPP_COUNT.load(Ordering::Relaxed));
        report!("num_shortcuts_active", ACTIVE_SHORTCUTS.load(Ordering::Relaxed));
        report!("num_ipps_reduced_by_approx", SAVED_BY_APPROX.load(Ordering::Relaxed));
        report!("num_ipps_considered_for_approx", CONSIDERED_FOR_APPROX.load(Ordering::Relaxed));
        report!("num_shortcut_merge_points", PATH_SOURCES_COUNT.load(Ordering::Relaxed));
        report!("num_performed_merges", ACTUALLY_MERGED.load(Ordering::Relaxed));
        report!("num_performed_links", ACTUALLY_LINKED.load(Ordering::Relaxed));
        report!("num_performed_unnecessary_links", UNNECESSARY_LINKED.load(Ordering::Relaxed));
    }
    report!("approx", f64::from(APPROX));
    report!("approx_threshold", APPROX_THRESHOLD);

    // TODO Combine bounds
}

// Encapsulates the creation of the CATCHUp main customization lambdas
// The function signature gives us some additional control of lifetimes and stuff
fn create_live_customization_fn<'s, F: 's>(
    cch: &'s CCH,
    metric: &'s LiveGraph,
    upward_pred: &'s [Shortcut],
    downward_pred: &'s [Shortcut],
    merge_iter: F,
) -> impl Fn(Range<usize>, usize, &mut [LiveShortcut], &mut [LiveShortcut]) + 's
where
    for<'p> F: ForEachIter<'p, 's, LiveShortcut>,
{
    move |nodes, edge_offset, upward: &mut [LiveShortcut], downward: &mut [LiveShortcut]| {
        // for all nodes we should currently process
        for current_node in nodes {
            let (upward_below, upward_above) = upward.split_at_mut(cch.first_out[current_node as usize] as usize - edge_offset);
            let upward_active = &mut upward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
            let (downward_below, downward_above) = downward.split_at_mut(cch.first_out[current_node as usize] as usize - edge_offset);
            let downward_active = &mut downward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
            let shortcut_graph = PartialLiveShortcutGraph::new(metric, upward_below, downward_below, upward_pred, downward_pred, edge_offset);

            debug_assert_eq!(upward_active.len(), cch.degree(current_node as NodeId));
            debug_assert_eq!(downward_active.len(), cch.degree(current_node as NodeId));

            // for all outgoing edges - parallel or sequentially, depending on the type of `merge_iter`
            merge_iter.for_each(
                current_node as NodeId,
                upward_active,
                downward_active,
                |(((&node, edge_id), upward_shortcut), downward_shortcut)| {
                    MERGE_BUFFERS.with(|buffers| {
                        let mut buffers = buffers.borrow_mut();

                        // here, we enumerate lower triangles the classic way, as described in the CCH journal
                        // because it is completely dominated by linking and merging.
                        // Also storing the triangles allows us to sort them and process shorter triangles first,
                        // which gives better bounds, which allows skipping unnecessary operations.
                        let mut triangles = Vec::new();

                        // downward edges from both endpoints of the current edge
                        let mut current_iter = LinkIterable::<Link>::link_iter(&cch.inverted, current_node as NodeId).peekable();
                        let mut other_iter = LinkIterable::<Link>::link_iter(&cch.inverted, node as NodeId).peekable();

                        while let (
                            Some(Link {
                                node: lower_from_current,
                                weight: edge_from_cur_id,
                            }),
                            Some(Link {
                                node: lower_from_other,
                                weight: edge_from_oth_id,
                            }),
                        ) = (current_iter.peek(), other_iter.peek())
                        {
                            debug_assert_eq!(cch.head()[*edge_from_cur_id as usize], current_node as NodeId);
                            debug_assert_eq!(cch.head()[*edge_from_oth_id as usize], node);
                            debug_assert_eq!(cch.edge_id_to_tail(*edge_from_cur_id), *lower_from_current);
                            debug_assert_eq!(cch.edge_id_to_tail(*edge_from_oth_id), *lower_from_other);

                            match lower_from_current.cmp(&lower_from_other) {
                                Ord::Less => current_iter.next(),
                                Ord::Greater => other_iter.next(),
                                Ord::Equal => {
                                    // lower triangle
                                    triangles.push((*edge_from_cur_id, *edge_from_oth_id));

                                    current_iter.next();
                                    other_iter.next()
                                }
                            };
                        }
                        if cfg!(feature = "tdcch-triangle-sorting") {
                            triangles.sort_by_key(|&(down, up)| shortcut_graph.get_incoming(down).lower_bound + shortcut_graph.get_outgoing(up).lower_bound);
                        }
                        for &edges in &triangles {
                            // main work happening here
                            upward_shortcut.merge(edges, &shortcut_graph, &mut buffers);
                        }
                        upward_shortcut.reconstruct_cache(&upward_pred[edge_id as usize], &shortcut_graph, &mut buffers);
                        upward_shortcut.finalize_bounds(&shortcut_graph);

                        if cfg!(feature = "tdcch-triangle-sorting") {
                            triangles.sort_by_key(|&(up, down)| shortcut_graph.get_incoming(down).lower_bound + shortcut_graph.get_outgoing(up).lower_bound);
                        }
                        for &(up, down) in &triangles {
                            // an here
                            downward_shortcut.merge((down, up), &shortcut_graph, &mut buffers);
                        }
                        downward_shortcut.reconstruct_cache(&downward_pred[edge_id as usize], &shortcut_graph, &mut buffers);
                        downward_shortcut.finalize_bounds(&shortcut_graph);
                    });
                },
            );

            // free up space - we will never need the explicit functions again during customization
            for Link { weight: edge_id, .. } in LinkIterable::<Link>::link_iter(&cch.inverted, current_node as NodeId) {
                upward[edge_id as usize - edge_offset].clear_plf();
                downward[edge_id as usize - edge_offset].clear_plf();
            }

            NODES_CUSTOMIZED.fetch_add(1, Ordering::Relaxed);
        }
    }
}
