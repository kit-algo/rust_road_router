use super::*;
use crate::report::*;
use floating_time_dependent::*;
use std::{cmp::min, sync::atomic::Ordering};

scoped_thread_local!(static MERGE_BUFFERS: RefCell<MergeBuffers>);
scoped_thread_local!(static UPWARD_WORKSPACE: RefCell<Vec<(FlWeight, FlWeight)>>);
scoped_thread_local!(static DOWNWARD_WORKSPACE: RefCell<Vec<(FlWeight, FlWeight)>>);
scoped_thread_local!(static PERFECT_WORKSPACE: RefCell<Vec<InRangeOption<EdgeId>>>);

pub fn customize<'a, 'b: 'a>(cch: &'a CCH, metric: &'b TDGraph) -> ShortcutGraph<'a> {
    report!("algo", "Floating TDCCH Customization");

    let n = (cch.first_out.len() - 1) as NodeId;
    let m = cch.head.len();

    let mut upward: Vec<_> = std::iter::repeat_with(|| Shortcut::new(None, metric)).take(m).collect();
    let mut downward: Vec<_> = std::iter::repeat_with(|| Shortcut::new(None, metric)).take(m).collect();

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
                    } in cch.inverted.neighbor_iter(current_node)
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

    let customize_perfect = |nodes: Range<usize>, upward: *mut Shortcut, downward: *mut Shortcut| {
        PERFECT_WORKSPACE.with(|node_edge_ids| {
            let mut node_edge_ids = node_edge_ids.borrow_mut();

            for current_node in nodes.rev() {
                let current_node = current_node as NodeId;
                for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                    node_edge_ids[node as usize] = InRangeOption::new(Some(edge_id));
                }

                for (node, edge_id) in cch.neighbor_iter(current_node).zip(cch.neighbor_edge_indices(current_node)) {
                    let shortcut_edge_ids = cch.neighbor_edge_indices(node);
                    for (target, shortcut_edge_id) in cch.neighbor_iter(node).zip(shortcut_edge_ids) {
                        if let Some(other_edge_id) = node_edge_ids[target as usize].value() {
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

                for node in cch.neighbor_iter(current_node) {
                    node_edge_ids[node as usize] = InRangeOption::new(None);
                }
            }
        });
    };

    let static_customization = SeperatorBasedParallelCustomization::new(cch, customize, customize);
    let static_perfect_customization = SeperatorBasedPerfectParallelCustomization::new(cch, customize_perfect, customize_perfect);

    let disable_dominated = |(shortcut, &lower_bound): (&mut Shortcut, &FlWeight)| {
        if shortcut.upper_bound.fuzzy_lt(lower_bound) {
            shortcut.required = false;
            shortcut.lower_bound = FlWeight::INFINITY;
            shortcut.upper_bound = FlWeight::INFINITY;
        } else {
            shortcut.lower_bound = lower_bound;
        }
    };

    if cfg!(feature = "tdcch-precustomization") {
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

    {
        let subctxt = push_context("main".to_string());

        use std::sync::mpsc::{channel, RecvTimeoutError};
        use std::thread;

        let (tx, rx) = channel();
        let (events_tx, events_rx) = channel();

        let customization = SeperatorBasedParallelCustomization::new(
            cch,
            create_customization_fn(&cch, metric, SeqIter(&cch)),
            create_customization_fn(&cch, metric, ParIter(&cch)),
        );

        report_time("TD-CCH Customization", || {
            thread::spawn(move || {
                let timer = Timer::new();

                let mut events = Vec::new();

                loop {
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

            customization.customize(&mut upward, &mut downward, |cb| {
                MERGE_BUFFERS.set(&RefCell::new(MergeBuffers::new()), || {
                    cb();
                });
            });
        });

        tx.send(()).unwrap();

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
                    shortcut.invalidate_unneccesary_sources(&shortcut_graph);
                }

                for shortcut in &mut downward_active[..] {
                    shortcut.invalidate_unneccesary_sources(&shortcut_graph);
                }
            }
        });
    }

    ShortcutGraph::new(metric, &cch.first_out, &cch.head, upward, downward)
}

fn create_customization_fn<'s, F: 's>(cch: &'s CCH, metric: &'s TDGraph, merge_iter: F) -> impl Fn(Range<usize>, usize, &mut [Shortcut], &mut [Shortcut]) + 's
where
    for<'p> F: ForEachIter<'p, 's>,
{
    move |nodes, edge_offset, upward: &mut [Shortcut], downward: &mut [Shortcut]| {
        for current_node in nodes {
            let (upward_below, upward_above) = upward.split_at_mut(cch.first_out[current_node as usize] as usize - edge_offset);
            let upward_active = &mut upward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
            let (downward_below, downward_above) = downward.split_at_mut(cch.first_out[current_node as usize] as usize - edge_offset);
            let downward_active = &mut downward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
            let shortcut_graph = PartialShortcutGraph::new(metric, upward_below, downward_below, edge_offset);

            debug_assert_eq!(upward_active.len(), cch.degree(current_node as NodeId));
            debug_assert_eq!(downward_active.len(), cch.degree(current_node as NodeId));

            merge_iter.for_each(
                current_node as NodeId,
                upward_active,
                downward_active,
                |((&node, upward_shortcut), downward_shortcut)| {
                    MERGE_BUFFERS.with(|buffers| {
                        let mut buffers = buffers.borrow_mut();

                        let mut triangles = Vec::new();

                        let mut current_iter = cch.inverted.neighbor_iter(current_node as NodeId).peekable();
                        let mut other_iter = cch.inverted.neighbor_iter(node as NodeId).peekable();

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

                            if lower_from_current < lower_from_other {
                                current_iter.next();
                            } else if lower_from_other < lower_from_current {
                                other_iter.next();
                            } else {
                                triangles.push((*edge_from_cur_id, *edge_from_oth_id));

                                current_iter.next();
                                other_iter.next();
                            }
                        }
                        if cfg!(feature = "tdcch-triangle-sorting") {
                            triangles.sort_by_key(|&(down, up)| shortcut_graph.get_incoming(down).lower_bound + shortcut_graph.get_outgoing(up).lower_bound);
                        }
                        for &edges in &triangles {
                            upward_shortcut.merge(edges, &shortcut_graph, &mut buffers);
                        }
                        upward_shortcut.finalize_bounds(&shortcut_graph);

                        if cfg!(feature = "tdcch-triangle-sorting") {
                            triangles.sort_by_key(|&(up, down)| shortcut_graph.get_incoming(down).lower_bound + shortcut_graph.get_outgoing(up).lower_bound);
                        }
                        for &(up, down) in &triangles {
                            downward_shortcut.merge((down, up), &shortcut_graph, &mut buffers);
                        }
                        downward_shortcut.finalize_bounds(&shortcut_graph);
                    });
                },
            );

            for Link { weight: edge_id, .. } in cch.inverted.neighbor_iter(current_node as NodeId) {
                upward[edge_id as usize - edge_offset].clear_plf();
                downward[edge_id as usize - edge_offset].clear_plf();
            }

            NODES_CUSTOMIZED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

trait ForEachIter<'s, 'c> {
    fn for_each(
        &self,
        current_node: NodeId,
        upward_active: &'s mut [Shortcut],
        downward_active: &'s mut [Shortcut],
        f: impl Send + Sync + Fn(((&'c NodeId, &'s mut Shortcut), &'s mut Shortcut)),
    );
}

struct SeqIter<'c>(&'c CCH);

impl<'s, 'c> ForEachIter<'s, 'c> for SeqIter<'c> {
    fn for_each(
        &self,
        current_node: NodeId,
        upward_active: &'s mut [Shortcut],
        downward_active: &'s mut [Shortcut],
        f: impl Send + Sync + Fn(((&'c NodeId, &'s mut Shortcut), &'s mut Shortcut)),
    ) {
        self.0.head[self.0.neighbor_edge_indices_usize(current_node)]
            .iter()
            .zip(upward_active.iter_mut())
            .zip(downward_active.iter_mut())
            .for_each(f);
    }
}

struct ParIter<'c>(&'c CCH);

impl<'s, 'c> ForEachIter<'s, 'c> for ParIter<'c> {
    fn for_each(
        &self,
        current_node: NodeId,
        upward_active: &'s mut [Shortcut],
        downward_active: &'s mut [Shortcut],
        f: impl Send + Sync + Fn(((&'c NodeId, &'s mut Shortcut), &'s mut Shortcut)),
    ) {
        self.0.head[self.0.neighbor_edge_indices_usize(current_node)]
            .par_iter()
            .zip_eq(upward_active.par_iter_mut())
            .zip_eq(downward_active.par_iter_mut())
            .for_each(f);
    }
}
