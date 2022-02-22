//! CATCHUp Customization

use super::*;
use crate::report::*;
use floating_time_dependent::*;
use std::{
    cmp::{min, Ordering as Ord},
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

pub struct PotData {
    pub fw_static_bound: Vec<(Weight, Weight)>,
    pub bw_static_bound: Vec<(Weight, Weight)>,
    pub fw_bucket_bounds: Vec<Weight>,
    pub bw_bucket_bounds: Vec<Weight>,
}

impl<'a> crate::io::Deconstruct for PotData {
    fn store_each(&self, store: &dyn Fn(&str, &dyn Store) -> std::io::Result<()>) -> std::io::Result<()> {
        store("fw_static_bound", &self.fw_static_bound)?;
        store("bw_static_bound", &self.bw_static_bound)?;
        store("fw_bucket_bounds", &self.fw_bucket_bounds)?;
        store("bw_bucket_bounds", &self.bw_bucket_bounds)?;
        Ok(())
    }
}

impl<'a> crate::io::Reconstruct for PotData {
    fn reconstruct_with(loader: Loader) -> std::io::Result<Self> {
        Ok(Self {
            fw_static_bound: loader.load("fw_static_bound")?,
            bw_static_bound: loader.load("bw_static_bound")?,
            fw_bucket_bounds: loader.load("fw_bucket_bounds")?,
            bw_bucket_bounds: loader.load("bw_bucket_bounds")?,
        })
    }
}

pub fn customize_internal<'a, 'b: 'a, const K: usize>(cch: &'a CCH, metric: &'b TDGraph) -> PotData {
    report!("algo", "Floating TDCCH Customization");

    let n = (cch.first_out.len() - 1) as NodeId;
    let m = cch.head.len();

    // these will contain our customized shortcuts
    let mut upward: Vec<_> = std::iter::repeat_with(|| Shortcut::new(None, metric)).take(m).collect();
    let mut downward: Vec<_> = std::iter::repeat_with(|| Shortcut::new(None, metric)).take(m).collect();

    let mut fw_bucket_weights = vec![[INFINITY; K]; m];
    let mut bw_bucket_weights = vec![[INFINITY; K]; m];

    // start with respecting - set shortcuts to respective original edge.
    let subctxt = push_context("weight_applying");
    report_time("TD-CCH apply weights", || {
        upward
            .par_iter_mut()
            .zip(cch.forward_cch_edge_to_orig_arc.par_iter())
            .for_each(|(up_weight, up_arcs)| {
                assert!(up_arcs.len() <= 1);
                for &EdgeIdT(up_arc) in up_arcs {
                    *up_weight = Shortcut::new(Some(up_arc), metric);
                }
            });
        downward
            .par_iter_mut()
            .zip(cch.backward_cch_edge_to_orig_arc.par_iter())
            .for_each(|(down_weight, down_arcs)| {
                assert!(down_arcs.len() <= 1);
                for &EdgeIdT(down_arc) in down_arcs {
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

                    for (NodeIdT(low_node), Reversed(EdgeIdT(first_edge_id))) in cch.inverted.link_iter(current_node) {
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
                                (*upward.add(other_edge_id as usize)).upper_bound = min(
                                    (*upward.add(other_edge_id as usize)).upper_bound,
                                    (*upward.add(edge_id as usize)).upper_bound + (*upward.add(shortcut_edge_id as usize)).upper_bound,
                                );

                                (*upward.add(edge_id as usize)).upper_bound = min(
                                    (*upward.add(edge_id as usize)).upper_bound,
                                    (*upward.add(other_edge_id as usize)).upper_bound + (*downward.add(shortcut_edge_id as usize)).upper_bound,
                                );

                                (*downward.add(other_edge_id as usize)).upper_bound = min(
                                    (*downward.add(other_edge_id as usize)).upper_bound,
                                    (*downward.add(edge_id as usize)).upper_bound + (*downward.add(shortcut_edge_id as usize)).upper_bound,
                                );

                                (*downward.add(edge_id as usize)).upper_bound = min(
                                    (*downward.add(edge_id as usize)).upper_bound,
                                    (*downward.add(other_edge_id as usize)).upper_bound + (*upward.add(shortcut_edge_id as usize)).upper_bound,
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

    // parallelize precusotmization
    let static_customization = SeperatorBasedParallelCustomization::new_undirected(cch, customize, customize);
    let static_perfect_customization = SeperatorBasedPerfectParallelCustomization::new(cch, customize_perfect, customize_perfect);

    // routine to disable shortcuts for which the perfect precustomization determined them to be irrelevant
    let disable_dominated = |shortcut: &mut Shortcut| {
        // shortcut contains shortest path length, lower bound the length of the specific path represented by the shortcut (not necessarily the shortest)
        if shortcut.upper_bound.fuzzy_lt(shortcut.lower_bound) {
            shortcut.required = false;
            shortcut.lower_bound = FlWeight::INFINITY;
            shortcut.upper_bound = FlWeight::INFINITY;
        }
    };

    if cfg!(feature = "tdcch-precustomization") {
        // execute CATCHUp precustomization
        let _subctxt = push_context("precustomization");
        report_time("TD-CCH Pre-Customization", || {
            static_customization.customize(&mut upward, &mut downward, |cb| {
                UPWARD_WORKSPACE.set(&RefCell::new(vec![(FlWeight::INFINITY, FlWeight::INFINITY); n as usize]), || {
                    DOWNWARD_WORKSPACE.set(&RefCell::new(vec![(FlWeight::INFINITY, FlWeight::INFINITY); n as usize]), cb);
                });
            });

            static_perfect_customization.customize(&mut upward, &mut downward, |cb| {
                PERFECT_WORKSPACE.set(&RefCell::new(vec![InRangeOption::NONE; n as usize]), cb);
            });

            upward.par_iter_mut().for_each(disable_dominated);
            downward.par_iter_mut().for_each(disable_dominated);
        });
    }

    // block for main CATCHUp customization
    {
        let subctxt = push_context("main");

        use std::sync::mpsc::{channel, RecvTimeoutError};
        use std::thread;

        let (tx, rx) = channel();
        let (events_tx, events_rx) = channel();

        // use separator based parallelization
        let customization = SeperatorBasedParallelCustomization::new_undirected_with_aux(
            cch,
            // routines created in this function
            // we customize many cells in parallel - so iterate over triangles sequentially
            create_customization_fn(cch, metric, SeqIter(cch)),
            // the final separator can only be customized, once everything else is done, but it still takes up a significant amount of time
            // But we can still parallelize the processing of edges from one node within this separator.
            create_customization_fn(cch, metric, ParIter(cch)),
        );

        report_time("TD-CCH Customization", || {
            // spawn of a thread, which periodically reports the state of things
            thread::spawn(move || {
                let timer = Timer::new();

                let mut events = Vec::new();

                loop {
                    // this actually reports to stderr and stores the data in the `events` `Vec`
                    report!("at_s", (timer.get_passed_ms() / 1000) as usize);
                    report!("nodes_customized", NODES_CUSTOMIZED.load(Ordering::Relaxed));
                    if cfg!(feature = "detailed-stats") {
                        report!("arcs_processed", ARCS_PROCESSED.load(Ordering::Relaxed));
                        report!("triangles_processed", TRIANGLES_PROCESSED.load(Ordering::Relaxed));
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
                            (timer.get_passed_ms() / 1000) as usize,
                            NODES_CUSTOMIZED.load(Ordering::Relaxed),
                            ARCS_PROCESSED.load(Ordering::Relaxed),
                            TRIANGLES_PROCESSED.load(Ordering::Relaxed),
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
                        events.push((
                            (timer.get_passed_ms() / 1000) as usize,
                            NODES_CUSTOMIZED.load(Ordering::Relaxed),
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                            0,
                        ));
                    }

                    if let Ok(()) | Err(RecvTimeoutError::Disconnected) = rx.recv_timeout(std::time::Duration::from_secs(3)) {
                        events.push((
                            (timer.get_passed_ms() / 1000) as usize,
                            NODES_CUSTOMIZED.load(Ordering::Relaxed),
                            ARCS_PROCESSED.load(Ordering::Relaxed),
                            TRIANGLES_PROCESSED.load(Ordering::Relaxed),
                            IPP_COUNT.load(Ordering::Relaxed),
                            ACTIVE_SHORTCUTS.load(Ordering::Relaxed),
                            SAVED_BY_APPROX.load(Ordering::Relaxed),
                            CONSIDERED_FOR_APPROX.load(Ordering::Relaxed),
                            PATH_SOURCES_COUNT.load(Ordering::Relaxed),
                            ACTUALLY_MERGED.load(Ordering::Relaxed),
                            ACTUALLY_LINKED.load(Ordering::Relaxed),
                            UNNECESSARY_LINKED.load(Ordering::Relaxed),
                        ));
                        events_tx.send(events).unwrap();
                        break;
                    }
                }
            });

            // execute main customization
            customization.customize_with_aux(&mut upward, &mut downward, &mut fw_bucket_weights, &mut bw_bucket_weights, |cb| {
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
            let mut events_ctxt = push_collection_context("events");

            for event in events {
                let _event = events_ctxt.push_collection_item();

                report_silent!("at_s", event.0);
                report_silent!("nodes_customized", event.1);
                if cfg!(feature = "detailed-stats") {
                    report_silent!("arcs_processed", event.2);
                    report_silent!("triangles_processed", event.3);
                    report_silent!("num_ipps_stored", event.4);
                    report_silent!("num_shortcuts_active", event.5);
                    report_silent!("num_ipps_reduced_by_approx", event.6);
                    report_silent!("num_ipps_considered_for_approx", event.7);
                    report_silent!("num_shortcut_merge_points", event.8);
                    report_silent!("num_performed_merges", event.9);
                    report_silent!("num_performed_links", event.10);
                    report_silent!("num_performed_unnecessary_links", event.11);
                }
            }
        }

        drop(subctxt);
    }

    if cfg!(feature = "detailed-stats") {
        report!("arcs_processed", ARCS_PROCESSED.load(Ordering::Relaxed));
        report!("triangles_processed", TRIANGLES_PROCESSED.load(Ordering::Relaxed));
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
        let _subctxt = push_context("postcustomization");
        report_time("TD-CCH Post-Customization", || {
            static_perfect_customization.customize(&mut upward, &mut downward, |cb| {
                PERFECT_WORKSPACE.set(&RefCell::new(vec![InRangeOption::NONE; n as usize]), cb);
            });

            // routine to disable shortcuts for which the perfect precustomization determined them to be irrelevant
            let disable_dominated = |shortcut: &mut Shortcut| {
                // shortcut contains shortest path length, lower bound the length of the specific path represented by the shortcut (not necessarily the shortest)
                if shortcut.upper_bound.fuzzy_lt(shortcut.lower_bound) {
                    shortcut.required = false;
                }
            };

            upward.par_iter_mut().for_each(disable_dominated);
            downward.par_iter_mut().for_each(disable_dominated);

            for current_node in 0..n {
                let (upward_below, upward_above) = upward.split_at_mut(cch.first_out[current_node as usize] as usize);
                let upward_active = &mut upward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
                let (downward_below, downward_above) = downward.split_at_mut(cch.first_out[current_node as usize] as usize);
                let downward_active = &mut downward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
                let shortcut_graph = PartialShortcutGraph::new(metric, upward_below, downward_below, 0);

                for shortcut in upward_active {
                    shortcut.disable_if_unneccesary(&shortcut_graph);
                }

                for shortcut in downward_active {
                    shortcut.disable_if_unneccesary(&shortcut_graph);
                }
            }

            for current_node in (0..n).rev() {
                let (upward_below, upward_above) = upward.split_at_mut(cch.first_out[current_node as usize] as usize);
                let upward_active = &mut upward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];
                let (downward_below, downward_above) = downward.split_at_mut(cch.first_out[current_node as usize] as usize);
                let downward_active = &mut downward_above[0..cch.neighbor_edge_indices(current_node as NodeId).len()];

                for shortcut in upward_active {
                    shortcut.reenable_required(downward_below, upward_below);
                }

                for shortcut in downward_active {
                    shortcut.reenable_required(downward_below, upward_below);
                }
            }

            upward.par_iter_mut().for_each(|s| {
                if !s.required {
                    s.lower_bound = FlWeight::INFINITY;
                    s.upper_bound = FlWeight::INFINITY;
                }
            });
            downward.par_iter_mut().for_each(|s| {
                if !s.required {
                    s.lower_bound = FlWeight::INFINITY;
                    s.upper_bound = FlWeight::INFINITY;
                }
            });

            upward
                .par_iter()
                .for_each(|s| debug_assert!(!s.required || s.lower_bound.fuzzy_lt(FlWeight::INFINITY)));
            downward
                .par_iter()
                .for_each(|s| debug_assert!(!s.required || s.lower_bound.fuzzy_lt(FlWeight::INFINITY)));
        });
    }

    let fw_static_bound: Vec<_> = upward
        .into_iter()
        .map(|s| (extract_lower_bound(s.lower_bound), extract_upper_bound(s.upper_bound)))
        .collect();
    let bw_static_bound: Vec<_> = downward
        .into_iter()
        .map(|s| (extract_lower_bound(s.lower_bound), extract_upper_bound(s.upper_bound)))
        .collect();

    PotData {
        fw_bucket_bounds: (0..K)
            .flat_map(|i| {
                fw_bucket_weights
                    .iter()
                    .zip(fw_static_bound.iter())
                    .map(move |(buckets, bound)| std::cmp::max(buckets[i], bound.0))
            })
            .collect(),
        bw_bucket_bounds: (0..K)
            .flat_map(|i| {
                bw_bucket_weights
                    .iter()
                    .zip(bw_static_bound.iter())
                    .map(move |(buckets, bound)| std::cmp::max(buckets[i], bound.0))
            })
            .collect(),
        fw_static_bound,
        bw_static_bound,
    }
}

// Encapsulates the creation of the CATCHUp main customization lambdas
// The function signature gives us some additional control of lifetimes and stuff
fn create_customization_fn<'s, F: 's, const K: usize>(
    cch: &'s CCH,
    metric: &'s TDGraph,
    merge_iter: F,
) -> impl Fn(Range<usize>, usize, &mut [Shortcut], &mut [Shortcut], &mut [[Weight; K]], &mut [[Weight; K]]) + 's
where
    for<'p> F: ForEachIter<'p, 's, Shortcut>,
{
    move |nodes, edge_offset, upward: &mut [Shortcut], downward: &mut [Shortcut], fw_buckets: &mut [[Weight; K]], bw_buckets: &mut [[Weight; K]]| {
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
                        let mut current_iter = cch.inverted.link_iter(current_node as NodeId).peekable();
                        let mut other_iter = cch.inverted.link_iter(node as NodeId).peekable();

                        while let (
                            Some((NodeIdT(lower_from_current), Reversed(EdgeIdT(edge_from_cur_id)))),
                            Some((NodeIdT(lower_from_other), Reversed(EdgeIdT(edge_from_oth_id)))),
                        ) = (current_iter.peek(), other_iter.peek())
                        {
                            debug_assert_eq!(cch.head()[*edge_from_cur_id as usize], current_node as NodeId);
                            debug_assert_eq!(cch.head()[*edge_from_oth_id as usize], node);
                            debug_assert_eq!(cch.edge_id_to_tail(*edge_from_cur_id), *lower_from_current);
                            debug_assert_eq!(cch.edge_id_to_tail(*edge_from_oth_id), *lower_from_other);

                            match lower_from_current.cmp(lower_from_other) {
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
                            if cfg!(feature = "detailed-stats") {
                                TRIANGLES_PROCESSED.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        upward_shortcut.finalize_bounds(&shortcut_graph);
                        if cfg!(feature = "detailed-stats") {
                            ARCS_PROCESSED.fetch_add(1, Ordering::Relaxed);
                        }

                        if cfg!(feature = "tdcch-triangle-sorting") {
                            triangles.sort_by_key(|&(up, down)| shortcut_graph.get_incoming(down).lower_bound + shortcut_graph.get_outgoing(up).lower_bound);
                        }
                        for &(up, down) in &triangles {
                            // an here
                            downward_shortcut.merge((down, up), &shortcut_graph, &mut buffers);
                            if cfg!(feature = "detailed-stats") {
                                TRIANGLES_PROCESSED.fetch_add(1, Ordering::Relaxed);
                            }
                        }
                        downward_shortcut.finalize_bounds(&shortcut_graph);
                        if cfg!(feature = "detailed-stats") {
                            ARCS_PROCESSED.fetch_add(1, Ordering::Relaxed);
                        }
                    });
                },
            );

            let shortcut_graph = PartialShortcutGraph::new(metric, upward, downward, edge_offset);
            for (_, Reversed(EdgeIdT(edge_id))) in cch.inverted.link_iter(current_node as NodeId) {
                let sc_up = &upward[edge_id as usize - edge_offset];
                let sc_down = &downward[edge_id as usize - edge_offset];
                for (i, (fw_bucket, bw_bucket)) in fw_buckets[edge_id as usize - edge_offset]
                    .iter_mut()
                    .zip(bw_buckets[edge_id as usize - edge_offset].iter_mut())
                    .enumerate()
                {
                    let start = Timestamp::new(i as f64 * f64::from(period()) / K as f64);
                    let end = Timestamp::new((i + 1) as f64 * f64::from(period()) / K as f64);
                    *fw_bucket = extract_lower_bound(
                        sc_up
                            .partial_ttf(&shortcut_graph, start, end)
                            .map(|plf| plf.bound_plfs().0.lower_bound())
                            .unwrap_or(sc_up.lower_bound),
                    );
                    *bw_bucket = extract_lower_bound(
                        sc_down
                            .partial_ttf(&shortcut_graph, start, end)
                            .map(|plf| plf.bound_plfs().0.lower_bound())
                            .unwrap_or(sc_down.lower_bound),
                    );
                }
            }

            // free up space - we will never need the explicit functions again during customization
            for (_, Reversed(EdgeIdT(edge_id))) in cch.inverted.link_iter(current_node as NodeId) {
                upward[edge_id as usize - edge_offset].clear_plf();
                downward[edge_id as usize - edge_offset].clear_plf();
            }

            NODES_CUSTOMIZED.fetch_add(1, Ordering::Relaxed);
        }
    }
}

fn extract_lower_bound(w: FlWeight) -> Weight {
    if w.fuzzy_lt(FlWeight::INFINITY) {
        (f64::from(w) * 1000.0 - EPSILON).floor() as Weight
    } else {
        INFINITY
    }
}

fn extract_upper_bound(w: FlWeight) -> Weight {
    if w.fuzzy_lt(FlWeight::INFINITY) {
        (f64::from(w) * 1000.0 + EPSILON).floor() as Weight
    } else {
        INFINITY
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
    S: Send,
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
