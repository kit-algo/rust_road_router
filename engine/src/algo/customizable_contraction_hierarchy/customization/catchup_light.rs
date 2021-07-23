use super::*;
use crate::{
    datastr::{clearlist_vector::ClearlistVector, graph::floating_time_dependent::ShortcutId, rank_select_map::*},
    report::*,
    util::with_index,
};
use floating_time_dependent::shortcut_source::{ShortcutSource, ShortcutSourceData};
use std::cmp::{min, Ordering as Ord};
use time_dependent::*;

// Workspaces for static CATCHUp precustomization - similar to regular static customization - see parent module
scoped_thread_local!(static UPWARD_WORKSPACE: RefCell<Vec<(Weight, Weight)>>);
scoped_thread_local!(static DOWNWARD_WORKSPACE: RefCell<Vec<(Weight, Weight)>>);
// Workspace for perfect static CATCHUp precustomization - here we just need one for both directions
// because we map to the edge id instead of the values.
scoped_thread_local!(static PERFECT_WORKSPACE: RefCell<Vec<InRangeOption<EdgeId>>>);

/// Run CATCHUp customization
pub fn customize<'a, 'b: 'a>(cch: &'a CCH, metric: &'b TDGraph) -> CustomizedGraph<'a> {
    report!("algo", "CATCHUp light Customization");

    let n = (cch.first_out.len() - 1) as NodeId;
    let m = cch.head.len();

    // these will contain our customized shortcuts
    let mut upward: Vec<_> = std::iter::repeat_with(|| (INFINITY, INFINITY)).take(m).collect();
    let mut downward: Vec<_> = std::iter::repeat_with(|| (INFINITY, INFINITY)).take(m).collect();

    // start with respecting - set shortcuts to respective original edge.
    let subctxt = push_context("weight_applying".to_string());
    report_time("Apply weights", || {
        upward
            .par_iter_mut()
            .zip(downward.par_iter_mut())
            .zip(cch.cch_edge_to_orig_arc.par_iter())
            .for_each(|(((up_lower, up_upper), (down_lower, down_upper)), (up_arcs, down_arcs))| {
                for &EdgeIdT(up_arc) in up_arcs {
                    *up_lower = min(*up_lower, metric.travel_time_function(up_arc).lower_bound());
                    *up_upper = min(*up_upper, metric.travel_time_function(up_arc).upper_bound());
                }
                for &EdgeIdT(down_arc) in down_arcs {
                    *down_lower = min(*down_lower, metric.travel_time_function(down_arc).lower_bound());
                    *down_upper = min(*down_upper, metric.travel_time_function(down_arc).upper_bound());
                }
            });
    });
    drop(subctxt);

    // This is the routine for basic static customization with just the upper and lower bounds.
    // It runs completely analogue the standard customization algorithm.
    let customize = |nodes: Range<usize>, offset, upward_weights: &mut [(Weight, Weight)], downward_weights: &mut [(Weight, Weight)]| {
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
                        node_incoming_weights[node as usize] = (down.0, down.1);
                        node_outgoing_weights[node as usize] = (up.0, up.1);
                    }

                    for (NodeIdT(low_node), Reversed(EdgeIdT(first_edge_id))) in cch.inverted.link_iter(current_node) {
                        let first_down_weight: &(Weight, Weight) = &downward_weights[first_edge_id as usize - offset];
                        let first_up_weight: &(Weight, Weight) = &upward_weights[first_edge_id as usize - offset];
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
                            relax.0 = std::cmp::min(relax.0, upward_weight.0 + first_down_weight.0);
                            relax.1 = std::cmp::min(relax.1, upward_weight.1 + first_down_weight.1);
                            let relax = unsafe { node_incoming_weights.get_unchecked_mut(node as usize) };
                            relax.0 = std::cmp::min(relax.0, downward_weight.0 + first_up_weight.0);
                            relax.1 = std::cmp::min(relax.1, downward_weight.1 + first_up_weight.1);
                        }
                    }

                    for (((node, down), up), _edge_id) in cch
                        .neighbor_iter(current_node)
                        .zip(&mut downward_weights[edges.clone()])
                        .zip(&mut upward_weights[edges.clone()])
                        .zip(edges)
                    {
                        down.0 = node_incoming_weights[node as usize].0;
                        down.1 = node_incoming_weights[node as usize].1;
                        up.0 = node_outgoing_weights[node as usize].0;
                        up.1 = node_outgoing_weights[node as usize].1;
                    }
                }
            });
        });
    };

    // Routine for CATCHUp light perfect precustomization on the bounds.
    // The interface is similar to the one for the basic customization, but we need access to nonconsecutive ranges of edges,
    // so we can't use slices. Thus, we just take a mutable pointer to the shortcut vecs.
    // The logic of the perfect customization based on separators guarantees, that we will never concurrently modify
    // the same shortcuts, but so far I haven't found a way to express that in safe rust.
    let customize_perfect = |nodes: Range<usize>, upward: *mut (Weight, Weight), downward: *mut (Weight, Weight)| {
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
                                (*upward.add(other_edge_id as usize)).1 = min(
                                    (*upward.add(other_edge_id as usize)).1,
                                    (*upward.add(edge_id as usize)).1 + (*upward.add(shortcut_edge_id as usize)).1,
                                );
                                (*upward.add(edge_id as usize)).1 = min(
                                    (*upward.add(edge_id as usize)).1,
                                    (*upward.add(other_edge_id as usize)).1 + (*downward.add(shortcut_edge_id as usize)).1,
                                );
                                (*downward.add(other_edge_id as usize)).1 = min(
                                    (*downward.add(other_edge_id as usize)).1,
                                    (*downward.add(edge_id as usize)).1 + (*downward.add(shortcut_edge_id as usize)).1,
                                );
                                (*downward.add(edge_id as usize)).1 = min(
                                    (*downward.add(edge_id as usize)).1,
                                    (*downward.add(other_edge_id as usize)).1 + (*upward.add(shortcut_edge_id as usize)).1,
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
    let disable_dominated = |(lower, upper): &mut (Weight, Weight)| {
        // shortcut contains shortest path length, lower bound the length of the specific path represented by the shortcut (not necessarily the shortest)
        if upper < lower {
            *lower = INFINITY;
            *upper = INFINITY;
        }
    };

    let subctxt = push_context("customization".to_string());
    report_time("CATCHUp light Customization", || {
        static_customization.customize(&mut upward, &mut downward, |cb| {
            UPWARD_WORKSPACE.set(&RefCell::new(vec![(INFINITY, INFINITY); n as usize]), || {
                DOWNWARD_WORKSPACE.set(&RefCell::new(vec![(INFINITY, INFINITY); n as usize]), || cb());
            });
        });

        static_perfect_customization.customize(&mut upward, &mut downward, |cb| {
            PERFECT_WORKSPACE.set(&RefCell::new(vec![InRangeOption::new(None); n as usize]), || cb());
        });

        upward.par_iter_mut().for_each(disable_dominated);
        downward.par_iter_mut().for_each(disable_dominated);
    });
    drop(subctxt);

    let mut upward_sources: Vec<Vec<(ShortcutSourceData, Weight, Weight)>> = vec![Vec::new(); m];
    let mut downward_sources: Vec<Vec<(ShortcutSourceData, Weight, Weight)>> = vec![Vec::new(); m];

    let subctxt = push_context("triangle_listing".to_string());
    report_time("Triangle Listing", || {
        upward_sources
            .iter_mut()
            .zip(downward_sources.iter_mut())
            .zip(0..m)
            .for_each(|((up_sources, down_sources), edge_id)| {
                let tail = cch.tail()[edge_id];
                let head = cch.head()[edge_id];

                let mut tail_iter = cch.inverted.link_iter(tail).peekable();
                let mut head_iter = cch.inverted.link_iter(head).peekable();

                while let (
                    Some(&(NodeIdT(lower_from_tail), Reversed(EdgeIdT(edge_from_tail)))),
                    Some(&(NodeIdT(lower_from_head), Reversed(EdgeIdT(edge_from_head)))),
                ) = (tail_iter.peek(), head_iter.peek())
                {
                    let edge_from_tail = edge_from_tail as usize;
                    let edge_from_head = edge_from_head as usize;
                    debug_assert_eq!(cch.head()[edge_from_tail], tail);
                    debug_assert_eq!(cch.head()[edge_from_head], head);
                    debug_assert_eq!(cch.tail()[edge_from_tail], lower_from_tail);
                    debug_assert_eq!(cch.tail()[edge_from_head], lower_from_head);

                    match lower_from_tail.cmp(&lower_from_head) {
                        Ord::Less => tail_iter.next(),
                        Ord::Greater => head_iter.next(),
                        Ord::Equal => {
                            if downward[edge_from_tail].0 + upward[edge_from_head].0 <= upward[edge_id].1 {
                                up_sources.push((
                                    ShortcutSource::Shortcut(edge_from_tail as EdgeId, edge_from_head as EdgeId).into(),
                                    downward[edge_from_tail].0 + upward[edge_from_head].0,
                                    downward[edge_from_tail].1 + upward[edge_from_head].1,
                                ));
                            }

                            if downward[edge_from_head].0 + upward[edge_from_tail].0 <= downward[edge_id].1 {
                                down_sources.push((
                                    ShortcutSource::Shortcut(edge_from_head as EdgeId, edge_from_tail as EdgeId).into(),
                                    downward[edge_from_head].0 + upward[edge_from_tail].0,
                                    downward[edge_from_head].1 + upward[edge_from_tail].1,
                                ));
                            }

                            tail_iter.next();
                            head_iter.next()
                        }
                    };
                }

                let (up_arcs, down_arcs) = &cch.cch_edge_to_orig_arc[edge_id];

                for &EdgeIdT(up_arc) in up_arcs {
                    if metric.travel_time_function(up_arc).lower_bound() <= upward[edge_id].1 {
                        up_sources.push((
                            ShortcutSource::OriginalEdge(up_arc).into(),
                            metric.travel_time_function(up_arc).lower_bound(),
                            metric.travel_time_function(up_arc).upper_bound(),
                        ));
                    }
                }
                for &EdgeIdT(down_arc) in down_arcs {
                    if metric.travel_time_function(down_arc).lower_bound() <= downward[edge_id].1 {
                        down_sources.push((
                            ShortcutSource::OriginalEdge(down_arc).into(),
                            metric.travel_time_function(down_arc).lower_bound(),
                            metric.travel_time_function(down_arc).upper_bound(),
                        ));
                    }
                }

                if upward[edge_id].0 >= INFINITY {
                    up_sources.clear();
                }
                if downward[edge_id].0 >= INFINITY {
                    down_sources.clear();
                }

                up_sources.sort_unstable_by_key(|&(_, lower, _)| lower);
                down_sources.sort_unstable_by_key(|&(_, lower, _)| lower);

                if up_sources.is_empty() {
                    upward[edge_id] = (INFINITY, INFINITY);
                }
                if down_sources.is_empty() {
                    downward[edge_id] = (INFINITY, INFINITY);
                }

                debug_assert!(
                    upward[edge_id].0 == INFINITY || !up_sources.is_empty(),
                    "Upward: {}, {:?}",
                    edge_id,
                    upward[edge_id]
                );
                debug_assert!(
                    downward[edge_id].0 == INFINITY || !down_sources.is_empty(),
                    "Downward: {}, {:?}",
                    edge_id,
                    downward[edge_id]
                );
            });
    });
    drop(subctxt);

    report!("cch_arcs", cch.num_arcs());
    report!("up_necessary_arcs", upward_sources.iter().filter(|s| !s.is_empty()).count());
    report!(
        "up_mean_num_expansions",
        (upward_sources.iter().map(Vec::len).sum::<usize>() as f64) / (upward_sources.iter().filter(|s| !s.is_empty()).count() as f64)
    );
    report!("up_num_arcs_with_one_expansion", upward_sources.iter().filter(|s| s.len() == 1).count());
    report!("up_max_num_expansions", upward_sources.iter().map(Vec::len).max().unwrap());
    report!("down_necessary_arcs", downward_sources.iter().filter(|s| !s.is_empty()).count());
    report!(
        "down_mean_num_expansions",
        (downward_sources.iter().map(Vec::len).sum::<usize>() as f64) / (downward_sources.iter().filter(|s| !s.is_empty()).count() as f64)
    );
    report!("down_num_arcs_with_one_expansion", downward_sources.iter().filter(|s| s.len() == 1).count());
    report!("down_max_num_expansions", downward_sources.iter().map(Vec::len).max().unwrap());

    for (edge_id, (sources, bounds)) in upward_sources
        .iter()
        .zip(upward.iter())
        .chain(downward_sources.iter().zip(downward.iter()))
        .enumerate()
    {
        debug_assert!(bounds.0 <= bounds.1);
        debug_assert!(
            (sources.is_empty() && bounds.0 == INFINITY) || (!sources.is_empty() && bounds.0 < INFINITY),
            "{:#?}",
            bounds
        );
        for (source, _, _) in sources {
            if let ShortcutSource::Shortcut(down, up) = ShortcutSource::from(*source) {
                debug_assert!(edge_id as EdgeId > down);
                debug_assert!(edge_id as EdgeId > up);
            }
        }
    }

    CustomizedGraph::from(cch, metric, (upward, downward), (upward_sources, downward_sources))
}

pub struct CustomizedGraph<'a> {
    pub original_graph: &'a TDGraph,
    upward: CustomizedSingleDirGraph,
    downward: CustomizedSingleDirGraph,
}

impl<'a> CustomizedGraph<'a> {
    fn from(
        cch: &CCH,
        metric: &'a TDGraph,
        (mut up_bounds, mut down_bounds): (Vec<(Weight, Weight)>, Vec<(Weight, Weight)>),
        (mut up_sources, mut down_sources): (Vec<Vec<(ShortcutSourceData, Weight, Weight)>>, Vec<Vec<(ShortcutSourceData, Weight, Weight)>>),
    ) -> Self {
        let mut outgoing_required = BitVec::new(cch.num_arcs());
        let mut incoming_required = BitVec::new(cch.num_arcs());

        for (idx, s) in up_sources.iter().enumerate() {
            if !s.is_empty() {
                outgoing_required.set(idx)
            }
        }

        for (idx, s) in down_sources.iter().enumerate() {
            if !s.is_empty() {
                incoming_required.set(idx)
            }
        }

        let mut outgoing_first_out = Vec::with_capacity(cch.first_out.len());
        let mut incoming_first_out = Vec::with_capacity(cch.first_out.len());
        let mut outgoing_head = Vec::with_capacity(cch.num_arcs());
        let mut incoming_head = Vec::with_capacity(cch.num_arcs());

        outgoing_first_out.push(0);
        incoming_first_out.push(0);

        for range in cch.first_out.windows(2) {
            let range = range[0] as usize..range[1] as usize;
            outgoing_head.extend(
                cch.head[range.clone()]
                    .iter()
                    .zip(up_sources[range.clone()].iter())
                    .filter(|(_head, s)| !s.is_empty())
                    .map(|(head, _)| head),
            );
            outgoing_first_out.push(outgoing_first_out.last().unwrap() + up_sources[range.clone()].iter().filter(|s| !s.is_empty()).count() as u32);

            incoming_head.extend(
                cch.head[range.clone()]
                    .iter()
                    .zip(down_sources[range.clone()].iter())
                    .filter(|(_head, s)| !s.is_empty())
                    .map(|(head, _)| head),
            );
            incoming_first_out.push(incoming_first_out.last().unwrap() + down_sources[range.clone()].iter().filter(|s| !s.is_empty()).count() as u32);
        }

        let mut outgoing_tail = vec![0 as NodeId; outgoing_head.len()];
        for (node, range) in outgoing_first_out.windows(2).enumerate() {
            for tail in &mut outgoing_tail[range[0] as usize..range[1] as usize] {
                *tail = node as NodeId;
            }
        }

        let mut incoming_tail = vec![0 as NodeId; incoming_head.len()];
        for (node, range) in incoming_first_out.windows(2).enumerate() {
            for tail in &mut incoming_tail[range[0] as usize..range[1] as usize] {
                *tail = node as NodeId;
            }
        }

        up_bounds.retain(with_index(|index, _| outgoing_required.get(index)));
        up_sources.retain(with_index(|index, _| outgoing_required.get(index)));
        down_bounds.retain(with_index(|index, _| incoming_required.get(index)));
        down_sources.retain(with_index(|index, _| incoming_required.get(index)));

        let mapping_outgoing = RankSelectMap::new(outgoing_required);
        let mapping_incoming = RankSelectMap::new(incoming_required);

        for sources in up_sources.iter_mut().chain(down_sources.iter_mut()) {
            for (s, _, _) in sources {
                if let ShortcutSource::Shortcut(down, up) = ShortcutSource::from(*s) {
                    *s = ShortcutSource::Shortcut(
                        mapping_incoming.get(down as usize).unwrap() as EdgeId,
                        mapping_outgoing.get(up as usize).unwrap() as EdgeId,
                    )
                    .into();
                }
            }
        }

        for (edge_id, sources) in up_sources.iter().enumerate() {
            for (source, _, _) in sources {
                if let ShortcutSource::Shortcut(_down, up) = ShortcutSource::from(*source) {
                    debug_assert!(edge_id as EdgeId > up);
                }
            }
        }
        for (edge_id, sources) in down_sources.iter().enumerate() {
            for (source, _, _) in sources {
                if let ShortcutSource::Shortcut(down, _up) = ShortcutSource::from(*source) {
                    debug_assert!(edge_id as EdgeId > down);
                }
            }
        }

        Self {
            original_graph: metric,

            upward: CustomizedSingleDirGraph {
                first_out: outgoing_first_out,
                head: outgoing_head,
                tail: outgoing_tail,

                bounds: up_bounds,
                first_source: first_out_graph::degrees_to_first_out(up_sources.iter().map(|s| s.len() as u32)).collect(),
                sources: up_sources.into_iter().flat_map(Vec::into_iter).collect(),
            },

            downward: CustomizedSingleDirGraph {
                first_out: incoming_first_out,
                head: incoming_head,
                tail: incoming_tail,

                bounds: down_bounds,
                first_source: first_out_graph::degrees_to_first_out(down_sources.iter().map(|s| s.len() as u32)).collect(),
                sources: down_sources.into_iter().flat_map(Vec::into_iter).collect(),
            },
        }
    }

    pub fn evaluate_next_segment_at<F, G>(
        &self,
        edge_id: ShortcutId,
        t: Timestamp,
        lower_bounds_to_target: &mut ClearlistVector<Weight>,
        upper_bounds_from_source: &mut ClearlistVector<Weight>,
        down_relaxed: &mut FastClearBitVec,
        mark_upward: &mut F,
        evaluated: &mut G,
    ) where
        F: FnMut(EdgeId),
        G: FnMut(Weight, NodeIdT, ShortcutId, Weight),
    {
        if let ShortcutId::Incoming(edge_id) = edge_id {
            if down_relaxed.get(edge_id as usize) {
                return;
            }
            down_relaxed.set(edge_id as usize);
        }
        let &head = edge_id.get_from(&self.downward.tail, &self.upward.head);
        let lower_bound_to_target = lower_bounds_to_target[head as usize];
        let &(self_lower, self_upper) = edge_id.get_from(&self.downward.bounds, &self.upward.bounds);
        if self_lower == self_upper {
            evaluated(self_lower, NodeIdT(head), edge_id, lower_bounds_to_target[head as usize]);
            return;
        }

        if t + self_lower > upper_bounds_from_source[head as usize] {
            return;
        }

        let upward_sources = Slcs(&self.upward.first_source, &self.upward.sources);
        let downward_sources = Slcs(&self.downward.first_source, &self.downward.sources);
        let sources = edge_id.get_with(&downward_sources, &upward_sources, |dir, edge_id| &dir[edge_id as usize]);

        for &(source, source_lower, _source_upper) in sources {
            if t + source_lower > upper_bounds_from_source[head as usize] {
                return;
            }

            match source.into() {
                ShortcutSource::Shortcut(down, up) => {
                    mark_upward(up);
                    let middle = self.downward.tail[down as usize] as usize;
                    let lower_bound_to_middle = self.upward.bounds[up as usize].0 + lower_bound_to_target;
                    lower_bounds_to_target[middle] = min(lower_bounds_to_target[middle], lower_bound_to_middle);
                    self.evaluate_next_segment_at(
                        ShortcutId::Incoming(down),
                        t,
                        lower_bounds_to_target,
                        upper_bounds_from_source,
                        down_relaxed,
                        mark_upward,
                        evaluated,
                    );
                    upper_bounds_from_source[head as usize] = min(
                        upper_bounds_from_source[head as usize],
                        upper_bounds_from_source[middle as usize] + self.upward.bounds[up as usize].1,
                    );
                }
                ShortcutSource::OriginalEdge(edge) => {
                    let tt = self.original_graph.travel_time_function(edge).eval(t);
                    evaluated(tt, NodeIdT(head), edge_id, lower_bounds_to_target[head as usize]);
                    upper_bounds_from_source[head as usize] = min(upper_bounds_from_source[head as usize], t + tt);
                }
                _ => unreachable!(),
            }
        }
    }

    pub fn upward_bounds_graph(&self) -> SingleDirBoundsGraph {
        SingleDirBoundsGraph {
            first_out: &self.upward.first_out,
            head: &self.upward.head,
            bounds: &self.upward.bounds,
        }
    }

    pub fn downward_bounds_graph(&self) -> SingleDirBoundsGraph {
        SingleDirBoundsGraph {
            first_out: &self.downward.first_out,
            head: &self.downward.head,
            bounds: &self.downward.bounds,
        }
    }
}

pub struct CustomizedSingleDirGraph {
    first_out: Vec<EdgeId>,
    head: Vec<NodeId>,
    tail: Vec<NodeId>,

    bounds: Vec<(Weight, Weight)>,
    first_source: Vec<u32>,
    sources: Vec<(ShortcutSourceData, Weight, Weight)>,
}

pub struct SingleDirBoundsGraph<'a> {
    first_out: &'a [EdgeId],
    head: &'a [NodeId],
    pub bounds: &'a [(Weight, Weight)],
}

impl<'a> SingleDirBoundsGraph<'a> {
    pub fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }

    pub fn degree(&self, node: NodeId) -> usize {
        self.neighbor_edge_indices_usize(node).len()
    }

    fn neighbor_edge_indices_usize(&self, node: NodeId) -> Range<usize> {
        (self.first_out[node as usize] as usize)..(self.first_out[(node + 1) as usize] as usize)
    }

    pub fn neighbor_iter(&self, node: NodeId) -> impl Iterator<Item = ((NodeId, EdgeId), &(Weight, Weight))> {
        let range = self.neighbor_edge_indices_usize(node);
        let edge_ids = range.start as EdgeId..range.end as EdgeId;
        self.head[range.clone()].iter().copied().zip(edge_ids).zip(self.bounds[range].iter())
    }
}
