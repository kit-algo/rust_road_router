use super::a_star::*;
use super::customizable_contraction_hierarchy::{query::stepped_elimination_tree::EliminationTreeWalk, *};
use super::*;
use crate::{
    datastr::{graph::time_dependent::*, timestamped_vector::*},
    report::*,
    util::in_range_option::*,
};

use std::cmp::{max, min};

use rayon::prelude::*;

pub trait TDPotential {
    fn init(&mut self, source: NodeId, target: NodeId, departure: Timestamp);
    fn potential(&mut self, node: NodeId, t: Option<Timestamp>) -> Option<Weight>;
    fn report_stats(&self) {}
}

impl<T: Potential> TDPotential for T {
    fn init(&mut self, _source: NodeId, target: NodeId, _departure: Timestamp) {
        self.init(target)
    }
    fn potential(&mut self, node: NodeId, _t: Option<Timestamp>) -> Option<Weight> {
        self.potential(node)
    }
}

#[derive(Clone)]
pub struct TDPotentialForPermutated<P> {
    pub potential: P,
    pub order: NodeOrder,
}

impl<P> TDPotentialForPermutated<P> {
    pub fn inner(&self) -> &P {
        &self.potential
    }
}

impl<P: TDPotential> TDPotential for TDPotentialForPermutated<P> {
    fn init(&mut self, source: NodeId, target: NodeId, departure: Timestamp) {
        self.potential.init(self.order.node(source), self.order.node(target), departure)
    }

    fn potential(&mut self, node: NodeId, t: Option<Timestamp>) -> Option<Weight> {
        self.potential.potential(self.order.node(node), t)
    }

    fn report_stats(&self) {
        self.potential.report_stats()
    }
}

use std::ops::Range;

pub fn ranges() -> Vec<Range<Timestamp>> {
    // 1*24h
    let mut ranges = vec![0..24 * 60 * 60 * 1000];
    // 48*1h
    let half_an_hour = 30 * 60 * 1000;
    for i in 0..48 {
        ranges.push(i * half_an_hour..(i + 3) * half_an_hour);
    }
    // 24*2h
    let hour = 2 * half_an_hour;
    for i in 0..24 {
        ranges.push(i * hour..(i + 3) * hour);
    }
    // 12*4h
    let two_hours = 2 * hour;
    for i in 0..12 {
        ranges.push(i * two_hours..(i + 3) * two_hours);
    }
    // 6*8h
    let four_hours = 4 * hour;
    for i in 0..6 {
        ranges.push(i * four_hours..(i + 3) * four_hours);
    }
    // 4*12h
    let six_hours = 6 * hour;
    for i in 0..4 {
        ranges.push(i * six_hours..(i + 3) * six_hours);
    }
    ranges
}

pub struct MultiMetric<'a> {
    cch: &'a CCH,
    metric_ranges: Vec<(Range<Timestamp>, usize)>,
    fw_graph: UnweightedOwnedGraph,
    bw_graph: UnweightedOwnedGraph,
    fw_metrics: Vec<Weight>,
    bw_metrics: Vec<Weight>,
    stack: Vec<NodeId>,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    backward_distances: TimestampedVector<Weight>,
    backward_parents: Vec<(NodeId, EdgeId)>,
    upper_bound_dist: query::Server<CustomizedPerfect<'a, CCH>>,
    num_pot_computations: usize,
    current_metric: Option<usize>,
}

impl<'a> MultiMetric<'a> {
    pub fn build(cch: &'a CCH, metric_ranges: Vec<Range<Timestamp>>, graph: &TDGraph) -> Self {
        let metrics: Vec<Box<[_]>> = metric_ranges
            .iter()
            .map(|r| {
                (0..graph.num_arcs())
                    .map(|e| graph.travel_time_function(e as EdgeId).lower_bound_in_range(r.clone()))
                    .collect()
            })
            .collect();
        let metric_ranges = metric_ranges.into_iter().enumerate().map(|(i, r)| (r, i)).collect();
        let upper_bound: Box<[_]> = (0..graph.num_arcs()).map(|e| graph.travel_time_function(e as EdgeId).upper_bound()).collect();
        let metric_graphs: Box<[_]> = metrics.iter().map(|w| BorrowedGraph::new(graph.first_out(), graph.head(), w)).collect();
        Self::new(
            cch,
            metric_ranges,
            &metric_graphs,
            BorrowedGraph::new(graph.first_out(), graph.head(), &upper_bound),
        )
    }

    pub fn new(cch: &'a CCH, metric_ranges: Vec<(Range<Timestamp>, usize)>, metrics: &[BorrowedGraph], upper_bound: BorrowedGraph) -> Self {
        let n = cch.num_nodes();
        let m = cch.num_arcs();

        let mut upper_bound_customized = customize(cch, &upper_bound);
        let modified = customization::customize_perfect_without_rebuild(&mut upper_bound_customized);

        let global_lower_customized = customize(cch, &metrics[0]);

        let mut fw_first_out = vec![0];
        let mut bw_first_out = vec![0];
        let mut fw_head = Vec::with_capacity(m);
        let mut bw_head = Vec::with_capacity(m);

        for node in 0..n {
            for edge_id in cch.neighbor_edge_indices_usize(node) {
                if upper_bound_customized.forward_graph().weight()[edge_id] >= global_lower_customized.forward_graph().weight()[edge_id] {
                    fw_head.push(cch.head()[edge_id]);
                }
                if upper_bound_customized.backward_graph().weight()[edge_id] >= global_lower_customized.backward_graph().weight()[edge_id] {
                    bw_head.push(cch.head()[edge_id]);
                }
            }
            fw_first_out.push(fw_head.len() as EdgeId);
            bw_first_out.push(bw_head.len() as EdgeId);
        }

        let (mut fw_metrics, mut bw_metrics) = metrics
            .iter()
            .flat_map(|m| {
                let customized = customize(cch, m);
                customized
                    .forward_graph()
                    .weight()
                    .iter()
                    .copied()
                    .zip(customized.backward_graph().weight().iter().copied())
            })
            .unzip();

        fw_metrics.retain(crate::util::with_index(|idx, _| {
            upper_bound_customized.forward_graph().weight()[idx] >= global_lower_customized.forward_graph().weight()[idx]
        }));
        bw_metrics.retain(crate::util::with_index(|idx, _| {
            upper_bound_customized.backward_graph().weight()[idx] >= global_lower_customized.backward_graph().weight()[idx]
        }));

        Self {
            cch,
            metric_ranges,
            fw_graph: UnweightedOwnedGraph::new(fw_first_out, fw_head),
            bw_graph: UnweightedOwnedGraph::new(bw_first_out, bw_head),
            fw_metrics,
            bw_metrics,
            stack: Vec::new(),
            backward_distances: TimestampedVector::new(n),
            backward_parents: vec![(n as NodeId, m as EdgeId); n],
            potentials: TimestampedVector::new(n),
            num_pot_computations: 0,
            current_metric: None,
            upper_bound_dist: query::Server::new(customization::rebuild_customized_perfect(upper_bound_customized, modified.0, modified.1)),
        }
    }
}

fn range_included(covered: &Range<Timestamp>, to_cover: &Range<Timestamp>) -> bool {
    use crate::datastr::graph::time_dependent::math::RangeExtensions;
    debug_assert!(covered.start < period());
    debug_assert!(to_cover.start < period());
    let (covered_first, mut covered_second) = covered.clone().split(period());
    let (to_cover_first, mut to_cover_second) = to_cover.clone().split(period());
    covered_second.start %= period();
    covered_second.end %= period();
    to_cover_second.start %= period();
    to_cover_second.end %= period();
    (to_cover_first.is_empty() || range_contains(&covered_first, &to_cover_first) || range_contains(&covered_second, &to_cover_first))
        && (to_cover_second.is_empty() || range_contains(&covered_first, &to_cover_second) || range_contains(&covered_second, &to_cover_second))
}

fn range_contains(covered: &Range<Timestamp>, to_cover: &Range<Timestamp>) -> bool {
    covered.start <= to_cover.start && covered.end >= to_cover.end
}

impl TDPotential for MultiMetric<'_> {
    fn init(&mut self, source: NodeId, target: NodeId, departure: Timestamp) {
        let departure = departure % period();
        self.num_pot_computations = 0;
        self.potentials.reset();

        if let Some(upper_bound) = self.upper_bound_dist.query(Query { from: source, to: target }).distance() {
            let latest_arrival = departure + upper_bound;
            let mut best_range: Option<(Range<Timestamp>, usize)> = None;
            for (range, idx) in &self.metric_ranges {
                if range_included(range, &(departure..latest_arrival + 1)) {
                    if let Some(best_range) = &mut best_range {
                        if range.end - range.start < best_range.0.end - best_range.0.start {
                            *best_range = (range.clone(), *idx);
                        }
                    } else {
                        best_range = Some((range.clone(), *idx));
                    }
                }
            }

            let metric_idx = best_range.as_ref().unwrap().1;
            let weights = &self.bw_metrics[self.bw_graph.num_arcs() * metric_idx..self.bw_graph.num_arcs() * (metric_idx + 1)];
            let bw_graph = BorrowedGraph::new(self.bw_graph.first_out(), self.bw_graph.head(), weights);

            let target = self.cch.node_order().rank(target);

            let mut walk = EliminationTreeWalk::query(
                &bw_graph,
                self.cch.elimination_tree(),
                &mut self.backward_distances,
                &mut self.backward_parents,
                target,
            );

            while let Some(node) = walk.peek() {
                if walk.tentative_distance(node) > upper_bound {
                    walk.reset_distance(node);
                    walk.skip_next();
                } else {
                    walk.next();
                }
            }

            self.current_metric = Some(best_range.unwrap().1);
        } else {
            self.current_metric = None;
        }
    }
    fn potential(&mut self, node: NodeId, _t: Option<Timestamp>) -> Option<Weight> {
        self.current_metric.and_then(|metric_idx| {
            let weights = &self.fw_metrics[self.fw_graph.num_arcs() * metric_idx..self.fw_graph.num_arcs() * (metric_idx + 1)];
            let fw_graph = BorrowedGraph::new(self.fw_graph.first_out(), self.fw_graph.head(), weights);

            let node = self.cch.node_order().rank(node);

            let mut cur_node = node;
            while self.potentials[cur_node as usize].value().is_none() {
                self.num_pot_computations += 1;
                self.stack.push(cur_node);
                if let Some(parent) = self.cch.elimination_tree()[cur_node as usize].value() {
                    cur_node = parent;
                } else {
                    break;
                }
            }

            while let Some(node) = self.stack.pop() {
                let mut dist = self.backward_distances[node as usize];

                for edge in LinkIterable::<Link>::link_iter(&fw_graph, node) {
                    dist = min(dist, edge.weight + self.potentials[edge.node as usize].value().unwrap())
                }

                self.potentials[node as usize] = InRangeOption::some(dist);
            }

            let dist = self.potentials[node as usize].value().unwrap();
            if dist < INFINITY {
                Some(dist)
            } else {
                None
            }
        })
    }
}

use std::rc::Rc;

pub trait TDBounds: Copy {
    fn lower(&self) -> Weight;
    fn td_upper(&self, t: Timestamp) -> Weight;
    fn pessimistic_upper(&self) -> Weight;
}

impl TDBounds for (Weight, Weight) {
    fn lower(&self) -> Weight {
        self.0
    }
    fn td_upper(&self, _t: Timestamp) -> Weight {
        self.1
    }
    fn pessimistic_upper(&self) -> Weight {
        self.1
    }
}

#[derive(Debug, Clone, Copy)]
pub struct LiveToPredictedBounds {
    lower: Weight,
    live_upper: Weight,
    predicted_upper: Weight,
    predicted_valid_from: Timestamp,
}

impl TDBounds for LiveToPredictedBounds {
    fn lower(&self) -> Weight {
        self.lower
    }
    fn td_upper(&self, t: Timestamp) -> Weight {
        if t >= self.predicted_valid_from {
            self.predicted_upper
        } else {
            self.live_upper
        }
    }
    fn pessimistic_upper(&self) -> Weight {
        debug_assert!(self.live_upper >= self.predicted_upper);
        self.live_upper
    }
}

// Careful, this one works in reversed direction compared to most of the other CH-Pots
pub struct MinMaxPotential<'a, W> {
    cch: &'a CCH,
    stack: Vec<NodeId>,
    fw_distances: TimestampedVector<(Weight, Weight)>,
    potentials: TimestampedVector<InRangeOption<(Weight, Weight)>>,
    forward_cch_graph: FirstOutGraph<Rc<[EdgeId]>, Rc<[NodeId]>, Vec<W>, W>,
    backward_cch_graph: FirstOutGraph<Rc<[EdgeId]>, Rc<[NodeId]>, Vec<W>, W>,
    departure: Timestamp,
}

impl<'a, W: TDBounds> MinMaxPotential<'a, W> {
    fn init(&mut self, target: NodeId, departure: Timestamp) {
        self.departure = departure;
        self.potentials.reset();
        self.fw_distances.reset();
        self.fw_distances[target as usize] = (0, 0);
        let mut node = Some(target);
        while let Some(current) = node {
            for (NodeIdT(head), bounds, EdgeIdT(_)) in LinkIterable::<(NodeIdT, W, EdgeIdT)>::link_iter(&self.forward_cch_graph, current) {
                self.fw_distances[head as usize].0 = min(self.fw_distances[head as usize].0, self.fw_distances[current as usize].0 + bounds.lower());
                self.fw_distances[head as usize].1 = min(
                    self.fw_distances[head as usize].1,
                    self.fw_distances[current as usize].1 + bounds.td_upper(self.departure + self.fw_distances[current as usize].0),
                );
            }
            node = self.cch.elimination_tree()[current as usize].value();
        }
    }

    fn potential(&mut self, node: NodeId) -> Option<(u32, u32)> {
        let mut cur_node = node;
        while self.potentials[cur_node as usize].value().is_none() {
            self.stack.push(cur_node);
            if let Some(parent) = self.cch.elimination_tree()[cur_node as usize].value() {
                cur_node = parent;
            } else {
                break;
            }
        }

        while let Some(node) = self.stack.pop() {
            let mut dist = self.fw_distances[node as usize];

            for (NodeIdT(head), bounds, EdgeIdT(_)) in LinkIterable::<(NodeIdT, W, EdgeIdT)>::link_iter(&self.backward_cch_graph, node) {
                let (head_lower, head_upper) = unsafe { self.potentials.get_unchecked(head as usize).assume_some() };
                dist.0 = min(dist.0, bounds.lower() + head_lower);
                dist.1 = min(dist.1, bounds.td_upper(self.departure + head_lower) + head_upper);
            }

            self.potentials[node as usize] = InRangeOption::some(dist);
        }

        let dist = self.potentials[node as usize].value().unwrap();
        if dist.0 < INFINITY {
            Some(dist)
        } else {
            None
        }
    }
}

pub struct IntervalMinPotential<'a, W> {
    minmax_pot: MinMaxPotential<'a, W>,
    fw_graph: UnweightedFirstOutGraph<Rc<[EdgeId]>, Rc<[NodeId]>>,
    bw_graph: UnweightedFirstOutGraph<Rc<[EdgeId]>, Rc<[NodeId]>>,
    fw_weights: Vec<Weight>,
    bw_weights: Vec<Weight>,
    stack: Vec<NodeId>,
    potentials: TimestampedVector<InRangeOption<(Weight, u32)>>,
    backward_distances: TimestampedVector<Weight>,
    global_upper: Weight,
    departure: Timestamp,
    bucket_to_metric: Box<[usize]>,
    num_pot_computations: usize,
    num_metrics: usize,
}

impl<'a> IntervalMinPotential<'a, (Weight, Weight)> {
    pub fn new(cch: &'a CCH, mut catchup: customizable_contraction_hierarchy::customization::ftd_for_pot::PotData) -> Self {
        let mut fw_predicted_upper_bounds: Vec<_> = catchup.fw_static_bound.iter().map(|&(_, u)| u).collect();
        let mut bw_predicted_upper_bounds: Vec<_> = catchup.bw_static_bound.iter().map(|&(_, u)| u).collect();

        let mut fw_predicted_valid_from = vec![0; cch.num_arcs()];
        let mut bw_predicted_valid_from = vec![0; cch.num_arcs()];

        customization::validity::customize_perfect_with_validity(
            cch,
            &mut fw_predicted_upper_bounds,
            &mut bw_predicted_upper_bounds,
            &mut fw_predicted_valid_from,
            &mut bw_predicted_valid_from,
        );

        catchup
            .fw_static_bound
            .par_iter_mut()
            .zip(fw_predicted_upper_bounds)
            .for_each(|((_, pred_upper), perfect_upper)| {
                *pred_upper = min(*pred_upper, perfect_upper);
            });
        catchup
            .bw_static_bound
            .par_iter_mut()
            .zip(bw_predicted_upper_bounds)
            .for_each(|((_, pred_upper), perfect_upper)| {
                *pred_upper = min(*pred_upper, perfect_upper);
            });

        Self::new_int(
            cch,
            catchup.fw_static_bound,
            catchup.bw_static_bound,
            catchup.fw_bucket_bounds,
            catchup.bw_bucket_bounds,
            catchup.fw_required,
            catchup.bw_required,
            catchup.bucket_to_metric,
        )
    }

    pub fn new_for_simple_live(
        cch: &'a CCH,
        mut catchup: customizable_contraction_hierarchy::customization::ftd_for_pot::PotData,
        live_graph: &PessimisticLiveTDGraph,
        _t_live: Timestamp,
    ) -> Self {
        let upper_bound = (0..live_graph.num_arcs() as EdgeId)
            .map(|edge_id| live_graph.upper_bound(edge_id))
            .collect::<Box<[Weight]>>();
        let customized_live = {
            let _blocked = block_reporting();
            let mut customized = customize(
                cch,
                &FirstOutGraph::new(live_graph.graph().first_out(), live_graph.graph().head(), &upper_bound),
            );
            customization::customize_perfect_without_rebuild(&mut customized);
            customized
        };

        catchup
            .fw_static_bound
            .par_iter_mut()
            .zip(customized_live.forward_graph().weight())
            .for_each(|((_, pred_upper), live_upper)| {
                *pred_upper = *live_upper;
            });
        catchup
            .bw_static_bound
            .par_iter_mut()
            .zip(customized_live.backward_graph().weight())
            .for_each(|((_, pred_upper), live_upper)| {
                *pred_upper = *live_upper;
            });

        Self::new_int(
            cch,
            catchup.fw_static_bound,
            catchup.bw_static_bound,
            catchup.fw_bucket_bounds,
            catchup.bw_bucket_bounds,
            catchup.fw_required,
            catchup.bw_required,
            catchup.bucket_to_metric,
        )
    }
}

impl<'a> IntervalMinPotential<'a, LiveToPredictedBounds> {
    pub fn new_for_live(
        cch: &'a CCH,
        mut catchup: customizable_contraction_hierarchy::customization::ftd_for_pot::PotData,
        live_graph: &PessimisticLiveTDGraph,
        t_live: Timestamp,
    ) -> Self {
        use crate::{
            algo::customizable_contraction_hierarchy::customization::parallelization::SeperatorBasedParallelCustomization,
            datastr::graph::floating_time_dependent::{
                shortcut_source::{ShortcutSource, ShortcutSourceData, SourceCursor},
                Timestamp as FlTimestamp,
            },
        };

        let longest_live = (0..live_graph.num_arcs())
            .into_par_iter()
            .map(|edge| live_graph.live_ended_at(edge as EdgeId))
            .max()
            .unwrap_or(0);

        let m = cch.num_arcs();
        let mut fw_predicted_valid_from = vec![0; m];
        let mut bw_predicted_valid_from = vec![0; m];

        let set_prediction_validity_from_sources = |nodes: Range<usize>, edge_offset: usize, fw_validity: &mut [Timestamp], bw_validity: &mut [Timestamp]| {
            for current_node in nodes {
                let edges = cch.neighbor_edge_indices_usize(current_node as NodeId);
                let (fw_validity_below, fw_validity) = fw_validity.split_at_mut(edges.start - edge_offset);
                let (bw_validity_below, bw_validity) = bw_validity.split_at_mut(edges.start - edge_offset);
                let set_validity_for_edges = |validities: &mut [Timestamp], first_source: &[u32], sources: &[(FlTimestamp, ShortcutSourceData)]| {
                    for (validity, edge_idx) in validities.iter_mut().zip(edges.clone()) {
                        let sources_idxs = first_source[edge_idx] as usize..first_source[edge_idx + 1] as usize;
                        let sources = &sources[sources_idxs];
                        if !sources.is_empty() {
                            let mut cursor = SourceCursor::valid_at(sources, FlTimestamp::new(t_live as f64 / 1000.0));
                            while cursor.cur().0.fuzzy_lt(FlTimestamp::new(longest_live as f64 / 1000.0)) {
                                let source_live_until = match cursor.cur().1.into() {
                                    ShortcutSource::Shortcut(down, up) => {
                                        max(bw_validity_below[down as usize - edge_offset], fw_validity_below[up as usize - edge_offset])
                                    }
                                    ShortcutSource::OriginalEdge(edge) => live_graph.live_ended_at(edge),
                                    _ => 0,
                                };
                                *validity = max(*validity, min(source_live_until, (f64::from(cursor.next().0) * 1000.0).ceil() as Timestamp));
                                cursor.advance();
                            }
                        }
                    }
                };
                set_validity_for_edges(&mut fw_validity[..edges.end - edges.start], &catchup.fw_first_source, &catchup.fw_sources);
                set_validity_for_edges(&mut bw_validity[..edges.end - edges.start], &catchup.bw_first_source, &catchup.bw_sources);
            }
        };

        SeperatorBasedParallelCustomization::new_undirected(cch, set_prediction_validity_from_sources, set_prediction_validity_from_sources).customize(
            &mut fw_predicted_valid_from,
            &mut bw_predicted_valid_from,
            |cb| cb(),
        );

        let upper_bound = (0..live_graph.num_arcs() as EdgeId)
            .map(|edge_id| live_graph.upper_bound(edge_id))
            .collect::<Box<[Weight]>>();
        let customized_live = {
            let _blocked = block_reporting();
            let mut customized = customize(
                cch,
                &FirstOutGraph::new(live_graph.graph().first_out(), live_graph.graph().head(), &upper_bound),
            );
            customization::customize_perfect_without_rebuild(&mut customized);
            customized
        };

        let mut fw_predicted_upper_bounds: Vec<_> = catchup.fw_static_bound.iter().map(|&(_, u)| u).collect();
        let mut bw_predicted_upper_bounds: Vec<_> = catchup.bw_static_bound.iter().map(|&(_, u)| u).collect();

        customization::validity::customize_perfect_with_validity(
            cch,
            &mut fw_predicted_upper_bounds,
            &mut bw_predicted_upper_bounds,
            &mut fw_predicted_valid_from,
            &mut bw_predicted_valid_from,
        );

        catchup
            .fw_required
            .par_iter_mut()
            .zip(&fw_predicted_upper_bounds)
            .zip(&catchup.fw_static_bound)
            .zip(customized_live.forward_graph().weight())
            .for_each(|(((required, pred_upper), (pred_lower, _)), live_upper)| {
                debug_assert!(pred_upper <= live_upper);
                if pred_lower > pred_upper {
                    *required = false;
                }
            });
        catchup
            .bw_required
            .par_iter_mut()
            .zip(&bw_predicted_upper_bounds)
            .zip(&catchup.bw_static_bound)
            .zip(customized_live.backward_graph().weight())
            .for_each(|(((required, pred_upper), (pred_lower, _)), live_upper)| {
                debug_assert!(pred_upper <= live_upper);
                if pred_lower > pred_upper {
                    *required = false;
                }
            });

        let disable_shortcuts_with_invalid_sources = |nodes: Range<usize>, edge_offset: usize, fw_required: &mut [bool], bw_required: &mut [bool]| {
            for current_node in nodes {
                let edges = cch.neighbor_edge_indices_usize(current_node as NodeId);
                let (fw_required_below, fw_required) = fw_required.split_at_mut(edges.start - edge_offset);
                let (bw_required_below, bw_required) = bw_required.split_at_mut(edges.start - edge_offset);
                let process_edges = |requireds: &mut [bool], first_source: &[u32], sources: &[(FlTimestamp, ShortcutSourceData)]| {
                    for (required, edge_idx) in requireds.iter_mut().zip(edges.clone()) {
                        let sources_idxs = first_source[edge_idx] as usize..first_source[edge_idx + 1] as usize;
                        let sources = &sources[sources_idxs];
                        let any_required = sources.iter().map(|(_, s)| ShortcutSource::from(*s)).any(|source| match source {
                            ShortcutSource::Shortcut(down, up) => {
                                bw_required_below[down as usize - edge_offset] && fw_required_below[up as usize - edge_offset]
                            }
                            ShortcutSource::OriginalEdge(_) => true,
                            _ => false,
                        });
                        if !any_required {
                            *required = false;
                        }
                    }
                };
                process_edges(&mut fw_required[..edges.end - edges.start], &catchup.fw_first_source, &catchup.fw_sources);
                process_edges(&mut bw_required[..edges.end - edges.start], &catchup.bw_first_source, &catchup.bw_sources);
            }
        };

        SeperatorBasedParallelCustomization::new_undirected(cch, disable_shortcuts_with_invalid_sources, disable_shortcuts_with_invalid_sources).customize(
            &mut catchup.fw_required,
            &mut catchup.bw_required,
            |cb| cb(),
        );

        let fw_bounds = catchup
            .fw_static_bound
            .into_iter()
            .map(|(l, _)| l)
            .zip(fw_predicted_upper_bounds)
            .zip(customized_live.forward_graph().weight())
            .zip(fw_predicted_valid_from)
            .map(|(((pred_l, pred_u), &live_u), pred_valid)| LiveToPredictedBounds {
                lower: pred_l,
                live_upper: live_u,
                predicted_upper: pred_u,
                predicted_valid_from: pred_valid,
            })
            .collect();

        let bw_bounds = catchup
            .bw_static_bound
            .into_iter()
            .map(|(l, _)| l)
            .zip(bw_predicted_upper_bounds)
            .zip(customized_live.backward_graph().weight())
            .zip(bw_predicted_valid_from)
            .map(|(((pred_l, pred_u), &live_u), pred_valid)| LiveToPredictedBounds {
                lower: pred_l,
                live_upper: live_u,
                predicted_upper: pred_u,
                predicted_valid_from: pred_valid,
            })
            .collect();

        Self::new_int(
            cch,
            fw_bounds,
            bw_bounds,
            catchup.fw_bucket_bounds,
            catchup.bw_bucket_bounds,
            catchup.fw_required,
            catchup.bw_required,
            catchup.bucket_to_metric,
        )
    }
}

impl<'a, W: TDBounds> IntervalMinPotential<'a, W> {
    fn keep(b: &W) -> bool {
        b.lower() < INFINITY && b.lower() <= b.pessimistic_upper()
    }

    fn new_int(
        cch: &'a CCH,
        mut fw_static_bound: Vec<W>,
        mut bw_static_bound: Vec<W>,
        mut fw_bucket_bounds: Vec<Weight>,
        mut bw_bucket_bounds: Vec<Weight>,
        fw_required: Vec<bool>,
        bw_required: Vec<bool>,
        bucket_to_metric: Vec<usize>,
    ) -> Self {
        let g = UnweightedFirstOutGraph::new(cch.first_out(), cch.head());
        let n = cch.num_nodes();
        let m = cch.num_arcs();
        assert_eq!(fw_static_bound.len(), m);
        assert_eq!(bw_static_bound.len(), m);

        let mut fw_first_out = Vec::with_capacity(n + 1);
        fw_first_out.push(0);
        let mut bw_first_out = Vec::with_capacity(n + 1);
        bw_first_out.push(0);
        let mut fw_head = Vec::with_capacity(m);
        let mut bw_head = Vec::with_capacity(m);
        for node in 0..n {
            for (NodeIdT(head), EdgeIdT(edge_id)) in LinkIterable::<(NodeIdT, EdgeIdT)>::link_iter(&g, node as NodeId) {
                if Self::keep(&fw_static_bound[edge_id as usize]) && fw_required[edge_id as usize] {
                    fw_head.push(head);
                }
                if Self::keep(&bw_static_bound[edge_id as usize]) && bw_required[edge_id as usize] {
                    bw_head.push(head);
                }
            }
            fw_first_out.push(fw_head.len() as EdgeId);
            bw_first_out.push(bw_head.len() as EdgeId);
        }
        let fw_first_out: std::rc::Rc<[_]> = fw_first_out.into();
        let bw_first_out: std::rc::Rc<[_]> = bw_first_out.into();
        let fw_head: std::rc::Rc<[_]> = fw_head.into();
        let bw_head: std::rc::Rc<[_]> = bw_head.into();

        let fw_bucket_graph = UnweightedFirstOutGraph::new(fw_first_out, fw_head);
        let bw_bucket_graph = UnweightedFirstOutGraph::new(bw_first_out, bw_head);

        let mut fw_first_out = Vec::with_capacity(n + 1);
        fw_first_out.push(0);
        let mut bw_first_out = Vec::with_capacity(n + 1);
        bw_first_out.push(0);
        let mut fw_head = Vec::with_capacity(m);
        let mut bw_head = Vec::with_capacity(m);
        for node in 0..n {
            for (NodeIdT(head), EdgeIdT(edge_id)) in LinkIterable::<(NodeIdT, EdgeIdT)>::link_iter(&g, node as NodeId) {
                if Self::keep(&fw_static_bound[edge_id as usize]) {
                    fw_head.push(head);
                }
                if Self::keep(&bw_static_bound[edge_id as usize]) {
                    bw_head.push(head);
                }
            }
            fw_first_out.push(fw_head.len() as EdgeId);
            bw_first_out.push(bw_head.len() as EdgeId);
        }

        let fw_first_out: std::rc::Rc<[_]> = fw_first_out.into();
        let bw_first_out: std::rc::Rc<[_]> = bw_first_out.into();
        let fw_head: std::rc::Rc<[_]> = fw_head.into();
        let bw_head: std::rc::Rc<[_]> = bw_head.into();

        fw_bucket_bounds.retain(crate::util::with_index(|idx, _| Self::keep(&fw_static_bound[idx % m]) && fw_required[idx % m]));
        bw_bucket_bounds.retain(crate::util::with_index(|idx, _| Self::keep(&bw_static_bound[idx % m]) && bw_required[idx % m]));

        fw_static_bound.retain(Self::keep);
        bw_static_bound.retain(Self::keep);

        Self {
            minmax_pot: MinMaxPotential {
                cch,
                stack: Vec::new(),
                potentials: TimestampedVector::new(n),
                fw_distances: TimestampedVector::new(n),
                forward_cch_graph: FirstOutGraph::new(fw_first_out, fw_head, fw_static_bound),
                backward_cch_graph: FirstOutGraph::new(bw_first_out, bw_head, bw_static_bound),
                departure: INFINITY,
            },
            fw_graph: fw_bucket_graph,
            bw_graph: bw_bucket_graph,
            fw_weights: fw_bucket_bounds,
            bw_weights: bw_bucket_bounds,
            stack: Vec::new(),
            potentials: TimestampedVector::new(n),
            backward_distances: TimestampedVector::new(n),
            global_upper: INFINITY,
            departure: INFINITY,
            bucket_to_metric: bucket_to_metric.into(),
            num_pot_computations: 0,
            num_metrics: 0,
        }
    }

    fn num_buckets(&self) -> usize {
        self.bucket_to_metric.len()
    }

    fn to_bucket_idx(&self, t: Timestamp) -> usize {
        t as usize * self.num_buckets() / period() as usize
    }

    fn fw_bucket_slice(&self, bucket_idx: usize) -> &[Weight] {
        let m = self.fw_graph.num_arcs();
        let metric_idx = self.bucket_to_metric[bucket_idx % self.num_buckets()];
        &self.fw_weights[metric_idx * m..(metric_idx + 1) * m]
    }

    fn bw_bucket_slice(&self, bucket_idx: usize) -> &[Weight] {
        let m = self.bw_graph.num_arcs();
        let metric_idx = self.bucket_to_metric[bucket_idx % self.num_buckets()];
        &self.bw_weights[metric_idx * m..(metric_idx + 1) * m]
    }
}

impl<W: TDBounds> TDPotential for IntervalMinPotential<'_, W> {
    fn report_stats(&self) {
        report!("num_pot_computations", self.num_pot_computations);
        report!("num_metrics", self.num_metrics);
    }

    fn init(&mut self, source: NodeId, target: NodeId, departure: Timestamp) {
        self.num_pot_computations = 0;
        self.num_metrics = 0;
        let target = self.minmax_pot.cch.node_order().rank(target);
        let source = self.minmax_pot.cch.node_order().rank(source);
        self.departure = departure;

        self.minmax_pot.init(source, departure);

        self.potentials.reset();
        self.backward_distances.reset();
        self.backward_distances[target as usize] = 0;
        self.global_upper = self.minmax_pot.potential(target).map(|(_, u)| u).unwrap_or(INFINITY);

        let mut node = Some(target);
        while let Some(current) = node {
            if self.backward_distances[current as usize] + self.minmax_pot.potential(current).map(|(l, _)| l).unwrap_or(INFINITY) <= self.global_upper {
                for (NodeIdT(head), EdgeIdT(edge_id)) in LinkIterable::<(NodeIdT, EdgeIdT)>::link_iter(&self.bw_graph, current) {
                    if let Some((lower, upper)) = self.minmax_pot.potential(head) {
                        if lower <= self.global_upper {
                            let metric_indices = self.to_bucket_idx(departure + lower)..=self.to_bucket_idx(departure + upper);
                            let mut weight_lower = INFINITY;
                            for idx in metric_indices {
                                weight_lower = min(weight_lower, self.bw_bucket_slice(idx)[edge_id as usize]);
                            }
                            self.backward_distances[head as usize] =
                                min(self.backward_distances[head as usize], weight_lower + self.backward_distances[current as usize]);
                        }
                    }
                }
            }
            node = self.minmax_pot.cch.elimination_tree()[current as usize].value();
        }
    }

    fn potential(&mut self, node: NodeId, t: Option<Timestamp>) -> Option<Weight> {
        let node = self.minmax_pot.cch.node_order().rank(node);
        let min_metric_idx = self.to_bucket_idx(t.unwrap_or(self.departure));

        let mut cur_node = node;
        loop {
            if let Some((_estimate, earliest_metric_idx)) = self.potentials[cur_node as usize].value() {
                if earliest_metric_idx as usize <= min_metric_idx {
                    break;
                }
            }
            self.stack.push(cur_node);
            if let Some(parent) = self.minmax_pot.cch.elimination_tree()[cur_node as usize].value() {
                cur_node = parent;
            } else {
                break;
            }
        }

        let mut earliest_metric_idx = self.to_bucket_idx(self.departure);

        while let Some(node) = self.stack.pop() {
            self.num_pot_computations += 1;

            let mut dist = INFINITY;
            let mut metric_idx_to_store = earliest_metric_idx;

            if let Some((lower, upper)) = self.minmax_pot.potential(node) {
                if lower <= self.global_upper {
                    let current_pot = self.potentials[node as usize].value();
                    dist = current_pot.map(|(estimate, _)| estimate).unwrap_or(self.backward_distances[node as usize]);

                    let lower_bound_metric_idx = self.to_bucket_idx(self.departure + lower);
                    if lower_bound_metric_idx < min_metric_idx {
                        metric_idx_to_store = min_metric_idx;
                    }
                    earliest_metric_idx = max(lower_bound_metric_idx, min_metric_idx);
                    let latest_metric_idx = current_pot
                        .map(|(_, prev_lowest_idx)| prev_lowest_idx as usize)
                        .unwrap_or_else(|| self.to_bucket_idx(self.departure + min(upper, self.global_upper)) + 1);
                    let metric_indices = earliest_metric_idx..latest_metric_idx;

                    for idx in metric_indices {
                        self.num_metrics += 1;
                        let g = BorrowedGraph::new(self.fw_graph.first_out(), self.fw_graph.head(), self.fw_bucket_slice(idx));
                        for l in LinkIterable::<Link>::link_iter(&g, node) {
                            dist = min(dist, unsafe { self.potentials.get_unchecked(l.node as usize).assume_some().0 } + l.weight);
                        }
                    }
                }
            }

            self.potentials[node as usize] = InRangeOption::some((dist, metric_idx_to_store as u32));
        }

        let dist = self.potentials[node as usize].value().unwrap().0;
        if dist < INFINITY {
            Some(dist)
        } else {
            None
        }
    }
}
