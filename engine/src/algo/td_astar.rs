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

#[derive(Debug, Clone, Copy, Default)]
pub struct TRange<T> {
    pub start: T,
    pub end: T,
}

impl TRange<Timestamp> {
    fn includes_instant(self, instant: Timestamp) -> bool {
        self.start <= instant && instant < self.end
    }

    fn duration(&self) -> Weight {
        self.end - self.start
    }
}

pub fn ranges() -> Vec<TRange<Timestamp>> {
    let mut ranges = Vec::new();

    let half_an_hour = 30 * 60 * 1000;
    // 6am until 10pm
    for i in 12..44 {
        let start = i * half_an_hour;
        for len_hours in [1, 2, 4, 8] {
            let end = start + len_hours * half_an_hour * 2;
            if end <= 44 * half_an_hour {
                ranges.push(TRange { start, end });
            }
        }
    }
    ranges
}

pub struct MultiMetric<'a> {
    cch: &'a CCH,
    metric_ranges: Vec<(TRange<Timestamp>, u16, bool)>,
    fw_graph: UnweightedOwnedGraph,
    bw_graph: UnweightedOwnedGraph,
    fw_metrics: Vec<Weight>,
    bw_metrics: Vec<Weight>,
    stack: Vec<NodeId>,
    potentials: TimestampedVector<InRangeOption<(Weight, u32)>>,
    backward_distances: TimestampedVector<Weight>,
    backward_parents: Vec<(NodeId, EdgeId)>,
    upper_bound_dist: query::Server<CustomizedPerfect<'a, CCH>>,
    num_pot_computations: usize,
    current_metrics: Vec<(TRange<Timestamp>, usize)>,
}

impl crate::io::Deconstruct for MultiMetric<'_> {
    fn save_each(&self, store: &dyn Fn(&str, &dyn crate::io::Save) -> std::io::Result<()>) -> std::io::Result<()> {
        store("metric_ranges", &self.metric_ranges)?;
        store("fw_graph", &crate::io::Sub(&self.fw_graph))?;
        store("bw_graph", &crate::io::Sub(&self.bw_graph))?;
        store("fw_metrics", &self.fw_metrics)?;
        store("bw_metrics", &self.bw_metrics)?;
        store("upper_bound_customized", &crate::io::Sub(self.upper_bound_dist.customized()))?;
        Ok(())
    }
}

impl<'a> crate::io::ReconstructPrepared<MultiMetric<'a>> for &'a CCH {
    fn reconstruct_with(self, loader: crate::io::Loader) -> std::io::Result<MultiMetric<'a>> {
        let _blocked = block_reporting();
        let n = self.num_nodes();
        let m = self.num_arcs();
        Ok(MultiMetric {
            cch: self,
            metric_ranges: loader.load("metric_ranges")?,
            fw_metrics: loader.load("fw_metrics")?,
            bw_metrics: loader.load("bw_metrics")?,
            fw_graph: loader.reconstruct("fw_graph")?,
            bw_graph: loader.reconstruct("bw_graph")?,
            upper_bound_dist: query::Server::new(loader.reconstruct_prepared("upper_bound_customized", self)?),
            stack: Vec::new(),
            backward_distances: TimestampedVector::new(n),
            backward_parents: vec![(n as NodeId, m as EdgeId); n],
            potentials: TimestampedVector::new(n),
            num_pot_computations: 0,
            current_metrics: Vec::new(),
        })
    }
}

pub struct MultiMetricPreprocessed<'a> {
    metric_ranges: Vec<(TRange<Timestamp>, u16, bool)>,
    fw_metrics: Vec<Weight>,
    bw_metrics: Vec<Weight>,
    fw_upper: Vec<Weight>,
    bw_upper: Vec<Weight>,
    customized_upper: CustomizedPerfect<'a, CCH>,
}

impl<'a> MultiMetricPreprocessed<'a> {
    pub fn new(cch: &'a CCH, mut times: Vec<TRange<Timestamp>>, graph: &TDGraph, reduce: Option<usize>) -> Self {
        let m = graph.num_arcs();
        times.push(TRange { start: 0, end: INFINITY });
        let mut metric_ranges: Vec<_> = times.iter().enumerate().map(|(i, &r)| (r, i as u16, true)).collect();
        metric_ranges.last_mut().unwrap().2 = false;
        let mut metrics: Vec<Box<[_]>> = times
            .par_iter()
            .map(|r| {
                (0..graph.num_arcs())
                    .map(|e| graph.travel_time_function(e as EdgeId).lower_bound_in_range(r.start..r.end))
                    .collect()
            })
            .collect();

        if let Some(reduce) = reduce {
            let refs: Box<[_]> = metrics.iter().map(|m| &m[..]).collect();
            let merged = crate::algo::metric_merging::merge(&refs, reduce);
            for (new_idx, metrics) in merged.iter().enumerate() {
                for &metric in metrics {
                    metric_ranges[metric].1 = new_idx as u16;
                }
            }
            metrics = merged
                .iter()
                .map(|group| {
                    (0..m)
                        .map(|edge_idx| group.iter().map(|&metric_idx| metrics[metric_idx][edge_idx]).min().unwrap())
                        .collect()
                })
                .collect();
        }

        let upper_bound: Box<[_]> = (0..graph.num_arcs()).map(|e| graph.travel_time_function(e as EdgeId).upper_bound()).collect();
        let mut upper_bound_customized = customize(cch, &BorrowedGraph::new(graph.first_out(), graph.head(), &upper_bound));
        let modified = customization::customize_perfect_without_rebuild(&mut upper_bound_customized);

        let (fw_metrics, bw_metrics): (Vec<_>, Vec<_>) = metrics
            .iter()
            .flat_map(|m| {
                let (fw, bw) = customize(cch, &BorrowedGraph::new(graph.first_out(), graph.head(), m)).into_weights();
                fw.into_iter().zip(bw.into_iter())
            })
            .unzip();

        MultiMetricPreprocessed {
            metric_ranges,
            fw_metrics,
            bw_metrics,
            fw_upper: upper_bound_customized.forward_graph().weight().to_vec(),
            bw_upper: upper_bound_customized.backward_graph().weight().to_vec(),
            customized_upper: customization::rebuild_customized_perfect(upper_bound_customized, &modified.0, &modified.1),
        }
    }

    pub fn customize_simple_live(&mut self, graph: &PessimisticLiveTDGraph, t_live: Timestamp) {
        let cch = self.customized_upper.cch;
        let upper_bound: Box<[_]> = (0..graph.num_arcs()).map(|e| graph.upper_bound(e as EdgeId, t_live)).collect();
        let mut upper_bound_customized = customize(cch, &BorrowedGraph::new(graph.graph().first_out(), graph.graph().head(), &upper_bound));
        let modified = customization::customize_perfect_without_rebuild(&mut upper_bound_customized);
        self.fw_upper = upper_bound_customized.forward_graph().weight().to_vec();
        self.bw_upper = upper_bound_customized.backward_graph().weight().to_vec();
        self.customized_upper = customization::rebuild_customized_perfect(upper_bound_customized, &modified.0, &modified.1);
    }

    pub fn customize_live(&mut self, graph: &PessimisticLiveTDGraph, t_live: Timestamp) {
        let cch = self.customized_upper.cch;
        let num_metrics = self.fw_metrics.len() / cch.num_arcs();
        let end_of_live_metric = t_live + 59 * 60 * 1000;
        self.metric_ranges.push((
            TRange {
                start: t_live,
                end: end_of_live_metric,
            },
            num_metrics as u16,
            false,
        ));

        let live_metric: Vec<_> = (0..graph.num_arcs())
            .map(|e| graph.live_lower_bound(e as EdgeId, t_live, end_of_live_metric))
            .collect();
        let live_customized = customize(cch, &BorrowedGraph::new(graph.graph().first_out(), graph.graph().head(), &live_metric));

        let (fw_cap, bw_cap) = (self.fw_metrics.capacity(), self.bw_metrics.capacity());
        self.fw_metrics.extend_from_slice(live_customized.forward_graph().weight());
        self.bw_metrics.extend_from_slice(live_customized.backward_graph().weight());
        if fw_cap < self.fw_metrics.len() || bw_cap < self.bw_metrics.len() {
            dbg!("unnecessary expensive reallocation happened");
        }

        self.customize_simple_live(graph, t_live);
    }

    pub fn reserve_space_for_additional_metrics(&mut self, num_metrics: usize) {
        let additional = num_metrics * self.customized_upper.cch.num_arcs();
        self.fw_metrics.reserve_exact(additional);
        self.bw_metrics.reserve_exact(additional);
    }
}

impl crate::io::Deconstruct for MultiMetricPreprocessed<'_> {
    fn save_each(&self, store: &dyn Fn(&str, &dyn crate::io::Save) -> std::io::Result<()>) -> std::io::Result<()> {
        store("metric_ranges", &self.metric_ranges)?;
        store("fw_metrics", &self.fw_metrics)?;
        store("bw_metrics", &self.bw_metrics)?;
        store("fw_upper", &self.fw_upper)?;
        store("bw_upper", &self.bw_upper)?;
        store("customized_upper", &crate::io::Sub(&self.customized_upper))?;
        Ok(())
    }
}

impl<'a> crate::io::ReconstructPrepared<MultiMetricPreprocessed<'a>> for &'a CCH {
    fn reconstruct_with(self, loader: crate::io::Loader) -> std::io::Result<MultiMetricPreprocessed<'a>> {
        Ok(MultiMetricPreprocessed {
            metric_ranges: loader.load("metric_ranges")?,
            fw_metrics: loader.load("fw_metrics")?,
            bw_metrics: loader.load("bw_metrics")?,
            fw_upper: loader.load("fw_upper")?,
            bw_upper: loader.load("bw_upper")?,
            customized_upper: loader.reconstruct_prepared("customized_upper", self)?,
        })
    }
}

impl<'a> MultiMetric<'a> {
    pub fn new(mut mmp: MultiMetricPreprocessed<'a>) -> Self {
        let cch = mmp.customized_upper.cch;
        let n = cch.num_nodes();
        let m = cch.num_arcs();

        let global_lower_idx = mmp
            .metric_ranges
            .iter()
            .find(|(r, _, periodic)| !periodic && r.start == 0 && r.end == INFINITY)
            .unwrap()
            .1 as usize;
        let fw_lower = mmp.fw_metrics[m * global_lower_idx..m * (global_lower_idx + 1)].to_vec();
        let bw_lower = mmp.bw_metrics[m * global_lower_idx..m * (global_lower_idx + 1)].to_vec();

        let k = rayon::current_num_threads() * 4;

        let mut fw_first_out = vec![0; cch.first_out().len()];
        let mut bw_first_out = vec![0; cch.first_out().len()];

        let mut edges_of_each_thread = vec![(0, 0); k + 1];
        let mut local_edge_counts = &mut edges_of_each_thread[1..];
        let target_edges_per_thread = (m + k - 1) / k;
        let first_node_of_chunk: Vec<_> = cch
            .forward_tail()
            .chunks(target_edges_per_thread)
            .map(|chunk| chunk[0] as usize)
            .chain(std::iter::once(n))
            .collect();

        rayon::scope(|s| {
            let fw_lower = &fw_lower;
            let bw_lower = &bw_lower;
            let fw_upper = &mmp.fw_upper;
            let bw_upper = &mmp.bw_upper;

            for i in 0..k {
                let (local_count, rest_counts) = local_edge_counts.split_first_mut().unwrap();
                local_edge_counts = rest_counts;
                let local_nodes = first_node_of_chunk[i]..first_node_of_chunk[i + 1];
                let local_edges = cch.first_out()[local_nodes.start] as usize..cch.first_out()[local_nodes.end] as usize;
                s.spawn(move |_| {
                    local_count.0 = fw_lower[local_edges.clone()]
                        .iter()
                        .zip(fw_upper[local_edges.clone()].iter())
                        .filter(|&(l, u)| l <= u)
                        .count();
                    local_count.1 = bw_lower[local_edges.clone()]
                        .iter()
                        .zip(bw_upper[local_edges.clone()].iter())
                        .filter(|&(l, u)| l <= u)
                        .count();
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

        let m_fw = edges_of_each_thread[k].0;
        let m_bw = edges_of_each_thread[k].1;
        let mut fw_head = vec![0; m_fw];
        let mut bw_head = vec![0; m_bw];

        rayon::scope(|s| {
            let mut forward_first_out = &mut fw_first_out[..];
            let mut forward_head = &mut fw_head[..];
            let mut backward_first_out = &mut bw_first_out[..];
            let mut backward_head = &mut bw_head[..];

            let fw_lower = &fw_lower;
            let bw_lower = &bw_lower;
            let fw_upper = &mmp.fw_upper;
            let bw_upper = &mmp.bw_upper;

            for i in 0..k {
                let local_nodes = first_node_of_chunk[i]..first_node_of_chunk[i + 1];
                debug_assert!(local_nodes.start <= local_nodes.end);
                let num_fw_edges_before = edges_of_each_thread[i].0;
                let num_bw_edges_before = edges_of_each_thread[i].1;

                let (local_fw_fo, rest_fw_fo) = forward_first_out.split_at_mut(local_nodes.end - local_nodes.start);
                forward_first_out = rest_fw_fo;
                let (local_bw_fo, rest_bw_fo) = backward_first_out.split_at_mut(local_nodes.end - local_nodes.start);
                backward_first_out = rest_bw_fo;
                let (local_fw_head, rest_fw_head) = forward_head.split_at_mut(edges_of_each_thread[i + 1].0 - num_fw_edges_before);
                forward_head = rest_fw_head;
                let (local_bw_head, rest_bw_head) = backward_head.split_at_mut(edges_of_each_thread[i + 1].1 - num_bw_edges_before);
                backward_head = rest_bw_head;

                s.spawn(move |_| {
                    let mut fw_edge_count = 0;
                    let mut bw_edge_count = 0;
                    for (local_node_idx, node) in local_nodes.enumerate() {
                        local_fw_fo[local_node_idx] = (num_fw_edges_before + fw_edge_count) as EdgeId;
                        local_bw_fo[local_node_idx] = (num_bw_edges_before + bw_edge_count) as EdgeId;

                        let edge_ids = cch.neighbor_edge_indices_usize(node as NodeId);
                        for ((head, lower), upper) in cch.head()[edge_ids.clone()]
                            .iter()
                            .zip(&fw_lower[edge_ids.clone()])
                            .zip(&fw_upper[edge_ids.clone()])
                        {
                            if lower <= upper {
                                local_fw_head[fw_edge_count] = *head;
                                fw_edge_count += 1;
                            }
                        }
                        for ((head, lower), upper) in cch.head()[edge_ids.clone()]
                            .iter()
                            .zip(&bw_lower[edge_ids.clone()])
                            .zip(&bw_upper[edge_ids.clone()])
                        {
                            if lower <= upper {
                                local_bw_head[bw_edge_count] = *head;
                                bw_edge_count += 1;
                            }
                        }
                    }
                });
            }
        });

        fw_first_out[n] = m_fw as EdgeId;
        bw_first_out[n] = m_bw as EdgeId;

        let num_metrics = mmp.fw_metrics.len() / m;
        let mut fw_metrics = vec![0; m_fw * num_metrics];
        let mut bw_metrics = vec![0; m_bw * num_metrics];

        rayon::scope(|s| {
            let prev_fw_metrics = &mmp.fw_metrics;
            let prev_bw_metrics = &mmp.bw_metrics;
            let mut fw_metrics = &mut fw_metrics[..];
            let mut bw_metrics = &mut bw_metrics[..];

            let fw_lower = &fw_lower;
            let bw_lower = &bw_lower;
            let fw_upper = &mmp.fw_upper;
            let bw_upper = &mmp.bw_upper;

            for metric_idx in 0..num_metrics {
                for i in 0..k {
                    let local_nodes = first_node_of_chunk[i]..first_node_of_chunk[i + 1];
                    debug_assert!(local_nodes.start <= local_nodes.end);
                    let num_fw_edges_before = edges_of_each_thread[i].0;
                    let num_bw_edges_before = edges_of_each_thread[i].1;

                    let (local_fw_metrics, rest_fw_metrics) = fw_metrics.split_at_mut(edges_of_each_thread[i + 1].0 - num_fw_edges_before);
                    fw_metrics = rest_fw_metrics;
                    let (local_bw_metrics, rest_bw_metrics) = bw_metrics.split_at_mut(edges_of_each_thread[i + 1].1 - num_bw_edges_before);
                    bw_metrics = rest_bw_metrics;

                    s.spawn(move |_| {
                        let mut fw_edge_count = 0;
                        let mut bw_edge_count = 0;
                        for node in local_nodes {
                            let edge_ids = cch.neighbor_edge_indices_usize(node as NodeId);
                            let metric_offset = m * metric_idx;
                            let weights = &prev_fw_metrics[metric_offset + edge_ids.start..metric_offset + edge_ids.end];
                            for ((weight, lower), upper) in weights.iter().zip(&fw_lower[edge_ids.clone()]).zip(&fw_upper[edge_ids.clone()]) {
                                if lower <= upper {
                                    local_fw_metrics[fw_edge_count] = *weight;
                                    fw_edge_count += 1;
                                }
                            }
                            let weights = &prev_bw_metrics[metric_offset + edge_ids.start..metric_offset + edge_ids.end];
                            for ((weight, lower), upper) in weights.iter().zip(&bw_lower[edge_ids.clone()]).zip(&bw_upper[edge_ids.clone()]) {
                                if lower <= upper {
                                    local_bw_metrics[bw_edge_count] = *weight;
                                    bw_edge_count += 1;
                                }
                            }
                        }
                    });
                }
            }
        });

        mmp.fw_metrics = fw_metrics;
        mmp.bw_metrics = bw_metrics;

        MultiMetric {
            cch,
            metric_ranges: mmp.metric_ranges,
            fw_graph: UnweightedOwnedGraph::new(fw_first_out, fw_head),
            bw_graph: UnweightedOwnedGraph::new(bw_first_out, bw_head),
            fw_metrics: mmp.fw_metrics,
            bw_metrics: mmp.bw_metrics,
            stack: Vec::new(),
            backward_distances: TimestampedVector::new(n),
            backward_parents: vec![(n as NodeId, m as EdgeId); n],
            potentials: TimestampedVector::new(n),
            num_pot_computations: 0,
            current_metrics: Vec::new(),
            upper_bound_dist: query::Server::new(mmp.customized_upper),
        }
    }
}

impl TDPotential for MultiMetric<'_> {
    fn init(&mut self, source: NodeId, target: NodeId, departure: Timestamp) {
        let departure = departure % period();
        self.num_pot_computations = 0;
        self.potentials.reset();
        self.current_metrics.clear();

        if let Some(upper_bound) = self.upper_bound_dist.query(Query { from: source, to: target }).distance() {
            let latest_arrival = departure + upper_bound;
            let end_on_day = latest_arrival / period();
            for &(mut range, idx, periodic) in &self.metric_ranges {
                for _ in 0..=if periodic { end_on_day } else { 0 } {
                    if range.includes_instant(latest_arrival) {
                        self.current_metrics.push((range, idx as usize));
                    }
                    range.start += period();
                    range.end += period();
                }
            }

            self.current_metrics.sort_unstable_by_key(|(r, _)| (r.start, r.end));
            let first_inner = self
                .current_metrics
                .iter()
                .position(|(r, _)| !r.includes_instant(departure))
                .unwrap_or(self.current_metrics.len());
            if first_inner > 1 {
                self.current_metrics.drain(..first_inner - 1);
            }

            let mut prev_duration = self.current_metrics[0].0.duration() + 1;
            self.current_metrics.retain(|(r, _)| {
                if r.duration() < prev_duration {
                    prev_duration = r.duration();
                    true
                } else {
                    false
                }
            });

            let metric_idx = self.current_metrics[0].1;
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
        }
    }
    fn potential(&mut self, node: NodeId, t: Option<Timestamp>) -> Option<Weight> {
        let relevant_idx = if let Some(t) = t {
            debug_assert!(t < period() * 2);
            let mut last_valid = 0;
            for (idx, (r, _)) in self.current_metrics.iter().enumerate() {
                if r.includes_instant(t) {
                    last_valid = idx;
                } else {
                    break;
                }
            }
            last_valid
        } else {
            0
        };

        self.current_metrics.get(relevant_idx).and_then(|(_, metric_idx)| {
            let weights = &self.fw_metrics[self.fw_graph.num_arcs() * metric_idx..self.fw_graph.num_arcs() * (metric_idx + 1)];
            let fw_graph = BorrowedGraph::new(self.fw_graph.first_out(), self.fw_graph.head(), weights);

            let node = self.cch.node_order().rank(node);

            let mut cur_node = node;
            loop {
                if let Some((_estimate, earliest_metric_idx)) = self.potentials[cur_node as usize].value() {
                    if earliest_metric_idx as usize <= relevant_idx {
                        break;
                    }
                }
                self.stack.push(cur_node);
                if let Some(parent) = self.cch.elimination_tree()[cur_node as usize].value() {
                    cur_node = parent;
                } else {
                    break;
                }
            }

            while let Some(node) = self.stack.pop() {
                self.num_pot_computations += 1;
                let mut dist = self.potentials[node as usize]
                    .value()
                    .map(|(est, _)| est)
                    .unwrap_or(self.backward_distances[node as usize]);

                for edge in LinkIterable::<Link>::link_iter(&fw_graph, node) {
                    dist = min(dist, edge.weight + unsafe { self.potentials.get_unchecked(edge.node as usize).assume_some().0 })
                }

                self.potentials[node as usize] = InRangeOption::some((dist, relevant_idx as u32));
            }

            let dist = self.potentials[node as usize].value().unwrap().0;
            if dist < INFINITY {
                Some(dist)
            } else {
                None
            }
        })
    }
}

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

#[derive(Debug, Clone, Copy, Default)]
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
    fw_graph: OwnedGraph<W>,
    bw_graph: OwnedGraph<W>,
    stack: Vec<NodeId>,
    fw_distances: TimestampedVector<(Weight, Weight)>,
    potentials: TimestampedVector<InRangeOption<(Weight, Weight)>>,
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
            for (NodeIdT(head), bounds, EdgeIdT(_)) in LinkIterable::<(NodeIdT, W, EdgeIdT)>::link_iter(&self.fw_graph, current) {
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

            for (NodeIdT(head), bounds, EdgeIdT(_)) in LinkIterable::<(NodeIdT, W, EdgeIdT)>::link_iter(&self.bw_graph, node) {
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

impl<W: Copy> crate::io::Deconstruct for MinMaxPotential<'_, W> {
    fn save_each(&self, store: &dyn Fn(&str, &dyn crate::io::Save) -> std::io::Result<()>) -> std::io::Result<()> {
        store("fw_graph", &crate::io::Sub(&self.fw_graph))?;
        store("bw_graph", &crate::io::Sub(&self.bw_graph))?;
        Ok(())
    }
}

impl<'a, W: Copy + Default> crate::io::ReconstructPrepared<MinMaxPotential<'a, W>> for &'a CCH {
    fn reconstruct_with(self, loader: crate::io::Loader) -> std::io::Result<MinMaxPotential<'a, W>> {
        let _blocked = block_reporting();
        let n = self.num_nodes();
        Ok(MinMaxPotential {
            cch: self,
            fw_graph: loader.reconstruct("fw_graph")?,
            bw_graph: loader.reconstruct("bw_graph")?,
            stack: Vec::new(),
            fw_distances: TimestampedVector::new(n),
            potentials: TimestampedVector::new(n),
            departure: INFINITY,
        })
    }
}

pub struct IntervalMinPotential<'a, W> {
    minmax_pot: MinMaxPotential<'a, W>,
    fw_graph: UnweightedOwnedGraph,
    bw_graph: UnweightedOwnedGraph,
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

impl<W: Copy> crate::io::Deconstruct for IntervalMinPotential<'_, W> {
    fn save_each(&self, store: &dyn Fn(&str, &dyn crate::io::Save) -> std::io::Result<()>) -> std::io::Result<()> {
        store("minmax_pot", &crate::io::Sub(&self.minmax_pot))?;
        store("fw_graph", &crate::io::Sub(&self.fw_graph))?;
        store("bw_graph", &crate::io::Sub(&self.bw_graph))?;
        store("fw_weights", &self.fw_weights)?;
        store("bw_weights", &self.bw_weights)?;
        store("bucket_to_metric", &self.bucket_to_metric)?;
        Ok(())
    }
}

impl<'a, W: Copy + Default> crate::io::ReconstructPrepared<IntervalMinPotential<'a, W>> for &'a CCH {
    fn reconstruct_with(self, loader: crate::io::Loader) -> std::io::Result<IntervalMinPotential<'a, W>> {
        let _blocked = block_reporting();
        let n = self.num_nodes();
        Ok(IntervalMinPotential {
            minmax_pot: loader.reconstruct_prepared("minmax_pot", self)?,
            fw_graph: loader.reconstruct("fw_graph")?,
            bw_graph: loader.reconstruct("bw_graph")?,
            fw_weights: loader.load("fw_weights")?,
            bw_weights: loader.load("bw_weights")?,
            bucket_to_metric: loader.load("bucket_to_metric")?,
            stack: Vec::new(),
            backward_distances: TimestampedVector::new(n),
            potentials: TimestampedVector::new(n),
            departure: INFINITY,
            global_upper: INFINITY,
            num_pot_computations: 0,
            num_metrics: 0,
        })
    }
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
        t_live: Timestamp,
    ) -> Self {
        let upper_bound = (0..live_graph.num_arcs() as EdgeId)
            .map(|edge_id| live_graph.upper_bound(edge_id, t_live))
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
        use std::ops::Range;

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
            .map(|edge_id| live_graph.upper_bound(edge_id, t_live))
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

impl<'a, W: TDBounds + Default + Send + Sync> IntervalMinPotential<'a, W> {
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
        let n = cch.num_nodes();
        let m = cch.num_arcs();
        assert_eq!(fw_static_bound.len(), m);
        assert_eq!(bw_static_bound.len(), m);

        let k = rayon::current_num_threads() * 4;

        let mut edges_of_each_thread = vec![(0, 0); k + 1];
        let mut local_edge_counts = &mut edges_of_each_thread[1..];
        let target_edges_per_thread = (m + k - 1) / k;
        let first_node_of_chunk: Vec<_> = cch
            .forward_tail()
            .chunks(target_edges_per_thread)
            .map(|chunk| chunk[0] as usize)
            .chain(std::iter::once(n))
            .collect();

        // BUCKETS
        let mut fw_first_out = vec![0; cch.first_out().len()];
        let mut bw_first_out = vec![0; cch.first_out().len()];

        rayon::scope(|s| {
            let fw_static = &fw_static_bound;
            let bw_static = &bw_static_bound;
            let fw_required = &fw_required;
            let bw_required = &bw_required;

            for i in 0..k {
                let (local_count, rest_counts) = local_edge_counts.split_first_mut().unwrap();
                local_edge_counts = rest_counts;
                let local_nodes = first_node_of_chunk[i]..first_node_of_chunk[i + 1];
                let local_edges = cch.first_out()[local_nodes.start] as usize..cch.first_out()[local_nodes.end] as usize;
                s.spawn(move |_| {
                    local_count.0 = fw_static[local_edges.clone()]
                        .iter()
                        .zip(&fw_required[local_edges.clone()])
                        .filter(|&(stat, req)| Self::keep(stat) && *req)
                        .count();
                    local_count.1 = bw_static[local_edges.clone()]
                        .iter()
                        .zip(&bw_required[local_edges.clone()])
                        .filter(|&(stat, req)| Self::keep(stat) && *req)
                        .count();
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

        let m_fw = edges_of_each_thread[k].0;
        let m_bw = edges_of_each_thread[k].1;
        let mut fw_head = vec![0; m_fw];
        let mut bw_head = vec![0; m_bw];

        rayon::scope(|s| {
            let mut forward_first_out = &mut fw_first_out[..];
            let mut forward_head = &mut fw_head[..];
            let mut backward_first_out = &mut bw_first_out[..];
            let mut backward_head = &mut bw_head[..];

            let fw_static = &fw_static_bound;
            let bw_static = &bw_static_bound;
            let fw_required = &fw_required;
            let bw_required = &bw_required;

            for i in 0..k {
                let local_nodes = first_node_of_chunk[i]..first_node_of_chunk[i + 1];
                debug_assert!(local_nodes.start <= local_nodes.end);
                let num_fw_edges_before = edges_of_each_thread[i].0;
                let num_bw_edges_before = edges_of_each_thread[i].1;

                let (local_fw_fo, rest_fw_fo) = forward_first_out.split_at_mut(local_nodes.end - local_nodes.start);
                forward_first_out = rest_fw_fo;
                let (local_bw_fo, rest_bw_fo) = backward_first_out.split_at_mut(local_nodes.end - local_nodes.start);
                backward_first_out = rest_bw_fo;
                let (local_fw_head, rest_fw_head) = forward_head.split_at_mut(edges_of_each_thread[i + 1].0 - num_fw_edges_before);
                forward_head = rest_fw_head;
                let (local_bw_head, rest_bw_head) = backward_head.split_at_mut(edges_of_each_thread[i + 1].1 - num_bw_edges_before);
                backward_head = rest_bw_head;

                s.spawn(move |_| {
                    let mut fw_edge_count = 0;
                    let mut bw_edge_count = 0;
                    for (local_node_idx, node) in local_nodes.enumerate() {
                        local_fw_fo[local_node_idx] = (num_fw_edges_before + fw_edge_count) as EdgeId;
                        local_bw_fo[local_node_idx] = (num_bw_edges_before + bw_edge_count) as EdgeId;

                        let edge_ids = cch.neighbor_edge_indices_usize(node as NodeId);
                        for ((head, stat), req) in cch.head()[edge_ids.clone()]
                            .iter()
                            .zip(&fw_static[edge_ids.clone()])
                            .zip(&fw_required[edge_ids.clone()])
                        {
                            if Self::keep(stat) && *req {
                                local_fw_head[fw_edge_count] = *head;
                                fw_edge_count += 1;
                            }
                        }
                        for ((head, stat), req) in cch.head()[edge_ids.clone()]
                            .iter()
                            .zip(&bw_static[edge_ids.clone()])
                            .zip(&bw_required[edge_ids.clone()])
                        {
                            if Self::keep(stat) && *req {
                                local_bw_head[bw_edge_count] = *head;
                                bw_edge_count += 1;
                            }
                        }
                    }
                });
            }
        });

        fw_first_out[n] = m_fw as EdgeId;
        bw_first_out[n] = m_bw as EdgeId;

        let num_buckets = fw_bucket_bounds.len() / m;
        let mut fw_buckets = vec![0; m_fw * num_buckets];
        let mut bw_buckets = vec![0; m_bw * num_buckets];

        rayon::scope(|s| {
            let prev_fw_buckets = &fw_bucket_bounds;
            let prev_bw_buckets = &bw_bucket_bounds;
            let mut fw_buckets = &mut fw_buckets[..];
            let mut bw_buckets = &mut bw_buckets[..];

            let fw_static = &fw_static_bound;
            let bw_static = &bw_static_bound;
            let fw_required = &fw_required;
            let bw_required = &bw_required;

            for metric_idx in 0..num_buckets {
                for i in 0..k {
                    let local_nodes = first_node_of_chunk[i]..first_node_of_chunk[i + 1];
                    debug_assert!(local_nodes.start <= local_nodes.end);
                    let num_fw_edges_before = edges_of_each_thread[i].0;
                    let num_bw_edges_before = edges_of_each_thread[i].1;

                    let (local_fw_buckets, rest_fw_buckets) = fw_buckets.split_at_mut(edges_of_each_thread[i + 1].0 - num_fw_edges_before);
                    fw_buckets = rest_fw_buckets;
                    let (local_bw_buckets, rest_bw_buckets) = bw_buckets.split_at_mut(edges_of_each_thread[i + 1].1 - num_bw_edges_before);
                    bw_buckets = rest_bw_buckets;

                    s.spawn(move |_| {
                        let mut fw_edge_count = 0;
                        let mut bw_edge_count = 0;
                        for node in local_nodes {
                            let edge_ids = cch.neighbor_edge_indices_usize(node as NodeId);
                            let metric_offset = m * metric_idx;
                            let weights = &prev_fw_buckets[metric_offset + edge_ids.start..metric_offset + edge_ids.end];
                            for ((weight, stat), req) in weights.iter().zip(&fw_static[edge_ids.clone()]).zip(&fw_required[edge_ids.clone()]) {
                                if Self::keep(stat) && *req {
                                    local_fw_buckets[fw_edge_count] = *weight;
                                    fw_edge_count += 1;
                                }
                            }
                            let weights = &prev_bw_buckets[metric_offset + edge_ids.start..metric_offset + edge_ids.end];
                            for ((weight, stat), req) in weights.iter().zip(&bw_static[edge_ids.clone()]).zip(&bw_required[edge_ids.clone()]) {
                                if Self::keep(stat) && *req {
                                    local_bw_buckets[bw_edge_count] = *weight;
                                    bw_edge_count += 1;
                                }
                            }
                        }
                    });
                }
            }
        });
        fw_bucket_bounds = fw_buckets;
        bw_bucket_bounds = bw_buckets;

        let fw_bucket_graph = UnweightedFirstOutGraph::new(fw_first_out, fw_head);
        let bw_bucket_graph = UnweightedFirstOutGraph::new(bw_first_out, bw_head);

        // STATIC BOUNDS
        let mut fw_first_out = vec![0; cch.first_out().len()];
        let mut bw_first_out = vec![0; cch.first_out().len()];

        let mut local_edge_counts = &mut edges_of_each_thread[1..];

        rayon::scope(|s| {
            let fw_static = &fw_static_bound;
            let bw_static = &bw_static_bound;

            for i in 0..k {
                let (local_count, rest_counts) = local_edge_counts.split_first_mut().unwrap();
                local_edge_counts = rest_counts;
                let local_nodes = first_node_of_chunk[i]..first_node_of_chunk[i + 1];
                let local_edges = cch.first_out()[local_nodes.start] as usize..cch.first_out()[local_nodes.end] as usize;
                s.spawn(move |_| {
                    local_count.0 = fw_static[local_edges.clone()].iter().filter(|stat| Self::keep(stat)).count();
                    local_count.1 = bw_static[local_edges.clone()].iter().filter(|stat| Self::keep(stat)).count();
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

        let m_fw = edges_of_each_thread[k].0;
        let m_bw = edges_of_each_thread[k].1;
        let mut fw_head = vec![0; m_fw];
        let mut bw_head = vec![0; m_bw];
        let mut fw_new_static = vec![W::default(); m_fw];
        let mut bw_new_static = vec![W::default(); m_bw];

        rayon::scope(|s| {
            let mut forward_first_out = &mut fw_first_out[..];
            let mut forward_head = &mut fw_head[..];
            let mut fw_new_static = &mut fw_new_static[..];
            let mut backward_first_out = &mut bw_first_out[..];
            let mut backward_head = &mut bw_head[..];
            let mut bw_new_static = &mut bw_new_static[..];

            let fw_static = &fw_static_bound;
            let bw_static = &bw_static_bound;

            for i in 0..k {
                let local_nodes = first_node_of_chunk[i]..first_node_of_chunk[i + 1];
                debug_assert!(local_nodes.start <= local_nodes.end);
                let num_fw_edges_before = edges_of_each_thread[i].0;
                let num_bw_edges_before = edges_of_each_thread[i].1;

                let (local_fw_fo, rest_fw_fo) = forward_first_out.split_at_mut(local_nodes.end - local_nodes.start);
                forward_first_out = rest_fw_fo;
                let (local_bw_fo, rest_bw_fo) = backward_first_out.split_at_mut(local_nodes.end - local_nodes.start);
                backward_first_out = rest_bw_fo;
                let (local_fw_head, rest_fw_head) = forward_head.split_at_mut(edges_of_each_thread[i + 1].0 - num_fw_edges_before);
                forward_head = rest_fw_head;
                let (local_bw_head, rest_bw_head) = backward_head.split_at_mut(edges_of_each_thread[i + 1].1 - num_bw_edges_before);
                backward_head = rest_bw_head;
                let (local_fw_static, rest_fw_static) = fw_new_static.split_at_mut(edges_of_each_thread[i + 1].0 - num_fw_edges_before);
                fw_new_static = rest_fw_static;
                let (local_bw_static, rest_bw_static) = bw_new_static.split_at_mut(edges_of_each_thread[i + 1].1 - num_bw_edges_before);
                bw_new_static = rest_bw_static;

                s.spawn(move |_| {
                    let mut fw_edge_count = 0;
                    let mut bw_edge_count = 0;
                    for (local_node_idx, node) in local_nodes.enumerate() {
                        local_fw_fo[local_node_idx] = (num_fw_edges_before + fw_edge_count) as EdgeId;
                        local_bw_fo[local_node_idx] = (num_bw_edges_before + bw_edge_count) as EdgeId;

                        let edge_ids = cch.neighbor_edge_indices_usize(node as NodeId);
                        for (head, stat) in cch.head()[edge_ids.clone()].iter().zip(&fw_static[edge_ids.clone()]) {
                            if Self::keep(stat) {
                                local_fw_head[fw_edge_count] = *head;
                                local_fw_static[fw_edge_count] = *stat;
                                fw_edge_count += 1;
                            }
                        }
                        for (head, stat) in cch.head()[edge_ids.clone()].iter().zip(&bw_static[edge_ids.clone()]) {
                            if Self::keep(stat) {
                                local_bw_head[bw_edge_count] = *head;
                                local_bw_static[bw_edge_count] = *stat;
                                bw_edge_count += 1;
                            }
                        }
                    }
                });
            }
        });

        fw_first_out[n] = m_fw as EdgeId;
        bw_first_out[n] = m_bw as EdgeId;
        fw_static_bound = fw_new_static;
        bw_static_bound = bw_new_static;

        Self {
            minmax_pot: MinMaxPotential {
                cch,
                stack: Vec::new(),
                potentials: TimestampedVector::new(n),
                fw_distances: TimestampedVector::new(n),
                fw_graph: FirstOutGraph::new(fw_first_out, fw_head, fw_static_bound),
                bw_graph: FirstOutGraph::new(bw_first_out, bw_head, bw_static_bound),
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
}

impl<'a, W: TDBounds> IntervalMinPotential<'a, W> {
    fn keep(b: &W) -> bool {
        b.lower() < INFINITY && b.lower() <= b.pessimistic_upper()
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
