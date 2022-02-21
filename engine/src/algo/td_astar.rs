use super::a_star::*;
use super::customizable_contraction_hierarchy::{query::stepped_elimination_tree::EliminationTreeWalk, *};
use super::*;
use crate::{
    datastr::{graph::time_dependent::*, timestamped_vector::*},
    util::in_range_option::*,
};

use std::cmp::min;

pub trait TDPotential {
    fn init(&mut self, source: NodeId, target: NodeId, departure: Timestamp);
    fn potential(&mut self, node: NodeId, t: Option<Timestamp>) -> Option<Weight>;
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
}

use std::ops::Range;

pub struct MultiMetric<C> {
    metric_ranges: Vec<(Range<Timestamp>, usize)>,
    metrics: Vec<C>,
    stack: Vec<NodeId>,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    backward_distances: TimestampedVector<Weight>,
    backward_parents: Vec<(NodeId, EdgeId)>,
    upper_bound_dist: query::Server<C>,
    num_pot_computations: usize,
    current_metric: Option<usize>,
}

impl<'a> MultiMetric<CustomizedBasic<'a, CCH>> {
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
        Self {
            metric_ranges,
            metrics: metrics.iter().map(|m| customize(cch, m)).collect(),
            stack: Vec::new(),
            backward_distances: TimestampedVector::new(n),
            backward_parents: vec![(n as NodeId, m as EdgeId); n],
            potentials: TimestampedVector::new(n),
            num_pot_computations: 0,
            current_metric: None,
            upper_bound_dist: query::Server::new(customize(cch, &upper_bound)),
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
    covered_second.start %= period();
    to_cover_second.end %= period();
    (to_cover_first.is_empty() || range_contains(&covered_first, &to_cover_first) || range_contains(&covered_second, &to_cover_first))
        && (to_cover_second.is_empty() || range_contains(&covered_first, &to_cover_second) || range_contains(&covered_second, &to_cover_second))
}

fn range_contains(covered: &Range<Timestamp>, to_cover: &Range<Timestamp>) -> bool {
    covered.start <= to_cover.start && covered.end >= to_cover.end
}

impl<C: Customized> TDPotential for MultiMetric<C> {
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

            let metric = &self.metrics[best_range.as_ref().unwrap().1];
            let bw_graph = metric.backward_graph();

            let target = metric.cch().node_order().rank(target);

            let mut walk = EliminationTreeWalk::query(
                &bw_graph,
                metric.cch().elimination_tree(),
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
        self.current_metric.and_then(|best_metric| {
            let metric = &self.metrics[best_metric];

            let node = metric.cch().node_order().rank(node);

            let mut cur_node = node;
            while self.potentials[cur_node as usize].value().is_none() {
                self.num_pot_computations += 1;
                self.stack.push(cur_node);
                if let Some(parent) = metric.cch().elimination_tree()[cur_node as usize].value() {
                    cur_node = parent;
                } else {
                    break;
                }
            }

            while let Some(node) = self.stack.pop() {
                let mut dist = self.backward_distances[node as usize];

                for edge in LinkIterable::<Link>::link_iter(&metric.forward_graph(), node) {
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

pub struct CorridorCCHPotential<'a> {
    cch: &'a CCH,
    stack: Vec<NodeId>,
    potentials: TimestampedVector<InRangeOption<(Weight, Weight)>>,
    forward_cch_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<(Weight, Weight)>, (Weight, Weight)>,
    backward_distances: TimestampedVector<(Weight, Weight)>,
    backward_cch_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<(Weight, Weight)>, (Weight, Weight)>,
}

impl<'a> CorridorCCHPotential<'a> {
    fn init(&mut self, target: NodeId) {
        self.potentials.reset();
        self.backward_distances.reset();
        self.backward_distances[target as usize] = (0, 0);
        let mut node = Some(target);
        while let Some(current) = node {
            for (NodeIdT(head), (lower, upper), _) in LinkIterable::<(NodeIdT, (Weight, Weight), EdgeIdT)>::link_iter(&self.backward_cch_graph, current) {
                self.backward_distances[head as usize].0 = min(self.backward_distances[head as usize].0, self.backward_distances[current as usize].0 + lower);
                self.backward_distances[head as usize].1 = min(self.backward_distances[head as usize].1, self.backward_distances[current as usize].1 + upper);
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
            let mut dist = self.backward_distances[node as usize];

            for (NodeIdT(head), (lower, upper), _) in LinkIterable::<(NodeIdT, (Weight, Weight), EdgeIdT)>::link_iter(&self.forward_cch_graph, node) {
                let (head_lower, head_upper) = self.potentials[head as usize].value().unwrap();
                dist.0 = min(dist.0, lower + head_lower);
                dist.1 = min(dist.1, upper + head_upper);
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

pub struct CorridorBounds<'a> {
    corridor_pot: CorridorCCHPotential<'a>,
    fw_graph: UnweightedOwnedGraph,
    bw_graph: UnweightedOwnedGraph,
    fw_weights: Vec<Box<[Weight]>>,
    bw_weights: Vec<Box<[Weight]>>,
    stack: Vec<NodeId>,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    backward_distances: TimestampedVector<Weight>,
    global_upper: Weight,
    departure: Timestamp,
}

impl<'a> CorridorBounds<'a> {
    pub fn new(cch: &'a CCH, catchup: customizable_contraction_hierarchy::customization::ftd_for_pot::PotData) -> Self {
        let n = cch.num_nodes();
        Self {
            corridor_pot: CorridorCCHPotential {
                cch,
                stack: Vec::new(),
                potentials: TimestampedVector::new(n),
                backward_distances: TimestampedVector::new(n),
                // switched directions
                forward_cch_graph: FirstOutGraph::new(cch.first_out(), cch.head(), catchup.bw_static_bound),
                backward_cch_graph: FirstOutGraph::new(cch.first_out(), cch.head(), catchup.fw_static_bound),
            },
            fw_graph: UnweightedOwnedGraph::new(cch.first_out().to_vec(), cch.head().to_vec()),
            bw_graph: UnweightedOwnedGraph::new(cch.first_out().to_vec(), cch.head().to_vec()),
            fw_weights: catchup.fw_bucket_bounds,
            bw_weights: catchup.bw_bucket_bounds,
            stack: Vec::new(),
            potentials: TimestampedVector::new(n),
            backward_distances: TimestampedVector::new(n),
            global_upper: INFINITY,
            departure: INFINITY,
        }
    }
}

impl CorridorBounds<'_> {
    fn to_metric_idx(&self, t: Timestamp) -> usize {
        t as usize * self.fw_weights.len() / period() as usize
    }
}

impl TDPotential for CorridorBounds<'_> {
    fn init(&mut self, source: NodeId, target: NodeId, departure: Timestamp) {
        let target = self.corridor_pot.cch.node_order().rank(target);
        let source = self.corridor_pot.cch.node_order().rank(source);
        self.departure = departure;

        self.corridor_pot.init(source);

        self.potentials.reset();
        self.backward_distances.reset();
        self.backward_distances[target as usize] = 0;
        self.global_upper = self.corridor_pot.potential(target).map(|(_, u)| u).unwrap_or(INFINITY);

        let mut node = Some(target);
        while let Some(current) = node {
            if self.backward_distances[current as usize] + self.corridor_pot.potential(current).map(|(l, _)| l).unwrap_or(INFINITY) <= self.global_upper {
                for (NodeIdT(head), EdgeIdT(edge_id)) in LinkIterable::<(NodeIdT, EdgeIdT)>::link_iter(&self.bw_graph, current) {
                    if let Some((lower, upper)) = self.corridor_pot.potential(head) {
                        if lower <= self.global_upper {
                            let metric_indices = self.to_metric_idx(departure + lower)..=self.to_metric_idx(departure + upper);
                            let mut weight_lower = INFINITY;
                            for idx in metric_indices {
                                weight_lower = min(weight_lower, self.bw_weights[idx % self.bw_weights.len()][edge_id as usize]);
                            }
                            self.backward_distances[head as usize] =
                                min(self.backward_distances[head as usize], weight_lower + self.backward_distances[current as usize]);
                        }
                    }
                }
            }
            node = self.corridor_pot.cch.elimination_tree()[current as usize].value();
        }
    }

    fn potential(&mut self, node: NodeId, _t: Option<Timestamp>) -> Option<Weight> {
        let node = self.corridor_pot.cch.node_order().rank(node);

        let mut cur_node = node;
        while self.potentials[cur_node as usize].value().is_none() {
            self.stack.push(cur_node);
            if let Some(parent) = self.corridor_pot.cch.elimination_tree()[cur_node as usize].value() {
                cur_node = parent;
            } else {
                break;
            }
        }

        while let Some(node) = self.stack.pop() {
            let mut dist = INFINITY;
            if let Some((lower, upper)) = self.corridor_pot.potential(node) {
                if lower <= self.global_upper {
                    dist = self.backward_distances[node as usize];

                    for (NodeIdT(head), EdgeIdT(edge_id)) in LinkIterable::<(NodeIdT, EdgeIdT)>::link_iter(&self.fw_graph, node) {
                        let metric_indices = self.to_metric_idx(self.departure + lower)..=self.to_metric_idx(self.departure + upper);
                        let mut weight_lower = INFINITY;
                        for idx in metric_indices {
                            weight_lower = min(weight_lower, self.fw_weights[idx % self.bw_weights.len()][edge_id as usize]);
                        }
                        dist = min(dist, self.potentials[head as usize].value().unwrap() + weight_lower);
                    }
                }
            }

            self.potentials[node as usize] = InRangeOption::some(dist);
        }

        let dist = self.potentials[node as usize].value().unwrap();
        if dist < INFINITY {
            Some(dist)
        } else {
            None
        }
    }
}
