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
    fn potential(&mut self, node: NodeId, t: Timestamp) -> Option<Weight>;
}

impl<T: Potential> TDPotential for T {
    fn init(&mut self, _source: NodeId, target: NodeId, _departure: Timestamp) {
        self.init(target)
    }
    fn potential(&mut self, node: NodeId, _t: Timestamp) -> Option<Weight> {
        self.potential(node)
    }
}

pub struct MultiMetric<C> {
    metric_ranges: Vec<std::ops::Range<Timestamp>>,
    metrics: Vec<C>,
    stack: Vec<NodeId>,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    backward_distances: TimestampedVector<Weight>,
    backward_parents: Vec<(NodeId, EdgeId)>,
    upper_bound_dist: query::Server<C>,
    num_pot_computations: usize,
    current_metric: Option<usize>,
}

impl<C: Customized> TDPotential for MultiMetric<C> {
    fn init(&mut self, source: NodeId, target: NodeId, departure: Timestamp) {
        self.num_pot_computations = 0;
        self.potentials.reset();
        // TODO permutation foo server vs the ones here?
        if let Some(upper_bound) = self.upper_bound_dist.query(Query { from: source, to: target }).distance() {
            let latest_arrival = departure + upper_bound;
            let mut best_range_idx: Option<usize> = None;
            for (idx, range) in self.metric_ranges.iter().enumerate() {
                // TODO wrapping foo
                if range.start <= departure && range.end > latest_arrival {
                    if let Some(best_range_idx) = &mut best_range_idx {
                        let best_range = &self.metric_ranges[*best_range_idx];
                        if range.start >= best_range.start && range.end <= best_range.end {
                            *best_range_idx = idx;
                        }
                    } else {
                        best_range_idx = Some(idx);
                    }
                }
            }

            let metric = &self.metrics[best_range_idx.unwrap()];
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

            self.current_metric = Some(best_range_idx.unwrap());
        } else {
            self.current_metric = None;
        }
    }
    fn potential(&mut self, node: NodeId, _t: Timestamp) -> Option<Weight> {
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
    forward_cch_graph: BorrowedGraph<'a, (Weight, Weight)>,
    backward_distances: TimestampedVector<(Weight, Weight)>,
    backward_cch_graph: BorrowedGraph<'a, (Weight, Weight)>,
}

impl<'a> CorridorCCHPotential<'a> {
    fn init(&mut self, target: NodeId) {
        self.potentials.reset();
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

impl CorridorBounds<'_> {
    fn to_metric_idx(&self, t: Timestamp) -> usize {
        (t % period()) as usize / self.fw_weights.len()
    }
}

impl TDPotential for CorridorBounds<'_> {
    fn init(&mut self, source: NodeId, target: NodeId, departure: Timestamp) {
        let target = self.corridor_pot.cch.node_order().rank(target);
        let source = self.corridor_pot.cch.node_order().rank(source);
        self.departure = departure;

        self.corridor_pot.init(source);

        self.backward_distances.reset();
        self.backward_distances[target as usize] = 0;
        self.global_upper = self.corridor_pot.potential(target).unwrap().1;

        let mut node = Some(target);
        while let Some(current) = node {
            if self.backward_distances[current as usize] + self.corridor_pot.potential(current).unwrap().0 <= self.global_upper {
                for (NodeIdT(head), EdgeIdT(edge_id)) in LinkIterable::<(NodeIdT, EdgeIdT)>::link_iter(&self.bw_graph, current) {
                    if let Some((lower, upper)) = self.corridor_pot.potential(head) {
                        if lower <= self.global_upper {
                            let metric_indices = self.to_metric_idx(departure + lower)..=self.to_metric_idx(departure + upper);
                            let mut weight_lower = INFINITY;
                            for idx in metric_indices {
                                weight_lower = min(weight_lower, self.bw_weights[idx][edge_id as usize]);
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

    fn potential(&mut self, node: NodeId, _t: Timestamp) -> Option<Weight> {
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
                            weight_lower = min(weight_lower, self.fw_weights[idx][edge_id as usize]);
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
