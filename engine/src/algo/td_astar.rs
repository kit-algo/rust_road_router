use super::a_star::*;
use super::customizable_contraction_hierarchy::{query::stepped_elimination_tree::EliminationTreeWalk, *};
use super::*;
use crate::{
    datastr::{graph::time_dependent::*, timestamped_vector::*},
    util::in_range_option::*,
};

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
                    dist = std::cmp::min(dist, edge.weight + self.potentials[edge.node as usize].value().unwrap())
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
