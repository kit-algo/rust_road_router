//! Dijkstras algorithm with optimization for degree 2 chains

use super::*;
use crate::datastr::{index_heap::*, timestamped_vector::*};
use crate::util::TapOps;

#[derive(Debug)]
pub struct TopoDijkstra<Graph: for<'a> LinkIterGraph<'a>> {
    graph: Graph,
    distances: TimestampedVector<Weight>,
    predecessors: Vec<NodeId>,
    closest_node_priority_queue: IndexdMinHeap<State<Weight>>,
    // the current query
    query: Option<Query>,
    // first option: algorithm finished? second option: final result of algorithm
    #[allow(clippy::option_option)]
    result: Option<Option<Weight>>,
    prev_dist: Weight,
    symmetric_degrees: Vec<u8>,
}

#[derive(Debug)]
struct ChainStep {
    prev_node: NodeId,
    next_node: NodeId,
    next_distance: NodeId,
}

impl<Graph: for<'a> LinkIterGraph<'a>> TopoDijkstra<Graph> {
    pub fn new(graph: Graph) -> TopoDijkstra<Graph> {
        let n = graph.num_nodes();

        let reversed = graph.reverse();

        let symmetric_degrees = (0..graph.num_nodes())
            .map(|node| {
                graph
                    .neighbor_iter(node as NodeId)
                    .map(|l| l.node)
                    .chain(reversed.neighbor_iter(node as NodeId).map(|l| l.node))
                    .collect::<Vec<NodeId>>()
                    .tap(|neighbors| neighbors.sort_unstable())
                    .tap(|neighbors| neighbors.dedup())
                    .len() as u8
            })
            .collect();

        TopoDijkstra {
            graph,
            // initialize tentative distances to INFINITY
            distances: TimestampedVector::new(n, INFINITY),
            predecessors: vec![n as NodeId; n],
            closest_node_priority_queue: IndexdMinHeap::new(n),
            query: None,
            result: None,
            prev_dist: 0,
            symmetric_degrees,
        }
    }

    pub fn initialize_query(&mut self, query: Query) {
        let from = query.from;
        // initialize
        self.query = Some(query);
        self.result = None;
        self.closest_node_priority_queue.clear();
        self.distances.reset();
        self.distances[from as usize] = 0;
        self.prev_dist = 0;

        self.closest_node_priority_queue.push(State { distance: 0, node: from });
    }

    pub fn next_step(&mut self) -> QueryProgress<Weight> {
        match self.result {
            Some(result) => QueryProgress::Done(result),
            None => self.settle_next_node(|_, dist| Some(dist)),
        }
    }

    pub fn next_step_with_potential(&mut self, potential: impl FnMut(NodeId, Weight) -> Option<Weight>) -> QueryProgress<Weight> {
        match self.result {
            Some(result) => QueryProgress::Done(result),
            None => self.settle_next_node(potential),
        }
    }

    fn settle_next_node(&mut self, mut potential: impl FnMut(NodeId, Weight) -> Option<Weight>) -> QueryProgress<Weight> {
        let to = self.query.as_ref().expect("query was not initialized properly").to;

        // Examine the frontier with lower distance nodes first (min-heap)
        if let Some(State {
            node,
            distance: _dist_with_pot,
        }) = self.closest_node_priority_queue.pop()
        {
            let distance = self.distances[node as usize];
            debug_assert!(_dist_with_pot >= self.prev_dist);
            debug_assert!(distance >= self.prev_dist);
            self.prev_dist = distance;

            if node == to {
                self.result = Some(Some(distance));
            }

            if distance >= self.distances[to as usize] {
                self.result = Some(Some(self.distances[to as usize]));
            }

            for edge in self.graph.neighbor_iter(node) {
                let mut chain = Some(ChainStep {
                    prev_node: node,
                    next_node: edge.node,
                    next_distance: distance + edge.weight,
                });
                let mut deg_three = None;
                let mut had_deg_three = false;

                while let Some(ChainStep {
                    prev_node,
                    next_node,
                    next_distance,
                }) = chain.take()
                {
                    if next_distance < self.distances[next_node as usize] {
                        let mut next_edge = None;
                        let mut endpoint = false;
                        debug_assert!(next_distance >= distance);

                        match self.symmetric_degrees[next_node as usize] {
                            2 => {
                                for edge in self.graph.neighbor_iter(next_node) {
                                    if edge.node != prev_node {
                                        next_edge = Some(edge);
                                    }
                                }
                            }
                            3 => {
                                if had_deg_three
                                    || self.closest_node_priority_queue.contains_index(
                                        State {
                                            distance: next_distance,
                                            node: next_node,
                                        }
                                        .as_index(),
                                    )
                                {
                                    endpoint = true;
                                } else {
                                    had_deg_three = true;
                                    for edge in self.graph.neighbor_iter(next_node) {
                                        if edge.node != prev_node {
                                            if next_edge.is_none() {
                                                next_edge = Some(edge);
                                            } else {
                                                deg_three = Some((next_node, next_distance, edge));
                                            }
                                        }
                                    }
                                }
                            }
                            d if d > 3 => {
                                endpoint = true;
                            }
                            _ => {}
                        }

                        self.distances.set(next_node as usize, next_distance);
                        self.predecessors[next_node as usize] = prev_node;

                        if let Some(next_edge) = next_edge {
                            chain = Some(ChainStep {
                                prev_node: next_node,
                                next_node: next_edge.node,
                                next_distance: next_distance + next_edge.weight,
                            });
                        } else if endpoint {
                            debug_assert!(self.symmetric_degrees[next_node as usize] > 2);
                            if let Some(pot) = potential(next_node, next_distance) {
                                let next = State {
                                    distance: pot,
                                    node: next_node,
                                };
                                if let Some(other) = self.closest_node_priority_queue.get(next.as_index()) {
                                    assert!(other.distance >= next.distance);
                                    self.closest_node_priority_queue.decrease_key(next);
                                } else {
                                    self.closest_node_priority_queue.push(next);
                                }
                            }
                        }
                    }

                    if chain.is_none() {
                        if let Some((deg_three_node, deg_three_distance, edge)) = deg_three.take() {
                            chain = Some(ChainStep {
                                prev_node: deg_three_node,
                                next_distance: deg_three_distance + edge.weight,
                                next_node: edge.node,
                            });
                        }
                    }
                }
            }

            QueryProgress::Settled(State { distance, node })
        } else {
            self.result = Some(None);
            QueryProgress::Done(None)
        }
    }

    pub fn tentative_distance(&self, node: NodeId) -> Weight {
        self.distances[node as usize]
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.predecessors[node as usize]
    }

    pub fn query(&self) -> Query {
        self.query.unwrap()
    }

    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    pub fn queue(&self) -> &IndexdMinHeap<State<Weight>> {
        &self.closest_node_priority_queue
    }
}
