//! Dijkstras algorithm with optimization for degree 2 chains

use super::*;
use crate::datastr::{index_heap::*, timestamped_vector::*};

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
}

impl<Graph: for<'a> LinkIterGraph<'a>> TopoDijkstra<Graph> {
    pub fn new(graph: Graph) -> TopoDijkstra<Graph> {
        let n = graph.num_nodes();

        TopoDijkstra {
            graph,
            // initialize tentative distances to INFINITY
            distances: TimestampedVector::new(n, INFINITY),
            predecessors: vec![n as NodeId; n],
            closest_node_priority_queue: IndexdMinHeap::new(n),
            query: None,
            result: None,
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

            if node == to {
                self.result = Some(Some(distance));
            }

            if distance >= self.distances[to as usize] {
                self.result = Some(Some(self.distances[to as usize]));
            }

            for edge in self.graph.neighbor_iter(node) {
                let mut prev_node = node;
                let mut next_node = edge.node;
                let mut next_distance = distance + edge.weight;
                let mut deg_three = None;
                let mut had_deg_three = false;

                while next_distance < self.distances[next_node as usize] || deg_three.is_some() {
                    // dbg!(prev_node);
                    // dbg!(next_node);
                    // dbg!(next_distance);
                    // dbg!(deg_three);
                    // dbg!(had_deg_three);
                    let mut next_edge = None;
                    let mut cur_deg_three = None;

                    if next_distance < self.distances[next_node as usize] {
                        if self.graph.degree(next_node) <= 3 {
                            for edge in self.graph.neighbor_iter(next_node) {
                                if edge.node != prev_node {
                                    if next_edge.is_some() {
                                        // deg > 2
                                        if cur_deg_three.is_none()
                                            && deg_three.is_none()
                                            && !had_deg_three
                                            && !self.closest_node_priority_queue.contains_index(
                                                State {
                                                    distance: next_distance,
                                                    node: next_node,
                                                }
                                                .as_index(),
                                            )
                                        {
                                            cur_deg_three = Some((next_node, next_distance, edge));
                                        } else {
                                            // degree > 3 or part of virtual core
                                            next_edge = None;
                                            cur_deg_three = None;
                                            break;
                                        }
                                    } else {
                                        next_edge = Some(edge);
                                    }
                                }
                            }
                        }

                        if deg_three.is_none() && cur_deg_three.is_some() {
                            deg_three = cur_deg_three;
                        }

                        self.distances.set(next_node as usize, next_distance);
                        self.predecessors[next_node as usize] = prev_node;
                    }

                    // dbg!(next_node);
                    // dbg!(next_edge);

                    if let Some(next_edge) = next_edge {
                        prev_node = next_node;
                        next_node = next_edge.node;
                        next_distance += next_edge.weight;
                    } else {
                        if let Some(pot) = potential(next_node, next_distance) {
                            let next = State {
                                distance: pot,
                                node: next_node,
                            };
                            if self.closest_node_priority_queue.contains_index(next.as_index()) {
                                self.closest_node_priority_queue.decrease_key(next);
                            } else {
                                self.closest_node_priority_queue.push(next);
                            }
                        }
                        if let Some((deg_three_node, deg_three_distance, edge)) = deg_three {
                            prev_node = deg_three_node;
                            next_distance = deg_three_distance + edge.weight;
                            next_node = edge.node;
                            deg_three = None;
                            had_deg_three = true;
                        } else {
                            break;
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
