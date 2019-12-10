//! Basic variant of dijkstras algorithm

use super::*;
use crate::datastr::{index_heap::*, timestamped_vector::*};

#[derive(Debug)]
pub struct Trash {
    distances: TimestampedVector<Weight>,
    predecessors: Vec<NodeId>,
    closest_node_priority_queue: IndexdMinHeap<State<Weight>>,
}

#[derive(Debug)]
pub struct SteppedDijkstra<Graph: for<'a> LinkIterGraph<'a>> {
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

impl<Graph: for<'a> LinkIterGraph<'a>> SteppedDijkstra<Graph> {
    pub fn new(graph: Graph) -> SteppedDijkstra<Graph> {
        let n = graph.num_nodes();

        SteppedDijkstra {
            graph,
            // initialize tentative distances to INFINITY
            distances: TimestampedVector::new(n, INFINITY),
            predecessors: vec![n as NodeId; n],
            closest_node_priority_queue: IndexdMinHeap::new(n),
            query: None,
            result: None,
        }
    }

    /// For CH preprocessing we reuse the distance array and the queue to reduce allocations.
    /// This method creates an algo struct from recycled data.
    /// The counterpart is the `recycle` method.
    pub fn from_recycled(graph: Graph, recycled: Trash) -> SteppedDijkstra<Graph> {
        let n = graph.num_nodes();
        assert!(recycled.distances.len() >= n);
        assert!(recycled.predecessors.len() >= n);

        SteppedDijkstra {
            graph,
            // initialize tentative distances to INFINITY
            distances: recycled.distances,
            predecessors: recycled.predecessors,
            closest_node_priority_queue: recycled.closest_node_priority_queue,
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

            // Alternatively we could have continued to find all shortest paths
            if node == to {
                self.result = Some(Some(distance));
            }

            // For each node we can reach, see if we can find a way with
            // a lower distance going through this node
            for edge in self.graph.neighbor_iter(node) {
                let next_distance = distance + edge.weight;

                // If so, add it to the frontier and continue
                if next_distance < self.distances[edge.node as usize] {
                    // Relaxation, we have now found a better way
                    self.distances.set(edge.node as usize, next_distance);
                    self.predecessors[edge.node as usize] = node;

                    if let Some(pot) = potential(edge.node, next_distance) {
                        let next = State {
                            distance: pot,
                            node: edge.node,
                        };
                        if self.closest_node_priority_queue.contains_index(next.as_index()) {
                            self.closest_node_priority_queue.decrease_key(next);
                        } else {
                            self.closest_node_priority_queue.push(next);
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

    /// For CH preprocessing we reuse the distance array and the queue to reduce allocations.
    /// This method decomposes this algo struct for later reuse.
    /// The counterpart is `from_recycled`
    pub fn recycle(self) -> Trash {
        Trash {
            distances: self.distances,
            predecessors: self.predecessors,
            closest_node_priority_queue: self.closest_node_priority_queue,
        }
    }
}
