use std::cmp::Ordering;
use std::collections::BinaryHeap;

use super::Query;
use ::graph::*;
use graph::first_out_graph::FirstOutGraph as Graph;
use super::timestamped_vector::TimestampedVector;

#[derive(Debug, Clone)]
pub enum QueryProgress {
    Progress(State),
    Done(Option<Weight>),
}

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct State {
    pub distance: Weight,
    pub node: NodeId,
}

// The priority queue depends on `Ord`.
// Explicitly implement the trait so the queue becomes a min-heap
// instead of a max-heap.
impl Ord for State {
    fn cmp(&self, other: &State) -> Ordering {
        // Notice that the we flip the ordering on distances.
        // In case of a tie we compare nodes - this step is necessary
        // to make implementations of `PartialEq` and `Ord` consistent.
        other.distance.cmp(&self.distance)
            .then_with(|| self.node.cmp(&other.node))
    }
}

// `PartialOrd` needs to be implemented as well.
impl PartialOrd for State {
    fn partial_cmp(&self, other: &State) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub struct SteppedDijkstra {
    graph: Graph,
    distances: TimestampedVector<Weight>,
    heap: BinaryHeap<State>,
    query: Option<Query>,
    result: Option<Option<Weight>>
}

impl SteppedDijkstra {
    pub fn new(graph: Graph) -> SteppedDijkstra {
        let n = graph.num_nodes();

        SteppedDijkstra {
            graph,
            // initialize tentative distances to INFINITY
            distances: TimestampedVector::new(n, INFINITY),
            // priority queue
            heap: BinaryHeap::new(),
            // the current query
            query: None,
            // the progress of the current query
            result: None
        }
    }

    pub fn initialize_query(&mut self, query: Query) {
        let from = query.from;
        // initialize
        self.query = Some(query);
        self.result = None;
        self.heap.clear();
        self.distances.reset();

        // Starte with origin
        self.distances.set(from as usize, 0);
        self.heap.push(State { distance: 0, node: from });
    }

    pub fn next_step(&mut self) -> QueryProgress {
        match self.result {
            Some(result) => QueryProgress::Done(result),
            None => {
                self.settle_next_node()
            }
        }
    }

    fn settle_next_node(&mut self) -> QueryProgress {
        let to = self.query.as_ref().expect("query was not initialized properly").to;

        // Examine the frontier with lower distance nodes first (min-heap)
        if let Some(State { distance, node }) = self.heap.pop() {
            // Alternatively we could have continued to find all shortest paths
            if node == to {
                self.result = Some(Some(distance));
                return QueryProgress::Done(Some(distance));
            }

            // Important as we may have already found a better way
            if distance <= self.distances[node as usize] {
                // For each node we can reach, see if we can find a way with
                // a lower distance going through this node
                for edge in self.graph.neighbor_iter(node) {
                    let next = State { distance: distance + edge.weight, node: edge.node };

                    // If so, add it to the frontier and continue
                    if next.distance < self.distances[next.node as usize] {
                        // Relaxation, we have now found a better way
                        self.distances.set(next.node as usize, next.distance);
                        self.heap.push(next);
                    }
                }
            }

            QueryProgress::Progress(State { distance, node })
        } else {
            self.result = Some(None);
            QueryProgress::Done(None)
        }
    }

    pub fn tentative_distance(&self, node: NodeId) -> Weight {
        self.distances[node as usize]
    }
}
