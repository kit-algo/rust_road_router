use std::cmp::Ordering;
use std::collections::BinaryHeap;
use super::graph::*;

#[derive(Copy, Clone, Eq, PartialEq, Debug)]
struct State {
    cost: Weight,
    position: NodeId,
}

// The priority queue depends on `Ord`.
// Explicitly implement the trait so the queue becomes a min-heap
// instead of a max-heap.
impl Ord for State {
    fn cmp(&self, other: &State) -> Ordering {
        // Notice that the we flip the ordering on costs.
        // In case of a tie we compare positions - this step is necessary
        // to make implementations of `PartialEq` and `Ord` consistent.
        other.cost.cmp(&self.cost)
            .then_with(|| self.position.cmp(&other.position))
    }
}

// `PartialOrd` needs to be implemented as well.
impl PartialOrd for State {
    fn partial_cmp(&self, other: &State) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

#[derive(Debug)]
pub struct ShortestPathServer {
    graph: Graph,
    distances: Vec<Weight>,
    run: u32,
    last_update: Vec<u32>,
    heap: BinaryHeap<State>
}

impl ShortestPathServer {
    pub fn new(graph: Graph) -> ShortestPathServer {
        let n = graph.num_nodes();

        ShortestPathServer {
            graph,
            // initialize tentative distances to INFINITY
            distances: (0..n).map(|_| INFINITY).collect(),
            // initialize run counter to 0
            // will be incremented on every query
            run: 0,
            // vector containing a timestamp (by the run counter)to indicate
            // whether the tentative distance is valid in the current query
            last_update: (0..n).map(|_| 0).collect(),
            heap: BinaryHeap::new()
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        // initialize
        self.run += 1;
        self.heap.clear();

        // Starte with origin
        self.distances[from as usize] = 0;
        self.last_update[from as usize] = self.run;
        self.heap.push(State { cost: 0, position: from });

        // Examine the frontier with lower cost nodes first (min-heap)
        while let Some(State { cost, position }) = self.heap.pop() {
            // Alternatively we could have continued to find all shortest paths
            if position == to { return Some(cost); }

            // Important as we may have already found a better way
            if self.last_update[position as usize] == self.run && cost > self.distances[position as usize] { continue; }

            // For each node we can reach, see if we can find a way with
            // a lower cost going through this node
            for edge in self.graph.neighbor_iter(position) {
                let next = State { cost: cost + edge.cost, position: edge.node };

                // If so, add it to the frontier and continue
                if self.last_update[next.position as usize] != self.run || next.cost < self.distances[next.position as usize] {
                    // Relaxation, we have now found a better way
                    self.distances[next.position as usize] = next.cost;
                    self.last_update[next.position as usize] = self.run;
                    self.heap.push(next);

                }
            }
        }

        None
    }
}
