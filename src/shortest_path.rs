use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::cmp::min;
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

#[derive(Debug)]
pub struct ShortestPathServerBiDirDijk {
    forward_graph: Graph,
    backward_graph: Graph,
    forward_distances: Vec<Weight>,
    backward_distances: Vec<Weight>,
    run: u32,
    forward_last_update: Vec<u32>,
    backward_last_update: Vec<u32>,
    forward_heap: BinaryHeap<State>,
    backward_heap: BinaryHeap<State>,
    tentative_distance: Weight
}

impl ShortestPathServerBiDirDijk {
    pub fn new(graph: Graph) -> ShortestPathServerBiDirDijk {
        let n = graph.num_nodes();
        let reversed = graph.reverse();

        ShortestPathServerBiDirDijk {
            forward_graph: graph,
            backward_graph: reversed,
            // initialize tentative distances to INFINITY
            forward_distances: (0..n).map(|_| INFINITY).collect(),
            backward_distances: (0..n).map(|_| INFINITY).collect(),
            // initialize run counter to 0
            // will be incremented on every query
            run: 0,
            // vector containing a timestamp (by the run counter)to indicate
            // whether the tentative distance is valid in the current query
            forward_last_update: (0..n).map(|_| 0).collect(),
            backward_last_update: (0..n).map(|_| 0).collect(),
            forward_heap: BinaryHeap::new(),
            backward_heap: BinaryHeap::new(),
            tentative_distance: INFINITY
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        // initialize
        self.run += 1;
        self.tentative_distance = INFINITY;
        self.forward_heap.clear();
        self.backward_heap.clear();

        // Starte with origin
        self.forward_distances[from as usize] = 0;
        self.backward_distances[to as usize] = 0;
        self.forward_last_update[from as usize] = self.run;
        self.backward_last_update[to as usize] = self.run;
        self.forward_heap.push(State { cost: 0, position: from });
        self.backward_heap.push(State { cost: 0, position: to });

        let mut forward_progress = 0;
        let mut backward_progress = 0;
        let mut forward_done = false;
        let mut backward_done = true;

        while self.tentative_distance > forward_progress + backward_progress && !(forward_done && backward_done) {
            if let Some(&State { cost: progress, position: _ }) = self.forward_heap.peek() {
                forward_progress = progress;
            }
            if let Some(tentative_distance) = self.forward_step(to) {
                self.tentative_distance = min(tentative_distance, self.tentative_distance);
            } else {
                forward_done = true;
            }
            if let Some(&State { cost: progress, position: _ }) = self.backward_heap.peek() {
                backward_progress = progress;
            }
            if let Some(tentative_distance) = self.backward_step(from) {
                self.tentative_distance = min(tentative_distance, self.tentative_distance);
            } else {
                backward_done = true;
            }
        }

        match self.tentative_distance {
            INFINITY => None,
            dist => Some(dist)
        }
    }

    fn forward_step(&mut self, to: NodeId) -> Option<Weight> {
        // Examine the frontier with lower cost nodes first (min-heap)
        if let Some(State { cost, position }) = self.forward_heap.pop() {
            // Alternatively we could have continued to find all shortest paths
            if position == to { return Some(cost); }

            // Important as we may have already found a better way
            if self.forward_last_update[position as usize] != self.run || cost <= self.forward_distances[position as usize] {
                // For each node we can reach, see if we can find a way with
                // a lower cost going through this node
                for edge in self.forward_graph.neighbor_iter(position) {
                    let next = State { cost: cost + edge.cost, position: edge.node };

                    // If so, add it to the frontier and continue
                    if self.forward_last_update[next.position as usize] != self.run || next.cost < self.forward_distances[next.position as usize] {
                        // Relaxation, we have now found a better way
                        self.forward_distances[next.position as usize] = next.cost;
                        self.forward_last_update[next.position as usize] = self.run;
                        self.forward_heap.push(next);
                    }
                }
            }

            if self.backward_last_update[position as usize] == self.run {
                Some(cost + self.backward_distances[position as usize])
            } else {
                Some(INFINITY)
            }
        } else {
            None
        }
    }

    fn backward_step(&mut self, from: NodeId) -> Option<Weight> {
        // Examine the frontier with lower cost nodes first (min-heap)
        if let Some(State { cost, position }) = self.backward_heap.pop() {
            // Alternatively we could have continued to find all shortest paths
            if position == from { return Some(cost); }

            // Important as we may have already found a better way
            if self.backward_last_update[position as usize] != self.run || cost <= self.backward_distances[position as usize] {
                // For each node we can reach, see if we can find a way with
                // a lower cost going through this node
                for edge in self.backward_graph.neighbor_iter(position) {
                    let next = State { cost: cost + edge.cost, position: edge.node };

                    // If so, add it to the frontier and continue
                    if self.backward_last_update[next.position as usize] != self.run || next.cost < self.backward_distances[next.position as usize] {
                        // Relaxation, we have now found a better way
                        self.backward_distances[next.position as usize] = next.cost;
                        self.backward_last_update[next.position as usize] = self.run;
                        self.backward_heap.push(next);
                    }
                }
            }

            if self.forward_last_update[position as usize] == self.run {
                Some(cost + self.forward_distances[position as usize])
            } else {
                Some(INFINITY)
            }
        } else {
            None
        }
    }
}
