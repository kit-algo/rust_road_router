use std::cmp::Ordering;
use std::collections::BinaryHeap;
use super::*;

#[derive(Copy, Clone, Eq, PartialEq)]
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

// Dijkstra's shortest path algorithm.

// Start at `start` and use `dist` to track the current shortest distance
// to each node. This implementation isn't memory-efficient as it may leave duplicate
// nodes in the queue. It also uses `usize::MAX` as a sentinel value,
// for a simpler implementation.
pub fn shortest_path(graph: &Graph, start: NodeId, goal: NodeId) -> Option<Weight> {
    // dist[node] = current shortest distance from `start` to `node`
    let mut dist: Vec<_> = (0..graph.num_nodes()).map(|_| INFINITY).collect();

    let mut heap = BinaryHeap::new();

    // We're at `start`, with a zero cost
    dist[start as usize] = 0;
    heap.push(State { cost: 0, position: start });

    // Examine the frontier with lower cost nodes first (min-heap)
    while let Some(State { cost, position }) = heap.pop() {
        // Alternatively we could have continued to find all shortest paths
        if position == goal { return Some(cost); }

        // Important as we may have already found a better way
        if cost > dist[position as usize] { continue; }

        // For each node we can reach, see if we can find a way with
        // a lower cost going through this node
        for edge in graph.neighbor_iter(position) {
            let next = State { cost: cost + edge.cost, position: edge.node };

            // If so, add it to the frontier and continue
            if next.cost < dist[next.position as usize] {
                heap.push(next);
                // Relaxation, we have now found a better way
                dist[next.position as usize] = next.cost;
            }
        }
    }

    // Goal not reachable
    None
}
