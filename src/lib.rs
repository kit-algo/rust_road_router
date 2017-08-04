pub type NodeId = u32;
pub type Weight = u32;
pub const INFINITY: u32 = std::u32::MAX / 2;

#[derive(Debug)]
pub struct Graph {
    first_out: Vec<u32>,
    head: Vec<NodeId>,
    weight: Vec<Weight>
}

pub struct Link {
    pub node: NodeId,
    pub cost: Weight
}

impl Graph {
    pub fn new(first_out: Vec<u32>, head: Vec<NodeId>, weight: Vec<Weight>) -> Graph {
        Graph {
            first_out, head, weight
        }
    }

    pub fn neighbor_iter(&self, node: NodeId) -> std::iter::Map<std::iter::Zip<std::slice::Iter<NodeId>, std::slice::Iter<Weight>>, fn((&NodeId, &Weight))->Link> {
        let range = (self.first_out[node as usize] as usize)..(self.first_out[(node + 1) as usize] as usize);
        self.head[range.clone()].iter()
            .zip(self.weight[range].iter())
            .map( |(&neighbor, &weight)| Link { node: neighbor, cost: weight } )
    }

    pub fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }
}

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::usize;

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


#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn it_works() {
        // This is the directed graph we're going to use.
        // The node numbers correspond to the different states,
        // and the edge weights symbolize the cost of moving
        // from one node to another.
        // Note that the edges are one-way.
        //
        //                  7
        //          +-----------------+
        //          |                 |
        //          v   1        2    |  2
        //          0 -----> 1 -----> 3 ---> 4
        //          |        ^        ^      ^
        //          |        | 1      |      |
        //          |        |        | 3    | 1
        //          +------> 2 -------+      |
        //           10      |               |
        //                   +---------------+
        //
        // The graph is represented as an adjacency list where each index,
        // corresponding to a node value, has a list of outgoing edges.
        // Chosen for its efficiency.
        let graph = Graph::new(
            vec![0,      2,  3,        6,    8, 8, 8],
            vec![2,  1,  3,  1, 3, 4,  0, 4],
            vec![10, 1,  2,  1, 3, 1,  7, 2]);
        // let graph = vec![
        //     // Node 0
        //     vec![Edge { node: 2, cost: 10 },
        //          Edge { node: 1, cost: 1 }],
        //     // Node 1
        //     vec![Edge { node: 3, cost: 2 }],
        //     // Node 2
        //     vec![Edge { node: 1, cost: 1 },
        //          Edge { node: 3, cost: 3 },
        //          Edge { node: 4, cost: 1 }],
        //     // Node 3
        //     vec![Edge { node: 0, cost: 7 },
        //          Edge { node: 4, cost: 2 }],
        //     // Node 4
        //     vec![]];

        assert_eq!(shortest_path(&graph, 0, 1), Some(1));
        assert_eq!(shortest_path(&graph, 0, 3), Some(3));
        assert_eq!(shortest_path(&graph, 3, 0), Some(7));
        assert_eq!(shortest_path(&graph, 0, 4), Some(5));
        assert_eq!(shortest_path(&graph, 4, 0), None);
    }
}


