use super::*;
use index_heap::{IndexdMinHeap, Indexing};
use super::timestamped_vector::TimestampedVector;

#[derive(Debug, Clone)]
pub enum QueryProgress {
    Progress(State),
    Done(Option<Weight>),
}

#[derive(Copy, Clone, Eq, PartialEq, Debug, Ord, PartialOrd)]
pub struct State {
    pub distance: Weight,
    pub node: NodeId,
}

impl Indexing for State {
    fn as_index(&self) -> usize {
        self.node as usize
    }
}

#[derive(Debug)]
pub struct SteppedDijkstra<Graph: DijkstrableGraph> {
    graph: Graph,
    distances: TimestampedVector<Weight>,
    heap: IndexdMinHeap<State>,
    query: Option<Query>,
    result: Option<Option<Weight>>
}

impl<Graph: DijkstrableGraph> SteppedDijkstra<Graph> {
    pub fn new(graph: Graph) -> SteppedDijkstra<Graph> {
        let n = graph.num_nodes();

        SteppedDijkstra {
            graph,
            // initialize tentative distances to INFINITY
            distances: TimestampedVector::new(n, INFINITY),
            // priority queue
            heap: IndexdMinHeap::new(n),
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

            // these are necessary because otherwise the borrow checker could not figure out
            // that we're only borrowing parts of self
            let heap = &mut self.heap;
            let distances = &mut self.distances;

            // For each node we can reach, see if we can find a way with
            // a lower distance going through this node
            self.graph.for_each_neighbor(node, &mut |edge: Link| {
                let next = State { distance: distance + edge.weight, node: edge.node };

                // If so, add it to the frontier and continue
                if next.distance < distances[next.node as usize] {
                    // Relaxation, we have now found a better way
                    distances.set(next.node as usize, next.distance);
                    if heap.contains_index(next.as_index()) {
                        heap.decrease_key(next);
                    } else {
                        heap.push(next);
                    }
                }
            });

            QueryProgress::Progress(State { distance, node })
        } else {
            self.result = Some(None);
            QueryProgress::Done(None)
        }
    }

    pub fn tentative_distance(&self, node: NodeId) -> Weight {
        self.distances[node as usize]
    }

    pub fn distances_pointer(&self) -> *const TimestampedVector<Weight> {
        &self.distances
    }
}
