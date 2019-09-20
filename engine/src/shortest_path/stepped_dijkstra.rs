use super::*;
use self::timestamped_vector::TimestampedVector;
use crate::index_heap::{IndexdMinHeap, Indexing};

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

impl std::cmp::PartialOrd for State {
    #[inline]
    fn partial_cmp(&self, rhs: &Self) -> Option<std::cmp::Ordering> {
        self.distance.partial_cmp(&rhs.distance)
    }
}

impl std::cmp::Ord for State {
    #[inline]
    fn cmp(&self, rhs: &Self) -> std::cmp::Ordering {
        self.distance.cmp(&rhs.distance)
    }
}

impl Indexing for State {
    #[inline]
    fn as_index(&self) -> usize {
        self.node as usize
    }
}

#[derive(Debug)]
pub struct SteppedDijkstra<Graph: for<'a> LinkIterGraph<'a>> {
    graph: Graph,
    distances: TimestampedVector<Weight>,
    predecessors: Vec<NodeId>,
    closest_node_priority_queue: IndexdMinHeap<State>,
    // the current query
    query: Option<Query>,
    // first option: algorithm finished? second option: final result of algorithm
    result: Option<Option<Weight>>
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
            result: None
        }
    }

    pub fn initialize_query(&mut self, query: Query) {
        let from = query.from;
        // initialize
        self.query = Some(query);
        self.result = None;
        self.closest_node_priority_queue.clear();
        self.distances.reset();

        self.closest_node_priority_queue.push(State { distance: 0, node: from });
    }

    pub fn next_step(&mut self) -> QueryProgress {
        match self.result {
            Some(result) => QueryProgress::Done(result),
            None => {
                self.settle_next_node(|_, _| {}, |_| {})
            }
        }
    }

    pub fn next_step_with_callbacks(&mut self, on_improve: impl FnMut(NodeId, Weight), on_queue_insert: impl FnMut(NodeId)) -> QueryProgress {
        match self.result {
            Some(result) => QueryProgress::Done(result),
            None => {
                self.settle_next_node(on_improve, on_queue_insert)
            }
        }
    }

    fn settle_next_node(&mut self, mut on_improve: impl FnMut(NodeId, Weight), mut on_queue_insert: impl FnMut(NodeId)) -> QueryProgress {
        let to = self.query.as_ref().expect("query was not initialized properly").to;

        // Examine the frontier with lower distance nodes first (min-heap)
        if let Some(State { distance, node }) = self.closest_node_priority_queue.pop() {
            // Alternatively we could have continued to find all shortest paths
            if node == to {
                self.result = Some(Some(distance));
                return QueryProgress::Done(Some(distance));
            }

            // For each node we can reach, see if we can find a way with
            // a lower distance going through this node
            for edge in self.graph.neighbor_iter(node) {
                let next = State { distance: distance + edge.weight, node: edge.node };

                // If so, add it to the frontier and continue
                if next.distance < self.distances[next.node as usize] {
                    // Relaxation, we have now found a better way
                    self.distances.set(next.node as usize, next.distance);
                    self.predecessors[next.node as usize] = node;
                    if self.closest_node_priority_queue.contains_index(next.as_index()) {
                        self.closest_node_priority_queue.decrease_key(next);
                    } else {
                        on_queue_insert(next.node);
                        self.closest_node_priority_queue.push(next);
                    }

                    on_improve(next.node, next.distance);
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

    pub fn distances_pointer(&self) -> *const TimestampedVector<Weight> {
        &self.distances
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
}
