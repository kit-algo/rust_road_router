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
pub struct Trash {
    distances: TimestampedVector<Weight>,
    predecessors: Vec<NodeId>,
    closest_node_priority_queue: IndexdMinHeap<State>,
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
        self.distances[from as usize] = 0;

        self.closest_node_priority_queue.push(State { distance: 0, node: from });
    }

    pub fn next_step(&mut self) -> QueryProgress {
        match self.result {
            Some(result) => QueryProgress::Done(result),
            None => {
                self.settle_next_node(|_, dist| { dist }, |_, _, _| true)
            }
        }
    }

    pub fn next_step_with_callbacks(&mut self, potential: impl FnMut(NodeId, Weight) -> Weight, relax_edge: impl FnMut(NodeId, Link, &Self) -> bool) -> QueryProgress {
        match self.result {
            Some(result) => QueryProgress::Done(result),
            None => {
                self.settle_next_node(potential, relax_edge)
            }
        }
    }

    fn settle_next_node(&mut self, mut potential: impl FnMut(NodeId, Weight) -> Weight, mut relax_edge: impl FnMut(NodeId, Link, &Self) -> bool) -> QueryProgress {
        let to = self.query.as_ref().expect("query was not initialized properly").to;

        // Examine the frontier with lower distance nodes first (min-heap)
        if let Some(State { node, distance: _dist_with_pot }) = self.closest_node_priority_queue.pop() {
            let distance = self.distances[node as usize];

            // Alternatively we could have continued to find all shortest paths
            if node == to {
                self.result = Some(Some(distance));
            }

            // For each node we can reach, see if we can find a way with
            // a lower distance going through this node
            for edge in self.graph.neighbor_iter(node) {
                if !relax_edge(node, edge, &self) { continue; }
                let next_distance = distance + edge.weight;

                // If so, add it to the frontier and continue
                if next_distance < self.distances[edge.node as usize] {
                    // Relaxation, we have now found a better way
                    self.distances.set(edge.node as usize, next_distance);
                    self.predecessors[edge.node as usize] = node;

                    let next = State { distance: potential(edge.node, next_distance), node: edge.node };
                    if self.closest_node_priority_queue.contains_index(next.as_index()) {
                        self.closest_node_priority_queue.decrease_key(next);
                    } else {
                        self.closest_node_priority_queue.push(next);
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

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.predecessors[node as usize]
    }

    pub fn query(&self) -> Query {
        self.query.unwrap()
    }

    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    pub fn queue(&self) -> &IndexdMinHeap<State> {
        &self.closest_node_priority_queue
    }

    pub fn recycle(self) -> Trash {
        Trash {
            distances: self.distances,
            predecessors: self.predecessors,
            closest_node_priority_queue: self.closest_node_priority_queue,
        }
    }
}
