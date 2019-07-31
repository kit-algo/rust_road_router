use super::*;
use self::timestamped_vector::TimestampedVector;
use crate::index_heap::{IndexdMinHeap, Indexing};
use crate::graph::time_dependent::TDGraph;

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
pub struct TDSteppedDijkstra {
    graph: TDGraph,
    distances: TimestampedVector<Weight>,
    predecessors: Vec<NodeId>,
    closest_node_priority_queue: IndexdMinHeap<State>,
    // the current query
    query: Option<TDQuery>,
    // first option: algorithm finished? second option: final result of algorithm
    result: Option<Option<Weight>>,
}

impl TDSteppedDijkstra {
    pub fn new(graph: TDGraph) -> TDSteppedDijkstra {
        let n = graph.num_nodes();

        TDSteppedDijkstra {
            graph,
            // initialize tentative distances to INFINITY
            distances: TimestampedVector::new(n, INFINITY),
            predecessors: vec![n as NodeId; n],
            closest_node_priority_queue: IndexdMinHeap::new(n),
            query: None,
            result: None,
        }
    }

    pub fn initialize_query(&mut self, query: TDQuery) {
        let from = query.from;
        // initialize
        self.query = Some(query);
        self.result = None;
        self.closest_node_priority_queue.clear();
        self.distances.reset();

        // Starte with origin
        self.distances.set(from as usize, query.departure_time);
        self.closest_node_priority_queue.push(State { distance: query.departure_time, node: from });
    }

    pub fn next_step<F: Fn(EdgeId) -> bool, G: FnMut(NodeId) -> Weight>(&mut self, check_edge: F, potential: G) -> QueryProgress {
        match self.result {
            Some(result) => QueryProgress::Done(result),
            None => {
                self.settle_next_node(check_edge, potential)
            }
        }
    }

    fn settle_next_node<F: Fn(EdgeId) -> bool, G: FnMut(NodeId) -> Weight>(&mut self, check_edge: F, mut potential: G) -> QueryProgress {
        let to = self.query().to;

        // Examine the frontier with lower distance nodes first (min-heap)
        if let Some(State { node, .. }) = self.closest_node_priority_queue.pop() {
            let distance = self.distances[node as usize];

            // Alternatively we could have continued to find all shortest paths
            if node == to {
                let result = Some(distance - self.query().departure_time);
                self.result = Some(result);
                return QueryProgress::Done(result);
            }

            // For each node we can reach, see if we can find a way with
            // a lower distance going through this node
            for (&neighbor, edge_id) in self.graph.neighbor_and_edge_id_iter(node) {
                if check_edge(edge_id) {
                    let plf = self.graph.travel_time_function(edge_id);
                    let next_distance = distance + plf.eval(distance);

                    if next_distance < self.distances[neighbor as usize] {
                        self.distances.set(neighbor as usize, next_distance);
                        self.predecessors[neighbor as usize] = node;

                        let next = State { distance: next_distance + potential(neighbor), node: neighbor };
                        if self.closest_node_priority_queue.contains_index(next.as_index()) {
                            self.closest_node_priority_queue.decrease_key(next);
                        } else {
                            self.closest_node_priority_queue.push(next);
                        }
                    }
                }
            }

            QueryProgress::Progress(State { distance: distance - self.query().departure_time, node })
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

    pub fn query(&self) -> &TDQuery {
        self.query.as_ref().expect("query was not initialized properly")
    }

    pub fn graph(&self) -> &TDGraph {
        &self.graph
    }
}
