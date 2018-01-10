use super::*;
use super::timestamped_vector::TimestampedVector;
use super::stepped_dijkstra::QueryProgress;
use ::inrange_option::InrangeOption;

#[derive(Debug)]
pub struct SteppedEliminationTree<Graph: DijkstrableGraph> {
    graph: Graph,
    distances: TimestampedVector<Weight>,
    predecessors: Vec<NodeId>,
    elimination_tree: Vec<InrangeOption<NodeId>>,
    next: Option<NodeId>,
    origin: Option<NodeId>
}

impl<Graph: DijkstrableGraph> SteppedEliminationTree<Graph> {
    pub fn new(graph: Graph, elimination_tree: Vec<InrangeOption<NodeId>>) -> SteppedEliminationTree<Graph> {
        let n = graph.num_nodes();

        SteppedEliminationTree {
            graph,
            // initialize tentative distances to INFINITY
            distances: TimestampedVector::new(n, INFINITY),
            predecessors: vec![n as NodeId; n],
            elimination_tree,
            next: None,
            origin: None
        }
    }

    pub fn initialize_query(&mut self, from: NodeId) {
        // initialize
        self.origin = Some(from);
        self.next = Some(from);
        self.distances.reset();

        // Starte with origin
        self.distances.set(from as usize, 0);
    }

    pub fn next_step(&mut self) -> QueryProgress {
        self.settle_next_node()
    }

    fn settle_next_node(&mut self) -> QueryProgress {
        // Examine the frontier with lower distance nodes first (min-heap)
        if let Some(node) = self.next {
            let distance = self.distances[node as usize];
            self.next = self.elimination_tree[node as usize].value();

            // these are necessary because otherwise the borrow checker could not figure out
            // that we're only borrowing parts of self
            let distances = &mut self.distances;
            let predecessors = &mut self.predecessors;

            // For each node we can reach, see if we can find a way with
            // a lower distance going through this node
            self.graph.for_each_neighbor(node, &mut |edge: Link| {
                let next = State { distance: distance + edge.weight, node: edge.node };

                // If so, add it to the frontier and continue
                if next.distance < distances[next.node as usize] {
                    // Relaxation, we have now found a better way
                    distances.set(next.node as usize, next.distance);
                    predecessors[next.node as usize] = node;
                }
            });

            QueryProgress::Progress(State { distance, node })
        } else {
            QueryProgress::Done(None) // TODO
        }
    }

    pub fn next(&self) -> Option<NodeId> {
        self.next
    }

    pub fn tentative_distance(&self, node: NodeId) -> Weight {
        self.distances[node as usize]
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.predecessors[node as usize]
    }

    pub fn origin(&self) -> NodeId {
        self.origin.unwrap()
    }

    pub fn graph(&self) -> &Graph {
        &self.graph
    }
}
