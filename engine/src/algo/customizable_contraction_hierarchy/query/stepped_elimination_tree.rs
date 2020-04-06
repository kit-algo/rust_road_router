//! Elimination Tree path to root traversal while relaxing edges.

use super::*;
use crate::as_slice::AsSlice;
use crate::datastr::timestamped_vector::TimestampedVector;
use crate::util::in_range_option::InRangeOption;

#[derive(Debug)]
pub struct SteppedEliminationTree<'b, Graph: for<'a> LinkIterGraph<'a>> {
    graph: Graph,
    distances: TimestampedVector<Weight>,
    predecessors: Vec<NodeId>,
    elimination_tree: &'b [InRangeOption<NodeId>],
    next: Option<NodeId>,
    origin: Option<NodeId>,
}

impl<'b, Graph: for<'a> LinkIterGraph<'a>> SteppedEliminationTree<'b, Graph> {
    pub fn new(graph: Graph, elimination_tree: &'b [InRangeOption<NodeId>]) -> SteppedEliminationTree<'b, Graph> {
        let n = graph.num_nodes();

        SteppedEliminationTree {
            graph,
            // initialize tentative distances to INFINITY
            distances: TimestampedVector::new(n, INFINITY),
            predecessors: vec![n as NodeId; n],
            elimination_tree,
            next: None,
            origin: None,
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

    pub fn next_step(&mut self) -> QueryProgress<Weight> {
        self.settle_next_node()
    }

    fn settle_next_node(&mut self) -> QueryProgress<Weight> {
        // Examine the next node on the path to the elimination tree node
        if let Some(node) = self.next {
            let distance = self.distances[node as usize];
            self.next = self.elimination_tree[node as usize].value();

            // For each node we can reach, see if we can find a way with
            // a lower distance going through this node
            for edge in self.graph.neighbor_iter(node) {
                let next = State {
                    distance: distance + edge.weight,
                    node: edge.node,
                };

                if next.distance < self.distances[next.node as usize] {
                    // Relaxation, we have now found a better way
                    self.distances.set(next.node as usize, next.distance);
                    self.predecessors[next.node as usize] = node;
                }
            }

            QueryProgress::Settled(State { distance, node })
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

    pub fn parent(&self, node: NodeId) -> InRangeOption<NodeId> {
        self.elimination_tree[node as usize]
    }

    pub fn origin(&self) -> NodeId {
        self.origin.unwrap()
    }

    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    pub fn graph_mut(&mut self) -> &mut Graph {
        &mut self.graph
    }
}

impl<'b, FirstOutContainer, HeadContainer, WeightContainer> SteppedEliminationTree<'b, FirstOutGraph<FirstOutContainer, HeadContainer, WeightContainer>>
where
    FirstOutContainer: AsSlice<EdgeId>,
    HeadContainer: AsSlice<NodeId>,
    WeightContainer: AsSlice<Weight>,
{
    /// Unpack path from a start node (the meeting node of the CCH query), so that parent pointers point along the unpacked path.
    pub fn unpack_path(&mut self, target: NodeId, forward: bool, cch: &dyn CCHT, other_weights: &[Weight]) {
        let origin = self.origin();
        let mut current = target;
        while current != origin {
            let pred = self.predecessor(current);
            let weight = self.tentative_distance(current) - self.tentative_distance(pred);

            let unpacked = if forward {
                cch.unpack_arc(pred, current, weight, self.graph.weight(), other_weights)
            } else {
                cch.unpack_arc(current, pred, weight, other_weights, self.graph.weight())
            };
            if let Some((middle, first_weight, second_weight)) = unpacked {
                self.predecessors[current as usize] = middle;
                self.predecessors[middle as usize] = pred;
                self.distances[middle as usize] = self.tentative_distance(pred) + if forward { first_weight } else { second_weight };
            } else {
                current = pred;
            }
        }
    }
}
