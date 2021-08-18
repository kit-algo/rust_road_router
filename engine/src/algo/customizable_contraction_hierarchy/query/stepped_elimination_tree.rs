//! Elimination Tree path to root traversal while relaxing edges.

use super::*;
use crate::util::in_range_option::InRangeOption;

#[derive(Debug)]
pub struct EliminationTreeWalk<'a, Graph> {
    graph: &'a Graph,
    distances: &'a mut [Weight],
    predecessors: &'a mut [NodeId],
    elimination_tree: &'a [InRangeOption<NodeId>],
    next: Option<NodeId>,
}

impl<'a, Graph: LinkIterGraph> EliminationTreeWalk<'a, Graph> {
    pub fn query(
        graph: &'a Graph,
        elimination_tree: &'a [InRangeOption<NodeId>],
        distances: &'a mut [Weight],
        predecessors: &'a mut [NodeId],
        from: NodeId,
    ) -> Self {
        // initialize
        let mut cur = from;
        while let Some(parent) = elimination_tree[cur as usize].value() {
            distances[parent as usize] = INFINITY;
            cur = parent;
        }

        // Starte with origin
        distances[from as usize] = 0;

        Self {
            graph,
            distances,
            predecessors,
            elimination_tree,
            next: Some(from),
        }
    }

    fn settle_next_node(&mut self) -> Option<NodeId> {
        // Examine the next node on the path to the elimination tree node
        if let Some(node) = self.next {
            let distance = self.distances[node as usize];
            self.next = self.elimination_tree[node as usize].value();

            // For each node we can reach, see if we can find a way with
            // a lower distance going through this node
            for edge in self.graph.link_iter(node) {
                let next_dist = distance + edge.weight;

                if next_dist < self.distances[edge.node as usize] {
                    // Relaxation, we have now found a better way
                    self.distances[edge.node as usize] = next_dist;
                    self.predecessors[edge.node as usize] = node;
                }
            }

            Some(node)
        } else {
            None
        }
    }

    pub fn peek(&self) -> Option<NodeId> {
        self.next
    }

    pub fn tentative_distance(&self, node: NodeId) -> Weight {
        self.distances[node as usize]
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.predecessors[node as usize]
    }
}

impl<'a, Graph: LinkIterGraph> Iterator for EliminationTreeWalk<'a, Graph> {
    type Item = NodeId;
    fn next(&mut self) -> Option<Self::Item> {
        self.settle_next_node()
    }
}
