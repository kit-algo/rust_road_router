//! Elimination Tree path to root traversal while relaxing edges.

use std::ops::IndexMut;

use super::*;
use crate::{datastr::timestamped_vector::TimestampedVector, util::in_range_option::InRangeOption};

#[derive(Debug)]
pub struct EliminationTreeWalk<'a, Graph, DistCont> {
    graph: &'a Graph,
    distances: &'a mut DistCont,
    predecessors: &'a mut [(NodeId, EdgeId)],
    elimination_tree: &'a [InRangeOption<NodeId>],
    next: Option<NodeId>,
}

impl<'a, Graph: LinkIterable<(NodeIdT, Weight, EdgeIdT)>> EliminationTreeWalk<'a, Graph, TimestampedVector<Weight>> {
    pub fn query(
        graph: &'a Graph,
        elimination_tree: &'a [InRangeOption<NodeId>],
        distances: &'a mut TimestampedVector<Weight>,
        predecessors: &'a mut [(NodeId, EdgeId)],
        from: NodeId,
    ) -> Self {
        distances.reset();
        Self::query_with_resetted(graph, elimination_tree, distances, predecessors, from)
    }
}

impl<'a, Graph: LinkIterable<(NodeIdT, Weight, EdgeIdT)>, DistCont: IndexMut<usize, Output = Weight>> EliminationTreeWalk<'a, Graph, DistCont> {
    pub fn query_with_resetted(
        graph: &'a Graph,
        elimination_tree: &'a [InRangeOption<NodeId>],
        distances: &'a mut DistCont,
        predecessors: &'a mut [(NodeId, EdgeId)],
        from: NodeId,
    ) -> Self {
        if cfg!(debug_assertions) {
            let mut cur_node = Some(from);
            while let Some(node) = cur_node {
                assert_eq!(distances[node as usize], INFINITY);
                cur_node = elimination_tree[node as usize].value();
            }
        }

        // Starte with origin
        distances[from as usize] = 0;
        predecessors[from as usize] = (from, 0);

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

            for (NodeIdT(head), weight, EdgeIdT(edge_idx)) in LinkIterable::<(NodeIdT, Weight, EdgeIdT)>::link_iter(self.graph, node) {
                let next_dist = distance + weight;

                if next_dist < self.distances[head as usize] {
                    // Relaxation, we have now found a better way
                    self.distances[head as usize] = next_dist;
                    self.predecessors[head as usize] = (node, edge_idx);
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

    pub fn skip_next(&mut self) {
        // Iterator::skip(n) would still call `next` and thus relax edges, we want to actually skip them
        if let Some(node) = self.next {
            self.next = self.elimination_tree[node as usize].value();
        }
    }

    pub fn tentative_distance(&self, node: NodeId) -> Weight {
        self.distances[node as usize]
    }

    pub fn reset_distance(&mut self, node: NodeId) {
        self.distances[node as usize] = INFINITY;
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.predecessors[node as usize].0
    }
}

impl<'a, Graph: LinkIterable<(NodeIdT, Weight, EdgeIdT)>, DistCont: IndexMut<usize, Output = Weight>> Iterator for EliminationTreeWalk<'a, Graph, DistCont> {
    type Item = NodeId;
    fn next(&mut self) -> Option<Self::Item> {
        self.settle_next_node()
    }
}
