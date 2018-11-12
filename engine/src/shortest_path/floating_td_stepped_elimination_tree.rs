use crate::graph::floating_time_dependent::*;
use std::cmp::{min, max};
use super::*;
use super::timestamped_vector::TimestampedVector;
use crate::in_range_option::InRangeOption;
use crate::graph::floating_time_dependent::SingleDirShortcutGraph;

#[derive(Debug, Clone)]
pub enum QueryProgress {
    Progress(NodeId),
    Done,
}

#[derive(Debug, Clone)]
pub struct Label {
    pub upper_bound: FlWeight,
    pub lower_bound: FlWeight, // TODO do we need this one?
    pub parent: NodeId,
    pub shortcut_id: EdgeId
}

#[derive(Debug, Clone)]
pub struct NodeData {
    pub labels: Vec<Label>,
    pub upper_bound: FlWeight,
    pub lower_bound: FlWeight,
}

#[derive(Debug)]
pub struct FloatingTDSteppedEliminationTree<'a, 'b> {
    graph: SingleDirShortcutGraph<'a>,
    distances: TimestampedVector<NodeData>,
    elimination_tree: &'b [InRangeOption<NodeId>],
    next: Option<NodeId>,
    origin: Option<NodeId>
}

impl<'a, 'b> FloatingTDSteppedEliminationTree<'a, 'b> {
    pub fn new(graph: SingleDirShortcutGraph<'a>, elimination_tree: &'b [InRangeOption<NodeId>]) -> FloatingTDSteppedEliminationTree<'a, 'b> {
        let n = graph.num_nodes();

        FloatingTDSteppedEliminationTree {
            graph,
            distances: TimestampedVector::new(n, NodeData { labels: Vec::new(), lower_bound: FlWeight::new(f64::from(INFINITY)), upper_bound: FlWeight::new(f64::from(INFINITY)) }),
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
        self.distances.set(from as usize, NodeData { labels: Vec::new(), lower_bound: FlWeight::zero(), upper_bound: FlWeight::zero() });
    }

    pub fn next_step(&mut self) -> QueryProgress {
        self.settle_next_node()
    }

    fn settle_next_node(&mut self) -> QueryProgress {
        if let Some(node) = self.next {
            let current_state_lower_bound = self.distances[node as usize].lower_bound;
            let current_state_upper_bound = self.distances[node as usize].upper_bound;
            self.next = self.elimination_tree[node as usize].value();

            for ((target, shortcut_id), shortcut) in self.graph.neighbor_iter(node) {
                let next = Label {
                    parent: node,
                    lower_bound: shortcut.lower_bound + current_state_lower_bound,
                    upper_bound: shortcut.upper_bound + current_state_upper_bound,
                    shortcut_id
                };

                debug_assert!(next.lower_bound <= next.upper_bound);

                if next.upper_bound <= self.distances[target as usize].lower_bound {
                    self.distances[target as usize].lower_bound = next.lower_bound;
                    self.distances[target as usize].upper_bound = next.upper_bound;
                    self.distances[target as usize].labels = vec![next];
                } else if next.lower_bound < self.distances[target as usize].upper_bound {
                    self.distances[target as usize].lower_bound = min(next.lower_bound, self.distances[target as usize].lower_bound);
                    self.distances[target as usize].upper_bound = min(next.upper_bound, self.distances[target as usize].upper_bound);
                    let upper_bound = self.distances[target as usize].upper_bound;
                    self.distances[target as usize].labels.retain(|other| other.lower_bound <= upper_bound);
                    self.distances[target as usize].labels.push(next);
                }
            }
            QueryProgress::Progress(node)
        } else {
            QueryProgress::Done
        }
    }

    pub fn node_data(&self, node: NodeId) -> &NodeData {
        &self.distances[node as usize]
    }
}
