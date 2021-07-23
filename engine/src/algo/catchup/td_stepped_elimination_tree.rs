//! Corridor elimination tree query.
//!
//! Same algorithm as the static elimination tree query.
//! Difference is, we only have upper and lower bounds for each edge.
//! Thus we may have multiple potentially optimal labels for each node and get a corridor instead of a path.

use crate::algo::customizable_contraction_hierarchy::catchup_light::*;
use crate::datastr::graph::*;
use crate::util::in_range_option::InRangeOption;
use std::cmp::min;

#[derive(Debug, Clone)]
pub enum QueryProgress {
    Progress(NodeId),
    Done,
}

#[derive(Debug, Clone)]
pub struct Label {
    pub lower_bound: Weight,
    pub parent: NodeId,
    pub shortcut_id: EdgeId,
}

#[derive(Clone)]
pub struct NodeData {
    pub labels: Vec<Label>,
    pub upper_bound: Weight,
    pub lower_bound: Weight,
}

pub struct TDSteppedEliminationTree<'a, 'b> {
    graph: SingleDirBoundsGraph<'a>,
    distances: Vec<NodeData>,
    elimination_tree: &'b [InRangeOption<NodeId>],
    next: Option<NodeId>,
    origin: Option<NodeId>,
}

impl<'a, 'b> TDSteppedEliminationTree<'a, 'b> {
    pub fn new(graph: SingleDirBoundsGraph<'a>, elimination_tree: &'b [InRangeOption<NodeId>]) -> TDSteppedEliminationTree<'a, 'b> {
        let n = graph.num_nodes();

        TDSteppedEliminationTree {
            graph,
            distances: vec![
                NodeData {
                    labels: Vec::new(),
                    lower_bound: INFINITY,
                    upper_bound: INFINITY
                };
                n
            ],
            elimination_tree,
            next: None,
            origin: None,
        }
    }

    pub fn initialize_query(&mut self, from: NodeId) {
        // clean up previous data.
        if let Some(from) = self.origin {
            let mut next = Some(from);
            while let Some(node) = next {
                self.distances[node as usize].labels.clear();
                self.distances[node as usize].upper_bound = INFINITY;
                self.distances[node as usize].lower_bound = INFINITY;
                next = self.elimination_tree[node as usize].value();
            }
        }

        // initialize
        self.origin = Some(from);
        self.next = Some(from);

        self.distances[from as usize].upper_bound = 0;
        self.distances[from as usize].lower_bound = 0;
    }

    pub fn next_step(&mut self) -> QueryProgress {
        self.settle_next_node()
    }

    fn settle_next_node(&mut self) -> QueryProgress {
        if let Some(node) = self.next {
            let current_state_lower_bound = self.distances[node as usize].lower_bound;
            let current_state_upper_bound = self.distances[node as usize].upper_bound;
            self.next = self.elimination_tree[node as usize].value();

            for ((target, shortcut_id), (shortcut_lower_bound, shortcut_upper_bound)) in self.graph.neighbor_iter(node) {
                let next;
                let next_upper_bound;
                next = Label {
                    parent: node,
                    lower_bound: shortcut_lower_bound + current_state_lower_bound,
                    shortcut_id,
                };
                next_upper_bound = shortcut_upper_bound + current_state_upper_bound;

                if next.lower_bound <= self.distances[target as usize].upper_bound {
                    self.distances[target as usize].lower_bound = min(next.lower_bound, self.distances[target as usize].lower_bound);
                    self.distances[target as usize].upper_bound = min(next_upper_bound, self.distances[target as usize].upper_bound);
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

    pub fn peek_next(&self) -> Option<NodeId> {
        self.next
    }

    pub fn skip_next(&mut self) {
        if let Some(node) = self.next {
            self.next = self.elimination_tree[node as usize].value();
        }
    }
}
