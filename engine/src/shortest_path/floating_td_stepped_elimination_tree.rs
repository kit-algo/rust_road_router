use super::*;
use crate::graph::floating_time_dependent::SingleDirBoundsGraph;
use crate::graph::floating_time_dependent::*;
use crate::in_range_option::InRangeOption;
use std::cmp::min;

#[derive(Debug, Clone)]
pub enum QueryProgress {
    Progress(NodeId),
    Done,
}

#[derive(Debug, Clone)]
pub struct Label {
    pub lower_bound: FlWeight,
    pub parent: NodeId,
    pub shortcut_id: EdgeId,
}

#[derive(Debug, Clone)]
pub struct NodeData {
    pub labels: Vec<Label>,
    pub upper_bound: FlWeight,
    pub lower_bound: FlWeight,
}

#[derive(Debug)]
pub struct FloatingTDSteppedEliminationTree<'a, 'b> {
    graph: SingleDirBoundsGraph<'a>,
    distances: Vec<NodeData>,
    elimination_tree: &'b [InRangeOption<NodeId>],
    next: Option<NodeId>,
    origin: Option<NodeId>,
}

impl<'a, 'b> FloatingTDSteppedEliminationTree<'a, 'b> {
    pub fn new(graph: SingleDirBoundsGraph<'a>, elimination_tree: &'b [InRangeOption<NodeId>]) -> FloatingTDSteppedEliminationTree<'a, 'b> {
        let n = graph.num_nodes();

        FloatingTDSteppedEliminationTree {
            graph,
            distances: vec![
                NodeData {
                    labels: Vec::new(),
                    lower_bound: FlWeight::INFINITY,
                    upper_bound: FlWeight::INFINITY
                };
                n
            ],
            elimination_tree,
            next: None,
            origin: None,
        }
    }

    pub fn initialize_query(&mut self, from: NodeId) {
        if let Some(from) = self.origin {
            let mut next = Some(from);
            while let Some(node) = next {
                self.distances[node as usize].labels.clear();
                self.distances[node as usize].upper_bound = FlWeight::INFINITY;
                self.distances[node as usize].lower_bound = FlWeight::INFINITY;
                next = self.elimination_tree[node as usize].value();
            }
        }

        // initialize
        self.origin = Some(from);
        self.next = Some(from);

        // Starte with origin
        self.distances[from as usize].upper_bound = FlWeight::zero();
        self.distances[from as usize].lower_bound = FlWeight::zero();
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
                let next = Label {
                    parent: node,
                    lower_bound: shortcut_lower_bound + current_state_lower_bound,
                    shortcut_id,
                };
                let next_upper_bound = shortcut_upper_bound + current_state_upper_bound;

                debug_assert!(!next_upper_bound.fuzzy_lt(next.lower_bound), "{:?} {:?}", next, next_upper_bound);

                if !self.distances[target as usize].upper_bound.fuzzy_lt(next.lower_bound) {
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
