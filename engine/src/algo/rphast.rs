use super::*;
use crate::{algo::dijkstra::*, util::in_range_option::InRangeOption};

pub struct RPHAST<GF, GB> {
    order: NodeOrder,
    forward: GF,
    backward: GB,

    selection_stack: Vec<NodeId>,
    restricted_ids: Vec<InRangeOption<NodeId>>,
    restricted_nodes: Vec<NodeId>,
    restricted_first_out: Vec<EdgeId>,
    restricted_head: Vec<NodeId>,
    restricted_weight: Vec<Weight>,
}

impl<GF, GB: LinkIterGraph> RPHAST<GF, GB> {
    pub fn new(forward: GF, backward: GB, order: NodeOrder) -> Self {
        let n = backward.num_nodes();
        Self {
            order,
            forward,
            backward,
            selection_stack: Vec::new(),
            restricted_ids: vec![InRangeOption::NONE; n],
            restricted_nodes: Vec::new(),
            restricted_first_out: vec![0],
            restricted_head: Vec::new(),
            restricted_weight: Vec::new(),
        }
    }

    pub fn select(&mut self, nodes: &[NodeId]) {
        // clear old stuff
        for node in self.restricted_nodes.drain(..) {
            self.restricted_ids[node as usize] = InRangeOption::NONE;
        }
        self.restricted_first_out.truncate(1);
        self.restricted_head.clear();
        self.restricted_weight.clear();

        // new selection
        let mut local_idx_counter = 0;
        for &node in nodes {
            // translate node ids
            let node = self.order.rank(node);
            self.selection_stack.push(node);

            while let Some(&node) = self.selection_stack.last() {
                if self.restricted_ids[node as usize].value().is_some() {
                    self.selection_stack.pop();
                    continue;
                }

                let mut all_upward_done = true;
                for link in self.backward.link_iter(node) {
                    if self.restricted_ids[link.node as usize].value().is_none() {
                        all_upward_done = false;
                        self.selection_stack.push(link.node);
                        break;
                    }
                }
                if all_upward_done {
                    self.selection_stack.pop();
                    self.restricted_ids[node as usize] = InRangeOption::some(local_idx_counter);
                    local_idx_counter += 1;

                    for link in self.backward.link_iter(node) {
                        self.restricted_head.push(self.restricted_ids[link.node as usize].value().unwrap());
                        self.restricted_weight.push(link.weight);
                    }
                    self.restricted_first_out.push(self.restricted_head.len() as EdgeId);
                    self.restricted_nodes.push(node);
                }
            }
        }
    }
}

pub struct RPHASTQuery {
    restricted_distances: Box<[Weight]>,
    dijkstra_data: DijkstraData<Weight>,
}

impl RPHASTQuery {
    pub fn new<GF: Graph, GB>(rphast: &RPHAST<GF, GB>) -> Self {
        let n = rphast.forward.num_nodes();
        Self {
            restricted_distances: std::iter::repeat(INFINITY).take(n).collect(),
            dijkstra_data: DijkstraData::new(n),
        }
    }

    pub fn query<'a, GF: LinkIterGraph, GB>(&'a mut self, node: NodeId, selection: &'a RPHAST<GF, GB>) -> RPHASTResult<GF, GB> {
        for dist in self.restricted_distances[0..selection.restricted_nodes.len()].iter_mut() {
            *dist = INFINITY;
        }
        let mut ops = DefaultOps();
        let mut query = DijkstraRun::query(
            &selection.forward,
            &mut self.dijkstra_data,
            &mut ops,
            DijkstraInit::from(selection.order.rank(node)),
        );
        while let Some(node) = query.next() {
            if let Some(restricted_id) = selection.restricted_ids[node as usize].value() {
                self.restricted_distances[restricted_id as usize] = *query.tentative_distance(node);
            }
        }

        for node in 0..selection.restricted_nodes.len() {
            unsafe {
                let min_dist: *mut Weight = self.restricted_distances.get_unchecked_mut(node);
                for edge_idx in *selection.restricted_first_out.get_unchecked(node)..*selection.restricted_first_out.get_unchecked(node + 1) {
                    let head = *selection.restricted_head.get_unchecked(edge_idx as usize);
                    let weight = *selection.restricted_weight.get_unchecked(edge_idx as usize);
                    let head_dist = *self.restricted_distances.get_unchecked(head as usize);
                    *min_dist = std::cmp::min(*min_dist, head_dist + weight)
                }
            }
        }

        RPHASTResult(self, selection)
    }
}

pub struct RPHASTResult<'a, GF, GB>(&'a RPHASTQuery, &'a RPHAST<GF, GB>);

impl<GF: LinkIterGraph, GB: LinkIterGraph> RPHASTResult<'_, GF, GB> {
    pub fn distance(&self, node: NodeId) -> Weight {
        self.0.restricted_distances[self.1.restricted_ids[self.1.order.rank(node) as usize].value().unwrap() as usize]
    }
}
