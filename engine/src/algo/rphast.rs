use super::*;
use crate::{algo::dijkstra::*, datastr::index_heap::*, util::in_range_option::InRangeOption};

pub struct RPHAST<GF, GB> {
    order: NodeOrder,
    restricted_distances: Vec<Weight>,
    forward: GF,
    backward: GB,
    dijkstra_data: DijkstraData<Weight>,
    selection_queue: IndexdMinHeap<NodeIdT>,
    restricted_ids: Vec<InRangeOption<NodeId>>,
    restricted_nodes: Vec<NodeId>,
    restricted_first_out: Vec<EdgeId>,
    restricted_head: Vec<NodeId>,
    restricted_weight: Vec<Weight>,
}

impl<GF: LinkIterGraph, GB: LinkIterGraph> RPHAST<GF, GB> {
    pub fn new(forward: GF, backward: GB, order: NodeOrder) -> Self {
        let n = forward.num_nodes();
        Self {
            order,
            forward,
            backward,
            restricted_distances: vec![INFINITY; n],
            dijkstra_data: DijkstraData::new(n),
            selection_queue: IndexdMinHeap::new(n),
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
        debug_assert!(self.selection_queue.is_empty());
        for &node in nodes {
            // translate node ids
            let node = self.order.rank(node);
            self.selection_queue.push(NodeIdT(node));
        }

        while let Some(NodeIdT(node)) = self.selection_queue.pop() {
            self.restricted_nodes.push(node);

            for link in self.backward.link_iter(node) {
                self.selection_queue.push_unless_contained(NodeIdT(link.node));
                self.restricted_head.push(link.node);
                self.restricted_weight.push(link.weight);
            }
            self.restricted_first_out.push(self.restricted_head.len() as EdgeId);
        }

        // local ids, reversed
        for (local_idx, &node) in self.restricted_nodes.iter().rev().enumerate() {
            self.restricted_ids[node as usize] = InRangeOption::some(local_idx as NodeId);
        }
        for head in &mut self.restricted_head {
            *head = self.restricted_ids[*head as usize].value().unwrap();
        }
        // reverse
        self.restricted_first_out.reverse();
        for first_out in &mut self.restricted_first_out {
            *first_out = self.restricted_head.len() as EdgeId - *first_out;
        }
        self.restricted_head.reverse();
        self.restricted_weight.reverse();
    }

    pub fn query(&mut self, node: NodeId) -> RPHASTResult<GF, GB> {
        for &node in self.restricted_nodes.iter() {
            self.restricted_distances[self.restricted_ids[node as usize].value().unwrap() as usize] = INFINITY;
        }
        let mut ops = DefaultOps();
        let mut query = DijkstraRun::query(&self.forward, &mut self.dijkstra_data, &mut ops, DijkstraInit::from(self.order.rank(node)));
        while let Some(node) = query.next() {
            if let Some(restricted_id) = self.restricted_ids[node as usize].value() {
                self.restricted_distances[restricted_id as usize] = *query.tentative_distance(node);
            }
        }

        let tails = self
            .restricted_first_out
            .array_windows::<2>()
            .enumerate()
            .flat_map(|(local_idx, &[sidx, eidx])| std::iter::repeat(local_idx as NodeId).take((eidx - sidx) as usize));
        for ((tail, &head), &weight) in tails.zip(self.restricted_head.iter()).zip(self.restricted_weight.iter()) {
            self.restricted_distances[tail as usize] =
                std::cmp::min(self.restricted_distances[tail as usize], self.restricted_distances[head as usize] + weight)
        }

        RPHASTResult(self)
    }

    fn distance(&self, node: NodeId) -> Weight {
        self.restricted_distances[self.restricted_ids[self.order.rank(node) as usize].value().unwrap() as usize]
    }
}

pub struct RPHASTResult<'a, GF, GB>(&'a RPHAST<GF, GB>);

impl<GF: LinkIterGraph, GB: LinkIterGraph> RPHASTResult<'_, GF, GB> {
    pub fn distance(&self, node: NodeId) -> Weight {
        self.0.distance(node)
    }
}
