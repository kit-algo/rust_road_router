use super::*;
use crate::{
    algo::{
        a_star::Potential,
        customizable_contraction_hierarchy::{query::stepped_elimination_tree::EliminationTreeWalk, *},
        dijkstra::*,
    },
    datastr::{graph::first_out_graph::BorrowedGraph, node_order::*, rank_select_map::FastClearBitVec, timestamped_vector::TimestampedVector},
    io::*,
    report::*,
    util::in_range_option::InRangeOption,
};

pub mod penalty;
pub mod query;
pub mod td_query;

pub struct CCHPotData<'a> {
    customized: CustomizedPerfect<'a, CCH>,
}

impl<'a> CCHPotData<'a> {
    pub fn new<Graph>(cch: &'a CCH, lower_bound: &Graph) -> Self
    where
        Graph: LinkIterGraph + EdgeRandomAccessGraph<Link> + Sync,
    {
        let customized = customize_perfect(customize(cch, lower_bound));
        Self { customized }
    }

    pub fn num_nodes(&self) -> usize {
        self.customized.forward_graph().num_nodes()
    }

    pub fn forward_potential(&self) -> BorrowedCCHPot {
        BorrowedCCHPot::new_from_customized(&self.customized)
    }

    pub fn backward_potential(&self) -> BorrowedCCHPot {
        let n = self.customized.forward_graph().num_nodes();
        let m = self.customized.forward_graph().num_arcs();

        CCHPotential {
            cch: self.customized.cch(),
            stack: Vec::new(),
            forward_cch_graph: self.customized.backward_graph(),
            backward_distances: TimestampedVector::new(n),
            backward_parents: vec![(n as NodeId, m as EdgeId); n],
            backward_cch_graph: self.customized.forward_graph(),
            potentials: TimestampedVector::new(n),
            num_pot_computations: 0,
        }
    }

    pub fn forward_path_potential(&self) -> CCHPotentialWithPathUnpacking {
        let n = self.customized.forward_graph().num_nodes();
        let m = self.customized.forward_graph().num_arcs();

        CCHPotentialWithPathUnpacking {
            cch: self.customized.cch(),
            stack: Vec::new(),
            forward_cch_graph: self.customized.forward_graph(),
            backward_distances: TimestampedVector::new(n),
            backward_parents: vec![(n as NodeId, m as EdgeId); n],
            backward_cch_graph: self.customized.backward_graph(),
            potentials: TimestampedVector::new(n),
            num_pot_computations: 0,
            path_unpacked: FastClearBitVec::new(n),
            forward_unpacking: self.customized.forward_unpacking(),
            backward_unpacking: self.customized.backward_unpacking(),
            forward_tail: self.customized.forward_tail(),
        }
    }

    pub fn backward_path_potential(&self) -> CCHPotentialWithPathUnpacking {
        let n = self.customized.forward_graph().num_nodes();
        let m = self.customized.forward_graph().num_arcs();

        CCHPotentialWithPathUnpacking {
            cch: self.customized.cch(),
            stack: Vec::new(),
            forward_cch_graph: self.customized.backward_graph(),
            backward_distances: TimestampedVector::new(n),
            backward_parents: vec![(n as NodeId, m as EdgeId); n],
            backward_cch_graph: self.customized.forward_graph(),
            potentials: TimestampedVector::new(n),
            num_pot_computations: 0,
            path_unpacked: FastClearBitVec::new(n),
            forward_unpacking: self.customized.backward_unpacking(),
            backward_unpacking: self.customized.forward_unpacking(),
            forward_tail: self.customized.backward_tail(),
        }
    }

    pub fn customized(&self) -> &CustomizedPerfect<'a, CCH> {
        &self.customized
    }
}

#[derive(Clone)]
pub struct CCHPotential<'a, GF, GB> {
    cch: &'a CCH,
    stack: Vec<NodeId>,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    forward_cch_graph: GF,
    backward_distances: TimestampedVector<Weight>,
    backward_parents: Vec<(NodeId, EdgeId)>,
    backward_cch_graph: GB,
    num_pot_computations: usize,
}

pub type BorrowedCCHPot<'a> = CCHPotential<'a, BorrowedGraph<'a>, BorrowedGraph<'a>>;

impl<'a> BorrowedCCHPot<'a> {
    pub fn new_from_customized<C: Customized<CCH = CCH>>(customized: &'a C) -> Self {
        let n = customized.forward_graph().num_nodes();
        let m = customized.forward_graph().num_arcs();

        Self {
            cch: customized.cch(),
            stack: Vec::new(),
            forward_cch_graph: customized.forward_graph(),
            backward_distances: TimestampedVector::new(n),
            backward_parents: vec![(n as NodeId, m as EdgeId); n],
            backward_cch_graph: customized.backward_graph(),
            potentials: TimestampedVector::new(n),
            num_pot_computations: 0,
        }
    }
}

impl<'a, GF, GB> CCHPotential<'a, GF, GB>
where
    GF: LinkIterGraph,
    GB: LinkIterable<(NodeIdT, Weight, EdgeIdT)>,
{
    pub fn num_pot_computations(&self) -> usize {
        self.num_pot_computations
    }

    pub fn init_with_cch_rank(&mut self, target: NodeId) {
        self.potentials.reset();
        for _ in EliminationTreeWalk::query(
            &self.backward_cch_graph,
            self.cch.elimination_tree(),
            &mut self.backward_distances,
            &mut self.backward_parents,
            target,
        ) {}
        self.num_pot_computations = 0;
    }

    pub fn potential_with_cch_rank(&mut self, node: NodeId) -> Option<u32> {
        let mut cur_node = node;
        while self.potentials[cur_node as usize].value().is_none() {
            self.num_pot_computations += 1;
            self.stack.push(cur_node);
            if let Some(parent) = self.cch.elimination_tree()[cur_node as usize].value() {
                cur_node = parent;
            } else {
                break;
            }
        }

        while let Some(node) = self.stack.pop() {
            let mut dist = self.backward_distances[node as usize];

            for edge in LinkIterable::<Link>::link_iter(&self.forward_cch_graph, node) {
                dist = std::cmp::min(dist, edge.weight + unsafe { self.potentials.get_unchecked(edge.node as usize).assume_some() })
            }

            self.potentials[node as usize] = InRangeOption::some(dist);
        }

        let dist = self.potentials[node as usize].value().unwrap();
        if dist < INFINITY {
            Some(dist)
        } else {
            None
        }
    }
}

impl<'a, GF, GB> Potential for CCHPotential<'a, GF, GB>
where
    GF: LinkIterGraph,
    GB: LinkIterable<(NodeIdT, Weight, EdgeIdT)>,
{
    fn init(&mut self, target: NodeId) {
        self.init_with_cch_rank(self.cch.node_order().rank(target))
    }

    fn potential(&mut self, node: NodeId) -> Option<u32> {
        self.potential_with_cch_rank(self.cch.node_order().rank(node))
    }
}

pub struct CCHPotentialWithPathUnpacking<'a> {
    cch: &'a CCH,
    stack: Vec<NodeId>,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    forward_cch_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], &'a [Weight]>,
    backward_distances: TimestampedVector<Weight>,
    backward_parents: Vec<(NodeId, EdgeId)>,
    backward_cch_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], &'a [Weight]>,
    num_pot_computations: usize,
    path_unpacked: FastClearBitVec,
    forward_unpacking: &'a [(InRangeOption<EdgeId>, InRangeOption<EdgeId>)],
    backward_unpacking: &'a [(InRangeOption<EdgeId>, InRangeOption<EdgeId>)],
    forward_tail: &'a [NodeId],
}

impl<'a> CCHPotentialWithPathUnpacking<'a> {
    pub fn num_pot_computations(&self) -> usize {
        self.num_pot_computations
    }
}

impl<'a> Potential for CCHPotentialWithPathUnpacking<'a> {
    fn init(&mut self, target: NodeId) {
        self.path_unpacked.clear();
        self.potentials.reset();
        for _ in EliminationTreeWalk::query(
            &self.backward_cch_graph,
            self.cch.elimination_tree(),
            &mut self.backward_distances,
            &mut self.backward_parents,
            target,
        ) {}
        self.num_pot_computations = 0;
    }

    fn potential(&mut self, node: NodeId) -> Option<u32> {
        let mut cur_node = node;
        while self.potentials[cur_node as usize].value().is_none() {
            self.num_pot_computations += 1;
            self.stack.push(cur_node);
            if let Some(parent) = self.cch.elimination_tree()[cur_node as usize].value() {
                cur_node = parent;
            } else {
                break;
            }
        }

        while let Some(node) = self.stack.pop() {
            let mut dist = self.backward_distances[node as usize];

            for (NodeIdT(head), weight, EdgeIdT(edge_idx)) in LinkIterable::<(NodeIdT, Weight, EdgeIdT)>::link_iter(&self.forward_cch_graph, node) {
                let relaxed = weight + self.potentials[head as usize].value().unwrap();
                if relaxed < dist {
                    self.backward_parents[node as usize] = (head, edge_idx);
                    dist = relaxed;
                }
            }

            self.potentials[node as usize] = InRangeOption::some(dist);
        }

        let dist = self.potentials[node as usize].value().unwrap();
        if dist < INFINITY {
            Some(dist)
        } else {
            None
        }
    }
}

impl<'a> CCHPotentialWithPathUnpacking<'a> {
    pub fn unpack_path(&mut self, NodeIdT(node): NodeIdT) {
        if self.path_unpacked.get(node as usize) {
            return;
        }
        let self_dist = self.potential(node).unwrap();
        let (parent, edge) = self.backward_parents[node as usize];
        if parent == node {
            self.path_unpacked.set(node as usize);
            return;
        }
        let parent_dist = self.potential(parent).unwrap();
        self.unpack_path(NodeIdT(parent));

        debug_assert!(self_dist >= parent_dist, "{:#?}", (node, parent, self_dist, parent_dist));
        let (down, up) = if parent > node {
            self.forward_unpacking[edge as usize]
        } else {
            self.backward_unpacking[edge as usize]
        };
        if let Some(down) = down.value() {
            let up = up.value().unwrap();
            let middle = self.forward_tail[up as usize];
            self.backward_parents[node as usize] = (middle, down);

            if !self.path_unpacked.get(middle as usize) {
                // will be called in the unpack_path call
                // but we need to make sure that the parent of middle is parent
                // so we call potential first, then set the parent
                // and then the call in unpack_path won't override it again.
                // This is only a problem with zero arcs and the induced non-unique shortest paths
                self.potential(middle);
                self.backward_parents[middle as usize] = (parent, up);
                self.unpack_path(NodeIdT(middle));
            }

            debug_assert_eq!(self.potential(middle).unwrap(), parent_dist + self.forward_cch_graph.weight()[up as usize]);
            self.unpack_path(NodeIdT(node));
        }
        self.path_unpacked.set(node as usize);
    }

    pub fn target_shortest_path_tree(&self) -> &[(NodeId, EdgeId)] {
        &self.backward_parents
    }

    pub fn cch(&self) -> &CCH {
        self.cch
    }

    pub fn forward_cch_graph(&self) -> &BorrowedGraph {
        &self.forward_cch_graph
    }

    pub fn backward_cch_graph(&self) -> &BorrowedGraph {
        &self.backward_cch_graph
    }
}

#[derive(Clone)]
pub struct CHPotential<GF, GB> {
    order: NodeOrder,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    forward: GF,
    backward: GB,
    dijkstra_data: DijkstraData<Weight>,
    num_pot_computations: usize,
    stack: Vec<NodeId>,
}

impl<GF: LinkIterGraph, GB: LinkIterGraph> CHPotential<GF, GB> {
    pub fn new(forward: GF, backward: GB, order: NodeOrder) -> Self {
        let n = forward.num_nodes();
        Self {
            order,
            potentials: TimestampedVector::new(n),
            forward,
            backward,
            dijkstra_data: DijkstraData::new(n),
            num_pot_computations: 0,
            stack: Vec::new(),
        }
    }

    pub fn num_pot_computations(&self) -> usize {
        self.num_pot_computations
    }
}

impl<GF: LinkIterGraph, GB: LinkIterGraph> Potential for CHPotential<GF, GB> {
    fn init(&mut self, target: NodeId) {
        self.num_pot_computations = 0;
        self.potentials.reset();

        let mut ops = DefaultOps();
        for _ in DijkstraRun::query(&self.backward, &mut self.dijkstra_data, &mut ops, DijkstraInit::from(self.order.rank(target))) {}
    }

    fn potential(&mut self, node: NodeId) -> Option<Weight> {
        let node = self.order.rank(node);
        let dist = if let Some(pot) = self.potentials[node as usize].value() {
            pot
        } else {
            self.stack.push(node);

            while let Some(&node) = self.stack.last() {
                if self.potentials[node as usize].value().is_some() {
                    self.stack.pop();
                    continue;
                }
                let mut dist = self.dijkstra_data.distances[node as usize];
                let mut missing = false;
                for l in LinkIterable::<Link>::link_iter(&self.forward, node) {
                    if let Some(pot) = self.potentials[l.node as usize].value() {
                        dist = std::cmp::min(dist, pot + l.weight);
                    } else {
                        missing = true;
                        self.stack.push(l.node);
                    }
                }
                if !missing {
                    self.num_pot_computations += 1;
                    self.potentials[node as usize] = InRangeOption::some(dist);
                    self.stack.pop();
                }
            }

            self.potentials[node as usize].value().unwrap()
        };

        if dist < INFINITY {
            Some(dist)
        } else {
            None
        }
    }
}

impl Reconstruct for CHPotential<OwnedGraph, OwnedGraph> {
    fn reconstruct_with(loader: Loader) -> std::io::Result<Self> {
        let forward_first_out = loader.load("forward_first_out")?;
        let forward_head = loader.load("forward_head")?;
        let forward_weight = loader.load("forward_weight")?;
        let backward_first_out = loader.load("backward_first_out")?;
        let backward_head = loader.load("backward_head")?;
        let backward_weight = loader.load("backward_weight")?;
        let order = NodeOrder::from_node_order(loader.load("order")?);
        Ok(Self::new(
            OwnedGraph::new(forward_first_out, forward_head, forward_weight),
            OwnedGraph::new(backward_first_out, backward_head, backward_weight),
            order,
        ))
    }
}

pub struct CHPotLoader {
    forward_first_out: Vec<EdgeId>,
    forward_head: Vec<NodeId>,
    forward_weight: Vec<Weight>,
    backward_first_out: Vec<EdgeId>,
    backward_head: Vec<NodeId>,
    backward_weight: Vec<Weight>,
    order: NodeOrder,
}

impl Reconstruct for CHPotLoader {
    fn reconstruct_with(loader: Loader) -> std::io::Result<Self> {
        Ok(Self {
            forward_first_out: loader.load("forward_first_out")?,
            forward_head: loader.load("forward_head")?,
            forward_weight: loader.load("forward_weight")?,
            backward_first_out: loader.load("backward_first_out")?,
            backward_head: loader.load("backward_head")?,
            backward_weight: loader.load("backward_weight")?,
            order: NodeOrder::from_node_order(loader.load("order")?),
        })
    }
}

impl CHPotLoader {
    pub fn forward_graph(&self) -> BorrowedGraph {
        FirstOutGraph::new(&self.forward_first_out[..], &self.forward_head[..], &self.forward_weight[..])
    }

    pub fn backward_graph(&self) -> BorrowedGraph {
        FirstOutGraph::new(&self.backward_first_out[..], &self.backward_head[..], &self.backward_weight[..])
    }

    pub fn order(&self) -> &NodeOrder {
        &self.order
    }

    pub fn potentials(&self) -> (CHPotential<BorrowedGraph, BorrowedGraph>, CHPotential<BorrowedGraph, BorrowedGraph>) {
        (
            CHPotential::new(self.forward_graph(), self.backward_graph(), self.order.clone()),
            CHPotential::new(self.backward_graph(), self.forward_graph(), self.order.clone()),
        )
    }

    pub fn bucket_ch_pot(&self) -> BucketCHPotential<BorrowedGraph, BorrowedGraph> {
        BucketCHPotential::new(self.forward_graph(), self.backward_graph(), self.order.clone())
    }
}

#[derive(Clone)]
pub struct BucketCHPotential<GF, GB> {
    order: NodeOrder,
    potentials: Vec<Option<Box<[Weight]>>>,
    pot_final: FastClearBitVec,
    to_clear: Vec<NodeId>,
    forward: GF,
    backward: GB,
    dijkstra_data: BucketCHSelectionData,
    num_pot_computations: usize,
    num_targets: usize,
}

impl<GF: LinkIterGraph, GB: LinkIterGraph + LinkIterable<(NodeIdT, EdgeIdT)> + EdgeRandomAccessGraph<Link>> BucketCHPotential<GF, GB> {
    pub fn new(forward: GF, backward: GB, order: NodeOrder) -> Self {
        let n = forward.num_nodes();
        Self {
            order,
            potentials: vec![None; n],
            pot_final: FastClearBitVec::new(n),
            to_clear: Vec::new(),
            forward,
            backward,
            dijkstra_data: BucketCHSelectionData::new(n),
            num_pot_computations: 0,
            num_targets: 0,
        }
    }

    pub fn num_pot_computations(&self) -> usize {
        self.num_pot_computations
    }

    fn compute_dists(
        potentials: &mut [Option<Box<[Weight]>>],
        dijkstra_data: &BucketCHSelectionData,
        forward: &GF,
        pot_final: &mut FastClearBitVec,
        to_clear: &mut Vec<NodeId>,
        node: NodeId,
        num_pot_computations: &mut usize,
        num_targets: usize,
    ) {
        if pot_final.get(node as usize) {
            return;
        }
        *num_pot_computations += 1;

        if potentials[node as usize].is_none() {
            to_clear.push(node);
            potentials[node as usize] = Some(std::iter::repeat(INFINITY).take(num_targets).collect());
            for &(i, dist) in &dijkstra_data.distances[node as usize] {
                potentials[node as usize].as_mut().unwrap()[i as usize] = dist;
            }
        }

        for l in LinkIterable::<Link>::link_iter(forward, node) {
            debug_assert!(l.node > node);
            Self::compute_dists(
                potentials,
                dijkstra_data,
                forward,
                pot_final,
                to_clear,
                l.node,
                num_pot_computations,
                num_targets,
            );
            debug_assert!(pot_final.get(node as usize));
            let (potentials, upper_dists) = potentials.split_at_mut(l.node as usize);
            let head_dists = upper_dists[0].as_ref().unwrap();
            let self_dists = potentials[node as usize].as_mut().unwrap();

            for (self_dist, head_dist) in self_dists.iter_mut().zip(head_dists.iter()) {
                *self_dist = std::cmp::min(*self_dist, *head_dist + l.weight);
            }
        }

        pot_final.set(node as usize);
    }

    pub fn init(&mut self, targets: &[NodeId]) {
        self.num_targets = targets.len();
        self.num_pot_computations = 0;
        for node in self.to_clear.drain(..) {
            self.potentials[node as usize] = None;
        }
        self.pot_final.clear();

        for _ in BucketCHSelectionRun::query(&self.backward, &mut self.dijkstra_data, targets.iter().map(|&node| self.order.rank(node))) {}
    }

    pub fn potential(&mut self, node: NodeId) -> &[Weight] {
        let node = self.order.rank(node);

        Self::compute_dists(
            &mut self.potentials,
            &self.dijkstra_data,
            &self.forward,
            &mut self.pot_final,
            &mut self.to_clear,
            node,
            &mut self.num_pot_computations,
            self.num_targets,
        );
        debug_assert!(self.pot_final.get(node as usize));
        self.potentials[node as usize].as_ref().unwrap()
    }
}

use crate::datastr::index_heap::*;

impl Indexing for NodeIdT {
    #[inline(always)]
    fn as_index(&self) -> usize {
        self.0 as usize
    }
}

#[derive(Clone)]
pub struct BucketCHSelectionData {
    distances: TimestampedVector<Vec<(NodeId, Weight)>>,
    queue: IndexdMinHeap<NodeIdT>,
    incoming: Vec<Vec<(NodeIdT, EdgeIdT)>>,
    terminal_dists: Vec<Weight>,
    used_terminals: Vec<NodeIdT>,
}

impl BucketCHSelectionData {
    pub fn new(n: usize) -> Self {
        Self {
            distances: TimestampedVector::new(n),
            queue: IndexdMinHeap::new(n),
            incoming: vec![Vec::new(); n],
            terminal_dists: vec![INFINITY; n],
            used_terminals: Vec::new(),
        }
    }

    pub fn buckets(&self, node: NodeId) -> &[(NodeId, Weight)] {
        &self.distances[node as usize]
    }
}

pub struct BucketCHSelectionRun<'a, G> {
    graph: &'a G,
    distances: &'a mut TimestampedVector<Vec<(NodeId, Weight)>>,
    queue: &'a mut IndexdMinHeap<NodeIdT>,
    incoming: &'a mut [Vec<(NodeIdT, EdgeIdT)>],
    terminal_dists: &'a mut [Weight],
    used_terminals: &'a mut Vec<NodeIdT>,
}

impl<'a, G: LinkIterable<(NodeIdT, EdgeIdT)> + EdgeRandomAccessGraph<Link>> BucketCHSelectionRun<'a, G> {
    pub fn query(graph: &'a G, data: &'a mut BucketCHSelectionData, terminals: impl Iterator<Item = NodeId>) -> Self {
        let mut s = Self {
            graph,
            distances: &mut data.distances,
            queue: &mut data.queue,
            incoming: &mut data.incoming,
            terminal_dists: &mut data.terminal_dists,
            used_terminals: &mut data.used_terminals,
        };
        s.initialize(terminals);
        s
    }

    fn initialize(&mut self, terminals: impl Iterator<Item = NodeId>) {
        self.queue.clear();
        self.distances.reset();
        for (i, terminal) in terminals.enumerate() {
            self.queue.push(NodeIdT(terminal));
            self.distances[terminal as usize].push((i as u32, 0));
        }
    }

    fn settle_next_node(&mut self) -> Option<NodeIdT> {
        self.queue.pop().map(|NodeIdT(node)| {
            for (NodeIdT(head), edge_id) in LinkIterable::<(NodeIdT, EdgeIdT)>::link_iter(self.graph, node) {
                self.incoming[head as usize].push((NodeIdT(node), edge_id));
                self.queue.push_unless_contained(NodeIdT(head));
            }

            for (NodeIdT(low_neighbor), EdgeIdT(edge_id)) in self.incoming[node as usize].drain(..) {
                let weight = self.graph.link(edge_id).weight;

                for &(terminal, dist) in &self.distances[low_neighbor as usize] {
                    if self.terminal_dists[terminal as usize] == INFINITY {
                        self.used_terminals.push(NodeIdT(terminal));
                    }
                    self.terminal_dists[terminal as usize] = std::cmp::min(self.terminal_dists[terminal as usize], dist + weight);
                }
            }

            self.distances[node as usize].extend(
                self.used_terminals
                    .iter()
                    .map(|&NodeIdT(terminal)| (terminal, self.terminal_dists[terminal as usize])),
            );

            for NodeIdT(terminal) in self.used_terminals.drain(..) {
                self.terminal_dists[terminal as usize] = INFINITY;
            }

            NodeIdT(node)
        })
    }

    pub fn tentative_distance(&self, node: NodeId) -> &[(u32, Weight)] {
        &self.distances[node as usize][..]
    }

    pub fn buckets_mut(&mut self, node: NodeId) -> &mut [(NodeId, Weight)] {
        &mut self.distances[node as usize]
    }
}

impl<'a, G: LinkIterable<(NodeIdT, EdgeIdT)> + EdgeRandomAccessGraph<Link>> Iterator for BucketCHSelectionRun<'a, G> {
    type Item = NodeIdT;

    fn next(&mut self) -> Option<Self::Item> {
        self.settle_next_node()
    }
}
