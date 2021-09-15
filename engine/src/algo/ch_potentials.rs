use super::*;
use crate::{
    algo::{
        a_star::Potential,
        customizable_contraction_hierarchy::{query::stepped_elimination_tree::EliminationTreeWalk, *},
        dijkstra::*,
    },
    datastr::{node_order::*, rank_select_map::FastClearBitVec, timestamped_vector::TimestampedVector},
    io::*,
    report::*,
    util::in_range_option::InRangeOption,
};

pub mod penalty;
pub mod query;

pub struct CCHPotData {
    customized: Customized<DirectedCCH, DirectedCCH>,
}

impl CCHPotData {
    pub fn new<Graph>(cch: &CCH, lower_bound: &Graph) -> Self
    where
        Graph: LinkIterGraph + EdgeRandomAccessGraph<Link> + Sync,
    {
        let customized = customize_perfect(customize(cch, lower_bound));
        Self { customized }
    }

    pub fn forward_potential(&self) -> CCHPotential<FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>, FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>> {
        let n = self.customized.forward_graph().num_nodes();

        CCHPotential {
            cch: self.customized.cch(),
            stack: Vec::new(),
            forward_cch_graph: self.customized.forward_graph(),
            backward_distances: TimestampedVector::new(n, INFINITY),
            backward_parents: vec![n as NodeId; n],
            backward_cch_graph: self.customized.backward_graph(),
            potentials: TimestampedVector::new(n, InRangeOption::new(None)),
            num_pot_computations: 0,
        }
    }

    pub fn backward_potential(&self) -> CCHPotential<FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>, FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>> {
        let n = self.customized.forward_graph().num_nodes();

        CCHPotential {
            cch: self.customized.cch(),
            stack: Vec::new(),
            forward_cch_graph: self.customized.backward_graph(),
            backward_distances: TimestampedVector::new(n, INFINITY),
            backward_parents: vec![n as NodeId; n],
            backward_cch_graph: self.customized.forward_graph(),
            potentials: TimestampedVector::new(n, InRangeOption::new(None)),
            num_pot_computations: 0,
        }
    }

    pub fn forward_path_potential(&self) -> CCHPotentialWithPathUnpacking {
        let n = self.customized.forward_graph().num_nodes();

        CCHPotentialWithPathUnpacking {
            cch: self.customized.cch(),
            stack: Vec::new(),
            forward_cch_graph: self.customized.forward_graph(),
            backward_distances: TimestampedVector::new(n, INFINITY),
            backward_parents: vec![n as NodeId; n],
            backward_cch_graph: self.customized.backward_graph(),
            potentials: TimestampedVector::new(n, InRangeOption::new(None)),
            num_pot_computations: 0,
            path_unpacked: FastClearBitVec::new(n),
        }
    }

    pub fn backward_path_potential(&self) -> CCHPotentialWithPathUnpacking {
        let n = self.customized.forward_graph().num_nodes();

        CCHPotentialWithPathUnpacking {
            cch: self.customized.cch(),
            stack: Vec::new(),
            forward_cch_graph: self.customized.backward_graph(),
            backward_distances: TimestampedVector::new(n, INFINITY),
            backward_parents: vec![n as NodeId; n],
            backward_cch_graph: self.customized.forward_graph(),
            potentials: TimestampedVector::new(n, InRangeOption::new(None)),
            num_pot_computations: 0,
            path_unpacked: FastClearBitVec::new(n),
        }
    }
}

pub struct CCHPotential<'a, GF, GB> {
    cch: &'a DirectedCCH,
    stack: Vec<NodeId>,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    forward_cch_graph: GF,
    backward_distances: TimestampedVector<Weight>,
    backward_parents: Vec<NodeId>,
    backward_cch_graph: GB,
    num_pot_computations: usize,
}

impl<'a, GF, GB> CCHPotential<'a, GF, GB> {
    pub fn num_pot_computations(&self) -> usize {
        self.num_pot_computations
    }
}

impl<'a, GF, GB> Potential for CCHPotential<'a, GF, GB>
where
    GF: LinkIterGraph,
    GB: LinkIterGraph,
{
    fn init(&mut self, target: NodeId) {
        let target = self.cch.node_order().rank(target);
        self.potentials.reset();
        let mut bw_walk = EliminationTreeWalk::query(
            &self.backward_cch_graph,
            self.cch.elimination_tree(),
            &mut self.backward_distances,
            &mut self.backward_parents,
            target,
        );
        while let Some(_) = bw_walk.next() {}
        self.num_pot_computations = 0;
    }

    fn potential(&mut self, node: NodeId) -> Option<u32> {
        let node = self.cch.node_order().rank(node);

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
                dist = std::cmp::min(dist, edge.weight + self.potentials[edge.node as usize].value().unwrap())
            }

            self.potentials[node as usize] = InRangeOption::new(Some(dist));
        }

        let dist = self.potentials[node as usize].value().unwrap();
        if dist < INFINITY {
            Some(dist)
        } else {
            None
        }
    }
}

pub struct CCHPotentialWithPathUnpacking<'a> {
    cch: &'a DirectedCCH,
    stack: Vec<NodeId>,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    forward_cch_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], &'a [Weight]>,
    backward_distances: TimestampedVector<Weight>,
    backward_parents: Vec<NodeId>,
    backward_cch_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], &'a [Weight]>,
    num_pot_computations: usize,
    path_unpacked: FastClearBitVec,
}

impl<'a> CCHPotentialWithPathUnpacking<'a> {
    pub fn num_pot_computations(&self) -> usize {
        self.num_pot_computations
    }
}

impl<'a> Potential for CCHPotentialWithPathUnpacking<'a> {
    fn init(&mut self, target: NodeId) {
        let target = self.cch.node_order().rank(target);
        self.path_unpacked.clear();
        self.potentials.reset();
        let mut bw_walk = EliminationTreeWalk::query(
            &self.backward_cch_graph,
            self.cch.elimination_tree(),
            &mut self.backward_distances,
            &mut self.backward_parents,
            target,
        );
        while let Some(_) = bw_walk.next() {}
        self.num_pot_computations = 0;
    }

    fn potential(&mut self, node: NodeId) -> Option<u32> {
        self.potential_int(self.cch.node_order().rank(node))
    }
}

impl<'a> CCHPotentialWithPathUnpacking<'a> {
    fn potential_int(&mut self, node: NodeId) -> Option<u32> {
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
                let relaxed = edge.weight + self.potentials[edge.node as usize].value().unwrap();
                if relaxed < dist {
                    self.backward_parents[node as usize] = edge.node;
                    dist = relaxed;
                }
            }

            self.potentials[node as usize] = InRangeOption::new(Some(dist));
        }

        let dist = self.potentials[node as usize].value().unwrap();
        if dist < INFINITY {
            Some(dist)
        } else {
            None
        }
    }

    pub fn unpack_path(&mut self, NodeIdT(node): NodeIdT) {
        self.unpack_path_int(NodeIdT(self.cch.node_order().rank(node)))
    }

    fn unpack_path_int(&mut self, NodeIdT(node): NodeIdT) {
        if self.path_unpacked.get(node as usize) {
            return;
        }
        let self_dist = self.potential_int(node).unwrap();
        let parent = self.backward_parents[node as usize];
        if parent == node {
            self.path_unpacked.set(node as usize);
            return;
        }
        let parent_dist = self.potential_int(parent).unwrap();
        self.unpack_path_int(NodeIdT(parent));

        if let Some((middle, _down, _up)) = self.cch.unpack_arc(
            node,
            parent,
            self_dist - parent_dist,
            self.forward_cch_graph.weight(),
            self.backward_cch_graph.weight(),
        ) {
            self.backward_parents[node as usize] = middle;
            self.backward_parents[middle as usize] = parent;
            self.unpack_path_int(NodeIdT(middle));
            self.unpack_path_int(NodeIdT(node));
        }
        self.path_unpacked.set(node as usize);
    }

    pub fn target_shortest_path_tree(&self) -> &[NodeId] {
        &self.backward_parents
    }

    pub fn cch(&self) -> &DirectedCCH {
        &self.cch
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
            potentials: TimestampedVector::new(n, InRangeOption::new(None)),
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
        let mut backward_dijkstra_run = DijkstraRun::query(
            &self.backward,
            &mut self.dijkstra_data,
            &mut ops,
            Query {
                from: self.order.rank(target),
                to: std::u32::MAX,
            },
        );
        while let Some(_) = backward_dijkstra_run.next() {}
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
                    self.potentials[node as usize] = InRangeOption::new(Some(dist));
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
    pub fn potentials(
        &self,
    ) -> (
        CHPotential<FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>, FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>>,
        CHPotential<FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>, FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>>,
    ) {
        (
            CHPotential::new(
                FirstOutGraph::new(&self.forward_first_out[..], &self.forward_head[..], &self.forward_weight[..]),
                FirstOutGraph::new(&self.backward_first_out[..], &self.backward_head[..], &self.backward_weight[..]),
                self.order.clone(),
            ),
            CHPotential::new(
                FirstOutGraph::new(&self.backward_first_out[..], &self.backward_head[..], &self.backward_weight[..]),
                FirstOutGraph::new(&self.forward_first_out[..], &self.forward_head[..], &self.forward_weight[..]),
                self.order.clone(),
            ),
        )
    }

    pub fn bucket_ch_pot(&self) -> BucketCHPotential<FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>, FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>> {
        BucketCHPotential::new(
            FirstOutGraph::new(&self.forward_first_out[..], &self.forward_head[..], &self.forward_weight[..]),
            FirstOutGraph::new(&self.backward_first_out[..], &self.backward_head[..], &self.backward_weight[..]),
            self.order.clone(),
        )
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
    dijkstra_data: DijkstraData<Vec<(NodeId, Weight)>>,
    num_pot_computations: usize,
    num_targets: usize,
}

impl<GF: LinkIterGraph, GB: LinkIterGraph> BucketCHPotential<GF, GB> {
    pub fn new(forward: GF, backward: GB, order: NodeOrder) -> Self {
        let n = forward.num_nodes();
        Self {
            order,
            potentials: vec![None; n],
            pot_final: FastClearBitVec::new(n),
            to_clear: Vec::new(),
            forward,
            backward,
            dijkstra_data: DijkstraData::new(n),
            num_pot_computations: 0,
            num_targets: 0,
        }
    }

    pub fn num_pot_computations(&self) -> usize {
        self.num_pot_computations
    }

    fn compute_dists(
        potentials: &mut [Option<Box<[Weight]>>],
        dijkstra_data: &DijkstraData<Vec<(NodeId, Weight)>>,
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

        let mut ops = SimulBucketCHOps();
        let mut backward_dijkstra_run = DijkstraRun::query(
            &self.backward,
            &mut self.dijkstra_data,
            &mut ops,
            SimulBucketQuery {
                node: self.order.rank(targets[0]),
                index: 0,
            },
        );
        for (i, &target) in targets.iter().enumerate().skip(1) {
            backward_dijkstra_run.add_start_node(SimulBucketQuery {
                node: self.order.rank(target),
                index: i as u32,
            });
        }
        while let Some(_) = backward_dijkstra_run.next() {}
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

struct SimulBucketQuery {
    node: NodeId,
    index: u32,
}

impl GenQuery<Vec<(NodeId, Weight)>> for SimulBucketQuery {
    fn new(_from: NodeId, _to: NodeId, _initial_state: Vec<(NodeId, Weight)>) -> Self {
        unimplemented!()
    }
    fn from(&self) -> NodeId {
        self.node
    }
    fn to(&self) -> NodeId {
        unimplemented!()
    }
    fn initial_state(&self) -> Vec<(NodeId, Weight)> {
        vec![(self.index, 0)]
    }
    fn permutate(&mut self, _order: &NodeOrder) {
        unimplemented!()
    }
}

struct SimulBucketCHOps();

impl<G: LinkIterGraph> DijkstraOps<G> for SimulBucketCHOps {
    type Label = Vec<(NodeId, Weight)>;
    type Arc = Link;
    type LinkResult = Vec<(NodeId, Weight)>;
    type PredecessorLink = ();

    #[inline(always)]
    fn link(&mut self, _graph: &G, _tail: NodeIdT, label: &Vec<(NodeId, Weight)>, link: &Link) -> Self::LinkResult {
        label.iter().map(move |&(n, w)| (n, w + link.weight)).collect()
    }

    #[inline(always)]
    fn merge<'g, 'l, 'a>(&mut self, label: &mut Vec<(NodeId, Weight)>, linked: Self::LinkResult) -> bool {
        let mut improved = false;
        let mut res: Self::Label = Vec::with_capacity(label.len() + linked.len());

        let mut cur_iter = label.iter().peekable();
        let mut linked_iter = linked.iter().peekable();

        loop {
            use std::cmp::*;
            match (cur_iter.peek(), linked_iter.peek()) {
                (Some(&&(cur_node, cur_dist)), Some(&&(linked_node, linked_dist))) => match cur_node.cmp(&linked_node) {
                    Ordering::Less => {
                        res.push((cur_node, cur_dist));
                        cur_iter.next();
                    }
                    Ordering::Greater => {
                        improved = true;
                        res.push((linked_node, linked_dist));
                        linked_iter.next();
                    }
                    Ordering::Equal => {
                        if linked_dist < cur_dist {
                            improved = true;
                        }
                        res.push((cur_node, min(cur_dist, linked_dist)));
                        cur_iter.next();
                        linked_iter.next();
                    }
                },
                (Some(&&(cur_node, cur_dist)), None) => {
                    res.push((cur_node, cur_dist));
                    cur_iter.next();
                }
                (None, Some(&&(linked_node, linked_dist))) => {
                    improved = true;
                    res.push((linked_node, linked_dist));
                    linked_iter.next();
                }
                _ => break,
            }
        }

        if improved {
            *label = res;
        }
        improved
    }

    #[inline(always)]
    fn predecessor_link(&self, _link: &Self::Arc) -> Self::PredecessorLink {
        ()
    }
}

impl Label for Vec<(NodeId, Weight)> {
    type Key = ();
    fn neutral() -> Self {
        Vec::new()
    }
    fn key(&self) -> Self::Key {
        ()
    }
}
