use super::*;
use crate::{algo::dijkstra::*, report::*};

pub trait Potential {
    fn init(&mut self, target: NodeId);
    fn potential(&mut self, node: NodeId) -> Option<Weight>;
}

#[derive(Debug)]
pub struct TurnExpandedPotential<Potential> {
    potential: Potential,
    tail: Vec<NodeId>,
}

impl<P> TurnExpandedPotential<P> {
    pub fn new(graph: &dyn Graph, potential: P) -> Self {
        let mut tail = Vec::with_capacity(graph.num_arcs());
        for node in 0..graph.num_nodes() {
            for _ in 0..graph.degree(node as NodeId) {
                tail.push(node as NodeId);
            }
        }

        Self { potential, tail }
    }

    pub fn inner(&self) -> &P {
        &self.potential
    }
}

impl<P: Potential> Potential for TurnExpandedPotential<P> {
    fn init(&mut self, target: NodeId) {
        self.potential.init(self.tail[target as usize])
    }
    fn potential(&mut self, node: NodeId) -> Option<Weight> {
        self.potential.potential(self.tail[node as usize])
    }
}

pub struct BaselinePotential {
    graph: OwnedGraph,
    data: DijkstraData<Weight>,
}

impl BaselinePotential {
    pub fn new<G: Graph>(graph: &G) -> Self
    where
        OwnedGraph: BuildReversed<G>,
    {
        Self {
            graph: OwnedGraph::reversed(&graph),
            data: DijkstraData::new(graph.num_nodes()),
        }
    }
}

impl Potential for BaselinePotential {
    fn init(&mut self, target: NodeId) {
        report_time_with_key("BaselinePotential init", "baseline_pot_init", || {
            let mut ops = DefaultOps();
            let mut dijkstra = DijkstraRun::query(
                &self.graph,
                &mut self.data,
                &mut ops,
                Query {
                    from: target,
                    to: self.graph.num_nodes() as NodeId,
                },
            );
            while let Some(_) = dijkstra.next() {}
        })
    }

    fn potential(&mut self, node: NodeId) -> Option<Weight> {
        if self.data.distances[node as usize] < INFINITY {
            Some(self.data.distances[node as usize])
        } else {
            None
        }
    }
}

#[derive(Debug, Clone)]
pub struct RecyclingPotential<Potential> {
    potential: Potential,
    target: Option<NodeId>,
}

impl<P> RecyclingPotential<P> {
    pub fn new(potential: P) -> Self {
        Self { potential, target: None }
    }

    pub fn inner(&self) -> &P {
        &self.potential
    }
}

impl<P: Potential> Potential for RecyclingPotential<P> {
    fn init(&mut self, target: NodeId) {
        if self.target != Some(target) {
            self.potential.init(target);
            self.target = Some(target);
        }
    }
    fn potential(&mut self, node: NodeId) -> Option<Weight> {
        self.potential.potential(node)
    }
}

#[derive(Debug)]
pub struct ZeroPotential();

impl Potential for ZeroPotential {
    fn init(&mut self, _target: NodeId) {}
    fn potential(&mut self, _node: NodeId) -> Option<Weight> {
        Some(0)
    }
}

#[derive(Clone)]
pub struct PotentialForPermutated<P> {
    pub potential: P,
    pub order: NodeOrder,
}

impl<P> PotentialForPermutated<P> {
    pub fn inner(&self) -> &P {
        &self.potential
    }
}

impl<P: Potential> Potential for PotentialForPermutated<P> {
    fn init(&mut self, target: NodeId) {
        self.potential.init(self.order.node(target))
    }

    fn potential(&mut self, node: NodeId) -> Option<Weight> {
        self.potential.potential(self.order.node(node))
    }
}

pub trait BiDirPotential {
    fn init(&mut self, source: NodeId, target: NodeId);
    fn forward_potential(&mut self, node: NodeId) -> Option<Weight>;
    fn backward_potential(&mut self, node: NodeId) -> Option<Weight>;
    fn forward_potential_raw(&mut self, node: NodeId) -> Option<Weight> {
        self.forward_potential(node)
    }
    fn backward_potential_raw(&mut self, node: NodeId) -> Option<Weight> {
        self.backward_potential(node)
    }
    fn stop(&mut self, fw_min_queue: Option<Weight>, bw_min_queue: Option<Weight>, stop_dist: Weight) -> bool;
    fn stop_forward(&mut self, fw_min_queue: Option<Weight>, bw_min_queue: Option<Weight>, stop_dist: Weight) -> bool {
        self.stop(fw_min_queue, bw_min_queue, stop_dist)
    }
    fn stop_backward(&mut self, fw_min_queue: Option<Weight>, bw_min_queue: Option<Weight>, stop_dist: Weight) -> bool {
        self.stop(fw_min_queue, bw_min_queue, stop_dist)
    }
    fn prune_forward(&mut self, head: NodeIdT, fw_dist_head: Weight, reverse_min_queue: Weight, max_dist: Weight) -> bool;
    fn prune_backward(&mut self, head: NodeIdT, bw_dist_head: Weight, reverse_min_queue: Weight, max_dist: Weight) -> bool;
    fn bidir_pot_key() -> &'static str;

    fn report() {
        report!("bidir_pot", Self::bidir_pot_key());
    }
}

pub struct BiDirZeroPot;

impl BiDirPotential for BiDirZeroPot {
    fn init(&mut self, _source: NodeId, _target: NodeId) {}
    fn stop(&mut self, fw_min_queue: Option<Weight>, bw_min_queue: Option<Weight>, stop_dist: Weight) -> bool {
        match (fw_min_queue, bw_min_queue) {
            (Some(fw_min_queue), Some(bw_min_queue)) => fw_min_queue + bw_min_queue >= stop_dist,
            _ => true,
        }
    }
    fn forward_potential(&mut self, _node: NodeId) -> Option<Weight> {
        Some(0)
    }
    fn backward_potential(&mut self, _node: NodeId) -> Option<Weight> {
        Some(0)
    }
    fn prune_forward(&mut self, _head: NodeIdT, _fw_dist_head: Weight, _reverse_min_queue: Weight, _max_dist: Weight) -> bool {
        false
    }
    fn prune_backward(&mut self, _head: NodeIdT, _bw_dist_head: Weight, _reverse_min_queue: Weight, _max_dist: Weight) -> bool {
        false
    }
    fn bidir_pot_key() -> &'static str {
        "Zero"
    }
}

#[derive(Clone)]
pub struct AveragePotential<PF, PB> {
    forward_potential: PF,
    backward_potential: PB,
    fw_add: i32,
    bw_add: i32,
    mu_add: Weight,
}

impl<PF: Potential, PB: Potential> AveragePotential<PF, PB> {
    pub fn new(forward_potential: PF, backward_potential: PB) -> Self {
        Self {
            forward_potential,
            backward_potential,
            fw_add: 0,
            bw_add: 0,
            mu_add: 0,
        }
    }

    pub fn forward_potential(&mut self, node: NodeId) -> Option<Weight> {
        self.forward_potential.potential(node)
    }
    pub fn backward_potential(&mut self, node: NodeId) -> Option<Weight> {
        self.backward_potential.potential(node)
    }

    pub fn potential(&mut self, node: NodeId) -> Option<i32> {
        self.forward_potential
            .potential(node)
            .and_then(|dist_t| self.backward_potential.potential(node).map(|dist_s| (dist_t as i32 - dist_s as i32) / 2))
    }
}

impl<PF: Potential, PB: Potential> BiDirPotential for AveragePotential<PF, PB> {
    fn init(&mut self, source: NodeId, target: NodeId) {
        self.forward_potential.init(target);
        self.backward_potential.init(source);

        self.fw_add = (self.backward_potential.potential(target).unwrap_or(INFINITY) / 2) as i32;
        self.bw_add = (self.forward_potential.potential(source).unwrap_or(INFINITY) / 2) as i32;
        self.mu_add = self.potential(source).map(|p| (p + self.fw_add) as Weight).unwrap_or(0);
    }
    fn stop(&mut self, fw_min_queue: Option<Weight>, bw_min_queue: Option<Weight>, stop_dist: Weight) -> bool {
        match (fw_min_queue, bw_min_queue) {
            (Some(fw_min_queue), Some(bw_min_queue)) => fw_min_queue + bw_min_queue >= stop_dist + self.mu_add,
            _ => true,
        }
    }
    fn forward_potential(&mut self, node: NodeId) -> Option<Weight> {
        self.potential(node).map(|p| (p + self.fw_add) as Weight)
    }
    fn backward_potential(&mut self, node: NodeId) -> Option<Weight> {
        self.potential(node).map(|p| (self.bw_add - p) as Weight)
    }
    fn forward_potential_raw(&mut self, node: NodeId) -> Option<Weight> {
        self.forward_potential.potential(node)
    }
    fn backward_potential_raw(&mut self, node: NodeId) -> Option<Weight> {
        self.backward_potential.potential(node)
    }
    fn prune_forward(&mut self, head: NodeIdT, fw_dist_head: Weight, reverse_min_queue: Weight, max_dist: Weight) -> bool {
        prune_edge(
            &mut self.forward_potential,
            &mut self.backward_potential,
            head,
            fw_dist_head,
            reverse_min_queue,
            max_dist,
        )
    }
    fn prune_backward(&mut self, head: NodeIdT, bw_dist_head: Weight, reverse_min_queue: Weight, max_dist: Weight) -> bool {
        prune_edge(
            &mut self.backward_potential,
            &mut self.forward_potential,
            head,
            bw_dist_head,
            reverse_min_queue,
            max_dist,
        )
    }
    fn bidir_pot_key() -> &'static str {
        "Average"
    }
}

#[derive(Clone)]
pub struct SymmetricBiDirPotential<PF, PB> {
    forward_potential: PF,
    backward_potential: PB,
}

impl<PF: Potential, PB: Potential> SymmetricBiDirPotential<PF, PB> {
    pub fn new(forward_potential: PF, backward_potential: PB) -> Self {
        Self {
            forward_potential,
            backward_potential,
        }
    }

    pub fn forward(&self) -> &PF {
        &self.forward_potential
    }

    pub fn backward(&self) -> &PB {
        &self.backward_potential
    }
}

impl<PF: Potential, PB: Potential> BiDirPotential for SymmetricBiDirPotential<PF, PB> {
    fn init(&mut self, source: NodeId, target: NodeId) {
        self.forward_potential.init(target);
        self.backward_potential.init(source);
    }
    fn stop(&mut self, fw_min_queue: Option<Weight>, bw_min_queue: Option<Weight>, stop_dist: Weight) -> bool {
        self.stop_forward(fw_min_queue, bw_min_queue, stop_dist) && self.stop_backward(fw_min_queue, bw_min_queue, stop_dist)
    }
    fn stop_forward(&mut self, fw_min_queue: Option<Weight>, _bw_min_queue: Option<Weight>, stop_dist: Weight) -> bool {
        fw_min_queue.map(|key| key >= stop_dist).unwrap_or(true)
    }
    fn stop_backward(&mut self, _fw_min_queue: Option<Weight>, bw_min_queue: Option<Weight>, stop_dist: Weight) -> bool {
        bw_min_queue.map(|key| key >= stop_dist).unwrap_or(true)
    }
    fn forward_potential(&mut self, node: NodeId) -> Option<Weight> {
        self.forward_potential.potential(node)
    }
    fn backward_potential(&mut self, node: NodeId) -> Option<Weight> {
        self.backward_potential.potential(node)
    }
    fn prune_forward(&mut self, head: NodeIdT, fw_dist_head: Weight, reverse_min_queue: Weight, max_dist: Weight) -> bool {
        prune_edge(
            &mut self.forward_potential,
            &mut self.backward_potential,
            head,
            fw_dist_head,
            reverse_min_queue,
            max_dist,
        )
    }
    fn prune_backward(&mut self, head: NodeIdT, bw_dist_head: Weight, reverse_min_queue: Weight, max_dist: Weight) -> bool {
        prune_edge(
            &mut self.backward_potential,
            &mut self.forward_potential,
            head,
            bw_dist_head,
            reverse_min_queue,
            max_dist,
        )
    }
    fn bidir_pot_key() -> &'static str {
        "Symmetric"
    }
}

fn prune_edge<PF: Potential, PB: Potential>(
    forward_pot: &mut PF,
    backward_pot: &mut PB,
    NodeIdT(head): NodeIdT,
    fw_dist_head: Weight,
    reverse_min_queue: Weight,
    max_dist: Weight,
) -> bool {
    if max_dist < INFINITY {
        if fw_dist_head + forward_pot.potential(head).unwrap_or(INFINITY) >= max_dist {
            return true;
        }
        let remaining_by_queue = reverse_min_queue.saturating_sub(backward_pot.potential(head).unwrap_or(INFINITY));
        if fw_dist_head + remaining_by_queue >= max_dist {
            return true;
        }
    }
    return false;
}
