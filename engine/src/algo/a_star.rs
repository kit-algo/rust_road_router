use super::*;
use crate::{algo::dijkstra::generic_dijkstra::*, report::*};

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
    dijkstra: GenericDijkstra<OwnedGraph>,
}

impl BaselinePotential {
    pub fn new<G>(graph: &G) -> Self
    where
        OwnedGraph: BuildReversed<G>,
    {
        Self {
            dijkstra: GenericDijkstra::new(OwnedGraph::reversed(&graph)),
        }
    }
}

impl Potential for BaselinePotential {
    fn init(&mut self, target: NodeId) {
        report_time_with_key("BaselinePotential init", "baseline_pot_init", || {
            self.dijkstra.initialize_query(Query {
                from: target,
                to: self.dijkstra.graph().num_nodes() as NodeId,
            });
            while let Some(_) = self.dijkstra.next() {}
        })
    }

    fn potential(&mut self, node: NodeId) -> Option<Weight> {
        if *self.dijkstra.tentative_distance(node) < INFINITY {
            Some(*self.dijkstra.tentative_distance(node))
        } else {
            None
        }
    }
}

#[derive(Debug)]
pub struct RecyclingPotential<Potential> {
    potential: Potential,
    target: Option<NodeId>,
}

impl<P> RecyclingPotential<P> {
    pub fn new(potential: P) -> Self {
        Self { potential, target: None }
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

#[derive(Debug)]
pub struct AveragePotential<PF, PB> {
    forward_potential: PF,
    backward_potential: PB,
}

impl<PF: Potential, PB: Potential> AveragePotential<PF, PB> {
    pub fn new(forward_potential: PF, backward_potential: PB) -> Self {
        AveragePotential {
            forward_potential,
            backward_potential,
        }
    }

    pub fn forward_potential(&mut self, node: NodeId) -> Option<Weight> {
        self.forward_potential.potential(node)
    }
    pub fn backward_potential(&mut self, node: NodeId) -> Option<Weight> {
        self.backward_potential.potential(node)
    }

    pub fn init(&mut self, source: NodeId, target: NodeId) {
        self.forward_potential.init(target);
        self.backward_potential.init(source);
    }

    pub fn potential(&mut self, node: NodeId) -> Option<i32> {
        self.forward_potential
            .potential(node)
            .and_then(|dist_t| self.backward_potential.potential(node).map(|dist_s| (dist_t as i32 - dist_s as i32) / 2))
    }
}

#[derive(Debug)]
pub struct PotentialForPermutated<P> {
    pub potential: P,
    pub order: NodeOrder,
}

impl<P: Potential> Potential for PotentialForPermutated<P> {
    fn init(&mut self, target: NodeId) {
        self.potential.init(self.order.node(target))
    }

    fn potential(&mut self, node: NodeId) -> Option<Weight> {
        self.potential.potential(self.order.node(node))
    }
}
