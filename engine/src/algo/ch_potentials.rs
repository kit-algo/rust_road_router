use super::*;
use crate::{
    algo::{
        customizable_contraction_hierarchy::{query::stepped_elimination_tree::SteppedEliminationTree, *},
        dijkstra::generic_dijkstra::*,
    },
    datastr::{node_order::*, timestamped_vector::TimestampedVector},
    report::*,
    util::in_range_option::InRangeOption,
};

pub mod query;

pub trait Potential {
    fn init(&mut self, target: NodeId);
    fn potential(&mut self, node: NodeId) -> Option<Weight>;
    fn num_pot_evals(&self) -> usize;
}

#[derive(Debug)]
pub struct CCHPotential<'a> {
    cch: &'a CCH,
    stack: Vec<NodeId>,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    forward_cch_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<Weight>>,
    backward_elimination_tree: SteppedEliminationTree<'a, FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<Weight>>>,
    num_pot_evals: usize,
}

impl<'a> CCHPotential<'a> {
    pub fn new<Graph>(cch: &'a CCH, lower_bound: &Graph) -> Self
    where
        Graph: for<'b> LinkIterGraph<'b> + RandomLinkAccessGraph + Sync,
    {
        let customized = customize(cch, lower_bound);
        let (forward_up_graph, backward_up_graph) = customized.into_ch_graphs();
        let backward_elimination_tree = SteppedEliminationTree::new(backward_up_graph, cch.elimination_tree());

        Self {
            cch,
            stack: Vec::new(),
            forward_cch_graph: forward_up_graph,
            backward_elimination_tree,
            potentials: TimestampedVector::new(cch.num_nodes(), InRangeOption::new(None)),
            num_pot_evals: 0,
        }
    }
}

impl<'a> Potential for CCHPotential<'a> {
    fn init(&mut self, target: NodeId) {
        self.potentials.reset();
        self.backward_elimination_tree.initialize_query(self.cch.node_order().rank(target));
        while self.backward_elimination_tree.next().is_some() {
            self.backward_elimination_tree.next_step();
        }
        self.num_pot_evals = 0;
    }

    fn potential(&mut self, node: NodeId) -> Option<u32> {
        let node = self.cch.node_order().rank(node);

        let mut cur_node = node;
        while self.potentials[cur_node as usize].value().is_none() {
            self.num_pot_evals += 1;
            self.stack.push(cur_node);
            if let Some(parent) = self.backward_elimination_tree.parent(cur_node).value() {
                cur_node = parent;
            } else {
                break;
            }
        }

        while let Some(node) = self.stack.pop() {
            let min_by_up = LinkIterable::<Link>::link_iter(&self.forward_cch_graph, node)
                .map(|edge| edge.weight + self.potentials[edge.node as usize].value().unwrap())
                .min()
                .unwrap_or(INFINITY);

            self.potentials[node as usize] = InRangeOption::new(Some(std::cmp::min(self.backward_elimination_tree.tentative_distance(node), min_by_up)));
        }

        let dist = self.potentials[node as usize].value().unwrap();
        if dist < INFINITY {
            Some(dist)
        } else {
            None
        }
    }

    fn num_pot_evals(&self) -> usize {
        self.num_pot_evals
    }
}

pub struct CHPotential {
    order: NodeOrder,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    forward: OwnedGraph,
    backward_dijkstra: StandardDijkstra<OwnedGraph>,
    num_pot_evals: usize,
}

impl CHPotential {
    pub fn new(forward: OwnedGraph, backward: OwnedGraph, order: NodeOrder) -> Self {
        let n = forward.num_nodes();
        Self {
            order,
            potentials: TimestampedVector::new(n, InRangeOption::new(None)),
            forward,
            backward_dijkstra: StandardDijkstra::new(backward),
            num_pot_evals: 0,
        }
    }

    fn potential_internal(
        potentials: &mut TimestampedVector<InRangeOption<Weight>>,
        forward: &OwnedGraph,
        backward: &StandardDijkstra<OwnedGraph>,
        node: NodeId,
        num_pot_evals: &mut usize,
    ) -> Weight {
        if let Some(pot) = potentials[node as usize].value() {
            return pot;
        }
        *num_pot_evals += 1;

        let min_by_up = LinkIterable::<Link>::link_iter(forward, node)
            .map(|edge| edge.weight + Self::potential_internal(potentials, forward, backward, edge.node, num_pot_evals))
            .min()
            .unwrap_or(INFINITY);

        potentials[node as usize] = InRangeOption::new(Some(std::cmp::min(*backward.tentative_distance(node), min_by_up)));

        potentials[node as usize].value().unwrap()
    }
}

impl Potential for CHPotential {
    fn init(&mut self, target: NodeId) {
        self.num_pot_evals = 0;
        self.potentials.reset();
        self.backward_dijkstra.initialize_query(Query {
            from: self.order.rank(target),
            to: std::u32::MAX,
        });
        while let Some(_) = self.backward_dijkstra.next() {}
    }

    fn potential(&mut self, node: NodeId) -> Option<Weight> {
        let node = self.order.rank(node);

        let dist = Self::potential_internal(&mut self.potentials, &self.forward, &self.backward_dijkstra, node, &mut self.num_pot_evals);

        if dist < INFINITY {
            Some(dist)
        } else {
            None
        }
    }

    fn num_pot_evals(&self) -> usize {
        self.num_pot_evals
    }
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
}

impl<P: Potential> Potential for TurnExpandedPotential<P> {
    fn init(&mut self, target: NodeId) {
        self.potential.init(self.tail[target as usize])
    }
    fn potential(&mut self, node: NodeId) -> Option<Weight> {
        self.potential.potential(self.tail[node as usize])
    }
    fn num_pot_evals(&self) -> usize {
        self.potential.num_pot_evals()
    }
}

pub struct BaselinePotential {
    dijkstra: StandardDijkstra<OwnedGraph>,
    num_pot_evals: usize,
}

impl BaselinePotential {
    pub fn new<G>(graph: &G) -> Self
    where
        OwnedGraph: BuildReversed<G>,
    {
        Self {
            dijkstra: StandardDijkstra::new(OwnedGraph::reversed(&graph)),
            num_pot_evals: 0,
        }
    }
}

impl Potential for BaselinePotential {
    fn init(&mut self, target: NodeId) {
        self.num_pot_evals = 0;
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

    fn num_pot_evals(&self) -> usize {
        self.num_pot_evals
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
    fn num_pot_evals(&self) -> usize {
        self.potential.num_pot_evals()
    }
}

#[derive(Debug)]
pub struct ZeroPotential();

impl Potential for ZeroPotential {
    fn init(&mut self, _target: NodeId) {}
    fn potential(&mut self, _node: NodeId) -> Option<Weight> {
        Some(0)
    }
    fn num_pot_evals(&self) -> usize {
        0
    }
}
