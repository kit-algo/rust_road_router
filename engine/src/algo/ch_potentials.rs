use super::*;
use crate::{
    algo::{
        a_star::Potential,
        customizable_contraction_hierarchy::{query::stepped_elimination_tree::SteppedEliminationTree, *},
        dijkstra::generic_dijkstra::*,
    },
    datastr::{node_order::*, timestamped_vector::TimestampedVector},
    report::*,
    util::in_range_option::InRangeOption,
};

pub mod penalty;
pub mod query;

#[derive(Debug)]
pub struct CCHPotential<'a> {
    cch: &'a CCH,
    stack: Vec<NodeId>,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    forward_cch_graph: FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<Weight>>,
    backward_elimination_tree: SteppedEliminationTree<'a, FirstOutGraph<&'a [EdgeId], &'a [NodeId], Vec<Weight>>>,
    num_pot_computations: usize,
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
            num_pot_computations: 0,
        }
    }

    pub fn num_pot_computations(&self) -> usize {
        self.num_pot_computations
    }
}

impl<'a> Potential for CCHPotential<'a> {
    fn init(&mut self, target: NodeId) {
        self.potentials.reset();
        self.backward_elimination_tree.initialize_query(self.cch.node_order().rank(target));
        while self.backward_elimination_tree.next().is_some() {
            self.backward_elimination_tree.next_step();
        }
        self.num_pot_computations = 0;
    }

    fn potential(&mut self, node: NodeId) -> Option<u32> {
        let node = self.cch.node_order().rank(node);

        let mut cur_node = node;
        while self.potentials[cur_node as usize].value().is_none() {
            self.num_pot_computations += 1;
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
}

pub struct CHPotential<GF, GB> {
    order: NodeOrder,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    forward: GF,
    backward_dijkstra: GenericDijkstra<GB>,
    num_pot_computations: usize,
}

impl<GF: for<'a> LinkIterGraph<'a>, GB: for<'a> LinkIterGraph<'a>> CHPotential<GF, GB> {
    pub fn new(forward: GF, backward: GB, order: NodeOrder) -> Self {
        let n = forward.num_nodes();
        Self {
            order,
            potentials: TimestampedVector::new(n, InRangeOption::new(None)),
            forward,
            backward_dijkstra: GenericDijkstra::new(backward),
            num_pot_computations: 0,
        }
    }

    pub fn num_pot_computations(&self) -> usize {
        self.num_pot_computations
    }

    fn potential_internal(
        potentials: &mut TimestampedVector<InRangeOption<Weight>>,
        forward: &GF,
        backward: &GenericDijkstra<GB>,
        node: NodeId,
        num_pot_computations: &mut usize,
    ) -> Weight {
        if let Some(pot) = potentials[node as usize].value() {
            return pot;
        }
        *num_pot_computations += 1;

        let min_by_up = LinkIterable::<Link>::link_iter(forward, node)
            .map(|edge| edge.weight + Self::potential_internal(potentials, forward, backward, edge.node, num_pot_computations))
            .min()
            .unwrap_or(INFINITY);

        potentials[node as usize] = InRangeOption::new(Some(std::cmp::min(*backward.tentative_distance(node), min_by_up)));

        potentials[node as usize].value().unwrap()
    }
}

impl<GF: for<'a> LinkIterGraph<'a>, GB: for<'a> LinkIterGraph<'a>> Potential for CHPotential<GF, GB> {
    fn init(&mut self, target: NodeId) {
        self.num_pot_computations = 0;
        self.potentials.reset();
        self.backward_dijkstra.initialize_query(Query {
            from: self.order.rank(target),
            to: std::u32::MAX,
        });
        while let Some(_) = self.backward_dijkstra.next() {}
    }

    fn potential(&mut self, node: NodeId) -> Option<Weight> {
        let node = self.order.rank(node);

        let dist = Self::potential_internal(
            &mut self.potentials,
            &self.forward,
            &self.backward_dijkstra,
            node,
            &mut self.num_pot_computations,
        );

        if dist < INFINITY {
            Some(dist)
        } else {
            None
        }
    }
}
