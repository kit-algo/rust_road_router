use super::*;
use crate::{
    algo::{
        a_star::Potential,
        customizable_contraction_hierarchy::{query::stepped_elimination_tree::SteppedEliminationTree, *},
        dijkstra::*,
    },
    datastr::{node_order::*, timestamped_vector::TimestampedVector},
    io::*,
    report::*,
    util::in_range_option::InRangeOption,
};

pub mod penalty;
pub mod query;

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
        Graph: LinkIterGraph + EdgeRandomAccessGraph<Link> + Sync,
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

#[derive(Clone)]
pub struct CHPotential<GF, GB> {
    order: NodeOrder,
    potentials: TimestampedVector<InRangeOption<Weight>>,
    forward: GF,
    backward: GB,
    dijkstra_data: DijkstraData<Weight>,
    num_pot_computations: usize,
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
        }
    }

    pub fn num_pot_computations(&self) -> usize {
        self.num_pot_computations
    }

    fn potential_internal(
        potentials: &mut TimestampedVector<InRangeOption<Weight>>,
        forward: &GF,
        backward_data: &DijkstraData<Weight>,
        node: NodeId,
        num_pot_computations: &mut usize,
    ) -> Weight {
        if let Some(pot) = potentials[node as usize].value() {
            return pot;
        }
        *num_pot_computations += 1;

        let min_by_up = LinkIterable::<Link>::link_iter(forward, node)
            .map(|edge| edge.weight + Self::potential_internal(potentials, forward, backward_data, edge.node, num_pot_computations))
            .min()
            .unwrap_or(INFINITY);

        potentials[node as usize] = InRangeOption::new(Some(std::cmp::min(backward_data.distances[node as usize], min_by_up)));

        potentials[node as usize].value().unwrap()
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

        let dist = Self::potential_internal(&mut self.potentials, &self.forward, &self.dijkstra_data, node, &mut self.num_pot_computations);

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
}
