use crate::in_range_option::InRangeOption;
use crate::shortest_path::timestamped_vector::TimestampedVector;
use crate::shortest_path::node_order::NodeOrder;
use super::stepped_elimination_tree::SteppedEliminationTree;
use crate::shortest_path::customizable_contraction_hierarchy::*;
use super::*;

#[derive(Debug)]
pub struct Server<'a> {
    forward_dijkstra: SteppedDijkstra<OwnedGraph>,
    backward_dijkstra: SteppedDijkstra<OwnedGraph>,
    order: NodeOrder,
    core_size: NodeId,
    meeting_node: NodeId,
    stack: Vec<NodeId>,

    cch: &'a CCH,

    potentials: TimestampedVector<InRangeOption<i32>>,
    forward_elimination_tree: SteppedEliminationTree<'a, FirstOutGraph<&'a[EdgeId], &'a[NodeId], Vec<Weight>>>,
    backward_elimination_tree: SteppedEliminationTree<'a, FirstOutGraph<&'a[EdgeId], &'a[NodeId], Vec<Weight>>>,
}

impl<'a> Server<'a> {
    pub fn new<Graph>(forward: OwnedGraph, backward: OwnedGraph, order: NodeOrder, core_size: NodeId, cch: &'a CCH, lower_bound: &Graph) -> Server<'a> where
        Graph: for<'b> LinkIterGraph<'b> + RandomLinkAccessGraph + Sync
    {
        let (forward_up_graph, backward_up_graph) = customize(cch, lower_bound);
        let forward_elimination_tree = SteppedEliminationTree::new(forward_up_graph, cch.elimination_tree());
        let backward_elimination_tree = SteppedEliminationTree::new(backward_up_graph, cch.elimination_tree());

        Server {
            forward_dijkstra: SteppedDijkstra::new(forward),
            backward_dijkstra: SteppedDijkstra::new(backward),
            core_size,
            meeting_node: 0,
            stack: Vec::new(),
            order,
            cch,
            forward_elimination_tree,
            backward_elimination_tree,
            potentials: TimestampedVector::new(cch.num_nodes(), InRangeOption::new(None)),
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        self.potentials.reset();
        self.backward_elimination_tree.initialize_query(self.cch.node_order().rank(to));
        while self.backward_elimination_tree.next().is_some() {
            self.backward_elimination_tree.next_step();
        }
        self.forward_elimination_tree.initialize_query(self.cch.node_order().rank(from));
        while self.forward_elimination_tree.next().is_some() {
            self.forward_elimination_tree.next_step();
        }

        let from = self.order.rank(from);
        let to = self.order.rank(to);
        // initialize
        let mut tentative_distance = INFINITY;
        let mut forward_done = false;
        let mut backward_done = false;

        // node ids were reordered so that core nodes are at the "front" of the order, that is come first
        // so we can check if a node is in core by checking if its id is smaller than the number of nodes in the core
        let in_core = { let core_size = self.core_size; move |node| {
            node < core_size
        } };

        self.forward_dijkstra.initialize_query(Query { from, to });
        self.backward_dijkstra.initialize_query(Query { from: to, to: from });

        let mut forward_progress = 0;
        let mut backward_progress = 0;

        let mut settled_core_nodes = 0;
        let mut settled_non_core_nodes = 0;

        let forward_dijkstra = &mut self.forward_dijkstra;
        let backward_dijkstra = &mut self.backward_dijkstra;
        let meeting_node = &mut self.meeting_node;
        let stack = &mut self.stack;

        let cch = &self.cch;
        let order = &self.order;

        let potentials = &mut self.potentials;
        let backward_elimination_tree = &mut self.backward_elimination_tree;
        let forward_elimination_tree = &mut self.forward_elimination_tree;

        // while !(forward_done && backward_done) {
        // while !(dbg!(forward_done) && dbg!(backward_done)) && (dbg!(forward_progress) + dbg!(backward_progress) <= dbg!(tentative_distance) + dbg!(lower_tent_dist)) {
        loop {
            if !forward_done && (backward_done || forward_progress <= backward_progress) {
                match forward_dijkstra.next_step_with_callbacks(
                    |node, distance| {
                        ((2 * distance) as i32
                            + Self::potential(potentials, forward_elimination_tree, backward_elimination_tree, cch.node_order().rank(order.node(node)), stack)) as Weight
                            / 2
                        // distance
                            + if in_core(node) { INFINITY } else { 0 }
                    }) {
                    QueryProgress::Progress(State { distance, node }) => {
                        if in_core(node) { settled_core_nodes += 1 } else { settled_non_core_nodes += 1 }

                        if distance + backward_dijkstra.tentative_distance(node) < tentative_distance {
                            tentative_distance = distance + backward_dijkstra.tentative_distance(node);
                            *meeting_node = node;
                        }

                        forward_progress = distance;
                        if backward_dijkstra.tentative_distance(node) != INFINITY && !backward_dijkstra.queue().contains_index(node as usize) {
                            break;
                        }
                    },
                    QueryProgress::Done(_result) => {
                        forward_done = true;
                    }
                }
            } else {
                match backward_dijkstra.next_step_with_callbacks(
                    |node, distance| {
                        ((2 * distance) as i32
                            - Self::potential(potentials, forward_elimination_tree, backward_elimination_tree, cch.node_order().rank(order.node(node)), stack)) as Weight
                            / 2
                        // distance
                            + if in_core(node) { INFINITY } else { 0 }
                    }) {
                    QueryProgress::Progress(State { distance, node }) => {
                        if in_core(node) { settled_core_nodes += 1 } else { settled_non_core_nodes += 1 }

                        if distance + forward_dijkstra.tentative_distance(node) < tentative_distance {
                            tentative_distance = distance + forward_dijkstra.tentative_distance(node);
                            *meeting_node = node;
                        }

                        backward_progress = distance;
                        if forward_dijkstra.tentative_distance(node) != INFINITY && !forward_dijkstra.queue().contains_index(node as usize) {
                            break;
                        }
                    },
                    QueryProgress::Done(_result) => {
                        backward_done = true;
                    }
                }
            }
        }

        dbg!(settled_core_nodes, settled_non_core_nodes);

        match tentative_distance {
            INFINITY => None,
            dist => Some(dist)
        }
    }

    fn potential(
        potentials: &mut TimestampedVector<InRangeOption<i32>>,
        forward: &mut SteppedEliminationTree<'_, FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>>,
        backward: &mut SteppedEliminationTree<'_, FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>>,
        node: NodeId,
        stack: &mut Vec<NodeId>
    ) -> i32
    {
        let mut cur_node = node;
        while potentials[cur_node as usize].value().is_none() {
            stack.push(cur_node);
            if let Some(parent) = backward.parent(cur_node).value() {
                cur_node = parent;
            } else {
                break;
            }
        }

        while let Some(node) = stack.pop() {
            let min_by_up = forward.graph().neighbor_iter(node)
                .map(|edge| edge.weight + backward.tentative_distance(edge.node))
                .min()
                .unwrap_or(INFINITY);
            backward.distances_mut()[node as usize] = std::cmp::min(backward.tentative_distance(node), min_by_up);

            let min_by_up = backward.graph().neighbor_iter(node)
                .map(|edge| edge.weight + forward.tentative_distance(edge.node))
                .min()
                .unwrap_or(INFINITY);
            forward.distances_mut()[node as usize] = std::cmp::min(forward.tentative_distance(node), min_by_up);

            potentials[node as usize] = InRangeOption::new(Some(backward.tentative_distance(node) as i32 - forward.tentative_distance(node) as i32));
        }

        potentials[node as usize].value().unwrap()
    }

    pub fn is_in_searchspace(&self, node: NodeId) -> bool {
        self.forward_dijkstra.tentative_distance(node) < INFINITY
            || self.backward_dijkstra.tentative_distance(node) < INFINITY
    }

    pub fn path(&self) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(self.meeting_node);

        while *path.last().unwrap() != self.forward_dijkstra.query().from {
            let next = self.forward_dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        path.reverse();

        while *path.last().unwrap() != self.backward_dijkstra.query().from {
            let next = self.backward_dijkstra.predecessor(*path.last().unwrap());
            path.push(next);
        }

        let mut core_nodes = 0;
        let mut non_core_nodes = 0;
        for node in &mut path {
            if *node < self.core_size {
                core_nodes += 1;
            } else {
                non_core_nodes += 1;
            }
            *node = self.order.node(*node);
        }
        dbg!(core_nodes);
        dbg!(non_core_nodes);

        path
    }
}
