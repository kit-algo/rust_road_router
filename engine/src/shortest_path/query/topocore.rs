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

    cch: &'a CCH,

    backward_elimination_tree: SteppedEliminationTree<'a, FirstOutGraph<&'a[EdgeId], &'a[NodeId], Vec<Weight>>>,
    forward_up_graph: FirstOutGraph<&'a[EdgeId], &'a[NodeId], Vec<Weight>>,
    forward_potentials: TimestampedVector<InRangeOption<Weight>>,

    forward_elimination_tree: SteppedEliminationTree<'a, FirstOutGraph<&'a[EdgeId], &'a[NodeId], Vec<Weight>>>,
    backward_up_graph: FirstOutGraph<&'a[EdgeId], &'a[NodeId], Vec<Weight>>,
    backward_potentials: TimestampedVector<InRangeOption<Weight>>,
}

impl<'a> Server<'a> {
    pub fn new<Graph>(forward: OwnedGraph, backward: OwnedGraph, order: NodeOrder, core_size: NodeId, cch: &'a CCH, lower_bound: &Graph) -> Server<'a> where
        Graph: for<'b> LinkIterGraph<'b> + RandomLinkAccessGraph + Sync
    {
        let (forward_up_graph, backward_up_graph) = customize(cch, lower_bound);
        let forward_elimination_tree = SteppedEliminationTree::new(forward_up_graph.clone(), cch.elimination_tree());
        let backward_elimination_tree = SteppedEliminationTree::new(backward_up_graph.clone(), cch.elimination_tree());

        Server {
            forward_dijkstra: SteppedDijkstra::new(forward),
            backward_dijkstra: SteppedDijkstra::new(backward),
            core_size,
            meeting_node: 0,
            order,
            cch,
            forward_up_graph,
            backward_up_graph,
            forward_elimination_tree,
            backward_elimination_tree,
            forward_potentials: TimestampedVector::new(cch.num_nodes(), InRangeOption::new(None)),
            backward_potentials: TimestampedVector::new(cch.num_nodes(), InRangeOption::new(None)),
        }
    }

    pub fn distance(&mut self, from: NodeId, to: NodeId) -> Option<Weight> {
        self.forward_potentials.reset();
        self.backward_elimination_tree.initialize_query(self.cch.node_order().rank(to));
        while self.backward_elimination_tree.next().is_some() {
            self.backward_elimination_tree.next_step();
        }
        self.backward_potentials.reset();
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
        let mut forward_non_core_count = 0;
        let mut backward_non_core_count = 0;

        // node ids were reordered so that core nodes are at the "front" of the order, that is come first
        // so we can check if a node is in core by checking if its id is smaller than the number of nodes in the core
        let in_core = { let core_size = self.core_size; move |node| {
            node < core_size
        } };

        if !in_core(from) {
            forward_non_core_count += 1;
        }
        if !in_core(to) {
            backward_non_core_count += 1;
        }

        self.forward_dijkstra.initialize_query(Query { from, to });
        self.backward_dijkstra.initialize_query(Query { from: to, to: from });

        let mut forward_progress = 0;
        let mut backward_progress = 0;

        let forward_dijkstra = &mut self.forward_dijkstra;
        let backward_dijkstra = &mut self.backward_dijkstra;
        let meeting_node = &mut self.meeting_node;

        let cch = &self.cch;
        let order = &self.order;

        let forward_potentials = &mut self.forward_potentials;
        let forward_up_graph = &self.forward_up_graph;
        let backward_elimination_tree = &self.backward_elimination_tree;

        let backward_potentials = &mut self.backward_potentials;
        let backward_up_graph = &self.backward_up_graph;
        let forward_elimination_tree = &self.forward_elimination_tree;

        // while !(forward_done && backward_done) {
        while !(forward_done && backward_done) && (forward_progress + backward_progress < tentative_distance || forward_non_core_count > 0 || backward_non_core_count > 0) {
            if !forward_done && (backward_done || forward_progress <= backward_progress) {
                match forward_dijkstra.next_step_with_callbacks(
                    |node| Self::potential(forward_potentials, forward_up_graph, backward_elimination_tree, cch.node_order().rank(order.node(node))),
                    // |_| 0,
                    |node, distance| {
                        if distance + backward_dijkstra.tentative_distance(node) < tentative_distance {
                            tentative_distance = distance + backward_dijkstra.tentative_distance(node);
                            *meeting_node = node;
                        }
                    },
                    |node| if !in_core(node) { forward_non_core_count += 1; }) {
                    QueryProgress::Progress(State { distance, node }) => {
                        if !in_core(node) { forward_non_core_count -= 1; }

                        forward_progress = distance;
                    },
                    QueryProgress::Done(result) => {
                        if let Some(distance) = result {
                            if !in_core(to) { forward_non_core_count -= 1; }
                            forward_progress = distance;

                            if distance < tentative_distance {
                                tentative_distance = distance;
                                *meeting_node = to;
                            }
                        }
                        forward_done = true;
                    }
                }
            } else {
                match backward_dijkstra.next_step_with_callbacks(
                    |node| Self::potential(backward_potentials, backward_up_graph, forward_elimination_tree, cch.node_order().rank(order.node(node))),
                    // |_| 0,
                    |node, distance| {
                        if distance + forward_dijkstra.tentative_distance(node) < tentative_distance {
                            tentative_distance = distance + forward_dijkstra.tentative_distance(node);
                            *meeting_node = node;
                        }
                    },
                    |node| if !in_core(node) { backward_non_core_count += 1; }) {
                    QueryProgress::Progress(State { distance, node }) => {
                        if !in_core(node) { backward_non_core_count -= 1; }

                        backward_progress = distance;
                    },
                    QueryProgress::Done(result) => {
                        if let Some(distance) = result {
                            if !in_core(from) { backward_non_core_count -= 1; }
                            backward_progress = distance;

                            if distance < tentative_distance {
                                tentative_distance = distance;
                                *meeting_node = from;
                            }
                        }
                        backward_done = true;
                    }
                }
            }
        }

        match tentative_distance {
            INFINITY => None,
            dist => Some(dist)
        }
    }

    fn potential(
        potentials: &mut TimestampedVector<InRangeOption<Weight>>,
        upward: &FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>,
        backward: &SteppedEliminationTree<'_, FirstOutGraph<&[EdgeId], &[NodeId], Vec<Weight>>>,
        node: NodeId
    ) -> Weight
    {
        if let Some(pot) = potentials[node as usize].value() {
            return pot;
        }

        let min_by_up = upward.neighbor_iter(node)
            .map(|edge| edge.weight + Self::potential(potentials, upward, backward, edge.node))
            .min()
            .unwrap_or(INFINITY);

        potentials[node as usize] = InRangeOption::new(Some(std::cmp::min(backward.tentative_distance(node), min_by_up)));
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

        for node in &mut path {
            *node = self.order.node(*node);
        }

        path
    }
}
