//! Dijkstras algorithm with optimization for degree 2 chains

use super::*;
use crate::algo::topocore::*;
use crate::datastr::{index_heap::*, rank_select_map::FastClearBitVec, timestamped_vector::*};

#[derive(Debug)]
pub struct TopoDijkstra {
    graph: OwnedGraph,
    reversed: OwnedGraph,
    virtual_topocore: VirtualTopocore,
    visited: FastClearBitVec,
    distances: TimestampedVector<Weight>,
    predecessors: Vec<NodeId>,
    closest_node_priority_queue: IndexdMinHeap<State<Weight>>,
    // the current query
    query: Option<Query>,
    // first option: algorithm finished? second option: final result of algorithm
    #[allow(clippy::option_option)]
    result: Option<Option<Weight>>,
    border_node: NodeId,
    num_relaxed_arcs: usize,
}

#[derive(Debug)]
struct ChainStep {
    prev_node: NodeId,
    next_node: NodeId,
    next_distance: NodeId,
}

impl TopoDijkstra {
    pub fn new<Graph: for<'a> LinkIterGraph<'a>>(graph: Graph) -> TopoDijkstra {
        let n = graph.num_nodes();
        let virtual_topocore = virtual_topocore(&graph);

        let graph = graph.permute_node_ids(&virtual_topocore.order);
        let reversed = graph.reverse();

        TopoDijkstra {
            graph,
            reversed,
            virtual_topocore,
            visited: FastClearBitVec::new(n),
            // initialize tentative distances to INFINITY
            distances: TimestampedVector::new(n, INFINITY),
            predecessors: vec![n as NodeId; n],
            closest_node_priority_queue: IndexdMinHeap::new(n),
            query: None,
            result: None,
            num_relaxed_arcs: 0,
            border_node: 0,
        }
    }

    pub fn initialize_query(&mut self, mut query: Query) {
        query.from = self.virtual_topocore.order.rank(query.from);
        query.to = self.virtual_topocore.order.rank(query.to);
        let from = query.from;
        // initialize
        self.query = Some(query);
        self.result = None;
        self.closest_node_priority_queue.clear();
        self.distances.reset();
        self.distances[from as usize] = 0;
        self.num_relaxed_arcs = 0;

        self.closest_node_priority_queue.push(State { distance: 0, node: from });

        let border = self.border(query.to);
        if let Some(border_node) = border {
            self.border_node = border_node;
        } else {
            self.result = Some(None);
            return;
        }

        if self.in_core(query.to) {
            let mut counter = 0;
            self.visited.clear();
            Self::dfs(&self.reversed, query.to, &mut self.visited, &mut |_| {}, &mut |_| {
                if counter < 100 {
                    counter += 1;
                    false
                } else {
                    true
                }
            });

            if counter < 100 {
                self.result = Some(None);
            }
        }
    }

    fn dfs(
        graph: &OwnedGraph,
        node: NodeId,
        visited: &mut FastClearBitVec,
        border_callback: &mut impl FnMut(NodeId),
        in_core: &mut impl FnMut(NodeId) -> bool,
    ) {
        if visited.get(node as usize) {
            return;
        }
        visited.set(node as usize);
        if in_core(node) {
            border_callback(node);
            return;
        }
        for link in graph.neighbor_iter(node) {
            Self::dfs(graph, link.node, visited, border_callback, in_core);
        }
    }

    fn border(&mut self, node: NodeId) -> Option<NodeId> {
        let mut border = None;
        self.visited.clear();
        let virtual_topocore = &self.virtual_topocore;
        Self::dfs(
            &self.reversed,
            node,
            &mut self.visited,
            &mut |node| {
                let prev = border.replace(node);
                debug_assert_eq!(prev, None);
            },
            &mut |node| virtual_topocore.node_type(node).in_core(),
        );
        border
    }

    pub fn next_step(&mut self) -> QueryProgress<Weight> {
        match self.result {
            Some(result) => QueryProgress::Done(result),
            None => self.settle_next_node(|_| Some(0)),
        }
    }

    pub fn next_step_with_potential(&mut self, potential: impl FnMut(NodeId) -> Option<Weight>) -> QueryProgress<Weight> {
        match self.result {
            Some(result) => QueryProgress::Done(result),
            None => self.settle_next_node(potential),
        }
    }

    fn settle_next_node(&mut self, mut potential: impl FnMut(NodeId) -> Option<Weight>) -> QueryProgress<Weight> {
        let to = self.query.as_ref().expect("query was not initialized properly").to;

        let mut next_node = None;

        while let Some(State { node, distance: dist_with_pot }) = self.closest_node_priority_queue.pop() {
            if !(dist_with_pot > self.distances[self.border_node as usize] + potential(self.virtual_topocore.order.node(self.border_node)).unwrap()
                && self.in_core(node))
            {
                next_node = Some(State { node, distance: dist_with_pot });
                break;
            }
        }

        // Examine the frontier with lower distance nodes first (min-heap)
        if let Some(State { node, distance: dist_with_pot }) = next_node {
            let distance = self.distances[node as usize];

            if node == to {
                self.result = Some(Some(distance));
            }

            if dist_with_pot >= self.distances[to as usize] {
                self.result = Some(Some(self.distances[to as usize]));
            }

            for edge in self.graph.neighbor_iter(node) {
                let mut chain = Some(ChainStep {
                    prev_node: node,
                    next_node: edge.node,
                    next_distance: distance + edge.weight,
                });
                let mut deg_three = None;
                let mut had_deg_three = false;

                while let Some(ChainStep {
                    prev_node,
                    next_node,
                    next_distance,
                }) = chain.take()
                {
                    self.num_relaxed_arcs += 1;

                    if (cfg!(feature = "chpot-no-scc") || self.in_core(next_node) || !self.in_core(prev_node) || self.border_node == prev_node)
                        && next_distance < self.distances[next_node as usize]
                    {
                        let mut next_edge = None;
                        let mut endpoint = false;
                        debug_assert!(next_distance >= distance);

                        match self.virtual_topocore.node_type(next_node) {
                            NodeType::Deg2OrLess | NodeType::OtherSCC(2) => {
                                if cfg!(feature = "chpot-no-deg2") {
                                    endpoint = true;
                                } else {
                                    for edge in self.graph.neighbor_iter(next_node) {
                                        if edge.node != prev_node {
                                            next_edge = Some(edge);
                                        }
                                    }
                                }
                            }
                            NodeType::Deg3 | NodeType::OtherSCC(3) => {
                                if cfg!(feature = "chpot-no-deg3")
                                    || had_deg_three
                                    || self.closest_node_priority_queue.contains_index(
                                        State {
                                            distance: next_distance,
                                            node: next_node,
                                        }
                                        .as_index(),
                                    )
                                {
                                    endpoint = true;
                                } else {
                                    had_deg_three = true;
                                    for edge in self.graph.neighbor_iter(next_node) {
                                        if edge.node != prev_node {
                                            if next_edge.is_none() {
                                                next_edge = Some(edge);
                                            } else {
                                                deg_three = Some((next_node, next_distance, edge));
                                            }
                                        }
                                    }
                                }
                            }
                            NodeType::Deg4OrMore => {
                                endpoint = true;
                            }
                            NodeType::OtherSCC(d) if d > 3 => {
                                endpoint = true;
                            }
                            _ => {}
                        }

                        self.distances.set(next_node as usize, next_distance);
                        self.predecessors[next_node as usize] = prev_node;

                        if let Some(next_edge) = next_edge {
                            chain = Some(ChainStep {
                                prev_node: next_node,
                                next_node: next_edge.node,
                                next_distance: next_distance + next_edge.weight,
                            });
                        } else if endpoint {
                            if let Some(key) = potential(self.virtual_topocore.order.node(next_node)).map(|p| next_distance + p) {
                                let next = State {
                                    distance: key,
                                    node: next_node,
                                };
                                if let Some(other) = self.closest_node_priority_queue.get(next.as_index()) {
                                    debug_assert!(other.distance >= next.distance);
                                    self.closest_node_priority_queue.decrease_key(next);
                                } else {
                                    self.closest_node_priority_queue.push(next);
                                }
                            }
                        }
                    }

                    if chain.is_none() {
                        if let Some((deg_three_node, deg_three_distance, edge)) = deg_three.take() {
                            chain = Some(ChainStep {
                                prev_node: deg_three_node,
                                next_distance: deg_three_distance + edge.weight,
                                next_node: edge.node,
                            });
                        }
                    }
                }
            }

            QueryProgress::Settled(State {
                distance,
                node: self.virtual_topocore.order.node(node),
            })
        } else {
            self.result = Some(None);
            QueryProgress::Done(None)
        }
    }

    fn in_core(&self, node: NodeId) -> bool {
        self.virtual_topocore.node_type(node).in_core()
    }

    pub fn tentative_distance(&self, node: NodeId) -> Weight {
        self.distances[self.virtual_topocore.order.rank(node) as usize]
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.virtual_topocore
            .order
            .node(self.predecessors[self.virtual_topocore.order.rank(node) as usize])
    }

    pub fn query(&self) -> Query {
        let mut query = self.query.unwrap();
        query.from = self.virtual_topocore.order.node(query.from);
        query.to = self.virtual_topocore.order.node(query.to);
        query
    }

    pub fn num_relaxed_arcs(&self) -> usize {
        self.num_relaxed_arcs
    }

    pub fn queue(&self) -> &IndexdMinHeap<State<Weight>> {
        &self.closest_node_priority_queue
    }
}
