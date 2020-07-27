//! Dijkstras algorithm with optimization for degree 2 chains

use super::generic_dijkstra::*;
use super::*;
use crate::algo::topocore::*;
use crate::datastr::{index_heap::*, rank_select_map::FastClearBitVec, timestamped_vector::*};

pub struct GenTopoDijkstra<Ops: DijkstraOps<Graph>, Graph> {
    graph: Graph,
    reversed: UnweightedOwnedGraph,
    virtual_topocore: VirtualTopocore,
    visited: FastClearBitVec,

    distances: TimestampedVector<Ops::Label>,
    predecessors: Vec<NodeId>,
    queue: IndexdMinHeap<State<<Ops::Label as super::Label>::Key>>,

    border_node: NodeId,

    num_relaxed_arcs: usize,
    num_queue_pushs: usize,

    ops: Ops,
}

#[derive(Debug)]
struct ChainStep<LinkResult> {
    prev_node: NodeId,
    next_node: NodeId,
    next_distance: LinkResult,
}

impl<Ops, Graph> GenTopoDijkstra<Ops, Graph>
where
    Ops: DijkstraOps<Graph> + Default,
    <Ops::Label as super::Label>::Key: std::ops::Add<Output = <Ops::Label as super::Label>::Key>,
    Graph: for<'a> LinkIterable<'a, NodeId> + for<'a> LinkIterable<'a, Ops::Arc>,
{
    pub fn new<G>(graph: G) -> Self
    where
        G: for<'a> LinkIterable<'a, NodeId>,
        Graph: BuildPermutated<G>,
    {
        let n = graph.num_nodes();
        let virtual_topocore = virtual_topocore(&graph);

        let graph = <Graph as BuildPermutated<G>>::permutated(&graph, &virtual_topocore.order);
        let reversed = UnweightedOwnedGraph::reversed(&graph);

        Self {
            graph,
            reversed,
            virtual_topocore,
            visited: FastClearBitVec::new(n),

            distances: TimestampedVector::new(n, Label::neutral()),
            predecessors: vec![n as NodeId; n],
            queue: IndexdMinHeap::new(n),

            num_relaxed_arcs: 0,
            num_queue_pushs: 0,
            border_node: 0,

            ops: Default::default(),
        }
    }

    pub fn initialize_query(&mut self, query: impl GenQuery<Ops::Label>) {
        let from = self.virtual_topocore.order.rank(query.from());
        let to = self.virtual_topocore.order.rank(query.to());
        // initialize
        self.num_relaxed_arcs = 0;
        self.num_queue_pushs = 0;

        self.queue.clear();
        let init = query.initial_state();
        self.queue.push(State { key: init.key(), node: from });

        self.distances.reset();
        self.distances[from as usize] = init;

        let border = self.border(to);
        if let Some(border_node) = border {
            self.border_node = border_node;
        } else {
            self.queue.clear();
            return;
        }

        if self.in_core(to) {
            let mut counter = 0;
            self.visited.clear();
            Self::dfs(&self.reversed, to, &mut self.visited, &mut |_| {}, &mut |_| {
                if counter < 100 {
                    counter += 1;
                    false
                } else {
                    true
                }
            });

            if counter < 100 {
                self.queue.clear();
            }
        }
    }

    fn dfs(
        graph: &UnweightedOwnedGraph,
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
        for head in graph.link_iter(node) {
            Self::dfs(graph, head, visited, border_callback, in_core);
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

    // pub fn next_step(&mut self) -> Option<NodeId> {
    //     self.settle_next_node(|_| Some(0))
    // }

    pub fn next_step_with_potential(&mut self, potential: impl FnMut(NodeId) -> Option<<Ops::Label as super::Label>::Key>) -> Option<NodeId> {
        self.settle_next_node(potential)
    }

    fn settle_next_node(&mut self, mut potential: impl FnMut(NodeId) -> Option<<Ops::Label as super::Label>::Key>) -> Option<NodeId> {
        let mut next_node = None;

        while let Some(State { node, key: dist_with_pot }) = self.queue.pop() {
            if !(dist_with_pot > self.distances[self.border_node as usize].key() + potential(self.virtual_topocore.order.node(self.border_node)).unwrap()
                && self.in_core(node))
            {
                next_node = Some(State { node, key: dist_with_pot });
                break;
            }
        }

        next_node.map(|State { node, key: _dist_with_pot }| {
            for edge in LinkIterable::<Ops::Arc>::link_iter(&self.graph, node) {
                let mut chain = Some(ChainStep {
                    prev_node: node,
                    next_node: edge.head(),
                    next_distance: self.ops.link(&self.graph, &self.distances[node as usize], &edge),
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

                    if (cfg!(feature = "chpot-no-bcc") || self.in_core(next_node) || !self.in_core(prev_node) || self.border_node == prev_node)
                        && self.ops.merge(&mut self.distances[next_node as usize], next_distance)
                    {
                        let next_distance = &self.distances[next_node as usize];
                        let mut next_edge = None;
                        let mut endpoint = false;
                        // debug_assert!(next_distance >= distance);

                        match self.virtual_topocore.node_type(next_node) {
                            NodeType::Deg2OrLess | NodeType::OtherBCC(2) => {
                                if cfg!(feature = "chpot-no-deg2") {
                                    endpoint = true;
                                } else {
                                    for edge in LinkIterable::<Ops::Arc>::link_iter(&self.graph, next_node) {
                                        if edge.head() != prev_node {
                                            next_edge = Some(edge);
                                        }
                                    }
                                }
                            }
                            NodeType::Deg3 | NodeType::OtherBCC(3) => {
                                if cfg!(feature = "chpot-no-deg3")
                                    || had_deg_three
                                    || self.queue.contains_index(
                                        State {
                                            key: next_distance.key(),
                                            node: next_node,
                                        }
                                        .as_index(),
                                    )
                                {
                                    endpoint = true;
                                } else {
                                    had_deg_three = true;
                                    for edge in LinkIterable::<Ops::Arc>::link_iter(&self.graph, next_node) {
                                        if edge.head() != prev_node {
                                            if next_edge.is_none() {
                                                next_edge = Some(edge);
                                            } else {
                                                deg_three = Some((next_node, edge));
                                            }
                                        }
                                    }
                                }
                            }
                            NodeType::Deg4OrMore => {
                                endpoint = true;
                            }
                            NodeType::OtherBCC(d) if d > 3 => {
                                endpoint = true;
                            }
                            _ => {}
                        }

                        self.predecessors[next_node as usize] = prev_node;

                        if let Some(next_edge) = next_edge {
                            chain = Some(ChainStep {
                                prev_node: next_node,
                                next_node: next_edge.head(),
                                next_distance: self.ops.link(&self.graph, next_distance, &next_edge),
                            });
                        } else if endpoint {
                            if let Some(key) = potential(self.virtual_topocore.order.node(next_node)).map(|p| next_distance.key() + p) {
                                let next = State { key, node: next_node };
                                if let Some(other) = self.queue.get(next.as_index()) {
                                    debug_assert!(other.key >= next.key);
                                    self.queue.decrease_key(next);
                                } else {
                                    self.num_queue_pushs += 1;
                                    self.queue.push(next);
                                }
                            }
                        }
                    }

                    if chain.is_none() {
                        if let Some((deg_three_node, edge)) = deg_three.take() {
                            chain = Some(ChainStep {
                                prev_node: deg_three_node,
                                next_distance: self.ops.link(&self.graph, &self.distances[deg_three_node as usize], &edge),
                                next_node: edge.head(),
                            });
                        }
                    }
                }
            }

            self.virtual_topocore.order.node(node)
        })
    }

    fn in_core(&self, node: NodeId) -> bool {
        self.virtual_topocore.node_type(node).in_core()
    }

    pub fn tentative_distance(&self, node: NodeId) -> &Ops::Label {
        &self.distances[self.virtual_topocore.order.rank(node) as usize]
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.virtual_topocore
            .order
            .node(self.predecessors[self.virtual_topocore.order.rank(node) as usize])
    }

    pub fn queue(&self) -> &IndexdMinHeap<State<<Ops::Label as super::Label>::Key>> {
        &self.queue
    }

    pub fn num_relaxed_arcs(&self) -> usize {
        self.num_relaxed_arcs
    }

    pub fn num_queue_pushs(&self) -> usize {
        self.num_queue_pushs
    }
}

pub type StandardTopoDijkstra<G> = GenTopoDijkstra<DefaultOps, G>;
