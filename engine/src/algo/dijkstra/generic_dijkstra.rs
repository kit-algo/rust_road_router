//! Basic variant of dijkstras algorithm

use super::*;
use crate::algo::dijkstra::gen_topo_dijkstra::Neutral;
use std::borrow::Borrow;

pub trait DijkstraOps<Graph> {
    type Label: super::Label;
    type Arc: Arc;
    type LinkResult;

    fn link(&mut self, graph: &Graph, label: &Self::Label, link: &Self::Arc) -> Self::LinkResult;
    fn merge(&mut self, label: &mut Self::Label, linked: Self::LinkResult) -> bool;
}

#[derive(Debug, Clone, Copy)]
pub struct DefaultOps();

impl<G> DijkstraOps<G> for DefaultOps {
    type Label = Weight;
    type Arc = Link;
    type LinkResult = Weight;

    #[inline(always)]
    fn link(&mut self, _graph: &G, label: &Weight, link: &Link) -> Self::LinkResult {
        label + link.weight
    }

    #[inline(always)]
    fn merge(&mut self, label: &mut Weight, linked: Self::LinkResult) -> bool {
        if linked < *label {
            *label = linked;
            return true;
        }
        false
    }
}

impl Default for DefaultOps {
    fn default() -> Self {
        DefaultOps()
    }
}

pub struct DijkstraRun<'a, Graph = OwnedGraph, Ops = DefaultOps>
where
    Ops: DijkstraOps<Graph>,
{
    graph: &'a Graph,

    distances: &'a mut TimestampedVector<Ops::Label>,
    predecessors: &'a mut Vec<NodeId>,
    queue: &'a mut IndexdMinHeap<State<<Ops::Label as super::Label>::Key>>,

    ops: &'a mut Ops,

    num_relaxed_arcs: usize,
    num_queue_pushs: usize,
}

impl<'b, Graph, Ops> DijkstraRun<'b, Graph, Ops>
where
    Graph: LinkIterable<Ops::Arc>,
    Ops: DijkstraOps<Graph>,
{
    pub fn query(graph: &'b Graph, data: &'b mut DijkstraData<Ops::Label>, ops: &'b mut Ops, query: impl GenQuery<Ops::Label>) -> Self {
        let mut s = Self {
            graph,
            ops,
            predecessors: &mut data.predecessors,
            queue: &mut data.queue,
            distances: &mut data.distances,
            num_relaxed_arcs: 0,
            num_queue_pushs: 0,
        };
        s.initialize(query);
        s
    }

    pub fn continue_query(graph: &'b Graph, data: &'b mut DijkstraData<Ops::Label>, ops: &'b mut Ops, node: NodeId) -> Self {
        let mut s = Self {
            graph,
            ops,
            predecessors: &mut data.predecessors,
            queue: &mut data.queue,
            distances: &mut data.distances,
            num_relaxed_arcs: 0,
            num_queue_pushs: 0,
        };
        s.reinit_queue(node);
        s
    }

    pub fn initialize(&mut self, query: impl GenQuery<Ops::Label>) {
        self.queue.clear();
        self.distances.reset();
        self.add_start_node(query);
    }

    pub fn add_start_node(&mut self, query: impl GenQuery<Ops::Label>) {
        let from = query.from();
        let init = query.initial_state();
        self.queue.push(State { key: init.key(), node: from });
        self.distances[from as usize] = init;
        self.predecessors[from as usize] = from;
    }

    fn reinit_queue(&mut self, node: NodeId) {
        self.queue.clear();
        self.queue.push(State {
            key: self.distances[node as usize].key(),
            node,
        });
    }

    #[inline(always)]
    pub fn next_filtered_edges(&mut self, edge_predicate: impl FnMut(&Ops::Arc) -> bool) -> Option<NodeId> {
        self.settle_next_node(edge_predicate, |_, _| true, |_| Some(Neutral()))
    }

    #[inline(always)]
    pub fn next_step_with_potential<P, O>(&mut self, potential: P) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
    {
        self.settle_next_node(|_| true, |_, _| true, potential)
    }

    #[inline(always)]
    pub fn next_with_improve_callback(&mut self, improve_callback: impl FnMut(NodeId, &Ops::Label) -> bool) -> Option<NodeId> {
        self.settle_next_node(|_| true, improve_callback, |_| Some(Neutral()))
    }

    #[inline(always)]
    pub fn next_with_improve_callback_and_potential<P, O>(&mut self, improve_callback: impl FnMut(NodeId, &Ops::Label) -> bool, potential: P) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
    {
        self.settle_next_node(|_| true, improve_callback, potential)
    }

    #[inline(always)]
    fn settle_next_node<I, P, O>(&mut self, mut edge_predicate: impl FnMut(&Ops::Arc) -> bool, mut improve_callback: I, mut potential: P) -> Option<NodeId>
    where
        I: FnMut(NodeId, &Ops::Label) -> bool,
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
    {
        self.queue.pop().map(|State { node, .. }| {
            for link in self.graph.borrow().link_iter(node) {
                if edge_predicate(&link) {
                    self.num_relaxed_arcs += 1;
                    let linked = self.ops.link(self.graph.borrow(), &self.distances[node as usize], &link);

                    if self.ops.merge(&mut self.distances[link.head() as usize], linked) {
                        self.predecessors[link.head() as usize] = node;
                        let next_distance = &self.distances[link.head() as usize];

                        if improve_callback(link.head(), next_distance) {
                            if let Some(key) = potential(link.head()).map(|p| p + next_distance.key()) {
                                let next = State { key, node: link.head() };
                                if self.queue.contains_index(next.as_index()) {
                                    self.queue.decrease_key(next);
                                } else {
                                    self.num_queue_pushs += 1;
                                    self.queue.push(next);
                                }
                            }
                        }
                    }
                }
            }

            node
        })
    }

    pub fn exchange_potential<P, O>(&mut self, mut potential: P)
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
    {
        let active_nodes: Vec<_> = self.queue.elements().iter().map(|e| e.node).collect();
        for node in active_nodes {
            self.queue.update_key(State {
                node,
                key: potential(node).map(|p| p + self.distances[node as usize].key()).unwrap(),
            })
        }
    }

    pub fn tentative_distance(&self, node: NodeId) -> &Ops::Label {
        &self.distances[node as usize]
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.predecessors[node as usize]
    }

    pub fn graph(&self) -> &Graph {
        self.graph.borrow()
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

impl<'b, Ops, Graph> Iterator for DijkstraRun<'b, Graph, Ops>
where
    Ops: DijkstraOps<Graph>,
    Graph: LinkIterable<Ops::Arc>,
{
    type Item = NodeId;

    #[inline]
    fn next(&mut self) -> Option<NodeId> {
        self.settle_next_node(|_| true, |_, _| true, |_| Some(Neutral()))
    }
}
