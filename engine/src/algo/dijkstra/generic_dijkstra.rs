//! Basic variant of dijkstras algorithm

use super::*;
use crate::algo::dijkstra::gen_topo_dijkstra::Neutral;
use crate::datastr::{index_heap::*, timestamped_vector::*};
use std::borrow::Borrow;

pub trait DijkstraOps<Graph> {
    type Label: super::Label + Clone;
    type Arc: Arc;
    type LinkResult;

    fn link(&mut self, graph: &Graph, label: &Self::Label, link: &Self::Arc) -> Self::LinkResult;
    fn merge(&mut self, label: &mut Self::Label, linked: Self::LinkResult) -> bool;
}

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

pub struct GenericDijkstra<Graph = OwnedGraph, Ops = DefaultOps, BorrowGraph = Graph>
where
    Ops: DijkstraOps<Graph>,
{
    graph: BorrowGraph,

    distances: TimestampedVector<Ops::Label>,
    predecessors: Vec<NodeId>,
    queue: IndexdMinHeap<State<<Ops::Label as super::Label>::Key>>,

    ops: Ops,

    num_relaxed_arcs: usize,
    num_queue_pushs: usize,
}

impl<Graph, Ops, BorrowGraph> GenericDijkstra<Graph, Ops, BorrowGraph>
where
    Graph: for<'a> LinkIterable<'a, Ops::Arc>,
    Ops: DijkstraOps<Graph>,
    BorrowGraph: Borrow<Graph>,
{
    pub fn new(graph: BorrowGraph) -> Self
    where
        Ops: Default,
    {
        let n = graph.borrow().num_nodes();

        GenericDijkstra {
            graph,

            distances: TimestampedVector::new(n, Label::neutral()),
            predecessors: vec![n as NodeId; n],
            queue: IndexdMinHeap::new(n),

            ops: Default::default(),

            num_relaxed_arcs: 0,
            num_queue_pushs: 0,
        }
    }

    /// For CH preprocessing we reuse the distance array and the queue to reduce allocations.
    /// This method creates an algo struct from recycled data.
    /// The counterpart is the `recycle` method.
    pub fn from_recycled(graph: BorrowGraph, recycled: Trash<Ops::Label>) -> Self
    where
        Ops: Default,
    {
        let n = graph.borrow().num_nodes();
        assert!(recycled.distances.len() >= n);
        assert!(recycled.predecessors.len() >= n);

        Self {
            graph,
            distances: recycled.distances,
            predecessors: recycled.predecessors,
            queue: recycled.queue,
            ops: Default::default(),

            num_relaxed_arcs: 0,
            num_queue_pushs: 0,
        }
    }

    pub fn initialize_query(&mut self, query: impl GenQuery<Ops::Label>) {
        // reset
        self.queue.clear();
        self.distances.reset();

        self.num_relaxed_arcs = 0;
        self.num_queue_pushs = 0;

        self.add_start_node(query);
    }

    pub fn add_start_node(&mut self, query: impl GenQuery<Ops::Label>) {
        let from = query.from();
        let init = query.initial_state();
        self.queue.push(State { key: init.key(), node: from });
        self.distances[from as usize] = init;
        self.predecessors[from as usize] = from;
    }

    #[inline]
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

    #[inline]
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

    /// For CH preprocessing we reuse the distance array and the queue to reduce allocations.
    /// This method decomposes this algo struct for later reuse.
    /// The counterpart is `from_recycled`
    pub fn recycle(self) -> Trash<Ops::Label> {
        Trash {
            distances: self.distances,
            predecessors: self.predecessors,
            queue: self.queue,
        }
    }

    pub fn num_relaxed_arcs(&self) -> usize {
        self.num_relaxed_arcs
    }

    pub fn num_queue_pushs(&self) -> usize {
        self.num_queue_pushs
    }
}

impl<Ops, Graph, A> Iterator for GenericDijkstra<Graph, Ops, A>
where
    Ops: DijkstraOps<Graph>,
    Graph: for<'a> LinkIterable<'a, Ops::Arc>,
    A: Borrow<Graph>,
{
    type Item = NodeId;

    #[inline]
    fn next(&mut self) -> Option<NodeId> {
        self.settle_next_node(|_| true, |_, _| true, |_| Some(Neutral()))
    }
}

pub struct Trash<Label: super::Label> {
    distances: TimestampedVector<Label>,
    predecessors: Vec<NodeId>,
    queue: IndexdMinHeap<State<Label::Key>>,
}
