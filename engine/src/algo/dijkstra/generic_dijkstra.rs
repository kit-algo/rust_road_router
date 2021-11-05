//! Basic variant of dijkstras algorithm

use super::*;
use crate::algo::dijkstra::gen_topo_dijkstra::Neutral;

pub struct DijkstraRun<'a, Graph = OwnedGraph, Ops = DefaultOps>
where
    Ops: DijkstraOps<Graph>,
{
    graph: &'a Graph,

    distances: &'a mut TimestampedVector<Ops::Label>,
    predecessors: &'a mut [(NodeId, Ops::PredecessorLink)],
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
    pub fn query(graph: &'b Graph, data: &'b mut DijkstraData<Ops::Label, Ops::PredecessorLink>, ops: &'b mut Ops, query: impl GenQuery<Ops::Label>) -> Self {
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

    pub fn continue_query(graph: &'b Graph, data: &'b mut DijkstraData<Ops::Label, Ops::PredecessorLink>, ops: &'b mut Ops, node: NodeId) -> Self {
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
        self.predecessors[from as usize].0 = from;
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
            for link in self.graph.link_iter(node) {
                if edge_predicate(&link) {
                    self.num_relaxed_arcs += 1;
                    let linked = self
                        .ops
                        .link(self.graph, self.predecessors, NodeIdT(node), &self.distances[node as usize], &link);

                    if self.ops.merge(&mut self.distances[link.head() as usize], linked) {
                        self.predecessors[link.head() as usize] = (node, self.ops.predecessor_link(&link));
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
        self.predecessors[node as usize].0
    }

    pub fn graph(&self) -> &Graph {
        self.graph
    }

    pub fn queue(&self) -> &IndexdMinHeap<State<<Ops::Label as super::Label>::Key>> {
        self.queue
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

pub struct ComplexDijkstraRun<'a, Graph = OwnedGraph, Ops = DefaultOps>
where
    Ops: ComplexDijkstraOps<Graph>,
{
    graph: &'a Graph,

    distances: &'a mut TimestampedVector<Ops::Label>,
    predecessors: &'a mut [(NodeId, Ops::PredecessorLink)],
    queue: &'a mut IndexdMinHeap<State<<Ops::Label as super::Label>::Key>>,

    ops: &'a mut Ops,

    num_relaxed_arcs: usize,
    num_queue_pushs: usize,
}

impl<'b, Graph, Ops> ComplexDijkstraRun<'b, Graph, Ops>
where
    Graph: LinkIterable<Ops::Arc>,
    Ops: ComplexDijkstraOps<Graph>,
{
    pub fn query<P, O>(
        graph: &'b Graph,
        data: &'b mut DijkstraData<Ops::Label, Ops::PredecessorLink>,
        ops: &'b mut Ops,
        query: impl GenQuery<Ops::Label>,
        potential: P,
    ) -> Self
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
    {
        let mut s = Self {
            graph,
            ops,
            predecessors: &mut data.predecessors,
            queue: &mut data.queue,
            distances: &mut data.distances,
            num_relaxed_arcs: 0,
            num_queue_pushs: 0,
        };
        s.initialize(query, potential);
        s
    }

    pub fn continue_query(graph: &'b Graph, data: &'b mut DijkstraData<Ops::Label, Ops::PredecessorLink>, ops: &'b mut Ops, node: NodeId) -> Self {
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

    pub fn initialize<P, O>(&mut self, query: impl GenQuery<Ops::Label>, potential: P)
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
    {
        self.queue.clear();
        self.distances.reset();
        self.add_start_node(query, potential);
    }

    pub fn add_start_node<P, O>(&mut self, query: impl GenQuery<Ops::Label>, mut potential: P)
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
    {
        let from = query.from();
        let init = query.initial_state();
        if let Some(key) = potential(from).map(|p| p + init.key()) {
            self.queue.push(State { key, node: from });
        }
        self.distances[from as usize] = init;
        self.predecessors[from as usize].0 = from;
    }

    fn reinit_queue(&mut self, node: NodeId) {
        self.queue.clear();
        self.queue.push(State {
            key: self.distances[node as usize].key(),
            node,
        });
    }

    #[inline(always)]
    pub fn next_filtered_edges(&mut self, edge_predicate: impl FnMut(&Ops::Arc) -> bool) -> Option<NodeId>
    where
        <Ops::Label as super::Label>::Key: std::ops::Sub<Neutral, Output = <Ops::Label as super::Label>::Key> + Copy,
    {
        self.settle_next_node(edge_predicate, |_, _| true, |_| Some(Neutral()))
    }

    #[inline(always)]
    pub fn next_step_with_potential<P, O>(&mut self, potential: P) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
        <Ops::Label as super::Label>::Key: std::ops::Sub<O, Output = <Ops::Label as super::Label>::Key> + Copy,
    {
        self.settle_next_node(|_| true, |_, _| true, potential)
    }

    #[inline(always)]
    pub fn next_with_improve_callback(&mut self, improve_callback: impl FnMut(NodeId, &Ops::Label) -> bool) -> Option<NodeId>
    where
        <Ops::Label as super::Label>::Key: std::ops::Sub<Neutral, Output = <Ops::Label as super::Label>::Key> + Copy,
    {
        self.settle_next_node(|_| true, improve_callback, |_| Some(Neutral()))
    }

    #[inline(always)]
    pub fn next_with_improve_callback_and_potential<P, O>(&mut self, improve_callback: impl FnMut(NodeId, &Ops::Label) -> bool, potential: P) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
        <Ops::Label as super::Label>::Key: std::ops::Sub<O, Output = <Ops::Label as super::Label>::Key> + Copy,
    {
        self.settle_next_node(|_| true, improve_callback, potential)
    }

    #[inline(always)]
    fn settle_next_node<I, P, O>(&mut self, mut edge_predicate: impl FnMut(&Ops::Arc) -> bool, mut improve_callback: I, mut potential: P) -> Option<NodeId>
    where
        I: FnMut(NodeId, &Ops::Label) -> bool,
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
        <Ops::Label as super::Label>::Key: std::ops::Sub<O, Output = <Ops::Label as super::Label>::Key> + Copy,
    {
        self.queue.pop().map(|State { node, key }| {
            for link in self.graph.link_iter(node) {
                if edge_predicate(&link) {
                    self.num_relaxed_arcs += 1;
                    let linked = self.ops.link(
                        self.graph,
                        self.distances,
                        self.predecessors,
                        NodeIdT(node),
                        key - potential(node).unwrap(),
                        &self.distances[node as usize],
                        &link,
                    );

                    if let Some(improved) = self.ops.merge(&mut self.distances[link.head() as usize], linked) {
                        self.predecessors[link.head() as usize] = (node, self.ops.predecessor_link(&link));
                        let next_distance = &self.distances[link.head() as usize];

                        if improve_callback(link.head(), next_distance) {
                            if let Some(key) = potential(link.head()).map(|p| p + improved) {
                                let next = State { key, node: link.head() };
                                if let Some(prev) = self.queue.get(next.as_index()) {
                                    if next < *prev {
                                        self.queue.decrease_key(next);
                                    }
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
        self.predecessors[node as usize].0
    }

    pub fn graph(&self) -> &Graph {
        self.graph
    }

    pub fn queue(&self) -> &IndexdMinHeap<State<<Ops::Label as super::Label>::Key>> {
        self.queue
    }

    pub fn num_relaxed_arcs(&self) -> usize {
        self.num_relaxed_arcs
    }

    pub fn num_queue_pushs(&self) -> usize {
        self.num_queue_pushs
    }
}

impl<'b, Ops, Graph> Iterator for ComplexDijkstraRun<'b, Graph, Ops>
where
    Ops: ComplexDijkstraOps<Graph>,
    <Ops::Label as super::Label>::Key: std::ops::Sub<Neutral, Output = <Ops::Label as super::Label>::Key> + Copy,
    Graph: LinkIterable<Ops::Arc>,
{
    type Item = NodeId;

    #[inline]
    fn next(&mut self) -> Option<NodeId> {
        self.settle_next_node(|_| true, |_, _| true, |_| Some(Neutral()))
    }
}

#[derive(Clone)]
pub struct NodeQueueLabelOrder<L>(pub L);
pub type MultiCritNodeData<L> = crate::datastr::heap::Heap<NodeQueueLabelOrder<L>>;

pub struct MultiCritDijkstraRun<'a, Graph = OwnedGraph, Ops = DefaultOps>
where
    Ops: MultiCritDijkstraOps<Graph>,
{
    graph: &'a Graph,

    distances: &'a mut TimestampedVector<MultiCritNodeData<Ops::Label>>,
    predecessors: &'a mut [(NodeId, Ops::PredecessorLink)],
    queue: &'a mut IndexdMinHeap<State<<Ops::Label as super::Label>::Key>>,

    ops: &'a mut Ops,

    num_relaxed_arcs: usize,
    num_queue_pushs: usize,
}

impl<'b, Graph, Ops> MultiCritDijkstraRun<'b, Graph, Ops>
where
    Graph: LinkIterable<Ops::Arc>,
    Ops: MultiCritDijkstraOps<Graph>,
    NodeQueueLabelOrder<Ops::Label>: Ord,
{
    pub fn query<P, O>(
        graph: &'b Graph,
        data: &'b mut DijkstraData<Ops::Label, Ops::PredecessorLink, MultiCritNodeData<Ops::Label>>,
        ops: &'b mut Ops,
        query: impl GenQuery<Ops::Label>,
        potential: P,
    ) -> Self
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
    {
        let mut s = Self {
            graph,
            ops,
            predecessors: &mut data.predecessors,
            queue: &mut data.queue,
            distances: &mut data.distances,
            num_relaxed_arcs: 0,
            num_queue_pushs: 0,
        };
        s.initialize(query, potential);
        s
    }

    pub fn continue_query(
        graph: &'b Graph,
        data: &'b mut DijkstraData<Ops::Label, Ops::PredecessorLink, MultiCritNodeData<Ops::Label>>,
        ops: &'b mut Ops,
        node: NodeId,
    ) -> Self {
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

    pub fn initialize<P, O>(&mut self, query: impl GenQuery<Ops::Label>, potential: P)
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
    {
        self.queue.clear();
        self.distances.reset();
        self.add_start_node(query, potential);
    }

    pub fn add_start_node<P, O>(&mut self, query: impl GenQuery<Ops::Label>, mut potential: P)
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
    {
        let from = query.from();
        let init = query.initial_state();
        if let Some(key) = potential(from).map(|p| p + init.key()) {
            self.queue.push(State { key, node: from });
        }
        let mut node_queue = crate::datastr::heap::Heap::new();
        node_queue.push(NodeQueueLabelOrder(init));
        self.distances[from as usize] = node_queue;
        self.predecessors[from as usize].0 = from;
    }

    fn reinit_queue(&mut self, node: NodeId) {
        self.queue.clear();
        self.queue.push(State {
            key: self.distances[node as usize].peek().unwrap().0.key(),
            node,
        });
    }

    #[inline(always)]
    pub fn next_filtered_edges(&mut self, edge_predicate: impl FnMut(&Ops::Arc) -> bool) -> Option<NodeId>
    where
        <Ops::Label as super::Label>::Key: std::ops::Sub<Neutral, Output = <Ops::Label as super::Label>::Key> + Copy,
    {
        self.settle_next_node(edge_predicate, |_, _| true, |_| Some(Neutral()))
    }

    #[inline(always)]
    pub fn next_step_with_potential<P, O>(&mut self, potential: P) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
        <Ops::Label as super::Label>::Key: std::ops::Sub<O, Output = <Ops::Label as super::Label>::Key> + Copy,
    {
        self.settle_next_node(|_| true, |_, _| true, potential)
    }

    #[inline(always)]
    pub fn next_with_improve_callback(&mut self, improve_callback: impl FnMut(NodeId, &MultiCritNodeData<Ops::Label>) -> bool) -> Option<NodeId>
    where
        <Ops::Label as super::Label>::Key: std::ops::Sub<Neutral, Output = <Ops::Label as super::Label>::Key> + Copy,
    {
        self.settle_next_node(|_| true, improve_callback, |_| Some(Neutral()))
    }

    #[inline(always)]
    pub fn next_with_improve_callback_and_potential<P, O>(
        &mut self,
        improve_callback: impl FnMut(NodeId, &MultiCritNodeData<Ops::Label>) -> bool,
        potential: P,
    ) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
        <Ops::Label as super::Label>::Key: std::ops::Sub<O, Output = <Ops::Label as super::Label>::Key> + Copy,
    {
        self.settle_next_node(|_| true, improve_callback, potential)
    }

    #[inline(always)]
    fn settle_next_node<I, P, O>(&mut self, mut edge_predicate: impl FnMut(&Ops::Arc) -> bool, mut improve_callback: I, mut potential: P) -> Option<NodeId>
    where
        I: FnMut(NodeId, &MultiCritNodeData<Ops::Label>) -> bool,
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
        <Ops::Label as super::Label>::Key: std::ops::Sub<O, Output = <Ops::Label as super::Label>::Key> + Copy,
    {
        self.queue.pop().map(|State { node, key }| {
            let mut node_labels = MultiCritNodeData::<Ops::Label>::new();
            std::mem::swap(&mut self.distances[node as usize], &mut node_labels);
            let _ = node_labels.pop().unwrap();
            let NodeQueueLabelOrder(label) = &node_labels.popped()[0];
            if let Some(NodeQueueLabelOrder(next_label)) = node_labels.peek() {
                self.queue.push(State {
                    node,
                    key: potential(node).unwrap() + next_label.key(),
                })
            }

            for link in self.graph.link_iter(node) {
                if edge_predicate(&link) {
                    self.num_relaxed_arcs += 1;
                    let linked = self.ops.link(
                        self.graph,
                        self.distances,
                        self.predecessors,
                        NodeIdT(node),
                        key - potential(node).unwrap(),
                        &label,
                        &link,
                    );

                    if let Some(improved) = self.ops.merge(&mut self.distances[link.head() as usize], linked) {
                        self.predecessors[link.head() as usize] = (node, self.ops.predecessor_link(&link));
                        let next_distance = &self.distances[link.head() as usize];

                        if improve_callback(link.head(), next_distance) {
                            if let Some(key) = potential(link.head()).map(|p| p + improved) {
                                let next = State { key, node: link.head() };
                                if let Some(prev) = self.queue.get(next.as_index()) {
                                    if next < *prev {
                                        self.queue.decrease_key(next);
                                    }
                                } else {
                                    self.num_queue_pushs += 1;
                                    self.queue.push(next);
                                }
                            }
                        }
                    }
                }
            }

            std::mem::swap(&mut self.distances[node as usize], &mut node_labels);

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
                key: potential(node).map(|p| p + self.distances[node as usize].peek().unwrap().0.key()).unwrap(),
            })
        }
    }

    pub fn tentative_distance(&self, node: NodeId) -> &MultiCritNodeData<Ops::Label> {
        &self.distances[node as usize]
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.predecessors[node as usize].0
    }

    pub fn graph(&self) -> &Graph {
        self.graph
    }

    pub fn queue(&self) -> &IndexdMinHeap<State<<Ops::Label as super::Label>::Key>> {
        self.queue
    }

    pub fn num_relaxed_arcs(&self) -> usize {
        self.num_relaxed_arcs
    }

    pub fn num_queue_pushs(&self) -> usize {
        self.num_queue_pushs
    }
}

impl<'b, Ops, Graph> Iterator for MultiCritDijkstraRun<'b, Graph, Ops>
where
    Ops: MultiCritDijkstraOps<Graph>,
    <Ops::Label as super::Label>::Key: std::ops::Sub<Neutral, Output = <Ops::Label as super::Label>::Key> + Copy,
    Graph: LinkIterable<Ops::Arc>,
    NodeQueueLabelOrder<Ops::Label>: Ord,
{
    type Item = NodeId;

    #[inline]
    fn next(&mut self) -> Option<NodeId> {
        self.settle_next_node(|_| true, |_, _| true, |_| Some(Neutral()))
    }
}
