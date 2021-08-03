//! Dijkstras algorithm with optimization for degree 2 chains

use super::*;

pub struct TopoDijkstraRun<'a, Graph, Ops, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool>
where
    Ops: DijkstraOps<Graph>,
{
    graph: &'a Graph,

    distances: &'a mut TimestampedVector<Ops::Label>,
    predecessors: &'a mut [(NodeId, Ops::PredecessorLink)],
    queue: &'a mut IndexdMinHeap<State<<Ops::Label as super::Label>::Key>>,

    num_relaxed_arcs: usize,
    num_queue_pushs: usize,

    ops: &'a mut Ops,
}

impl<'b, Graph, Ops, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool> TopoDijkstraRun<'b, Graph, Ops, SKIP_DEG_2, SKIP_DEG_3>
where
    Ops: DijkstraOps<Graph>,
    Graph: LinkIterable<NodeIdT> + LinkIterable<Ops::Arc> + SymmetricDegreeGraph,
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

    fn initialize(&mut self, query: impl GenQuery<Ops::Label>) {
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
    pub fn next_step(&mut self) -> Option<NodeId> {
        self.settle_next_node(|_| Some(Neutral()), |_| (), |_, _| true)
    }

    #[inline(always)]
    pub fn next_step_with_potential<P, O>(&mut self, potential: P) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
    {
        self.settle_next_node(potential, |_| (), |_, _| true)
    }

    #[inline(always)]
    pub fn next_step_with_potential_and_edge_callback<P, O>(&mut self, potential: P, edge_callback: impl FnMut(&Ops::Arc)) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
    {
        self.settle_next_node(potential, edge_callback, |_, _| true)
    }

    #[inline(always)]
    pub fn next_with_improve_callback_and_potential<P, O>(&mut self, improve_callback: impl FnMut(NodeId, &Ops::Label) -> bool, potential: P) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
    {
        self.settle_next_node(potential, |_| (), improve_callback)
    }

    #[inline(always)]
    fn settle_next_node<P, O, I>(&mut self, mut potential: P, mut edge_callback: impl FnMut(&Ops::Arc), mut improve_callback: I) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<O>,
        O: std::ops::Add<<Ops::Label as super::Label>::Key, Output = <Ops::Label as super::Label>::Key>,
        I: FnMut(NodeId, &Ops::Label) -> bool,
    {
        self.queue.pop().map(|State { node, .. }| {
            for edge in LinkIterable::<Ops::Arc>::link_iter(self.graph, node) {
                edge_callback(&edge);
                let mut chain = Some(ChainStep {
                    prev_node: (node, self.ops.predecessor_link(&edge)),
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

                    if self.ops.merge(&mut self.distances[next_node as usize], next_distance) {
                        let next_distance = &self.distances[next_node as usize];
                        let mut next_edge = None;
                        let mut endpoint = false;
                        // debug_assert!(next_distance >= distance);

                        match self.graph.symmetric_degree(next_node) {
                            SymmetricDeg::LessEqTwo => {
                                if !SKIP_DEG_2 {
                                    endpoint = true;
                                } else {
                                    for edge in LinkIterable::<Ops::Arc>::link_iter(self.graph, next_node) {
                                        if edge.head() != prev_node.0 {
                                            next_edge = Some(edge);
                                        }
                                    }
                                }
                            }
                            SymmetricDeg::Three => {
                                if !SKIP_DEG_3
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
                                    for edge in LinkIterable::<Ops::Arc>::link_iter(self.graph, next_node) {
                                        if edge.head() != prev_node.0 {
                                            if next_edge.is_none() {
                                                next_edge = Some(edge);
                                            } else {
                                                deg_three = Some((next_node, edge));
                                            }
                                        }
                                    }
                                }
                            }
                            SymmetricDeg::GreaterEqFour => {
                                endpoint = true;
                            }
                        }

                        self.predecessors[next_node as usize] = prev_node;

                        if let Some(next_edge) = next_edge {
                            edge_callback(&next_edge);
                            chain = Some(ChainStep {
                                prev_node: (next_node, self.ops.predecessor_link(&next_edge)),
                                next_node: next_edge.head(),
                                next_distance: self.ops.link(&self.graph, next_distance, &next_edge),
                            });
                        } else if endpoint {
                            if improve_callback(next_node, next_distance) {
                                if let Some(key) = potential(next_node).map(|p| p + next_distance.key()) {
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
                    }

                    if chain.is_none() {
                        if let Some((deg_three_node, edge)) = deg_three.take() {
                            edge_callback(&edge);
                            chain = Some(ChainStep {
                                prev_node: (deg_three_node, self.ops.predecessor_link(&edge)),
                                next_distance: self.ops.link(&self.graph, &self.distances[deg_three_node as usize], &edge),
                                next_node: edge.head(),
                            });
                        }
                    }
                }
            }

            node
        })
    }

    pub fn graph(&self) -> &Graph {
        self.graph
    }

    pub fn tentative_distance(&self, node: NodeId) -> &Ops::Label {
        &self.distances[node as usize]
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.predecessors[node as usize].0
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

#[derive(Debug)]
struct ChainStep<LinkResult, PredecessorLink> {
    prev_node: (NodeId, PredecessorLink),
    next_node: NodeId,
    next_distance: LinkResult,
}

pub type StandardTopoDijkstra = DijkstraData<Weight>;

pub struct Neutral();

impl<T> std::ops::Add<T> for Neutral {
    type Output = T;

    fn add(self, rhs: T) -> Self::Output {
        rhs
    }
}

#[derive(Debug)]
pub enum SymmetricDeg {
    LessEqTwo,
    Three,
    GreaterEqFour,
}

pub trait SymmetricDegreeGraph {
    fn symmetric_degree(&self, node: NodeId) -> SymmetricDeg;
}

pub struct SendTopoDijkstraRun<'a, Graph, Ops, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool>
where
    Ops: DijkstraOps<Graph, Label = Weight>,
{
    graph: &'a Graph,

    distances: &'a AtomicDists,
    predecessors: &'a mut [(NodeId, Ops::PredecessorLink)],
    queue: &'a mut IndexdMinHeap<State<Weight>>,

    num_relaxed_arcs: usize,
    num_queue_pushs: usize,

    ops: &'a mut Ops,
}

impl<'b, Graph, Ops, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool> SendTopoDijkstraRun<'b, Graph, Ops, SKIP_DEG_2, SKIP_DEG_3>
where
    Ops: DijkstraOps<Graph, Label = Weight, LinkResult = Weight>,
    Graph: LinkIterable<NodeIdT> + LinkIterable<Ops::Arc> + SymmetricDegreeGraph,
{
    pub fn query(
        graph: &'b Graph,
        distances: &'b AtomicDists,
        predecessors: &'b mut [(NodeId, Ops::PredecessorLink)],
        queue: &'b mut IndexdMinHeap<State<Weight>>,
        ops: &'b mut Ops,
        query: impl GenQuery<Ops::Label>,
    ) -> Self {
        let mut s = Self {
            graph,
            ops,
            predecessors,
            queue,
            distances,
            num_relaxed_arcs: 0,
            num_queue_pushs: 0,
        };
        s.initialize(query);
        s
    }

    // pub fn continue_query(graph: &'b Graph, data: &'b mut SyncDijkstraData, ops: &'b mut Ops, node: NodeId) -> Self {
    //     let mut s = Self {
    //         graph,
    //         ops,
    //         predecessors: &mut data.predecessors,
    //         queue: &mut data.queue,
    //         distances: &mut data.distances,
    //         num_relaxed_arcs: 0,
    //         num_queue_pushs: 0,
    //     };
    //     s.reinit_queue(node);
    //     s
    // }

    fn initialize(&mut self, query: impl GenQuery<Ops::Label>) {
        self.queue.clear();
        // self.distances.reset();
        self.add_start_node(query);
    }

    pub fn add_start_node(&mut self, query: impl GenQuery<Ops::Label>) {
        let from = query.from();
        let init = query.initial_state();
        self.queue.push(State { key: init.key(), node: from });
        self.distances.set(from as usize, init);
        self.predecessors[from as usize].0 = from;
    }

    #[inline(always)]
    pub fn next_step(&mut self) -> Option<NodeId> {
        self.settle_next_node(|_| Some(0), |_| (), |_, _| true)
    }

    #[inline(always)]
    pub fn next_step_with_potential<P>(&mut self, potential: P) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<Weight>,
    {
        self.settle_next_node(potential, |_| (), |_, _| true)
    }

    #[inline(always)]
    pub fn next_step_with_potential_and_edge_callback<P>(&mut self, potential: P, edge_callback: impl FnMut(&Ops::Arc)) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<Weight>,
    {
        self.settle_next_node(potential, edge_callback, |_, _| true)
    }

    #[inline(always)]
    pub fn next_with_improve_callback_and_potential<P>(&mut self, improve_callback: impl FnMut(NodeId, &Ops::Label) -> bool, potential: P) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<Weight>,
    {
        self.settle_next_node(potential, |_| (), improve_callback)
    }

    #[inline(always)]
    fn settle_next_node<P, I>(&mut self, mut potential: P, mut edge_callback: impl FnMut(&Ops::Arc), mut improve_callback: I) -> Option<NodeId>
    where
        P: FnMut(NodeId) -> Option<Weight>,
        I: FnMut(NodeId, &Ops::Label) -> bool,
    {
        self.queue.pop().map(|State { node, .. }| {
            for edge in LinkIterable::<Ops::Arc>::link_iter(self.graph, node) {
                edge_callback(&edge);
                let mut chain = Some(ChainStep {
                    prev_node: (node, self.ops.predecessor_link(&edge)),
                    next_node: edge.head(),
                    next_distance: self.ops.link(&self.graph, &self.distances.get(node as usize), &edge),
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

                    let mut next_distance_tmp = self.distances.get(next_node as usize);
                    if self.ops.merge(&mut next_distance_tmp, next_distance) {
                        self.distances.set(next_node as usize, next_distance_tmp);
                        let mut next_edge = None;
                        let mut endpoint = false;
                        // debug_assert!(next_distance >= distance);

                        match self.graph.symmetric_degree(next_node) {
                            SymmetricDeg::LessEqTwo => {
                                if !SKIP_DEG_2 {
                                    endpoint = true;
                                } else {
                                    for edge in LinkIterable::<Ops::Arc>::link_iter(self.graph, next_node) {
                                        if edge.head() != prev_node.0 {
                                            next_edge = Some(edge);
                                        }
                                    }
                                }
                            }
                            SymmetricDeg::Three => {
                                if !SKIP_DEG_3
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
                                    for edge in LinkIterable::<Ops::Arc>::link_iter(self.graph, next_node) {
                                        if edge.head() != prev_node.0 {
                                            if next_edge.is_none() {
                                                next_edge = Some(edge);
                                            } else {
                                                deg_three = Some((next_node, edge));
                                            }
                                        }
                                    }
                                }
                            }
                            SymmetricDeg::GreaterEqFour => {
                                endpoint = true;
                            }
                        }

                        self.predecessors[next_node as usize] = prev_node;

                        if let Some(next_edge) = next_edge {
                            edge_callback(&next_edge);
                            chain = Some(ChainStep {
                                prev_node: (next_node, self.ops.predecessor_link(&next_edge)),
                                next_node: next_edge.head(),
                                next_distance: self.ops.link(&self.graph, &next_distance, &next_edge),
                            });
                        } else if endpoint {
                            if improve_callback(next_node, &next_distance) {
                                if let Some(key) = potential(next_node).map(|p| p + next_distance.key()) {
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
                    }

                    if chain.is_none() {
                        if let Some((deg_three_node, edge)) = deg_three.take() {
                            edge_callback(&edge);
                            chain = Some(ChainStep {
                                prev_node: (deg_three_node, self.ops.predecessor_link(&edge)),
                                next_distance: self.ops.link(&self.graph, &self.distances.get(deg_three_node as usize), &edge),
                                next_node: edge.head(),
                            });
                        }
                    }
                }
            }

            node
        })
    }

    pub fn tentative_distance(&self, node: NodeId) -> Weight {
        self.distances.get(node as usize)
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.predecessors[node as usize].0
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
