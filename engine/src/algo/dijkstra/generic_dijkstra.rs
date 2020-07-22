//! Basic variant of dijkstras algorithm

use super::*;
use crate::datastr::{index_heap::*, timestamped_vector::*};

pub trait DijkstraOps<Label, Graph> {
    type Arc;
    type LinkResult;

    fn link(&mut self, graph: &Graph, label: &Label, link: &Self::Arc) -> Self::LinkResult;
    fn merge(&mut self, label: &mut Label, linked: Self::LinkResult) -> bool;
}

pub struct DefaultOps();

impl<G> DijkstraOps<Weight, G> for DefaultOps {
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

pub struct GenericDijkstra<Label: super::Label, Ops, Graph> {
    graph: Graph,

    distances: TimestampedVector<Label>,
    predecessors: Vec<NodeId>,
    queue: IndexdMinHeap<State<Label::Key>>,

    ops: Ops,
}

impl<Label, Ops, Graph> GenericDijkstra<Label, Ops, Graph>
where
    Ops: DijkstraOps<Label, Graph> + Default,
    Graph: for<'a> LinkIterable<'a, Ops::Arc>,
    Label: Clone + super::Label,
    Label::Key: Ord,
    Ops::Arc: Arc,
{
    pub fn new(graph: Graph) -> Self {
        let n = graph.num_nodes();

        GenericDijkstra {
            graph,

            distances: TimestampedVector::new(n, Label::neutral()),
            predecessors: vec![n as NodeId; n],
            queue: IndexdMinHeap::new(n),

            ops: Default::default(),
        }
    }

    /// For CH preprocessing we reuse the distance array and the queue to reduce allocations.
    /// This method creates an algo struct from recycled data.
    /// The counterpart is the `recycle` method.
    pub fn from_recycled(graph: Graph, recycled: Trash<Label>) -> Self {
        let n = graph.num_nodes();
        assert!(recycled.distances.len() >= n);
        assert!(recycled.predecessors.len() >= n);

        Self {
            graph,
            distances: recycled.distances,
            predecessors: recycled.predecessors,
            queue: recycled.queue,
            ops: Default::default(),
        }
    }

    pub fn initialize_query(&mut self, query: impl GenQuery<Label>) {
        let from = query.from();
        // initialize
        self.queue.clear();
        let init = query.initial_state();
        self.queue.push(State { key: init.key(), node: from });

        self.distances.reset();
        self.distances[from as usize] = init;
    }

    #[inline]
    pub fn next_filtered_edges(&mut self, edge_predicate: impl FnMut(&Ops::Arc) -> bool) -> Option<NodeId> {
        self.settle_next_node(edge_predicate)
    }

    #[inline]
    fn settle_next_node(&mut self, mut edge_predicate: impl FnMut(&Ops::Arc) -> bool) -> Option<NodeId> {
        self.queue.pop().map(|State { node, .. }| {
            for link in self.graph.link_iter(node) {
                if edge_predicate(&link) {
                    let linked = self.ops.link(&self.graph, &self.distances[node as usize], &link);

                    if self.ops.merge(&mut self.distances[link.head() as usize], linked) {
                        self.predecessors[link.head() as usize] = node;

                        let next = State {
                            key: self.distances[link.head() as usize].key(),
                            node: link.head(),
                        };
                        if self.queue.contains_index(next.as_index()) {
                            self.queue.decrease_key(next);
                        } else {
                            self.queue.push(next);
                        }
                    }
                }
            }

            node
        })
    }

    pub fn tentative_distance(&self, node: NodeId) -> &Label {
        &self.distances[node as usize]
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.predecessors[node as usize]
    }

    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    pub fn queue(&self) -> &IndexdMinHeap<State<Label::Key>> {
        &self.queue
    }

    /// For CH preprocessing we reuse the distance array and the queue to reduce allocations.
    /// This method decomposes this algo struct for later reuse.
    /// The counterpart is `from_recycled`
    pub fn recycle(self) -> Trash<Label> {
        Trash {
            distances: self.distances,
            predecessors: self.predecessors,
            queue: self.queue,
        }
    }
}

impl<Label, Ops, Graph> Iterator for GenericDijkstra<Label, Ops, Graph>
where
    Ops: DijkstraOps<Label, Graph> + Default,
    Graph: for<'a> LinkIterable<'a, Ops::Arc>,
    Label: Clone + super::Label,
    Label::Key: Ord,
    Ops::Arc: Arc,
{
    type Item = NodeId;

    #[inline]
    fn next(&mut self) -> Option<NodeId> {
        self.settle_next_node(|_| true)
    }
}

pub type StandardDijkstra<G> = GenericDijkstra<Weight, DefaultOps, G>;

pub struct Trash<Label: super::Label> {
    distances: TimestampedVector<Label>,
    predecessors: Vec<NodeId>,
    queue: IndexdMinHeap<State<Label::Key>>,
}