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

pub struct GenericDijkstra<Label, Ops, Graph> {
    graph: Graph,

    distances: TimestampedVector<Label>,
    predecessors: Vec<NodeId>,
    queue: IndexdMinHeap<State<Label>>,

    ops: Ops,
}

impl<Label, Ops, Graph> GenericDijkstra<Label, Ops, Graph>
where
    Ops: DijkstraOps<Label, Graph> + Default,
    Graph: for<'a> LinkIterable<'a, Ops::Arc>,
    Label: Clone + Ord + super::Label,
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

    pub fn initialize_query(&mut self, query: impl GenQuery<Label>) {
        let from = query.from();
        // initialize
        self.queue.clear();
        self.distances.reset();
        self.distances[from as usize] = query.initial_state();

        self.queue.push(State {
            distance: query.initial_state(),
            node: from,
        });
    }

    pub fn next(&mut self) -> Option<NodeId> {
        self.settle_next_node(|_| true)
    }

    pub fn next_filtered_edges(&mut self, edge_predicate: impl FnMut(&Ops::Arc) -> bool) -> Option<NodeId> {
        self.settle_next_node(edge_predicate)
    }

    fn settle_next_node(&mut self, mut edge_predicate: impl FnMut(&Ops::Arc) -> bool) -> Option<NodeId> {
        self.queue.pop().map(|State { node, distance }| {
            for link in self.graph.link_iter(node) {
                if edge_predicate(&link) {
                    let linked = self.ops.link(&self.graph, &distance, &link);

                    if self.ops.merge(&mut self.distances[link.head() as usize], linked) {
                        self.predecessors[link.head() as usize] = node;

                        let next = State {
                            distance: self.distances[link.head() as usize].clone(),
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

    pub fn queue(&self) -> &IndexdMinHeap<State<Label>> {
        &self.queue
    }
}
