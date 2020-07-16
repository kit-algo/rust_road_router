//! Basic variant of dijkstras algorithm

use super::*;
use crate::datastr::{index_heap::*, timestamped_vector::*};

pub trait Lnk {
    fn head(&self) -> NodeId;
}

impl Lnk for Link {
    fn head(&self) -> NodeId {
        self.node
    }
}

pub trait Labl {
    fn neutral() -> Self;
}

impl Labl for Weight {
    fn neutral() -> Self {
        INFINITY
    }
}

pub trait DijkstraOps<Link, Label> {
    type LinkResult;
    fn link(&mut self, label: &Label, link: &Link) -> Self::LinkResult;
    fn merge(&mut self, label: &mut Label, linked: Self::LinkResult) -> bool;
}

pub struct DefaultOps();

impl DijkstraOps<Link, Weight> for DefaultOps {
    type LinkResult = Weight;
    fn link(&mut self, label: &Weight, link: &Link) -> Self::LinkResult {
        label + link.weight
    }
    fn merge(&mut self, label: &mut Weight, linked: Self::LinkResult) -> bool {
        // let min = std::cmp::min(*label, linked);
        // std::mem::swap(label, &mut min);
        // min != label

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

pub struct GenericDijkstra<Link, Graph, Label, Ops> {
    graph: Graph,

    settled_target: bool,
    to: NodeId,

    distances: TimestampedVector<Label>,
    predecessors: Vec<NodeId>,
    queue: IndexdMinHeap<State<Label>>,

    ops: Ops,
    _phantom: std::marker::PhantomData<Link>,
}

impl<Link, Graph, Label, Ops> GenericDijkstra<Link, Graph, Label, Ops>
where
    Label: Clone + Ord + Labl,
    Graph: for<'a> TemplLinkIterGraph<'a, Link>,
    Link: Lnk,
    Ops: DijkstraOps<Link, Label> + Default,
{
    pub fn new(graph: Graph) -> Self {
        let n = graph.num_nodes();

        GenericDijkstra {
            graph,

            settled_target: true,
            to: 0,

            distances: TimestampedVector::new(n, Label::neutral()),
            predecessors: vec![n as NodeId; n],
            queue: IndexdMinHeap::new(n),

            ops: Default::default(),
            _phantom: std::marker::PhantomData {},
        }
    }

    pub fn initialize_query(&mut self, query: impl GenQuery<Label>) {
        let from = query.from();
        // initialize
        self.to = query.to();
        self.settled_target = false;
        self.queue.clear();
        self.distances.reset();
        self.distances[from as usize] = query.initial_state();

        self.queue.push(State {
            distance: query.initial_state(),
            node: from,
        });
    }

    pub fn next_step(&mut self) -> Option<NodeId> {
        if self.settled_target {
            None
        } else {
            self.settle_next_node()
        }
    }

    fn settle_next_node(&mut self) -> Option<NodeId> {
        if let Some(State { node, distance }) = self.queue.pop() {
            if node == self.to {
                self.settled_target = true;
            }

            for link in self.graph.link_iter(node) {
                let linked = self.ops.link(&distance, &link);

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

            Some(node)
        } else {
            None
        }
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
