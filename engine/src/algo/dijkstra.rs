//! Several variants of Dijkstra

use super::*;
use crate::datastr::{index_heap::*, timestamped_vector::*};

pub mod gen_topo_dijkstra;
pub mod generic_dijkstra;
pub mod query;

pub use generic_dijkstra::{DefaultOps, DijkstraOps, DijkstraRun};
pub use query::dijkstra::Server;

/// Result of a single iteration
#[derive(Debug, Clone)]
pub enum QueryProgress<W> {
    Settled(State<W>),
    Done(Option<W>),
}

/// Priority Queue entries
#[derive(Copy, Clone, Eq, PartialEq, Debug)]
pub struct State<W> {
    pub key: W,
    pub node: NodeId,
}

// slightly optimized version of derived
impl<W: std::cmp::PartialOrd> std::cmp::PartialOrd for State<W> {
    #[inline]
    fn partial_cmp(&self, rhs: &Self) -> Option<std::cmp::Ordering> {
        self.key.partial_cmp(&rhs.key)
    }
}

// slightly optimized version of derived
impl<W: std::cmp::Ord> std::cmp::Ord for State<W> {
    #[inline]
    fn cmp(&self, rhs: &Self) -> std::cmp::Ordering {
        self.key.cmp(&rhs.key)
    }
}

impl<W> Indexing for State<W> {
    #[inline]
    fn as_index(&self) -> usize {
        self.node as usize
    }
}

pub trait Label: Clone {
    type Key: Ord;
    fn neutral() -> Self;
    fn key(&self) -> Self::Key;
}

impl Label for Weight {
    type Key = Self;

    fn neutral() -> Self {
        INFINITY
    }

    #[inline(always)]
    fn key(&self) -> Self::Key {
        *self
    }
}

#[derive(Clone)]
pub struct DijkstraData<L: Label> {
    pub distances: TimestampedVector<L>,
    pub predecessors: Vec<NodeId>,
    pub queue: IndexdMinHeap<State<L::Key>>,
}

impl<L: Label> DijkstraData<L> {
    pub fn new(n: usize) -> Self {
        Self {
            distances: TimestampedVector::new(n, L::neutral()),
            predecessors: vec![n as NodeId; n],
            queue: IndexdMinHeap::new(n),
        }
    }
}
