//! Several variants of Dijkstra

use super::*;
use crate::datastr::index_heap::Indexing;

pub mod generic_dijkstra;
pub mod query;
pub mod td_topo_dijkstra;
pub mod topo_dijkstra;

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

pub trait Label {
    type Key;
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
