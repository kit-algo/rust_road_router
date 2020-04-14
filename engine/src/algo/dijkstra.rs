//! Several variants of Dijkstra

use super::*;
use crate::datastr::index_heap::Indexing;

pub mod floating_td_stepped_dijkstra;
pub mod multicrit_dijkstra;
pub mod query;
pub mod stepped_dijkstra;
pub mod td_stepped_dijkstra;
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
    pub distance: W,
    pub node: NodeId,
}

// slightly optimized version of derived
impl<W: std::cmp::PartialOrd> std::cmp::PartialOrd for State<W> {
    #[inline]
    fn partial_cmp(&self, rhs: &Self) -> Option<std::cmp::Ordering> {
        self.distance.partial_cmp(&rhs.distance)
    }
}

// slightly optimized version of derived
impl<W: std::cmp::Ord> std::cmp::Ord for State<W> {
    #[inline]
    fn cmp(&self, rhs: &Self) -> std::cmp::Ordering {
        self.distance.cmp(&rhs.distance)
    }
}

impl<W> Indexing for State<W> {
    #[inline]
    fn as_index(&self) -> usize {
        self.node as usize
    }
}
