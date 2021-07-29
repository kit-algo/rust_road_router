//! Several variants of Dijkstra

use super::*;
use crate::datastr::{index_heap::*, timestamped_vector::*};
use crate::report::*;

pub mod gen_topo_dijkstra;
pub mod generic_dijkstra;
pub mod query;

pub use generic_dijkstra::DijkstraRun;
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
pub struct DijkstraData<L: Label, PredLink = ()> {
    pub distances: TimestampedVector<L>,
    pub predecessors: Vec<(NodeId, PredLink)>,
    pub queue: IndexdMinHeap<State<L::Key>>,
}

impl<L: Label, PredLink: Copy> DijkstraData<L, PredLink> {
    pub fn new(n: usize) -> Self
    where
        PredLink: Default,
    {
        Self::new_with_pred_link(n, Default::default())
    }

    pub fn new_with_pred_link(n: usize, pred_link_init: PredLink) -> Self {
        Self {
            distances: TimestampedVector::new(n, L::neutral()),
            predecessors: vec![(n as NodeId, pred_link_init); n],
            queue: IndexdMinHeap::new(n),
        }
    }

    pub fn node_path(&self, from: NodeId, to: NodeId) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(to);

        while *path.last().unwrap() != from {
            let next = self.predecessors[*path.last().unwrap() as usize].0;
            path.push(next);
        }

        path.reverse();

        path
    }

    pub fn edge_path(&self, from: NodeId, to: NodeId) -> Vec<PredLink> {
        let mut path = Vec::new();
        let mut cur = to;

        while cur != from {
            path.push(self.predecessors[cur as usize].1);
            cur = self.predecessors[cur as usize].0;
        }

        path.reverse();

        path
    }
}

pub trait DijkstraOps<Graph> {
    type Label: Label;
    type Arc: Arc;
    type LinkResult;
    type PredecessorLink: Default + Copy;

    fn link(&mut self, graph: &Graph, label: &Self::Label, link: &Self::Arc) -> Self::LinkResult;
    fn merge(&mut self, label: &mut Self::Label, linked: Self::LinkResult) -> bool;
    fn predecessor_link(&self, _link: &Self::Arc) -> Self::PredecessorLink;
}

#[derive(Debug, Clone, Copy)]
pub struct DefaultOps();

impl<G> DijkstraOps<G> for DefaultOps {
    type Label = Weight;
    type Arc = Link;
    type LinkResult = Weight;
    type PredecessorLink = ();

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

    #[inline(always)]
    fn predecessor_link(&self, _link: &Self::Arc) -> Self::PredecessorLink {
        ()
    }
}

impl Default for DefaultOps {
    fn default() -> Self {
        Self()
    }
}

#[derive(Debug, Clone, Copy)]
pub struct DefaultOpsWithLinkPath();

impl<G: EdgeIdGraph> DijkstraOps<G> for DefaultOpsWithLinkPath {
    type Label = Weight;
    type Arc = (NodeIdT, (Weight, EdgeIdT));
    type LinkResult = Weight;
    type PredecessorLink = EdgeIdT;

    #[inline(always)]
    fn link(&mut self, _graph: &G, label: &Weight, (_, (weight, _)): &Self::Arc) -> Self::LinkResult {
        label + weight
    }

    #[inline(always)]
    fn merge(&mut self, label: &mut Weight, linked: Self::LinkResult) -> bool {
        if linked < *label {
            *label = linked;
            return true;
        }
        false
    }

    #[inline(always)]
    fn predecessor_link(&self, &(_, (_, edge_id)): &Self::Arc) -> Self::PredecessorLink {
        edge_id
    }
}

impl Default for DefaultOpsWithLinkPath {
    fn default() -> Self {
        Self()
    }
}

pub trait BidirChooseDir: Default {
    fn choose(&mut self, fw_min_key: Option<Weight>, bw_min_key: Option<Weight>) -> bool;
    fn strategy_key() -> &'static str;
    fn report() {
        report!("choose_direction_strategy", Self::strategy_key());
    }
}

pub struct ChooseMinKeyDir();

impl Default for ChooseMinKeyDir {
    fn default() -> Self {
        Self()
    }
}

impl BidirChooseDir for ChooseMinKeyDir {
    fn choose(&mut self, fw_min_key: Option<Weight>, bw_min_key: Option<Weight>) -> bool {
        match (fw_min_key, bw_min_key) {
            (Some(fw_min_key), Some(bw_min_key)) => fw_min_key <= bw_min_key,
            (None, Some(_)) => false,
            _ => true,
        }
    }
    fn strategy_key() -> &'static str {
        "min_key"
    }
}

pub struct AlternatingDirs {
    prev: bool,
}

impl Default for AlternatingDirs {
    fn default() -> Self {
        Self { prev: false }
    }
}

impl BidirChooseDir for AlternatingDirs {
    fn choose(&mut self, fw_min_key: Option<Weight>, bw_min_key: Option<Weight>) -> bool {
        match (fw_min_key, bw_min_key) {
            (Some(_), Some(_)) => {
                self.prev = !self.prev;
                self.prev
            }
            (None, Some(_)) => false,
            _ => true,
        }
    }
    fn strategy_key() -> &'static str {
        "alternating"
    }
}

pub struct SyncDijkstraData {
    pub distances: AtomicDists,
    pub predecessors: Vec<(NodeId, EdgeIdT)>,
    pub queue: IndexdMinHeap<State<Weight>>,
}

impl SyncDijkstraData {
    pub fn new(n: usize) -> Self {
        Self {
            distances: AtomicDists::new(n),
            predecessors: vec![(n as NodeId, Default::default()); n],
            queue: IndexdMinHeap::new(n),
        }
    }
}
