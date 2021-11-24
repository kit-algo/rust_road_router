//! Several variants of Dijkstra

use self::generic_dijkstra::MultiCritNodeData;

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
#[derive(Copy, Clone, Eq, PartialEq, Debug, PartialOrd, Ord)]
pub struct State<W> {
    pub key: W,
    pub node: NodeId,
}

impl<W> Indexing for State<W> {
    #[inline]
    fn as_index(&self) -> usize {
        self.node as usize
    }
}

pub trait Label: Reset {
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

pub struct DijkstraInit<L> {
    pub source: NodeIdT,
    pub initial_state: L,
}

impl<L> DijkstraInit<L> {
    pub fn from_query(q: &impl GenQuery<L>) -> Self {
        Self {
            source: NodeIdT(q.from()),
            initial_state: q.initial_state(),
        }
    }
}

impl DijkstraInit<Weight> {
    pub fn from(node: NodeId) -> Self {
        Self {
            source: NodeIdT(node),
            initial_state: 0,
        }
    }
}

#[derive(Clone)]
pub struct DijkstraData<L: Label, PredLink = (), NodeData = L> {
    pub distances: TimestampedVector<NodeData>,
    pub predecessors: Vec<(NodeId, PredLink)>,
    pub queue: IndexdMinHeap<State<L::Key>>,
}

impl<L: Label, PredLink: Copy, NodeData: Reset> DijkstraData<L, PredLink, NodeData> {
    pub fn new(n: usize) -> Self
    where
        PredLink: Default,
    {
        Self::new_with_pred_link(n, Default::default())
    }

    pub fn new_with_pred_link(n: usize, pred_link_init: PredLink) -> Self {
        Self {
            distances: TimestampedVector::new(n),
            predecessors: vec![(n as NodeId, pred_link_init); n],
            queue: IndexdMinHeap::new(n),
        }
    }

    pub fn node_path(&self, from: NodeId, to: NodeId) -> Vec<NodeId> {
        let mut path = vec![to];

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

impl<L: Label, NodeData: Reset> DijkstraData<L, (), NodeData> {
    pub fn predecessors(&self) -> &[NodeId] {
        unsafe { std::mem::transmute(&self.predecessors[..]) }
    }
}

pub trait DijkstraOps<Graph> {
    type Label: Label;
    type Arc: Arc;
    type LinkResult;
    type PredecessorLink: Default + Copy;

    fn link(&mut self, graph: &Graph, parents: &[(NodeId, Self::PredecessorLink)], tail: NodeIdT, label: &Self::Label, link: &Self::Arc) -> Self::LinkResult;
    fn merge(&mut self, label: &mut Self::Label, linked: Self::LinkResult) -> bool;
    fn predecessor_link(&self, link: &Self::Arc) -> Self::PredecessorLink;
}

pub trait MultiCritDijkstraOps<Graph> {
    type Label: Label;
    type Arc: Arc;
    type LinkResult;
    type PredecessorLink: Default + Copy;

    fn link(
        &mut self,
        graph: &Graph,
        labels: &TimestampedVector<MultiCritNodeData<Self::Label>>,
        parents: &[(NodeId, Self::PredecessorLink)],
        tail: NodeIdT,
        key: <Self::Label as Label>::Key,
        label: &Self::Label,
        link: &Self::Arc,
    ) -> Self::LinkResult;
    fn merge(&mut self, label: &mut MultiCritNodeData<Self::Label>, linked: Self::LinkResult) -> Option<<Self::Label as Label>::Key>;
    fn predecessor_link(&self, _link: &Self::Arc) -> Self::PredecessorLink;
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultOps();

impl<G> DijkstraOps<G> for DefaultOps {
    type Label = Weight;
    type Arc = Link;
    type LinkResult = Weight;
    type PredecessorLink = ();

    #[inline(always)]
    fn link(&mut self, _graph: &G, _parents: &[(NodeId, Self::PredecessorLink)], _tail: NodeIdT, label: &Weight, link: &Link) -> Self::LinkResult {
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
    fn predecessor_link(&self, _link: &Self::Arc) -> Self::PredecessorLink {}
}

#[derive(Debug, Clone, Copy, Default)]
pub struct DefaultOpsWithLinkPath();

impl<G: EdgeIdGraph> DijkstraOps<G> for DefaultOpsWithLinkPath {
    type Label = Weight;
    type Arc = (NodeIdT, (Weight, EdgeIdT));
    type LinkResult = Weight;
    type PredecessorLink = EdgeIdT;

    #[inline(always)]
    fn link(
        &mut self,
        _graph: &G,
        _parents: &[(NodeId, Self::PredecessorLink)],
        _tail: NodeIdT,
        label: &Weight,
        (_, (weight, _)): &Self::Arc,
    ) -> Self::LinkResult {
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

pub trait BidirChooseDir: Default {
    fn choose(&mut self, fw_min_key: Option<Weight>, bw_min_key: Option<Weight>) -> bool;
    fn may_stop(&self) -> bool {
        true
    }
    fn strategy_key() -> &'static str;
    fn report() {
        report!("choose_direction_strategy", Self::strategy_key());
    }
}

#[derive(Default)]
pub struct ChooseMinKeyDir();

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

#[derive(Default)]
pub struct AlternatingDirs {
    prev: bool,
}

impl BidirChooseDir for AlternatingDirs {
    fn choose(&mut self, fw_min_key: Option<Weight>, bw_min_key: Option<Weight>) -> bool {
        self.prev = !self.prev;
        match (fw_min_key, bw_min_key) {
            (Some(_), Some(_)) => self.prev,
            (None, Some(_)) => false,
            _ => true,
        }
    }
    fn may_stop(&self) -> bool {
        !self.prev
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
