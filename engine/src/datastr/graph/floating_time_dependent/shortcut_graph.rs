//! Graph structs used during and after the customization.

use super::*;
use crate::datastr::clearlist_vector::ClearlistVector;
use crate::datastr::graph::first_out_graph::degrees_to_first_out;
use crate::datastr::index_heap::*;
use crate::datastr::rank_select_map::*;
use crate::io::*;
use crate::util::*;
use std::{
    cmp::{max, min, Reverse},
    convert::TryInto,
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum ShortcutId {
    Outgoing(EdgeId),
    Incoming(EdgeId),
}

impl PartialOrd for ShortcutId {
    fn partial_cmp(&self, other: &ShortcutId) -> Option<std::cmp::Ordering> {
        // Some(match self.discriminant().cmp(&other.discriminant()) {
        //     std::cmp::Ordering::Equal => self.edge_id().cmp(&other.edge_id()),
        //     res => res,
        // })
        Some(match self.edge_id().cmp(&other.edge_id()) {
            std::cmp::Ordering::Equal => self.discriminant().cmp(&other.discriminant()),
            res => res,
        })
    }
}

impl Ord for ShortcutId {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

impl ShortcutId {
    fn discriminant(&self) -> u32 {
        match self {
            Self::Incoming(_) => 1,
            Self::Outgoing(_) => 0,
        }
    }

    fn edge_id(&self) -> EdgeId {
        match *self {
            Self::Incoming(id) => id,
            Self::Outgoing(id) => id,
        }
    }

    fn get_from<'a, T>(self, incoming: &'a [T], outgoing: &'a [T]) -> &'a T {
        match self {
            Self::Incoming(id) => &incoming[id as usize],
            Self::Outgoing(id) => &outgoing[id as usize],
        }
    }

    fn get_mut_from<'a, T>(self, incoming: &'a mut [T], outgoing: &'a mut [T]) -> &'a mut T {
        match self {
            Self::Incoming(id) => &mut incoming[id as usize],
            Self::Outgoing(id) => &mut outgoing[id as usize],
        }
    }

    fn get_with<T, F, O>(self, incoming: T, outgoing: T, f: F) -> O
    where
        F: FnOnce(T, NodeId) -> O,
    {
        match self {
            Self::Incoming(id) => f(incoming, id),
            Self::Outgoing(id) => f(outgoing, id),
        }
    }
}

pub trait ShortcutGraphTrt {
    type OriginalGraph: for<'g> TDGraphTrait<'g>;

    fn periodic_ttf(&self, shortcut_id: ShortcutId) -> Option<PeriodicATTF>;
    fn partial_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> Option<PartialATTF>;
    fn is_valid_path(&self, shortcut_id: ShortcutId) -> bool;
    fn lower_bound(&self, shortcut_id: ShortcutId) -> FlWeight;
    fn upper_bound(&self, shortcut_id: ShortcutId) -> FlWeight;
    fn original_graph(&self) -> &Self::OriginalGraph;
    fn reconstruct_exact_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage);
    fn get_switchpoints(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> (Vec<(Timestamp, Vec<EdgeId>, FlWeight)>, FlWeight);
    fn unpack_at(&self, shortcut_id: ShortcutId, t: Timestamp, result: &mut Vec<(EdgeId, Timestamp)>);
    fn evaluate(&self, shortcut_id: ShortcutId, t: Timestamp) -> FlWeight;

    fn ttf_available(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> bool {
        self.partial_ttf(shortcut_id, start, end).is_some() || self.periodic_ttf(shortcut_id).is_some()
    }
}

/// Container for partial CCH graphs during CATCHUp customization.
/// Think split borrows.
#[derive(Debug)]
pub struct PartialShortcutGraph<'a> {
    pub original_graph: &'a TDGraph,
    outgoing: &'a [Shortcut],
    incoming: &'a [Shortcut],
    offset: usize,
}

impl<'a> PartialShortcutGraph<'a> {
    /// Create `PartialShortcutGraph` from original graph, shortcut slices in both directions and an offset to map CCH edge ids to slice indices
    pub fn new(original_graph: &'a TDGraph, outgoing: &'a [Shortcut], incoming: &'a [Shortcut], offset: usize) -> PartialShortcutGraph<'a> {
        PartialShortcutGraph {
            original_graph,
            outgoing,
            incoming,
            offset,
        }
    }

    fn get(&self, shortcut_id: ShortcutId) -> &Shortcut {
        match shortcut_id {
            ShortcutId::Incoming(id) => self.get_incoming(id),
            ShortcutId::Outgoing(id) => self.get_outgoing(id),
        }
    }

    /// Borrow upward `Shortcut` with given CCH EdgeId
    pub fn get_outgoing(&self, edge_id: EdgeId) -> &Shortcut {
        &self.outgoing[edge_id as usize - self.offset]
    }

    /// Borrow downward `Shortcut` with given CCH EdgeId
    pub fn get_incoming(&self, edge_id: EdgeId) -> &Shortcut {
        &self.incoming[edge_id as usize - self.offset]
    }
}

impl<'a> ShortcutGraphTrt for PartialShortcutGraph<'a> {
    type OriginalGraph = TDGraph;

    fn periodic_ttf(&self, shortcut_id: ShortcutId) -> Option<PeriodicATTF> {
        self.get(shortcut_id).periodic_ttf(self)
    }
    fn partial_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> Option<PartialATTF> {
        self.get(shortcut_id).partial_ttf(self, start, end)
    }
    fn is_valid_path(&self, shortcut_id: ShortcutId) -> bool {
        self.get(shortcut_id).is_valid_path()
    }
    fn lower_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        self.get(shortcut_id).lower_bound
    }
    fn upper_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        self.get(shortcut_id).upper_bound
    }
    fn original_graph(&self) -> &TDGraph {
        &self.original_graph
    }
    fn reconstruct_exact_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage) {
        self.get(shortcut_id).reconstruct_exact_ttf(start, end, self, target, tmp)
    }
    fn get_switchpoints(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> (Vec<(Timestamp, Vec<EdgeId>, FlWeight)>, FlWeight) {
        self.get(shortcut_id).get_switchpoints(start, end, self)
    }
    fn unpack_at(&self, shortcut_id: ShortcutId, t: Timestamp, result: &mut Vec<(EdgeId, Timestamp)>) {
        self.get(shortcut_id).unpack_at(t, self, result);
    }
    fn evaluate(&self, shortcut_id: ShortcutId, t: Timestamp) -> FlWeight {
        self.get(shortcut_id).evaluate(t, self)
    }
}

#[derive(Debug)]
pub struct PartialLiveShortcutGraph<'a> {
    pub original_graph: &'a LiveGraph,
    pub outgoing_live: &'a [LiveShortcut],
    pub incoming_live: &'a [LiveShortcut],
    pub outgoing: &'a [Shortcut],
    pub incoming: &'a [Shortcut],
    pub offset: usize,
}

impl<'a> PartialLiveShortcutGraph<'a> {
    pub fn new(
        original_graph: &'a LiveGraph,
        outgoing_live: &'a [LiveShortcut],
        incoming_live: &'a [LiveShortcut],
        outgoing: &'a [Shortcut],
        incoming: &'a [Shortcut],
        offset: usize,
    ) -> Self {
        Self {
            original_graph,
            outgoing_live,
            incoming_live,
            outgoing,
            incoming,
            offset,
        }
    }

    fn get(&self, shortcut_id: ShortcutId) -> &Shortcut {
        match shortcut_id {
            ShortcutId::Incoming(id) => self.get_incoming(id),
            ShortcutId::Outgoing(id) => self.get_outgoing(id),
        }
    }

    fn get_live(&self, shortcut_id: ShortcutId) -> &LiveShortcut {
        match shortcut_id {
            ShortcutId::Incoming(id) => self.get_incoming_live(id),
            ShortcutId::Outgoing(id) => self.get_outgoing_live(id),
        }
    }

    pub fn get_outgoing(&self, edge_id: EdgeId) -> &Shortcut {
        &self.outgoing[edge_id as usize - self.offset]
    }

    pub fn get_incoming(&self, edge_id: EdgeId) -> &Shortcut {
        &self.incoming[edge_id as usize - self.offset]
    }

    pub fn get_outgoing_live(&self, edge_id: EdgeId) -> &LiveShortcut {
        &self.outgoing_live[edge_id as usize - self.offset]
    }

    pub fn get_incoming_live(&self, edge_id: EdgeId) -> &LiveShortcut {
        &self.incoming_live[edge_id as usize - self.offset]
    }
}

impl<'a> ShortcutGraphTrt for PartialLiveShortcutGraph<'a> {
    type OriginalGraph = LiveGraph;

    fn periodic_ttf(&self, _: ShortcutId) -> Option<PeriodicATTF> {
        None
    }
    fn partial_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> Option<PartialATTF> {
        self.get_live(shortcut_id).partial_ttf(self, start, end)
    }
    fn is_valid_path(&self, shortcut_id: ShortcutId) -> bool {
        self.get_live(shortcut_id).is_valid_path()
    }
    fn lower_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        self.get_live(shortcut_id).lower_bound
    }
    fn upper_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        self.get_live(shortcut_id).upper_bound
    }
    fn original_graph(&self) -> &LiveGraph {
        &self.original_graph
    }
    fn reconstruct_exact_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage) {
        let live = self.get_live(shortcut_id);
        if live.live_until.map(|l| !start.fuzzy_lt(l)).unwrap_or(true) {
            self.get(shortcut_id).reconstruct_exact_ttf(start, end, self, target, tmp);
        } else {
            let live_until = live.live_until.unwrap();
            if !live_until.fuzzy_lt(end) {
                live.reconstruct_exact_ttf(start, end, self, target, tmp);
            } else {
                live.reconstruct_exact_ttf(start, live_until, self, target, tmp);
                let mut sub_target = tmp.push_plf();
                self.get(shortcut_id)
                    .reconstruct_exact_ttf(live_until, end, self, &mut sub_target, target.storage_mut());
                PartialPiecewiseLinearFunction::new(&sub_target).append(live_until, target);
            }
        }
    }
    fn get_switchpoints(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> (Vec<(Timestamp, Vec<EdgeId>, FlWeight)>, FlWeight) {
        let live = self.get_live(shortcut_id);
        if live.live_until.map(|l| !start.fuzzy_lt(l)).unwrap_or(true) {
            self.get(shortcut_id).get_switchpoints(start, end, self)
        } else {
            let live_until = live.live_until.unwrap();
            if !live_until.fuzzy_lt(end) {
                live.get_switchpoints(start, end, self)
            } else {
                let (mut switchpoints, _) = live.get_switchpoints(start, live_until, self);
                let (mut second_switchpoints, ttf_at_end) = self.get(shortcut_id).get_switchpoints(live_until, end, self);
                switchpoints.append(&mut second_switchpoints);
                (switchpoints, ttf_at_end)
            }
        }
    }
    fn unpack_at(&self, shortcut_id: ShortcutId, t: Timestamp, result: &mut Vec<(EdgeId, Timestamp)>) {
        let live = self.get_live(shortcut_id);
        if live.live_until.map(|l| l.fuzzy_lt(t)).unwrap_or(true) {
            self.get(shortcut_id).unpack_at(t, self, result);
        } else {
            live.unpack_at(t, self, result);
        }
    }
    fn evaluate(&self, shortcut_id: ShortcutId, t: Timestamp) -> FlWeight {
        let live = self.get_live(shortcut_id);
        if live.live_until.map(|l| l.fuzzy_lt(t)).unwrap_or(true) {
            self.get(shortcut_id).evaluate(t, self)
        } else {
            live.evaluate(t, self)
        }
    }
}

// Just a container to group some data
#[derive(Debug)]
struct ShortcutGraph<'a> {
    original_graph: &'a TDGraph,
    first_out: &'a [EdgeId],
    head: &'a [NodeId],
    outgoing: Vec<Shortcut>,
    incoming: Vec<Shortcut>,
}

/// Result of CATCHUp customization to be passed to query algorithm.
#[derive(Debug)]
pub struct CustomizedGraph<'a> {
    pub original_graph: &'a TDGraph,
    first_out: &'a [EdgeId],
    head: &'a [NodeId],
    pub outgoing: CustomizedSingleDirGraph,
    pub incoming: CustomizedSingleDirGraph,
}

impl<'a> From<ShortcutGraph<'a>> for CustomizedGraph<'a> {
    // cleaning up and compacting preprocessing results.
    fn from(shortcut_graph: ShortcutGraph<'a>) -> Self {
        let mut outgoing_required = BitVec::new(shortcut_graph.head.len());
        let mut incoming_required = BitVec::new(shortcut_graph.head.len());

        for (idx, s) in shortcut_graph.outgoing.iter().enumerate() {
            if s.required {
                outgoing_required.set(idx)
            }
        }

        for (idx, s) in shortcut_graph.incoming.iter().enumerate() {
            if s.required {
                incoming_required.set(idx)
            }
        }

        let mapping_outgoing = RankSelectMap::new(outgoing_required);
        let mapping_incoming = RankSelectMap::new(incoming_required);

        let mut outgoing_first_out = Vec::with_capacity(shortcut_graph.first_out.len());
        let mut incoming_first_out = Vec::with_capacity(shortcut_graph.first_out.len());
        let mut outgoing_head = Vec::with_capacity(shortcut_graph.head.len());
        let mut incoming_head = Vec::with_capacity(shortcut_graph.head.len());

        outgoing_first_out.push(0);
        incoming_first_out.push(0);

        for range in shortcut_graph.first_out.windows(2) {
            let range = range[0] as usize..range[1] as usize;
            outgoing_head.extend(
                shortcut_graph.head[range.clone()]
                    .iter()
                    .zip(shortcut_graph.outgoing[range.clone()].iter())
                    .filter(|(_head, s)| s.required)
                    .map(|(head, _)| head),
            );
            outgoing_first_out.push(outgoing_first_out.last().unwrap() + shortcut_graph.outgoing[range.clone()].iter().filter(|s| s.required).count() as u32);

            incoming_head.extend(
                shortcut_graph.head[range.clone()]
                    .iter()
                    .zip(shortcut_graph.incoming[range.clone()].iter())
                    .filter(|(_head, s)| s.required)
                    .map(|(head, _)| head),
            );
            incoming_first_out.push(incoming_first_out.last().unwrap() + shortcut_graph.incoming[range.clone()].iter().filter(|s| s.required).count() as u32);
        }

        let mut outgoing_constant = BitVec::new(outgoing_head.len());
        let mut incoming_constant = BitVec::new(incoming_head.len());

        let outgoing_iter = || shortcut_graph.outgoing.iter().filter(|s| s.required);
        let incoming_iter = || shortcut_graph.incoming.iter().filter(|s| s.required);

        for (idx, shortcut) in outgoing_iter().enumerate() {
            if shortcut.is_constant() {
                outgoing_constant.set(idx);
            }
        }

        for (idx, shortcut) in incoming_iter().enumerate() {
            if shortcut.is_constant() {
                incoming_constant.set(idx);
            }
        }

        let mut outgoing_tail = vec![0 as NodeId; outgoing_head.len()];
        for (node, range) in outgoing_first_out.windows(2).enumerate() {
            for tail in &mut outgoing_tail[range[0] as usize..range[1] as usize] {
                *tail = node as NodeId;
            }
        }

        let mut incoming_tail = vec![0 as NodeId; incoming_head.len()];
        for (node, range) in incoming_first_out.windows(2).enumerate() {
            for tail in &mut incoming_tail[range[0] as usize..range[1] as usize] {
                *tail = node as NodeId;
            }
        }

        CustomizedGraph {
            original_graph: shortcut_graph.original_graph,
            first_out: shortcut_graph.first_out,
            head: shortcut_graph.head,

            outgoing: CustomizedSingleDirGraph {
                first_out: outgoing_first_out,
                head: outgoing_head,
                tail: outgoing_tail,

                bounds: outgoing_iter().map(|shortcut| (shortcut.lower_bound, shortcut.upper_bound)).collect(),
                constant: outgoing_constant,
                first_source: degrees_to_first_out(outgoing_iter().map(|shortcut| shortcut.num_sources() as u32)).collect(),
                sources: outgoing_iter()
                    .flat_map(|shortcut| {
                        shortcut.sources_iter().map(|(t, s)| {
                            let s = if let ShortcutSource::Shortcut(down, up) = ShortcutSource::from(s) {
                                ShortcutSource::Shortcut(
                                    mapping_incoming.get(down as usize).unwrap() as EdgeId,
                                    mapping_outgoing.get(up as usize).unwrap() as EdgeId,
                                )
                            } else {
                                ShortcutSource::from(s)
                            };
                            (t, ShortcutSourceData::from(s))
                        })
                    })
                    .collect(),
            },

            incoming: CustomizedSingleDirGraph {
                first_out: incoming_first_out,
                head: incoming_head,
                tail: incoming_tail,

                bounds: incoming_iter().map(|shortcut| (shortcut.lower_bound, shortcut.upper_bound)).collect(),
                constant: incoming_constant,
                first_source: degrees_to_first_out(incoming_iter().map(|shortcut| shortcut.num_sources() as u32)).collect(),
                sources: incoming_iter()
                    .flat_map(|shortcut| {
                        shortcut.sources_iter().map(|(t, s)| {
                            let s = if let ShortcutSource::Shortcut(down, up) = ShortcutSource::from(s) {
                                ShortcutSource::Shortcut(
                                    mapping_incoming.get(down as usize).unwrap() as EdgeId,
                                    mapping_outgoing.get(up as usize).unwrap() as EdgeId,
                                )
                            } else {
                                ShortcutSource::from(s)
                            };
                            (t, ShortcutSourceData::from(s))
                        })
                    })
                    .collect(),
            },
        }
    }
}

impl<'a> CustomizedGraph<'a> {
    /// Create CustomizedGraph from original graph, CCH topology, and customized `Shortcut`s for each CCH edge in both directions
    pub fn new(original_graph: &'a TDGraph, first_out: &'a [EdgeId], head: &'a [NodeId], outgoing: Vec<Shortcut>, incoming: Vec<Shortcut>) -> Self {
        ShortcutGraph {
            original_graph,
            first_out,
            head,
            outgoing,
            incoming,
        }
        .into()
    }

    /// Get bounds graph for forward elimination tree interval query
    pub fn upward_bounds_graph(&self) -> SingleDirBoundsGraph {
        SingleDirBoundsGraph {
            first_out: &self.outgoing.first_out[..],
            head: &self.outgoing.head[..],
            bounds: &self.outgoing.bounds[..],
        }
    }

    /// Get bounds graph for backward elimination tree interval query
    pub fn downward_bounds_graph(&self) -> SingleDirBoundsGraph {
        SingleDirBoundsGraph {
            first_out: &self.incoming.first_out[..],
            head: &self.incoming.head[..],
            bounds: &self.incoming.bounds[..],
        }
    }

    pub fn unique_path_edges(&self) -> (BitVec, BitVec) {
        let mut incoming_unique = BitVec::new(self.incoming.head.len());
        let mut outgoing_unique = BitVec::new(self.outgoing.head.len());

        for n in 0..self.original_graph.num_nodes() {
            for ((_, edge), _) in self.downward_bounds_graph().neighbor_iter(n as NodeId) {
                if let &[(_, source)] = self.incoming.edge_sources(edge) {
                    match source.into() {
                        ShortcutSource::Shortcut(down, up) => {
                            if incoming_unique.get(down as usize) && outgoing_unique.get(up as usize) {
                                incoming_unique.set(edge as usize)
                            }
                        }
                        ShortcutSource::OriginalEdge(_) => incoming_unique.set(edge as usize),
                        ShortcutSource::None => incoming_unique.set(edge as usize),
                    }
                }
            }
            for ((_, edge), _) in self.upward_bounds_graph().neighbor_iter(n as NodeId) {
                if let &[(_, source)] = self.outgoing.edge_sources(edge) {
                    match source.into() {
                        ShortcutSource::Shortcut(down, up) => {
                            if incoming_unique.get(down as usize) && outgoing_unique.get(up as usize) {
                                outgoing_unique.set(edge as usize)
                            }
                        }
                        ShortcutSource::OriginalEdge(_) => outgoing_unique.set(edge as usize),
                        ShortcutSource::None => outgoing_unique.set(edge as usize),
                    }
                }
            }
        }

        (incoming_unique, outgoing_unique)
    }

    pub fn evaluate_and_path_length(&self, shortcut_id: ShortcutId, t: Timestamp) -> (FlWeight, usize) {
        ShortcutSource::from(
            shortcut_id
                .get_with(&self.incoming, &self.outgoing, |g_dir, id| g_dir.edge_source_at(id, t))
                .unwrap(),
        )
        .evaluate_and_path_length(t, self)
    }

    fn lower_node(&self, shortcut_id: ShortcutId) -> NodeId {
        *shortcut_id.get_from(&self.incoming.tail, &self.outgoing.tail)
    }
    fn upper_node(&self, shortcut_id: ShortcutId) -> NodeId {
        *shortcut_id.get_from(&self.incoming.head, &self.outgoing.head)
    }
}

impl<'a> Deconstruct for CustomizedGraph<'a> {
    fn store_each(&self, store: &dyn Fn(&str, &dyn Store) -> std::io::Result<()>) -> std::io::Result<()> {
        store("outgoing_first_out", &self.outgoing.first_out)?;
        store("outgoing_head", &self.outgoing.head)?;
        store("outgoing_bounds", &self.outgoing.bounds)?;
        store("outgoing_constant", &self.outgoing.constant)?;
        store("outgoing_first_source", &self.outgoing.first_source)?;
        store("outgoing_sources", &self.outgoing.sources)?;
        store("incoming_first_out", &self.incoming.first_out)?;
        store("incoming_head", &self.incoming.head)?;
        store("incoming_bounds", &self.incoming.bounds)?;
        store("incoming_constant", &self.incoming.constant)?;
        store("incoming_first_source", &self.incoming.first_source)?;
        store("incoming_sources", &self.incoming.sources)?;
        Ok(())
    }
}

/// Additional data to load CATCHUp customization results back from disk.
#[derive(Debug)]
pub struct CustomizedGraphReconstrctor<'a> {
    pub original_graph: &'a TDGraph,
    pub first_out: &'a [EdgeId],
    pub head: &'a [NodeId],
}

impl<'a> ReconstructPrepared<CustomizedGraph<'a>> for CustomizedGraphReconstrctor<'a> {
    fn reconstruct_with(self, loader: Loader) -> std::io::Result<CustomizedGraph<'a>> {
        let outgoing_first_out: Vec<EdgeId> = loader.load("outgoing_first_out")?;
        let outgoing_head: Vec<NodeId> = loader.load("outgoing_head")?;
        let incoming_first_out: Vec<EdgeId> = loader.load("incoming_first_out")?;
        let incoming_head: Vec<NodeId> = loader.load("incoming_head")?;

        let mut outgoing_tail = vec![0 as NodeId; outgoing_head.len()];
        for (node, range) in outgoing_first_out.windows(2).enumerate() {
            for tail in &mut outgoing_tail[range[0] as usize..range[1] as usize] {
                *tail = node as NodeId;
            }
        }

        let mut incoming_tail = vec![0 as NodeId; incoming_head.len()];
        for (node, range) in incoming_first_out.windows(2).enumerate() {
            for tail in &mut incoming_tail[range[0] as usize..range[1] as usize] {
                *tail = node as NodeId;
            }
        }

        Ok(CustomizedGraph {
            original_graph: self.original_graph,
            first_out: self.first_out,
            head: self.head,

            outgoing: CustomizedSingleDirGraph {
                first_out: outgoing_first_out,
                head: outgoing_head,
                tail: outgoing_tail,

                bounds: loader.load("outgoing_bounds")?,
                constant: loader.load("outgoing_constant")?,
                first_source: loader.load("outgoing_first_source")?,
                sources: loader.load("outgoing_sources")?,
            },

            incoming: CustomizedSingleDirGraph {
                first_out: incoming_first_out,
                head: incoming_head,
                tail: incoming_tail,

                bounds: loader.load("incoming_bounds")?,
                constant: loader.load("incoming_constant")?,
                first_source: loader.load("incoming_first_source")?,
                sources: loader.load("incoming_sources")?,
            },
        })
    }
}

/// Data for result of CATCHUp customization; one half/direction of it.
#[derive(Debug)]
pub struct CustomizedSingleDirGraph {
    first_out: Vec<EdgeId>,
    head: Vec<NodeId>,
    tail: Vec<NodeId>,

    bounds: Vec<(FlWeight, FlWeight)>,
    constant: BitVec,
    first_source: Vec<u32>,
    sources: Vec<(Timestamp, ShortcutSourceData)>,
}

impl CustomizedSingleDirGraph {
    /// Number of outgoing/incoming edges to/from higher ranked nodes for a given node
    pub fn degree(&self, node: NodeId) -> usize {
        (self.first_out[node as usize + 1] - self.first_out[node as usize]) as usize
    }

    /// Borrow full slice of upper and lower bounds for each edge in this graph
    pub fn bounds(&self) -> &[(FlWeight, FlWeight)] {
        &self.bounds[..]
    }

    /// Borrow full slice of head node for each edge in this graph
    pub fn head(&self) -> &[NodeId] {
        &self.head[..]
    }

    /// Borrow full slice of tail node for each edge in this graph
    pub fn tail(&self) -> &[NodeId] {
        &self.tail[..]
    }

    /// (Recursively) evaluate the travel time of edge with a given id for given point in time.
    /// The callback `f` can be used to do early returns if we reach a node that already has a better tentative distance.
    pub fn evaluate<F>(&self, edge_id: EdgeId, t: Timestamp, customized_graph: &CustomizedGraph, f: &mut F) -> FlWeight
    where
        F: (FnMut(bool, EdgeId, Timestamp) -> bool),
    {
        let edge_idx = edge_id as usize;
        if self.constant.get(edge_idx) {
            debug_assert_eq!(
                self.bounds[edge_idx].0,
                self.edge_source_at(edge_id, t)
                    .map(|source| ShortcutSource::from(source).evaluate(t, customized_graph))
                    .unwrap_or(FlWeight::INFINITY),
                "{:?}, {:?}, {}",
                self.bounds[edge_idx],
                self.edge_sources(edge_id),
                edge_id
            );
            return self.bounds[edge_idx].0;
        }

        self.edge_source_at(edge_id, t)
            .map(|source| ShortcutSource::from(source).evaluate_opt(t, customized_graph, f))
            .unwrap_or(FlWeight::INFINITY)
    }

    /// Evaluate the first original edge on the path that the edge with the given id represents at the given point in time.
    ///
    /// This means we recursively unpack the downward edges of all lower triangles of shortcuts.
    /// While doing so, we mark the respective up arc as contained in the search space using the `mark_upwards` callback.
    /// We also update lower bounds to the target of all middle nodes of unpacked triangles.
    /// The Dir parameter is used to distinguish the direction of the current edge - True means upward, False downward.
    /// We return an `Option` of a tuple with the evaluated `FlWeight`, the CCH `NodeId` of the head node of the evaluated edge, and the CCH `EdgeId` of the evaluated edge.
    /// The result will be `None` when this is an always infinity edge.
    pub fn evaluate_next_segment_at<Dir: Bool, F>(
        &self,
        edge_id: EdgeId,
        t: Timestamp,
        lower_bound_target: FlWeight,
        customized_graph: &CustomizedGraph,
        lower_bounds_to_target: &mut ClearlistVector<FlWeight>,
        mark_upward: &mut F,
    ) -> Option<(FlWeight, NodeId, EdgeId)>
    where
        F: FnMut(EdgeId),
    {
        let edge_idx = edge_id as usize;

        if self.constant.get(edge_idx) {
            return Some((
                self.bounds[edge_idx].0,
                if Dir::VALUE { self.head[edge_idx] } else { self.tail[edge_idx] },
                edge_id,
            ));
        }
        self.edge_source_at(edge_id, t).map(|source| match source.into() {
            ShortcutSource::Shortcut(down, up) => {
                mark_upward(up);
                let lower_bound_to_middle = customized_graph.outgoing.bounds()[up as usize].0 + lower_bound_target;
                lower_bounds_to_target[customized_graph.incoming.tail[down as usize] as usize] = min(
                    lower_bounds_to_target[customized_graph.incoming.tail[down as usize] as usize],
                    lower_bound_to_middle,
                );
                customized_graph
                    .incoming
                    .evaluate_next_segment_at::<False, _>(down, t, lower_bound_to_middle, customized_graph, lower_bounds_to_target, mark_upward)
                    .unwrap()
            }
            ShortcutSource::OriginalEdge(edge) => (
                customized_graph.original_graph.travel_time_function(edge).evaluate(t),
                if Dir::VALUE { self.head[edge_idx] } else { self.tail[edge_idx] },
                edge_id,
            ),
            ShortcutSource::None => (FlWeight::INFINITY, if Dir::VALUE { self.head[edge_idx] } else { self.tail[edge_idx] }, edge_id),
        })
    }

    pub fn add_first_original_arcs_to_searchspace<F>(
        &self,
        edge_id: EdgeId,
        lower_bound_target: FlWeight,
        customized_graph: &CustomizedGraph,
        lower_bounds_to_target: &mut ClearlistVector<FlWeight>,
        searchspace: &mut FastClearBitVec,
        mark_upward: &mut F,
    ) where
        F: FnMut(EdgeId),
    {
        // constant?? pruning with bounds?? avoid duplicate unpacking ops??

        for &(_, source) in self.edge_sources(edge_id) {
            match source.into() {
                ShortcutSource::Shortcut(down, up) => {
                    mark_upward(up);
                    let lower_bound_to_middle = customized_graph.outgoing.bounds()[up as usize].0 + lower_bound_target;
                    lower_bounds_to_target[customized_graph.incoming.tail[down as usize] as usize] = min(
                        lower_bounds_to_target[customized_graph.incoming.tail[down as usize] as usize],
                        lower_bound_to_middle,
                    );
                    customized_graph.incoming.add_first_original_arcs_to_searchspace(
                        down,
                        lower_bound_to_middle,
                        customized_graph,
                        lower_bounds_to_target,
                        searchspace,
                        mark_upward,
                    )
                }
                ShortcutSource::OriginalEdge(edge) => searchspace.set(edge as usize),
                _ => (),
            }
        }
    }

    /// Recursively unpack the edge with the given id at the given timestamp and add the path to `result`
    pub fn unpack_at(&self, edge_id: EdgeId, t: Timestamp, customized_graph: &CustomizedGraph, result: &mut Vec<(EdgeId, Timestamp)>) {
        self.edge_source_at(edge_id, t)
            .map(|source| ShortcutSource::from(source).unpack_at(t, customized_graph, result))
            .expect("can't unpack empty shortcut");
    }

    fn edge_source_at(&self, edge_id: EdgeId, t: Timestamp) -> Option<ShortcutSourceData> {
        PeriodicSources(self.edge_sources(edge_id)).edge_source_at(t)
    }

    /// Borrow slice of all the source of the edge with given id.
    pub fn edge_sources(&self, edge_id: EdgeId) -> &[(Timestamp, ShortcutSourceData)] {
        &self.sources[(self.first_source[edge_id as usize] as usize)..(self.first_source[edge_id as usize + 1] as usize)]
    }

    pub fn to_shortcut_vec(&self) -> Vec<Shortcut> {
        (0..self.head.len())
            .map(|edge_idx| Shortcut::new_finished(self.edge_sources(edge_idx as EdgeId), self.bounds[edge_idx]))
            .collect()
    }

    pub fn to_shortcut(&self, edge_idx: usize) -> Shortcut {
        Shortcut::new_finished(self.edge_sources(edge_idx as EdgeId), self.bounds[edge_idx])
    }

    // fn validate(self) -> Self {
    //     for edge in 0..self.head.len() {
    //         for &(_, source) in self.edge_sources(edge) {
    //             if let ShortcutSource::Shortcut(down, up) = source.into() {
    //                 assert!(down < edge as EdgeId);
    //                 assert!(up < edge as EdgeId);
    //             }
    //         }
    //     }
    //     self
    // }
}

impl<'a> ShortcutGraphTrt for CustomizedGraph<'a> {
    type OriginalGraph = TDGraph;

    fn periodic_ttf(&self, _: ShortcutId) -> Option<PeriodicATTF> {
        None
    }
    fn partial_ttf(&self, _: ShortcutId, _start: Timestamp, _end: Timestamp) -> Option<PartialATTF> {
        None
    }
    fn is_valid_path(&self, _: ShortcutId) -> bool {
        true
    }
    fn lower_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        shortcut_id.get_from(&self.incoming.bounds, &self.outgoing.bounds).0
    }
    fn upper_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        shortcut_id.get_from(&self.incoming.bounds, &self.outgoing.bounds).1
    }
    fn original_graph(&self) -> &TDGraph {
        &self.original_graph
    }
    fn reconstruct_exact_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage) {
        PeriodicSources(shortcut_id.get_with(&self.incoming, &self.outgoing, CustomizedSingleDirGraph::edge_sources))
            .reconstruct_exact_ttf(start, end, self, target, tmp)
    }
    fn get_switchpoints(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> (Vec<(Timestamp, Vec<EdgeId>, FlWeight)>, FlWeight) {
        PeriodicSources(shortcut_id.get_with(&self.incoming, &self.outgoing, CustomizedSingleDirGraph::edge_sources)).get_switchpoints(start, end, self)
    }
    fn unpack_at(&self, shortcut_id: ShortcutId, t: Timestamp, result: &mut Vec<(EdgeId, Timestamp)>) {
        ShortcutSource::from(
            shortcut_id
                .get_with(&self.incoming, &self.outgoing, |g_dir, id| g_dir.edge_source_at(id, t))
                .unwrap(),
        )
        .unpack_at(t, self, result)
    }
    fn evaluate(&self, shortcut_id: ShortcutId, t: Timestamp) -> FlWeight {
        ShortcutSource::from(
            shortcut_id
                .get_with(&self.incoming, &self.outgoing, |g_dir, id| g_dir.edge_source_at(id, t))
                .unwrap(),
        )
        .evaluate(t, self)
    }
}

/// Struct with borrowed slice of the relevant parts (topology, upper and lower bounds) for elimination tree corridor query.
#[derive(Debug)]
pub struct SingleDirBoundsGraph<'a> {
    first_out: &'a [EdgeId],
    head: &'a [NodeId],
    bounds: &'a [(FlWeight, FlWeight)],
}

impl<'a> SingleDirBoundsGraph<'a> {
    pub fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }

    fn neighbor_edge_indices_usize(&self, node: NodeId) -> Range<usize> {
        (self.first_out[node as usize] as usize)..(self.first_out[(node + 1) as usize] as usize)
    }

    pub fn neighbor_iter(&self, node: NodeId) -> impl Iterator<Item = ((NodeId, EdgeId), &(FlWeight, FlWeight))> {
        let range = self.neighbor_edge_indices_usize(node);
        let edge_ids = range.start as EdgeId..range.end as EdgeId;
        self.head[range.clone()].iter().cloned().zip(edge_ids).zip(self.bounds[range].iter())
    }
}

#[derive(Debug, Clone)]
pub struct ReconstructionState {
    missing_deps: usize,
    awaited_by: Vec<ShortcutId>,
    pub requested_times: Vec<(Timestamp, Timestamp)>,
}

impl Default for ReconstructionState {
    fn default() -> Self {
        ReconstructionState {
            missing_deps: 0,
            awaited_by: Vec::new(),
            requested_times: Vec::new(),
        }
    }
}

impl ReconstructionState {
    fn request_time(times: &mut Vec<(Timestamp, Timestamp)>, mut start: Timestamp, mut end: Timestamp) {
        if !cfg!(feature = "tdcch-profiles-with-holes") {
            start = Timestamp::ZERO;
            end = period();
        }
        if times.len() == 1 && times[0].0.fuzzy_eq(Timestamp::ZERO) && times[0].1.fuzzy_eq(period()) {
            return;
        }

        use std::cmp::Ordering;
        let start_pos = times.binary_search_by(|&(seg_start, seg_end)| {
            if start.fuzzy_lt(seg_start) {
                Ordering::Greater
            } else if seg_end.fuzzy_lt(start) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        });
        let end_pos = times.binary_search_by(|&(seg_start, seg_end)| {
            if end.fuzzy_lt(seg_start) {
                Ordering::Greater
            } else if seg_end.fuzzy_lt(end) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        });

        match (start_pos, end_pos) {
            (Ok(start_idx), Ok(end_idx)) => {
                if start_idx < end_idx {
                    times[start_idx].1 = times[end_idx].1;
                    times.drain(start_idx + 1..=end_idx);
                }
            }
            (Ok(start_idx), Err(end_idx)) => {
                times[start_idx].1 = end;
                if start_idx < end_idx {
                    times.drain(start_idx + 1..end_idx);
                }
            }
            (Err(start_idx), Ok(end_idx)) => {
                times[end_idx].0 = start;
                if start_idx < end_idx {
                    times.drain(start_idx..end_idx);
                }
            }
            (Err(start_idx), Err(end_idx)) => {
                if start_idx < end_idx {
                    times[start_idx].0 = start;
                    times[start_idx].1 = end;
                    times.drain(start_idx + 1..end_idx);
                } else {
                    times.insert(start_idx, (start, end));
                }
            }
        }

        // maybe periodic
        if times.iter().any(|&(start, end)| FlWeight::from(period()).fuzzy_leq(end - start)) {
            times.truncate(1);
            times[0] = (Timestamp::ZERO, period());
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct ReconstructionQueueElement {
    pub upper_node: NodeId,
    pub lower_node: NodeId,
    pub shortcut_id: ShortcutId,
    pub t: Timestamp,
}

impl Indexing for ReconstructionQueueElement {
    fn as_index(&self) -> usize {
        self.shortcut_id.edge_id() as usize * 2 + self.shortcut_id.discriminant() as usize
    }
}

impl Indexing for Reverse<ReconstructionQueueElement> {
    fn as_index(&self) -> usize {
        self.0.as_index()
    }
}

#[derive(Debug)]
pub struct ReconstructionGraph<'a> {
    pub customized_graph: &'a CustomizedGraph<'a>,
    pub outgoing_cache: &'a mut [Option<ApproxPartialsContainer<Box<[TTFPoint]>>>],
    pub incoming_cache: &'a mut [Option<ApproxPartialsContainer<Box<[TTFPoint]>>>],
}

impl<'a> ReconstructionGraph<'a> {
    fn cache(&mut self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, buffers: &mut MergeBuffers) {
        debug_assert!(start.fuzzy_lt(end));
        let inserts = shortcut_id
            .get_from(&self.incoming_cache, &self.outgoing_cache)
            .as_ref()
            .map(|cache| cache.missing(start, end))
            .unwrap_or(vec![(start, end)])
            .into_iter()
            .map(|(start, end)| {
                Partial {
                    start,
                    end,
                    ttf: if self.as_reconstructed().all_sources_exact(shortcut_id, start, end) {
                        let mut target = buffers.unpacking_target.push_plf();
                        self.as_reconstructed()
                            .reconstruct_exact_ttf(shortcut_id, start, end, &mut target, &mut buffers.unpacking_tmp);

                        ATTFContainer::Exact(Box::<[TTFPoint]>::from(&target[..]))
                    } else {
                        let mut target = buffers.unpacking_target.push_plf();

                        let edge_sources = shortcut_id.get_with(
                            &self.customized_graph.incoming,
                            &self.customized_graph.outgoing,
                            CustomizedSingleDirGraph::edge_sources,
                        );
                        let mut c = SourceCursor::valid_at(edge_sources, start);

                        while c.cur().0.fuzzy_lt(end) {
                            let mut inner_target = buffers.unpacking_tmp.push_plf();
                            ShortcutSource::from(c.cur().1).reconstruct_lower_bound(
                                max(start, c.cur().0),
                                min(end, c.next().0),
                                &self.as_reconstructed(),
                                &mut inner_target,
                                target.storage_mut(),
                            );
                            PartialPiecewiseLinearFunction::new(&inner_target[..]).append_bound(max(start, c.cur().0), &mut target, min);

                            c.advance();
                        }

                        let mut lower = Box::<[TTFPoint]>::from(&target[..]);
                        PartialPiecewiseLinearFunction::fifoize_down(&mut lower);
                        drop(target);

                        let mut target = buffers.unpacking_target.push_plf();

                        let mut c = SourceCursor::valid_at(edge_sources, start);

                        while c.cur().0.fuzzy_lt(end) {
                            let mut inner_target = buffers.unpacking_tmp.push_plf();
                            ShortcutSource::from(c.cur().1).reconstruct_upper_bound(
                                max(start, c.cur().0),
                                min(end, c.next().0),
                                &self.as_reconstructed(),
                                &mut inner_target,
                                target.storage_mut(),
                            );
                            PartialPiecewiseLinearFunction::new(&inner_target[..]).append_bound(max(start, c.cur().0), &mut target, max);

                            c.advance();
                        }

                        let mut upper = Box::<[TTFPoint]>::from(&target[..]);
                        PartialPiecewiseLinearFunction::fifoize_up(&mut upper);

                        ATTFContainer::Approx(lower, upper)
                        // for p in &lower[..] {
                        //     debug_assert!(p.val.fuzzy_leq(self.as_reconstructed().evaluate(shortcut_id, p.at)));
                        // }
                        // for p in &upper[..] {
                        //     debug_assert!(self.as_reconstructed().evaluate(shortcut_id, p.at).fuzzy_leq(p.val), "{:#?}", p);
                        // }

                        // let c = ApproxTTFContainer::Approx(lower, upper);
                        // ApproxPartialTTF::from(&c);
                        // c
                    },
                }
            })
            .collect();

        let old = shortcut_id.get_mut_from(&mut self.incoming_cache, &mut self.outgoing_cache);
        if let Some(old) = old.as_mut() {
            old.insert_all(inserts);
        } else {
            *old = Some(ApproxPartialsContainer::new(inserts));
        }

        self.approximate(shortcut_id, buffers);

        shortcut_id
            .get_mut_from(&mut self.incoming_cache, &mut self.outgoing_cache)
            .as_mut()
            .unwrap()
            .maybe_to_periodic();
    }

    pub fn cache_iterative_iteration(
        &mut self,
        shortcut_id: ShortcutId,
        state: &mut ReconstructionState,
        queue: &mut IndexdMinHeap<Reverse<ReconstructionQueueElement>>,
        incoming_reconstruction_states: &mut [ReconstructionState],
        outgoing_reconstruction_states: &mut [ReconstructionState],
        buffers: &mut MergeBuffers,
    ) {
        let edge_sources = shortcut_id.get_with(
            &self.customized_graph.incoming,
            &self.customized_graph.outgoing,
            CustomizedSingleDirGraph::edge_sources,
        );

        let mut any_down_missing = false;
        for &(start, end) in &state.requested_times {
            let mut c = SourceCursor::valid_at(edge_sources, start);

            while c.cur().0.fuzzy_lt(end) {
                match ShortcutSource::from(c.cur().1) {
                    ShortcutSource::Shortcut(down, up) => {
                        let first_start = max(start, c.cur().0);
                        let first_end = min(end, c.next().0);

                        if !self.ttf_available(ShortcutId::Incoming(down), first_start, first_end) {
                            any_down_missing = true;
                            ReconstructionState::request_time(&mut incoming_reconstruction_states[down as usize].requested_times, first_start, first_end);
                            if !incoming_reconstruction_states[down as usize].awaited_by.contains(&shortcut_id) {
                                incoming_reconstruction_states[down as usize].awaited_by.push(shortcut_id);
                                state.missing_deps += 1;
                            }

                            let new_el = ReconstructionQueueElement {
                                t: start,
                                upper_node: self.customized_graph.incoming.head[down as usize],
                                lower_node: self.customized_graph.incoming.tail[down as usize],
                                shortcut_id: ShortcutId::Incoming(down),
                            };
                            if let Some(Reverse(el)) = queue.get(new_el.as_index()) {
                                if &new_el < el {
                                    queue.decrease_key(Reverse(new_el));
                                }
                            } else {
                                queue.push(Reverse(new_el));
                            }

                            if !incoming_reconstruction_states[down as usize].awaited_by.contains(&ShortcutId::Outgoing(up)) {
                                incoming_reconstruction_states[down as usize].awaited_by.push(ShortcutId::Outgoing(up));
                                outgoing_reconstruction_states[up as usize].missing_deps += 1;
                            }
                        }
                    }
                    _ => (),
                }
                c.advance();
            }
        }
        if any_down_missing {
            return;
        }

        let mut any_up_missing = false;
        for &(start, end) in &state.requested_times {
            let mut c = SourceCursor::valid_at(edge_sources, start);

            while c.cur().0.fuzzy_lt(end) {
                match ShortcutSource::from(c.cur().1) {
                    ShortcutSource::Shortcut(down, up) => {
                        let first_start = max(start, c.cur().0);
                        let first_end = min(end, c.next().0);

                        let reconstructed = self.as_reconstructed();
                        let (start_val, end_val) = if let Some(ttf) = reconstructed.partial_ttf(ShortcutId::Incoming(down), first_start, first_end) {
                            (ttf.bound_plfs().0.eval(first_start), ttf.bound_plfs().1.eval(first_end))
                        } else {
                            let ttf = reconstructed.periodic_ttf(ShortcutId::Incoming(down)).unwrap();
                            (ttf.bound_plfs().0.evaluate(first_start), ttf.bound_plfs().1.evaluate(first_end))
                        };
                        let second_start = first_start + start_val;
                        let second_end = first_end + end_val;

                        if !self.ttf_available(ShortcutId::Outgoing(up), second_start, second_end) {
                            any_up_missing = true;

                            ReconstructionState::request_time(&mut outgoing_reconstruction_states[up as usize].requested_times, second_start, second_end);
                            if !outgoing_reconstruction_states[up as usize].awaited_by.contains(&shortcut_id) {
                                outgoing_reconstruction_states[up as usize].awaited_by.push(shortcut_id);
                                state.missing_deps += 1;
                            }

                            let new_el = ReconstructionQueueElement {
                                t: second_start,
                                upper_node: self.customized_graph.outgoing.head[up as usize],
                                lower_node: self.customized_graph.outgoing.tail[up as usize],
                                shortcut_id: ShortcutId::Outgoing(up),
                            };
                            if let Some(Reverse(el)) = queue.get(new_el.as_index()) {
                                if &new_el < el {
                                    queue.decrease_key(Reverse(new_el));
                                }
                            } else {
                                queue.push(Reverse(new_el));
                            }
                        }
                    }
                    _ => (),
                }
                c.advance();
            }
        }

        if any_up_missing {
            return;
        }

        if state.missing_deps > 0 {
            return;
        }

        for (start, end) in state.requested_times.drain(..) {
            self.cache(shortcut_id, start, end, buffers);
            debug_assert!(self.ttf_available(shortcut_id, start, end));
        }
        for waiting in state.awaited_by.drain(..) {
            let mut waiting_state = waiting.get_mut_from(incoming_reconstruction_states, outgoing_reconstruction_states);
            waiting_state.missing_deps -= 1;
            if waiting_state.missing_deps == 0 && !waiting_state.requested_times.is_empty() {
                let new_el = ReconstructionQueueElement {
                    t: waiting_state.requested_times[0].0,
                    upper_node: *waiting.get_from(&self.customized_graph.incoming.head, &self.customized_graph.outgoing.head),
                    lower_node: *waiting.get_from(&self.customized_graph.incoming.tail, &self.customized_graph.outgoing.tail),
                    shortcut_id: waiting,
                };
                if let Some(Reverse(el)) = queue.get(new_el.as_index()) {
                    if &new_el < el {
                        queue.decrease_key(Reverse(new_el));
                    }
                } else {
                    queue.push(Reverse(new_el));
                }
            }
        }
    }

    pub fn cache_iterative(
        &mut self,
        queue: &mut IndexdMinHeap<Reverse<ReconstructionQueueElement>>,
        incoming_reconstruction_states: &mut [ReconstructionState],
        outgoing_reconstruction_states: &mut [ReconstructionState],
        buffers: &mut MergeBuffers,
    ) {
        while let Some(Reverse(ReconstructionQueueElement {
            upper_node,
            lower_node,
            shortcut_id,
            ..
        })) = queue.pop()
        {
            debug_assert!(lower_node < upper_node);

            let mut state = Default::default();
            std::mem::swap(
                &mut state,
                shortcut_id.get_mut_from(incoming_reconstruction_states, outgoing_reconstruction_states),
            );

            self.cache_iterative_iteration(
                shortcut_id,
                &mut state,
                queue,
                incoming_reconstruction_states,
                outgoing_reconstruction_states,
                buffers,
            );

            std::mem::swap(
                &mut state,
                shortcut_id.get_mut_from(incoming_reconstruction_states, outgoing_reconstruction_states),
            );
        }
    }

    fn ttf_available(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> bool {
        self.as_reconstructed().ttf_available(shortcut_id, start, end)
    }

    pub fn cache_recursive(&mut self, shortcut_id: ShortcutId, mut start: Timestamp, mut end: Timestamp, buffers: &mut MergeBuffers) {
        if self.as_reconstructed().partial_ttf(shortcut_id, start, end).is_some() || self.as_reconstructed().periodic_ttf(shortcut_id).is_some() {
            return;
        }

        if !cfg!(feature = "tdcch-profiles-with-holes") || FlWeight::from(period()).fuzzy_leq(end - start) {
            start = Timestamp::ZERO;
            end = period();
        }

        let edge_sources = shortcut_id.get_with(
            &self.customized_graph.incoming,
            &self.customized_graph.outgoing,
            CustomizedSingleDirGraph::edge_sources,
        );

        let mut c = SourceCursor::valid_at(edge_sources, start);

        while c.cur().0.fuzzy_lt(end) {
            match ShortcutSource::from(c.cur().1) {
                ShortcutSource::Shortcut(down, _up) => {
                    let first_start = max(start, c.cur().0);
                    let first_end = min(end, c.next().0);
                    self.cache_recursive(ShortcutId::Incoming(down), first_start, first_end, buffers);
                }
                _ => (),
            }
            c.advance();
        }

        let mut c = SourceCursor::valid_at(edge_sources, start);

        while c.cur().0.fuzzy_lt(end) {
            match ShortcutSource::from(c.cur().1) {
                ShortcutSource::Shortcut(down, up) => {
                    let first_start = max(start, c.cur().0);
                    let first_end = min(end, c.next().0);

                    let reconstructed = self.as_reconstructed();
                    let (start_val, end_val) = if let Some(ttf) = reconstructed.partial_ttf(ShortcutId::Incoming(down), first_start, first_end) {
                        (ttf.bound_plfs().0.eval(first_start), ttf.bound_plfs().1.eval(first_end))
                    } else {
                        let ttf = reconstructed.periodic_ttf(ShortcutId::Incoming(down)).unwrap();
                        (ttf.bound_plfs().0.evaluate(first_start), ttf.bound_plfs().1.evaluate(first_end))
                    };
                    let second_start = first_start + start_val;
                    let second_end = first_end + end_val;

                    self.cache_recursive(ShortcutId::Outgoing(up), second_start, second_end, buffers);
                }
                _ => (),
            }
            c.advance();
        }

        self.cache(shortcut_id, start, end, buffers);
    }

    pub fn approximate(&mut self, shortcut_id: ShortcutId, buffers: &mut MergeBuffers) {
        if let Some(cache) = shortcut_id.get_mut_from(&mut self.incoming_cache, &mut self.outgoing_cache) {
            if cache.num_points() > APPROX_THRESHOLD {
                cache.approximate(buffers);
            }
        }
    }

    pub fn clear_recursive(&mut self, shortcut_id: ShortcutId) {
        if self.take_cache(shortcut_id).is_none() {
            return;
        }
        for (_, source) in shortcut_id.get_with(
            &self.customized_graph.incoming,
            &self.customized_graph.outgoing,
            CustomizedSingleDirGraph::edge_sources,
        ) {
            match ShortcutSource::from(*source) {
                ShortcutSource::Shortcut(down, up) => {
                    self.clear_recursive(ShortcutId::Incoming(down));
                    self.clear_recursive(ShortcutId::Outgoing(up));
                }
                _ => (),
            }
        }
    }

    pub fn take_cache(&mut self, shortcut_id: ShortcutId) -> Option<ApproxPartialsContainer<Box<[TTFPoint]>>> {
        shortcut_id.get_mut_from(&mut self.incoming_cache, &mut self.outgoing_cache).take()
    }

    pub fn as_reconstructed(&self) -> ReconstructedGraph {
        ReconstructedGraph {
            customized_graph: self.customized_graph,
            outgoing_cache: self.outgoing_cache,
            incoming_cache: self.incoming_cache,
        }
    }

    pub fn num_points_cached(&self) -> usize {
        self.incoming_cache
            .iter()
            .chain(self.outgoing_cache.iter())
            .flatten()
            .map(|c| c.num_points())
            .sum()
    }

    fn periodic_ttf(&self, shortcut_id: ShortcutId) -> Option<PeriodicATTF> {
        // TODO remove in favor of ReconstructedGraph::periodic_ttf
        if let Some(cache) = shortcut_id.get_from(&self.incoming_cache, &self.outgoing_cache) {
            return cache.ttf(Timestamp::ZERO, period())?.try_into().ok();
        }

        match shortcut_id.get_with(
            &self.customized_graph.incoming,
            &self.customized_graph.outgoing,
            CustomizedSingleDirGraph::edge_sources,
        ) {
            &[(_, source)] => match source.into() {
                ShortcutSource::OriginalEdge(id) => Some(PeriodicATTF::Exact(self.customized_graph.original_graph.travel_time_function(id))),
                _ => None,
            },
            _ => None,
        }
    }
    fn partial_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> Option<PartialATTF> {
        // TODO remove in favor of ReconstructedGraph::partial_ttf
        if let Some(cache) = shortcut_id.get_from(&self.incoming_cache, &self.outgoing_cache) {
            return cache.ttf(start, end);
        }

        match shortcut_id.get_with(
            &self.customized_graph.incoming,
            &self.customized_graph.outgoing,
            CustomizedSingleDirGraph::edge_sources,
        ) {
            &[(_, source)] => match source.into() {
                ShortcutSource::OriginalEdge(id) => self
                    .customized_graph
                    .original_graph
                    .travel_time_function(id)
                    .try_into()
                    .ok()
                    .and_then(|plf: PartialPiecewiseLinearFunction| plf.get_sub_plf(start, end))
                    .map(PartialATTF::Exact),
                _ => None,
            },
            _ => None,
        }
    }
}

#[derive(Debug)]
pub struct ReconstructedGraph<'a> {
    pub customized_graph: &'a CustomizedGraph<'a>,
    pub outgoing_cache: &'a [Option<ApproxPartialsContainer<Box<[TTFPoint]>>>],
    pub incoming_cache: &'a [Option<ApproxPartialsContainer<Box<[TTFPoint]>>>],
}

impl<'a> ReconstructedGraph<'a> {
    fn all_sources_exact(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> bool {
        let mut c = SourceCursor::valid_at(
            shortcut_id.get_with(
                &self.customized_graph.incoming,
                &self.customized_graph.outgoing,
                CustomizedSingleDirGraph::edge_sources,
            ),
            start,
        );

        while c.cur().0.fuzzy_lt(end) {
            match ShortcutSource::from(c.cur().1) {
                ShortcutSource::Shortcut(down, up) => {
                    if !(self.exact_ttf_available(ShortcutId::Incoming(down)) && self.exact_ttf_available(ShortcutId::Outgoing(up))) {
                        return false;
                    }
                }
                _ => (),
            }
            c.advance();
        }

        return true;
    }

    fn exact_ttf_available(&self, shortcut_id: ShortcutId) -> bool {
        if let Some(cache) = shortcut_id.get_from(&self.incoming_cache, &self.outgoing_cache) {
            return cache.exact();
        }

        match shortcut_id.get_with(
            &self.customized_graph.incoming,
            &self.customized_graph.outgoing,
            CustomizedSingleDirGraph::edge_sources,
        ) {
            &[(_, source)] => match source.into() {
                ShortcutSource::OriginalEdge(_) => true,
                _ => false,
            },
            _ => false,
        }
    }
}

impl<'a> ShortcutGraphTrt for ReconstructedGraph<'a> {
    type OriginalGraph = TDGraph;

    fn periodic_ttf(&self, shortcut_id: ShortcutId) -> Option<PeriodicATTF> {
        if let Some(cache) = shortcut_id.get_from(&self.incoming_cache, &self.outgoing_cache) {
            return cache.ttf(Timestamp::ZERO, period())?.try_into().ok();
        }

        match shortcut_id.get_with(
            &self.customized_graph.incoming,
            &self.customized_graph.outgoing,
            CustomizedSingleDirGraph::edge_sources,
        ) {
            &[(_, source)] => match source.into() {
                ShortcutSource::OriginalEdge(id) => Some(PeriodicATTF::Exact(self.customized_graph.original_graph.travel_time_function(id))),
                _ => None,
            },
            _ => None,
        }
    }
    fn partial_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> Option<PartialATTF> {
        if let Some(cache) = shortcut_id.get_from(&self.incoming_cache, &self.outgoing_cache) {
            return cache.ttf(start, end);
        }

        match shortcut_id.get_with(
            &self.customized_graph.incoming,
            &self.customized_graph.outgoing,
            CustomizedSingleDirGraph::edge_sources,
        ) {
            &[(_, source)] => match source.into() {
                ShortcutSource::OriginalEdge(id) => self
                    .customized_graph
                    .original_graph
                    .travel_time_function(id)
                    .try_into()
                    .ok()
                    .and_then(|plf: PartialPiecewiseLinearFunction| plf.get_sub_plf(start, end))
                    .map(PartialATTF::Exact),
                _ => None,
            },
            _ => None,
        }
    }
    fn is_valid_path(&self, _shortcut_id: ShortcutId) -> bool {
        true
    }
    fn lower_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        shortcut_id
            .get_from(&self.customized_graph.incoming.bounds, &self.customized_graph.outgoing.bounds)
            .0
    }
    fn upper_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        shortcut_id
            .get_from(&self.customized_graph.incoming.bounds, &self.customized_graph.outgoing.bounds)
            .1
    }
    fn original_graph(&self) -> &TDGraph {
        &self.customized_graph.original_graph
    }
    fn reconstruct_exact_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage) {
        let (dir_graph, edge_id) = match shortcut_id {
            ShortcutId::Incoming(id) => (&self.customized_graph.incoming, id),
            ShortcutId::Outgoing(id) => (&self.customized_graph.outgoing, id),
        };
        if dir_graph.constant.get(edge_id as usize) {
            debug_assert!(
                dir_graph.bounds[edge_id as usize].0.fuzzy_lt(FlWeight::INFINITY),
                "{:#?}",
                (shortcut_id, dir_graph.bounds[edge_id as usize], dir_graph.edge_sources(edge_id))
            );
            target.push(TTFPoint {
                at: start,
                val: dir_graph.bounds[edge_id as usize].0,
            });
            target.push(TTFPoint {
                at: end,
                val: dir_graph.bounds[edge_id as usize].0,
            });
            return;
        }
        match dir_graph.edge_sources(edge_id) {
            &[] => unreachable!("There are no TTFs for empty shortcuts"),
            &[(_, source)] => ShortcutSource::from(source).reconstruct_exact_ttf(start, end, self, target, tmp),
            sources => PeriodicSources(sources).reconstruct_exact_ttf(start, end, self, target, tmp),
        }
    }
    fn get_switchpoints(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> (Vec<(Timestamp, Vec<EdgeId>, FlWeight)>, FlWeight) {
        match shortcut_id.get_with(
            &self.customized_graph.incoming,
            &self.customized_graph.outgoing,
            CustomizedSingleDirGraph::edge_sources,
        ) {
            &[] => unreachable!("There are no switchpoints for empty shortcuts"),
            &[(_, source)] => ShortcutSource::from(source).get_switchpoints(start, end, self),
            sources => PeriodicSources(sources).get_switchpoints(start, end, self),
        }
    }
    fn unpack_at(&self, shortcut_id: ShortcutId, t: Timestamp, result: &mut Vec<(EdgeId, Timestamp)>) {
        ShortcutSource::from(
            shortcut_id
                .get_with(&self.customized_graph.incoming, &self.customized_graph.outgoing, |g_dir, id| {
                    g_dir.edge_source_at(id, t)
                })
                .unwrap(),
        )
        .unpack_at(t, self, result)
    }
    fn evaluate(&self, shortcut_id: ShortcutId, t: Timestamp) -> FlWeight {
        ShortcutSource::from(
            shortcut_id
                .get_with(&self.customized_graph.incoming, &self.customized_graph.outgoing, |g_dir, id| {
                    g_dir.edge_source_at(id, t)
                })
                .unwrap(),
        )
        .evaluate(t, self)
    }
}

pub struct ProfileGraphWrapper<'a> {
    pub profile_graph: ReconstructedGraph<'a>,
    pub down_shortcuts: &'a mut [Shortcut],
    pub up_shortcuts: &'a mut [Shortcut],
}

impl<'a> ProfileGraphWrapper<'a> {
    fn delegate(&self, shortcut_id: ShortcutId) -> bool {
        shortcut_id.get_with(
            &self.profile_graph.customized_graph.incoming,
            &self.profile_graph.customized_graph.outgoing,
            |g_dir, id| (id as usize) < g_dir.head.len(),
        )
    }

    fn get(&self, shortcut_id: ShortcutId) -> &Shortcut {
        match shortcut_id {
            ShortcutId::Incoming(id) => &self.down_shortcuts[(id as usize) - self.profile_graph.customized_graph.incoming.head.len()],
            ShortcutId::Outgoing(id) => &self.up_shortcuts[(id as usize) - self.profile_graph.customized_graph.outgoing.head.len()],
        }
    }
}

impl<'a> ShortcutGraphTrt for ProfileGraphWrapper<'a> {
    type OriginalGraph = TDGraph;

    fn periodic_ttf(&self, shortcut_id: ShortcutId) -> Option<PeriodicATTF> {
        if self.delegate(shortcut_id) {
            return self.profile_graph.periodic_ttf(shortcut_id);
        }
        self.get(shortcut_id).periodic_ttf(self)
    }
    fn partial_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> Option<PartialATTF> {
        if self.delegate(shortcut_id) {
            return self.profile_graph.partial_ttf(shortcut_id, start, end);
        }
        self.get(shortcut_id).partial_ttf(self, start, end)
    }
    fn is_valid_path(&self, _shortcut_id: ShortcutId) -> bool {
        true
    }
    fn lower_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        if self.delegate(shortcut_id) {
            return self.profile_graph.lower_bound(shortcut_id);
        }
        self.get(shortcut_id).lower_bound
    }
    fn upper_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        if self.delegate(shortcut_id) {
            return self.profile_graph.upper_bound(shortcut_id);
        }
        self.get(shortcut_id).upper_bound
    }
    fn original_graph(&self) -> &TDGraph {
        &self.profile_graph.customized_graph.original_graph
    }
    fn reconstruct_exact_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage) {
        if self.delegate(shortcut_id) {
            return self.profile_graph.reconstruct_exact_ttf(shortcut_id, start, end, target, tmp);
        }
        self.get(shortcut_id).reconstruct_exact_ttf(start, end, self, target, tmp)
    }
    fn get_switchpoints(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> (Vec<(Timestamp, Vec<EdgeId>, FlWeight)>, FlWeight) {
        if self.delegate(shortcut_id) {
            return self.profile_graph.get_switchpoints(shortcut_id, start, end);
        }
        self.get(shortcut_id).get_switchpoints(start, end, self)
    }
    fn unpack_at(&self, shortcut_id: ShortcutId, t: Timestamp, result: &mut Vec<(EdgeId, Timestamp)>) {
        if self.delegate(shortcut_id) {
            return self.profile_graph.unpack_at(shortcut_id, t, result);
        }
        self.get(shortcut_id).unpack_at(t, self, result);
    }
    fn evaluate(&self, shortcut_id: ShortcutId, t: Timestamp) -> FlWeight {
        if self.delegate(shortcut_id) {
            return self.profile_graph.evaluate(shortcut_id, t);
        }
        self.get(shortcut_id).evaluate(t, self)
    }
}

pub struct PartialProfileGraphWrapper<'a> {
    pub profile_graph: &'a mut ReconstructionGraph<'a>,
    pub down_shortcuts: &'a mut [Vec<PartialShortcut>],
    pub up_shortcuts: &'a mut [Vec<PartialShortcut>],
}

impl<'a> PartialProfileGraphWrapper<'a> {
    fn delegate(&self, shortcut_id: ShortcutId) -> bool {
        shortcut_id.get_with(
            &self.profile_graph.customized_graph.incoming,
            &self.profile_graph.customized_graph.outgoing,
            |g_dir, id| (id as usize) < g_dir.head.len(),
        )
    }

    fn is_dummy(&self, shortcut_id: ShortcutId) -> bool {
        match shortcut_id {
            ShortcutId::Incoming(id) => self.down_shortcuts.len() + self.profile_graph.customized_graph.incoming.head.len() == id as usize,
            ShortcutId::Outgoing(id) => self.up_shortcuts.len() + self.profile_graph.customized_graph.outgoing.head.len() == id as usize,
        }
    }

    fn get(&self, shortcut_id: ShortcutId) -> &Vec<PartialShortcut> {
        match shortcut_id {
            ShortcutId::Incoming(id) => &self.down_shortcuts[(id as usize) - self.profile_graph.customized_graph.incoming.head.len()],
            ShortcutId::Outgoing(id) => &self.up_shortcuts[(id as usize) - self.profile_graph.customized_graph.outgoing.head.len()],
        }
    }

    fn get_mut(&mut self, shortcut_id: ShortcutId) -> &mut Vec<PartialShortcut> {
        match shortcut_id {
            ShortcutId::Incoming(id) => &mut self.down_shortcuts[(id as usize) - self.profile_graph.customized_graph.incoming.head.len()],
            ShortcutId::Outgoing(id) => &mut self.up_shortcuts[(id as usize) - self.profile_graph.customized_graph.outgoing.head.len()],
        }
    }

    fn get_for_time(&self, shortcut_id: ShortcutId, t: Timestamp) -> Option<&PartialShortcut> {
        let partials = self.get(shortcut_id);
        let pos = partials.binary_search_by(|s| {
            use std::cmp::Ordering;
            if t.fuzzy_lt(s.start) {
                Ordering::Greater
            } else if s.end.fuzzy_lt(t) {
                Ordering::Less
            } else {
                Ordering::Equal
            }
        });
        pos.ok().map(|idx| &partials[idx])
    }

    fn get_for_time_range(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> Option<&PartialShortcut> {
        self.get_for_time(shortcut_id, start).filter(|s| end.fuzzy_leq(s.end))
    }

    pub fn cache_iterative(
        &mut self,
        queue: &mut IndexdMinHeap<Reverse<ReconstructionQueueElement>>,
        incoming_reconstruction_states: &mut [ReconstructionState],
        outgoing_reconstruction_states: &mut [ReconstructionState],
        buffers: &mut MergeBuffers,
        mut for_each_lower_triangle_of: impl FnMut(ShortcutId, &mut dyn FnMut(EdgeId, EdgeId, NodeId)),
        mut bounds: impl FnMut(ShortcutId) -> (FlWeight, FlWeight),
        mut lower_and_upper_node: impl FnMut(ShortcutId) -> (NodeId, NodeId),
    ) {
        while let Some(Reverse(ReconstructionQueueElement {
            shortcut_id,
            lower_node,
            upper_node,
            ..
        })) = queue.pop()
        {
            debug_assert!(lower_node < upper_node);

            let mut state = Default::default();
            std::mem::swap(
                &mut state,
                shortcut_id.get_mut_from(incoming_reconstruction_states, outgoing_reconstruction_states),
            );

            for &(start, end) in &state.requested_times {
                debug_assert!(end - start < FlWeight::new(3600.0 * 3.0));
            }

            // dbg!(shortcut_id, &state);
            // if matches!(shortcut_id, ShortcutId::Outgoing(127119)) {
            // }

            if self.delegate(shortcut_id) {
                self.cache_orig_shortcut_iterative_iteration(
                    shortcut_id,
                    &mut state,
                    queue,
                    incoming_reconstruction_states,
                    outgoing_reconstruction_states,
                    buffers,
                    &mut lower_and_upper_node,
                );
            } else {
                self.cache_iterative_iteration(
                    shortcut_id,
                    shortcut_id.get_with(upper_node, lower_node, |tail, _| tail),
                    shortcut_id.get_with(lower_node, upper_node, |head, _| head),
                    &mut state,
                    queue,
                    incoming_reconstruction_states,
                    outgoing_reconstruction_states,
                    buffers,
                    &mut for_each_lower_triangle_of,
                    &mut bounds,
                    &mut lower_and_upper_node,
                )
            }

            std::mem::swap(
                &mut state,
                shortcut_id.get_mut_from(incoming_reconstruction_states, outgoing_reconstruction_states),
            );
        }
    }

    pub fn cache_iterative_iteration(
        &mut self,
        shortcut_id: ShortcutId,
        tail: NodeId,
        head: NodeId,
        state: &mut ReconstructionState,
        queue: &mut IndexdMinHeap<Reverse<ReconstructionQueueElement>>,
        incoming_reconstruction_states: &mut [ReconstructionState],
        outgoing_reconstruction_states: &mut [ReconstructionState],
        buffers: &mut MergeBuffers,
        for_each_lower_triangle_of: &mut impl FnMut(ShortcutId, &mut dyn FnMut(EdgeId, EdgeId, NodeId)),
        bounds: &mut impl FnMut(ShortcutId) -> (FlWeight, FlWeight),
        lower_and_upper_node: &mut impl FnMut(ShortcutId) -> (NodeId, NodeId),
    ) {
        let mut any_down_missing = false;
        for &(start, end) in &state.requested_times {
            let mut deps_to_add = 0;
            for_each_lower_triangle_of(shortcut_id, &mut |down, up, middle_node| {
                if !self.ttf_available(ShortcutId::Incoming(down), start, end) {
                    any_down_missing = true;
                    ReconstructionState::request_time(&mut incoming_reconstruction_states[down as usize].requested_times, start, end);
                    if !incoming_reconstruction_states[down as usize].awaited_by.contains(&shortcut_id) {
                        incoming_reconstruction_states[down as usize].awaited_by.push(shortcut_id);
                        deps_to_add += 1;
                    }

                    debug_assert!(middle_node < tail);
                    let new_el = ReconstructionQueueElement {
                        t: start,
                        upper_node: tail,
                        lower_node: middle_node,
                        shortcut_id: ShortcutId::Incoming(down),
                    };
                    if let Some(Reverse(el)) = queue.get(new_el.as_index()) {
                        if &new_el < el {
                            queue.decrease_key(Reverse(new_el));
                        }
                    } else {
                        queue.push(Reverse(new_el));
                    }

                    if !incoming_reconstruction_states[down as usize].awaited_by.contains(&ShortcutId::Outgoing(up)) {
                        incoming_reconstruction_states[down as usize].awaited_by.push(ShortcutId::Outgoing(up));
                        outgoing_reconstruction_states[up as usize].missing_deps += 1;
                    }
                }
            });
            state.missing_deps += deps_to_add;
        }
        if any_down_missing {
            return;
        }

        let mut any_up_missing = false;
        for &(start, end) in &state.requested_times {
            let mut deps_to_add = 0;
            for_each_lower_triangle_of(shortcut_id, &mut |down, up, middle_node| {
                let (start_val, end_val) = if let Some(ttf) = self.partial_ttf(ShortcutId::Incoming(down), start, end) {
                    (ttf.bound_plfs().0.eval(start), ttf.bound_plfs().1.eval(end))
                } else {
                    let ttf = self.periodic_ttf(ShortcutId::Incoming(down)).unwrap();
                    (ttf.bound_plfs().0.evaluate(start), ttf.bound_plfs().1.evaluate(end))
                };
                let second_start = start + start_val;
                let second_end = end + end_val;

                if !self.ttf_available(ShortcutId::Outgoing(up), second_start, second_end) {
                    any_up_missing = true;

                    ReconstructionState::request_time(&mut outgoing_reconstruction_states[up as usize].requested_times, second_start, second_end);
                    if !outgoing_reconstruction_states[up as usize].awaited_by.contains(&shortcut_id) {
                        outgoing_reconstruction_states[up as usize].awaited_by.push(shortcut_id);
                        deps_to_add += 1;
                    }

                    debug_assert!(middle_node < head);
                    let new_el = ReconstructionQueueElement {
                        t: second_start,
                        upper_node: head,
                        lower_node: middle_node,
                        shortcut_id: ShortcutId::Outgoing(up),
                    };
                    if let Some(Reverse(el)) = queue.get(new_el.as_index()) {
                        if &new_el < el {
                            queue.decrease_key(Reverse(new_el));
                        }
                    } else {
                        queue.push(Reverse(new_el));
                    }
                }
            });
            state.missing_deps += deps_to_add;
        }

        if any_up_missing {
            return;
        }

        if state.missing_deps > 0 {
            return;
        }

        for (start, end) in state.requested_times.drain(..) {
            let bounds = bounds(shortcut_id);
            // if matches!(shortcut_id, ShortcutId::Outgoing(127119)) {
            //     dbg!(shortcut_id, start, end, bounds);
            // }
            let inserts: Vec<_> = PartialTrt::missing(self.get(shortcut_id), start, end)
                .into_iter()
                .map(|(start, end)| {
                    debug_assert!(start.fuzzy_lt(end));
                    let mut shortcut = PartialShortcut::new_finished(start, end, &[], bounds);

                    let mut triangles = Vec::new();
                    for_each_lower_triangle_of(shortcut_id, &mut |down, up, _| {
                        triangles.push((
                            (down, up),
                            self.lower_bound(ShortcutId::Incoming(down)) + self.lower_bound(ShortcutId::Outgoing(up)),
                        ));
                    });
                    triangles.sort_by_key(|&(_, bound)| bound);
                    for &(triangle, _) in &triangles {
                        shortcut.merge(triangle, self, buffers);
                    }
                    shortcut.finalize_bounds(self);
                    debug_assert!(
                        shortcut.partial_ttf(self, start, end).is_some() || !shortcut.is_valid_path(),
                        "{:#?}",
                        (start, end, shortcut_id, triangles, shortcut)
                    );
                    shortcut
                })
                .collect();
            // if matches!(shortcut_id, ShortcutId::Outgoing(127119)) {
            //     dbg!(&inserts);
            // }
            PartialTrt::insert_all(self.get_mut(shortcut_id), inserts);
            debug_assert!(self.ttf_available(shortcut_id, start, end));
        }
        for waiting in state.awaited_by.drain(..) {
            let mut waiting_state = waiting.get_mut_from(incoming_reconstruction_states, outgoing_reconstruction_states);
            waiting_state.missing_deps -= 1;
            if waiting_state.missing_deps == 0 && !waiting_state.requested_times.is_empty() {
                let (lower_node, upper_node) = if self.delegate(waiting) {
                    (
                        self.profile_graph.customized_graph.lower_node(waiting),
                        self.profile_graph.customized_graph.upper_node(waiting),
                    )
                } else {
                    lower_and_upper_node(waiting)
                };
                debug_assert!(lower_node < upper_node);
                let new_el = ReconstructionQueueElement {
                    t: waiting_state.requested_times[0].0,
                    upper_node,
                    lower_node,
                    shortcut_id: waiting,
                };
                if let Some(Reverse(el)) = queue.get(new_el.as_index()) {
                    if &new_el < el {
                        queue.decrease_key(Reverse(new_el));
                    }
                } else {
                    queue.push(Reverse(new_el));
                }
            }
        }
    }

    pub fn cache_orig_shortcut_iterative_iteration(
        &mut self,
        shortcut_id: ShortcutId,
        state: &mut ReconstructionState,
        queue: &mut IndexdMinHeap<Reverse<ReconstructionQueueElement>>,
        incoming_reconstruction_states: &mut [ReconstructionState],
        outgoing_reconstruction_states: &mut [ReconstructionState],
        buffers: &mut MergeBuffers,
        lower_and_upper_node: &mut impl FnMut(ShortcutId) -> (NodeId, NodeId),
    ) {
        let edge_sources = shortcut_id.get_with(
            &self.profile_graph.customized_graph.incoming,
            &self.profile_graph.customized_graph.outgoing,
            CustomizedSingleDirGraph::edge_sources,
        );

        let mut any_down_missing = false;
        for &(start, end) in &state.requested_times {
            let mut c = SourceCursor::valid_at(edge_sources, start);

            while c.cur().0.fuzzy_lt(end) {
                match ShortcutSource::from(c.cur().1) {
                    ShortcutSource::Shortcut(down, up) => {
                        let first_start = max(start, c.cur().0);
                        let first_end = min(end, c.next().0);

                        if !self.ttf_available(ShortcutId::Incoming(down), first_start, first_end) {
                            any_down_missing = true;
                            ReconstructionState::request_time(&mut incoming_reconstruction_states[down as usize].requested_times, first_start, first_end);
                            if !incoming_reconstruction_states[down as usize].awaited_by.contains(&shortcut_id) {
                                incoming_reconstruction_states[down as usize].awaited_by.push(shortcut_id);
                                state.missing_deps += 1;
                            }

                            let new_el = ReconstructionQueueElement {
                                t: start,
                                upper_node: self.profile_graph.customized_graph.incoming.head[down as usize],
                                lower_node: self.profile_graph.customized_graph.incoming.tail[down as usize],
                                shortcut_id: ShortcutId::Incoming(down),
                            };
                            if let Some(Reverse(el)) = queue.get(new_el.as_index()) {
                                if &new_el < el {
                                    queue.decrease_key(Reverse(new_el));
                                }
                            } else {
                                queue.push(Reverse(new_el));
                            }

                            if !incoming_reconstruction_states[down as usize].awaited_by.contains(&ShortcutId::Outgoing(up)) {
                                incoming_reconstruction_states[down as usize].awaited_by.push(ShortcutId::Outgoing(up));
                                outgoing_reconstruction_states[up as usize].missing_deps += 1;
                            }
                        }
                    }
                    _ => (),
                }
                c.advance();
            }
        }
        if any_down_missing {
            return;
        }

        let mut any_up_missing = false;
        for &(start, end) in &state.requested_times {
            let mut c = SourceCursor::valid_at(edge_sources, start);

            while c.cur().0.fuzzy_lt(end) {
                match ShortcutSource::from(c.cur().1) {
                    ShortcutSource::Shortcut(down, up) => {
                        let first_start = max(start, c.cur().0);
                        let first_end = min(end, c.next().0);

                        let reconstructed = self.profile_graph.as_reconstructed();
                        let (start_val, end_val) = if let Some(ttf) = reconstructed.partial_ttf(ShortcutId::Incoming(down), first_start, first_end) {
                            (ttf.bound_plfs().0.eval(first_start), ttf.bound_plfs().1.eval(first_end))
                        } else {
                            let ttf = reconstructed.periodic_ttf(ShortcutId::Incoming(down)).unwrap();
                            (ttf.bound_plfs().0.evaluate(first_start), ttf.bound_plfs().1.evaluate(first_end))
                        };
                        let second_start = first_start + start_val;
                        let second_end = first_end + end_val;

                        if !self.ttf_available(ShortcutId::Outgoing(up), second_start, second_end) {
                            any_up_missing = true;

                            ReconstructionState::request_time(&mut outgoing_reconstruction_states[up as usize].requested_times, second_start, second_end);
                            if !outgoing_reconstruction_states[up as usize].awaited_by.contains(&shortcut_id) {
                                outgoing_reconstruction_states[up as usize].awaited_by.push(shortcut_id);
                                state.missing_deps += 1;
                            }

                            let new_el = ReconstructionQueueElement {
                                t: second_start,
                                upper_node: self.profile_graph.customized_graph.outgoing.head[up as usize],
                                lower_node: self.profile_graph.customized_graph.outgoing.tail[up as usize],
                                shortcut_id: ShortcutId::Outgoing(up),
                            };
                            if let Some(Reverse(el)) = queue.get(new_el.as_index()) {
                                if &new_el < el {
                                    queue.decrease_key(Reverse(new_el));
                                }
                            } else {
                                queue.push(Reverse(new_el));
                            }
                        }
                    }
                    _ => (),
                }
                c.advance();
            }
        }

        if any_up_missing {
            return;
        }

        if state.missing_deps > 0 {
            return;
        }

        for (start, end) in state.requested_times.drain(..) {
            self.profile_graph.cache(shortcut_id, start, end, buffers);
            debug_assert!(self.ttf_available(shortcut_id, start, end));
        }
        for waiting in state.awaited_by.drain(..) {
            let mut waiting_state = waiting.get_mut_from(incoming_reconstruction_states, outgoing_reconstruction_states);
            waiting_state.missing_deps -= 1;
            if waiting_state.missing_deps == 0 && !waiting_state.requested_times.is_empty() {
                let (lower_node, upper_node) = if self.delegate(waiting) {
                    (
                        self.profile_graph.customized_graph.lower_node(waiting),
                        self.profile_graph.customized_graph.upper_node(waiting),
                    )
                } else {
                    lower_and_upper_node(waiting)
                };
                debug_assert!(lower_node < upper_node);
                let new_el = ReconstructionQueueElement {
                    t: waiting_state.requested_times[0].0,
                    upper_node,
                    lower_node,
                    shortcut_id: waiting,
                };
                if let Some(Reverse(el)) = queue.get(new_el.as_index()) {
                    if &new_el < el {
                        queue.decrease_key(Reverse(new_el));
                    }
                } else {
                    queue.push(Reverse(new_el));
                }
            }
        }
    }
}

impl<'a> ShortcutGraphTrt for PartialProfileGraphWrapper<'a> {
    type OriginalGraph = TDGraph;

    fn periodic_ttf(&self, shortcut_id: ShortcutId) -> Option<PeriodicATTF> {
        if self.delegate(shortcut_id) {
            return self.profile_graph.periodic_ttf(shortcut_id);
        }
        if self.is_dummy(shortcut_id) {
            Some(PeriodicATTF::Exact(PeriodicPiecewiseLinearFunction::ZERO))
        } else {
            self.get_for_time_range(shortcut_id, Timestamp::ZERO, period())
                .and_then(|shortcut| shortcut.periodic_ttf(self))
        }
    }
    fn partial_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> Option<PartialATTF> {
        if self.delegate(shortcut_id) {
            return self.profile_graph.partial_ttf(shortcut_id, start, end);
        }
        if self.is_dummy(shortcut_id) {
            Some(PartialATTF::Exact(PartialPiecewiseLinearFunction::ZERO))
        } else {
            self.get_for_time_range(shortcut_id, start, end)
                .and_then(|shortcut| shortcut.partial_ttf(self, start, end))
        }
    }
    fn is_valid_path(&self, shortcut_id: ShortcutId) -> bool {
        if self.delegate(shortcut_id) {
            return self.profile_graph.as_reconstructed().is_valid_path(shortcut_id);
        }
        if self.is_dummy(shortcut_id) {
            true
        } else {
            self.get(shortcut_id).iter().any(|s| s.is_valid_path())
        }
    }
    fn lower_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        if self.delegate(shortcut_id) {
            return self.profile_graph.as_reconstructed().lower_bound(shortcut_id);
        }
        if self.is_dummy(shortcut_id) {
            FlWeight::ZERO
        } else {
            self.get(shortcut_id).iter().map(|s| s.lower_bound).min().unwrap()
        }
    }
    fn upper_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        if self.delegate(shortcut_id) {
            return self.profile_graph.as_reconstructed().upper_bound(shortcut_id);
        }
        if self.is_dummy(shortcut_id) {
            FlWeight::ZERO
        } else {
            self.get(shortcut_id).iter().map(|s| s.upper_bound).max().unwrap()
        }
    }
    fn original_graph(&self) -> &TDGraph {
        &self.profile_graph.customized_graph.original_graph
    }
    fn reconstruct_exact_ttf(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage) {
        if self.delegate(shortcut_id) {
            return self
                .profile_graph
                .as_reconstructed()
                .reconstruct_exact_ttf(shortcut_id, start, end, target, tmp);
        }
        if self.is_dummy(shortcut_id) {
            target.push(TTFPoint {
                at: start,
                val: FlWeight::ZERO,
            });
            target.push(TTFPoint { at: end, val: FlWeight::ZERO });
        } else {
            self.get_for_time_range(shortcut_id, start, end)
                .unwrap()
                .reconstruct_exact_ttf(start, end, self, target, tmp)
        }
    }
    fn get_switchpoints(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp) -> (Vec<(Timestamp, Vec<EdgeId>, FlWeight)>, FlWeight) {
        if self.delegate(shortcut_id) {
            return self.profile_graph.as_reconstructed().get_switchpoints(shortcut_id, start, end);
        }
        if self.is_dummy(shortcut_id) {
            (vec![(start, vec![], FlWeight::ZERO)], FlWeight::ZERO)
        } else {
            self.get_for_time_range(shortcut_id, start, end).unwrap().get_switchpoints(start, end, self)
        }
    }
    fn unpack_at(&self, shortcut_id: ShortcutId, t: Timestamp, result: &mut Vec<(EdgeId, Timestamp)>) {
        if self.delegate(shortcut_id) {
            return self.profile_graph.as_reconstructed().unpack_at(shortcut_id, t, result);
        }
        if !self.is_dummy(shortcut_id) {
            self.get_for_time(shortcut_id, t).unwrap().unpack_at(t, self, result);
        }
    }
    fn evaluate(&self, shortcut_id: ShortcutId, t: Timestamp) -> FlWeight {
        if self.delegate(shortcut_id) {
            return self.profile_graph.as_reconstructed().evaluate(shortcut_id, t);
        }
        if self.is_dummy(shortcut_id) {
            FlWeight::ZERO
        } else {
            self.get_for_time(shortcut_id, t).unwrap().evaluate(t, self)
        }
    }
}
