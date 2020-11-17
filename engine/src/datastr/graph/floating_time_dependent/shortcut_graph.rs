//! Graph structs used during and after the customization.

use super::*;
use crate::datastr::clearlist_vector::ClearlistVector;
use crate::datastr::graph::first_out_graph::degrees_to_first_out;
use crate::datastr::rank_select_map::*;
use crate::io::*;
use crate::util::*;
use std::cmp::{max, min};

#[derive(Debug, Clone, Copy)]
pub enum ShortcutId {
    Outgoing(EdgeId),
    Incoming(EdgeId),
}

pub trait ShortcutGraphTrt<'a> {
    type ApproxTTF;
    type OriginalGraph: for<'g> TDGraphTrait<'g>;

    fn ttf(&'a self, shortcut_id: ShortcutId) -> Self::ApproxTTF;
    fn is_valid_path(&self, shortcut_id: ShortcutId) -> bool;
    fn lower_bound(&self, shortcut_id: ShortcutId) -> FlWeight;
    fn upper_bound(&self, shortcut_id: ShortcutId) -> FlWeight;
    fn original_graph(&self) -> &Self::OriginalGraph;
    fn exact_ttf_for(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage);
    fn get_switchpoints(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, switchpoints: &mut Vec<Timestamp>) -> (FlWeight, FlWeight);
    fn unpack_at(&self, shortcut_id: ShortcutId, t: Timestamp, result: &mut Vec<(EdgeId, Timestamp)>);
    fn evaluate(&self, shortcut_id: ShortcutId, t: Timestamp) -> FlWeight;
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

impl<'a> ShortcutGraphTrt<'a> for PartialShortcutGraph<'a> {
    type ApproxTTF = ApproxTTF<'a>;
    type OriginalGraph = TDGraph;

    fn ttf(&'a self, shortcut_id: ShortcutId) -> Self::ApproxTTF {
        self.get(shortcut_id).travel_time_function(self)
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
    fn exact_ttf_for(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage) {
        self.get(shortcut_id).exact_ttf_for(start, end, self, target, tmp)
    }
    fn get_switchpoints(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, switchpoints: &mut Vec<Timestamp>) -> (FlWeight, FlWeight) {
        self.get(shortcut_id).get_switchpoints(start, end, self, switchpoints)
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

impl<'a> ShortcutGraphTrt<'a> for PartialLiveShortcutGraph<'a> {
    type ApproxTTF = ApproxPartialTTF<'a>;
    type OriginalGraph = LiveGraph;

    fn ttf(&'a self, shortcut_id: ShortcutId) -> Self::ApproxTTF {
        self.get_live(shortcut_id).travel_time_function(self)
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
    fn exact_ttf_for(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage) {
        let live = self.get_live(shortcut_id);
        if live.live_until.map(|l| !start.fuzzy_lt(l)).unwrap_or(true) {
            self.get(shortcut_id).exact_ttf_for(start, end, self, target, tmp);
        } else {
            let live_until = live.live_until.unwrap();
            if !live_until.fuzzy_lt(end) {
                live.exact_ttf_for(start, end, self, target, tmp);
            } else {
                live.exact_ttf_for(start, live_until, self, target, tmp);
                let mut sub_target = tmp.push_plf();
                self.get(shortcut_id)
                    .exact_ttf_for(live_until, end, self, &mut sub_target, target.storage_mut());
                PartialPiecewiseLinearFunction::new(&sub_target).append(live_until, target);
            }
        }
    }
    fn get_switchpoints(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, switchpoints: &mut Vec<Timestamp>) -> (FlWeight, FlWeight) {
        let live = self.get_live(shortcut_id);
        if live.live_until.map(|l| !start.fuzzy_lt(l)).unwrap_or(true) {
            self.get(shortcut_id).get_switchpoints(start, end, self, switchpoints)
        } else {
            let live_until = live.live_until.unwrap();
            if !live_until.fuzzy_lt(end) {
                live.get_switchpoints(start, end, self, switchpoints)
            } else {
                let (ttf_at_start, _) = live.get_switchpoints(start, live_until, self, switchpoints);
                let (_, ttf_at_end) = self.get(shortcut_id).get_switchpoints(live_until, end, self, switchpoints);
                (ttf_at_start, ttf_at_end)
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
                if let &[(_, source)] = self.incoming.edge_sources(edge as usize) {
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
                if let &[(_, source)] = self.outgoing.edge_sources(edge as usize) {
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
                self.edge_source_at(edge_idx, t)
                    .map(|&source| ShortcutSource::from(source).evaluate(t, customized_graph))
                    .unwrap_or(FlWeight::INFINITY),
                "{:?}, {:?}, {}",
                self.bounds[edge_idx],
                self.edge_sources(edge_idx),
                edge_id
            );
            return self.bounds[edge_idx].0;
        }

        self.edge_source_at(edge_idx, t)
            .map(|&source| ShortcutSource::from(source).evaluate_opt(t, customized_graph, f))
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
        self.edge_source_at(edge_idx, t).map(|&source| match source.into() {
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
        let edge_idx = edge_id as usize;

        // TODO constant?? pruning with bounds?? avoid duplicate unpacking ops??

        for &(_, source) in self.edge_sources(edge_idx) {
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
        self.edge_source_at(edge_id as usize, t)
            .map(|&source| ShortcutSource::from(source).unpack_at(t, customized_graph, result))
            .expect("can't unpack empty shortcut");
    }

    fn edge_source_at(&self, edge_idx: usize, t: Timestamp) -> Option<&ShortcutSourceData> {
        self.edge_sources(edge_idx).edge_source_at(t)
    }

    /// Borrow slice of all the source of the edge with given id.
    pub fn edge_sources(&self, edge_idx: usize) -> &[(Timestamp, ShortcutSourceData)] {
        &self.sources[(self.first_source[edge_idx] as usize)..(self.first_source[edge_idx + 1] as usize)]
    }

    pub fn to_shortcut_vec(&self) -> Vec<Shortcut> {
        (0..self.head.len())
            .map(|edge_idx| Shortcut::new_finished(self.edge_sources(edge_idx), self.bounds[edge_idx], self.constant.get(edge_idx)))
            .collect()
    }

    pub fn to_shortcut(&self, edge_idx: usize) -> Shortcut {
        Shortcut::new_finished(self.edge_sources(edge_idx), self.bounds[edge_idx], self.constant.get(edge_idx))
    }
}

impl<'a> ShortcutGraphTrt<'a> for CustomizedGraph<'a> {
    type ApproxTTF = ();
    type OriginalGraph = TDGraph;

    fn ttf(&self, _: ShortcutId) -> Self::ApproxTTF {
        ()
    }
    fn is_valid_path(&self, _: ShortcutId) -> bool {
        true
    }
    fn lower_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        match shortcut_id {
            ShortcutId::Incoming(id) => self.incoming.bounds[id as usize].0,
            ShortcutId::Outgoing(id) => self.outgoing.bounds[id as usize].0,
        }
    }
    fn upper_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        match shortcut_id {
            ShortcutId::Incoming(id) => self.incoming.bounds[id as usize].1,
            ShortcutId::Outgoing(id) => self.outgoing.bounds[id as usize].1,
        }
    }
    fn original_graph(&self) -> &TDGraph {
        &self.original_graph
    }
    fn exact_ttf_for(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage) {
        match shortcut_id {
            ShortcutId::Incoming(id) => self.incoming.edge_sources(id as usize).exact_ttf_for(start, end, self, target, tmp),
            ShortcutId::Outgoing(id) => self.outgoing.edge_sources(id as usize).exact_ttf_for(start, end, self, target, tmp),
        }
    }
    fn get_switchpoints(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, switchpoints: &mut Vec<Timestamp>) -> (FlWeight, FlWeight) {
        match shortcut_id {
            ShortcutId::Incoming(id) => self.incoming.edge_sources(id as usize).get_switchpoints(start, end, self, switchpoints),
            ShortcutId::Outgoing(id) => self.outgoing.edge_sources(id as usize).get_switchpoints(start, end, self, switchpoints),
        }
    }
    fn unpack_at(&self, shortcut_id: ShortcutId, t: Timestamp, result: &mut Vec<(EdgeId, Timestamp)>) {
        match shortcut_id {
            ShortcutId::Incoming(id) => ShortcutSource::from(*self.incoming.edge_source_at(id as usize, t).unwrap()).unpack_at(t, self, result),
            ShortcutId::Outgoing(id) => ShortcutSource::from(*self.outgoing.edge_source_at(id as usize, t).unwrap()).unpack_at(t, self, result),
        }
    }
    fn evaluate(&self, shortcut_id: ShortcutId, t: Timestamp) -> FlWeight {
        match shortcut_id {
            ShortcutId::Incoming(id) => ShortcutSource::from(*self.incoming.edge_source_at(id as usize, t).unwrap()).evaluate(t, self),
            ShortcutId::Outgoing(id) => ShortcutSource::from(*self.outgoing.edge_source_at(id as usize, t).unwrap()).evaluate(t, self),
        }
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

#[derive(Debug)]
pub struct ReconstructionGraph<'a> {
    pub customized_graph: &'a CustomizedGraph<'a>,
    pub outgoing_cache: &'a mut [Option<ApproxTTFContainer<Box<[TTFPoint]>>>],
    pub incoming_cache: &'a mut [Option<ApproxTTFContainer<Box<[TTFPoint]>>>],
}

impl<'a> ReconstructionGraph<'a> {
    pub fn cache(&mut self, shortcut_id: ShortcutId, buffers: &mut MergeBuffers) {
        let cache = if self.as_reconstructed().all_sources_exact(shortcut_id) {
            let mut target = buffers.unpacking_target.push_plf();
            self.as_reconstructed()
                .exact_ttf_for(shortcut_id, Timestamp::zero(), period(), &mut target, &mut buffers.unpacking_tmp);
            ApproxTTFContainer::Exact(Box::<[TTFPoint]>::from(&target[..]))
        } else {
            let mut target = buffers.unpacking_target.push_plf();

            let (dir_graph, edge_id) = match shortcut_id {
                ShortcutId::Incoming(id) => (&self.customized_graph.incoming, id),
                ShortcutId::Outgoing(id) => (&self.customized_graph.outgoing, id),
            };

            let mut c = SourceCursor::valid_at(dir_graph.edge_sources(edge_id as usize), Timestamp::zero());

            while c.cur().0.fuzzy_lt(period()) {
                let mut inner_target = buffers.unpacking_tmp.push_plf();
                ShortcutSource::from(c.cur().1).partial_lower_bound(
                    max(Timestamp::zero(), c.cur().0),
                    min(period(), c.next().0),
                    &self.as_reconstructed(),
                    &mut inner_target,
                    target.storage_mut(),
                );
                PartialPiecewiseLinearFunction::new(&inner_target[..]).append_bound(max(Timestamp::zero(), c.cur().0), &mut target, min);

                c.advance();
            }

            let mut lower = Box::<[TTFPoint]>::from(&target[..]);
            PeriodicPiecewiseLinearFunction::fifoize_down(&mut lower[..]);
            drop(target);

            let mut target = buffers.unpacking_target.push_plf();

            let mut c = SourceCursor::valid_at(dir_graph.edge_sources(edge_id as usize), Timestamp::zero());

            while c.cur().0.fuzzy_lt(period()) {
                let mut inner_target = buffers.unpacking_tmp.push_plf();
                ShortcutSource::from(c.cur().1).partial_upper_bound(
                    max(Timestamp::zero(), c.cur().0),
                    min(period(), c.next().0),
                    &self.as_reconstructed(),
                    &mut inner_target,
                    target.storage_mut(),
                );
                PartialPiecewiseLinearFunction::new(&inner_target[..]).append_bound(max(Timestamp::zero(), c.cur().0), &mut target, max);

                c.advance();
            }

            let mut upper = Box::<[TTFPoint]>::from(&target[..]);
            PeriodicPiecewiseLinearFunction::fifoize_up(&mut upper[..]);

            ApproxTTFContainer::Approx(lower, upper)
        };

        match shortcut_id {
            ShortcutId::Incoming(id) => self.incoming_cache[id as usize] = Some(cache),
            ShortcutId::Outgoing(id) => self.outgoing_cache[id as usize] = Some(cache),
        };

        self.approximate(shortcut_id, buffers);
    }

    pub fn cache_recursive(&mut self, shortcut_id: ShortcutId, buffers: &mut MergeBuffers) {
        if self.as_reconstructed().get_ttf(shortcut_id).is_some() {
            return;
        }

        let (dir_graph, edge_id) = match shortcut_id {
            ShortcutId::Incoming(id) => (&self.customized_graph.incoming, id),
            ShortcutId::Outgoing(id) => (&self.customized_graph.outgoing, id),
        };
        for (_, source) in dir_graph.edge_sources(edge_id as usize) {
            match ShortcutSource::from(*source) {
                ShortcutSource::Shortcut(down, up) => {
                    self.cache_recursive(ShortcutId::Incoming(down), buffers);
                    self.cache_recursive(ShortcutId::Outgoing(up), buffers);
                }
                _ => (),
            }
        }
        self.cache(shortcut_id, buffers);
    }

    pub fn approximate(&mut self, shortcut_id: ShortcutId, buffers: &mut MergeBuffers) {
        let cache = match shortcut_id {
            ShortcutId::Incoming(id) => self.incoming_cache[id as usize].as_mut(),
            ShortcutId::Outgoing(id) => self.outgoing_cache[id as usize].as_mut(),
        };
        if let Some(cache) = cache {
            if cache.num_points() > APPROX_THRESHOLD {
                *cache = ApproxTTF::from(&*cache).approximate(buffers);
            }
        }
    }

    pub fn clear(&mut self, shortcut_id: ShortcutId) {
        match shortcut_id {
            ShortcutId::Incoming(id) => self.incoming_cache[id as usize] = None,
            ShortcutId::Outgoing(id) => self.outgoing_cache[id as usize] = None,
        };
    }

    pub fn clear_recursive(&mut self, shortcut_id: ShortcutId) {
        if self.as_reconstructed().get_ttf(shortcut_id).is_none() {
            return;
        }
        let (dir_graph, edge_id) = match shortcut_id {
            ShortcutId::Incoming(id) => (&self.customized_graph.incoming, id),
            ShortcutId::Outgoing(id) => (&self.customized_graph.outgoing, id),
        };
        for (_, source) in dir_graph.edge_sources(edge_id as usize) {
            match ShortcutSource::from(*source) {
                ShortcutSource::Shortcut(down, up) => {
                    self.clear_recursive(ShortcutId::Incoming(down));
                    self.clear_recursive(ShortcutId::Outgoing(up));
                }
                _ => (),
            }
        }
        self.clear(shortcut_id);
    }

    pub fn take_cache(&mut self, shortcut_id: ShortcutId) -> Option<ApproxTTFContainer<Box<[TTFPoint]>>> {
        match shortcut_id {
            ShortcutId::Incoming(id) => self.incoming_cache[id as usize].take(),
            ShortcutId::Outgoing(id) => self.outgoing_cache[id as usize].take(),
        }
    }

    pub fn as_reconstructed(&self) -> ReconstructedGraph {
        ReconstructedGraph {
            customized_graph: self.customized_graph,
            outgoing_cache: self.outgoing_cache,
            incoming_cache: self.incoming_cache,
        }
    }
}

#[derive(Debug)]
pub struct ReconstructedGraph<'a> {
    pub customized_graph: &'a CustomizedGraph<'a>,
    pub outgoing_cache: &'a [Option<ApproxTTFContainer<Box<[TTFPoint]>>>],
    pub incoming_cache: &'a [Option<ApproxTTFContainer<Box<[TTFPoint]>>>],
}

impl<'a> ReconstructedGraph<'a> {
    fn get_ttf(&self, shortcut_id: ShortcutId) -> Option<ApproxTTF> {
        let cache = match shortcut_id {
            ShortcutId::Incoming(id) => &self.incoming_cache[id as usize],
            ShortcutId::Outgoing(id) => &self.outgoing_cache[id as usize],
        };

        if let Some(cache) = cache {
            return Some(cache.into());
        }

        let (dir_graph, edge_id) = match shortcut_id {
            ShortcutId::Incoming(id) => (&self.customized_graph.incoming, id),
            ShortcutId::Outgoing(id) => (&self.customized_graph.outgoing, id),
        };

        match dir_graph.edge_sources(edge_id as usize) {
            &[(_, source)] => match source.into() {
                ShortcutSource::OriginalEdge(id) => Some(ApproxTTF::Exact(self.customized_graph.original_graph.travel_time_function(id))),
                _ => None,
            },
            _ => None,
        }
    }

    fn all_sources_exact(&self, shortcut_id: ShortcutId) -> bool {
        let (dir_graph, edge_id) = match shortcut_id {
            ShortcutId::Incoming(id) => (&self.customized_graph.incoming, id),
            ShortcutId::Outgoing(id) => (&self.customized_graph.outgoing, id),
        };

        dir_graph
            .edge_sources(edge_id as usize)
            .iter()
            .all(|(_, source)| match ShortcutSource::from(*source) {
                ShortcutSource::Shortcut(down, up) => self.ttf(ShortcutId::Incoming(down)).exact() && self.ttf(ShortcutId::Outgoing(up)).exact(),
                _ => true,
            })
    }
}

impl<'a> ShortcutGraphTrt<'a> for ReconstructedGraph<'a> {
    type ApproxTTF = ApproxTTF<'a>;
    type OriginalGraph = TDGraph;

    fn ttf(&'a self, shortcut_id: ShortcutId) -> Self::ApproxTTF {
        self.get_ttf(shortcut_id)
            .expect("invalid state of shortcut: ipps must be cached when shortcut not trivial")
    }
    fn is_valid_path(&self, _shortcut_id: ShortcutId) -> bool {
        true
    }
    fn lower_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        match shortcut_id {
            ShortcutId::Incoming(id) => self.customized_graph.incoming.bounds[id as usize],
            ShortcutId::Outgoing(id) => self.customized_graph.outgoing.bounds[id as usize],
        }
        .0
    }
    fn upper_bound(&self, shortcut_id: ShortcutId) -> FlWeight {
        match shortcut_id {
            ShortcutId::Incoming(id) => self.customized_graph.incoming.bounds[id as usize],
            ShortcutId::Outgoing(id) => self.customized_graph.outgoing.bounds[id as usize],
        }
        .1
    }
    fn original_graph(&self) -> &TDGraph {
        &self.customized_graph.original_graph
    }
    fn exact_ttf_for(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage) {
        let (dir_graph, edge_id) = match shortcut_id {
            ShortcutId::Incoming(id) => (&self.customized_graph.incoming, id),
            ShortcutId::Outgoing(id) => (&self.customized_graph.outgoing, id),
        };
        if dir_graph.constant.get(edge_id as usize) {
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
        if let Some(ApproxTTF::Exact(ttf)) = self.get_ttf(shortcut_id) {
            ttf.append_range(start, end, target);
        } else {
            match dir_graph.edge_sources(edge_id as usize) {
                &[] => unreachable!("There are no TTFs for empty shortcuts"),
                &[(_, source)] => ShortcutSource::from(source).exact_ttf_for(start, end, self, target, tmp),
                sources => sources.exact_ttf_for(start, end, self, target, tmp),
            }
        }
    }
    fn get_switchpoints(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, switchpoints: &mut Vec<Timestamp>) -> (FlWeight, FlWeight) {
        let (dir_graph, edge_id) = match shortcut_id {
            ShortcutId::Incoming(id) => (&self.customized_graph.incoming, id),
            ShortcutId::Outgoing(id) => (&self.customized_graph.outgoing, id),
        };
        match dir_graph.edge_sources(edge_id as usize) {
            &[] => unreachable!("There are no switchpoints for empty shortcuts"),
            &[(_, source)] => ShortcutSource::from(source).get_switchpoints(start, end, self, switchpoints),
            sources => sources.get_switchpoints(start, end, self, switchpoints),
        }
    }
    fn unpack_at(&self, shortcut_id: ShortcutId, t: Timestamp, result: &mut Vec<(EdgeId, Timestamp)>) {
        let (dir_graph, edge_id) = match shortcut_id {
            ShortcutId::Incoming(id) => (&self.customized_graph.incoming, id),
            ShortcutId::Outgoing(id) => (&self.customized_graph.outgoing, id),
        };
        ShortcutSource::from(*dir_graph.edge_source_at(edge_id as usize, t).unwrap()).unpack_at(t, self, result)
    }
    fn evaluate(&self, shortcut_id: ShortcutId, t: Timestamp) -> FlWeight {
        let (dir_graph, edge_id) = match shortcut_id {
            ShortcutId::Incoming(id) => (&self.customized_graph.incoming, id),
            ShortcutId::Outgoing(id) => (&self.customized_graph.outgoing, id),
        };
        ShortcutSource::from(*dir_graph.edge_source_at(edge_id as usize, t).unwrap()).evaluate(t, self)
    }
}

pub struct ProfileGraphWrapper<'a> {
    pub profile_graph: ReconstructedGraph<'a>,
    pub down_shortcuts: &'a mut [Shortcut],
    pub up_shortcuts: &'a mut [Shortcut],
}

impl<'a> ProfileGraphWrapper<'a> {
    fn delegate(&self, shortcut_id: ShortcutId) -> bool {
        match shortcut_id {
            ShortcutId::Incoming(id) => (id as usize) < self.profile_graph.customized_graph.incoming.bounds.len(),
            ShortcutId::Outgoing(id) => (id as usize) < self.profile_graph.customized_graph.outgoing.bounds.len(),
        }
    }

    fn get(&self, shortcut_id: ShortcutId) -> &Shortcut {
        match shortcut_id {
            ShortcutId::Incoming(id) => &self.down_shortcuts[(id as usize) - self.profile_graph.customized_graph.incoming.bounds.len()],
            ShortcutId::Outgoing(id) => &self.up_shortcuts[(id as usize) - self.profile_graph.customized_graph.outgoing.bounds.len()],
        }
    }
}

impl<'a> ShortcutGraphTrt<'a> for ProfileGraphWrapper<'a> {
    type ApproxTTF = ApproxTTF<'a>;
    type OriginalGraph = TDGraph;

    fn ttf(&'a self, shortcut_id: ShortcutId) -> Self::ApproxTTF {
        if self.delegate(shortcut_id) {
            return self.profile_graph.ttf(shortcut_id);
        }
        self.get(shortcut_id).travel_time_function(self)
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
    fn exact_ttf_for(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage) {
        if self.delegate(shortcut_id) {
            return self.profile_graph.exact_ttf_for(shortcut_id, start, end, target, tmp);
        }
        self.get(shortcut_id).exact_ttf_for(start, end, self, target, tmp)
    }
    fn get_switchpoints(&self, shortcut_id: ShortcutId, start: Timestamp, end: Timestamp, switchpoints: &mut Vec<Timestamp>) -> (FlWeight, FlWeight) {
        if self.delegate(shortcut_id) {
            return self.profile_graph.get_switchpoints(shortcut_id, start, end, switchpoints);
        }
        self.get(shortcut_id).get_switchpoints(start, end, self, switchpoints)
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
