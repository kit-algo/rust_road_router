use super::*;
use crate::graph::first_out_graph::degrees_to_first_out;
use crate::shortest_path::clearlist_vector::ClearlistVector;
use crate::rank_select_map::*;
use crate::io::*;
use crate::util::*;
use std::cmp::min;
use std::mem::swap;

#[derive(Debug)]
pub struct PartialShortcutGraph<'a> {
    pub original_graph: &'a TDGraph,
    outgoing: &'a [Shortcut],
    incoming: &'a [Shortcut],
    offset: usize
}

impl<'a> PartialShortcutGraph<'a> {
    pub fn new(original_graph: &'a TDGraph, outgoing: &'a [Shortcut], incoming: &'a [Shortcut], offset: usize) -> PartialShortcutGraph<'a> {
        PartialShortcutGraph { original_graph, outgoing, incoming, offset }
    }

    pub fn get_outgoing(&self, edge_id: EdgeId) -> &Shortcut {
        &self.outgoing[edge_id as usize - self.offset]
    }

    pub fn get_incoming(&self, edge_id: EdgeId) -> &Shortcut {
        &self.incoming[edge_id as usize - self.offset]
    }
}

#[derive(Debug)]
pub struct ShortcutGraph<'a> {
    original_graph: &'a TDGraph,
    first_out: &'a [EdgeId],
    head: &'a [NodeId],
    outgoing: Vec<Shortcut>,
    incoming: Vec<Shortcut>,
}

impl<'a> ShortcutGraph<'a> {
    pub fn new(original_graph: &'a TDGraph, first_out: &'a [EdgeId], head: &'a [NodeId], outgoing: Vec<Shortcut>, incoming: Vec<Shortcut>) -> ShortcutGraph<'a> {
        ShortcutGraph { original_graph, first_out, head, outgoing, incoming }
    }

    pub fn get_outgoing(&self, edge_id: EdgeId) -> &Shortcut {
        &self.outgoing[edge_id as usize]
    }

    pub fn get_incoming(&self, edge_id: EdgeId) -> &Shortcut {
        &self.incoming[edge_id as usize]
    }

    // a little crazy construction to make the borrow checker happy
    // so we take the shortcut we want to mutate temporarily out of the graph
    // that enables us to pass the reference to the graph as an argument to any calculation the mutation might need
    // the mutation just needs to guarantee that it will never refetch the edge under mutation.
    // But this is usually no problem since we know that a shortcut can only consist of paths of edges lower in the graph
    pub fn borrow_mut_outgoing<F: FnOnce(&mut Shortcut, &ShortcutGraph)>(&mut self, edge_id: EdgeId, f: F) {
        let mut shortcut = Shortcut::new(None, self.original_graph);
        swap(&mut self.outgoing[edge_id as usize], &mut shortcut);
        f(&mut shortcut, &self);
        swap(&mut self.outgoing[edge_id as usize], &mut shortcut);
    }

    pub fn borrow_mut_incoming<F: FnOnce(&mut Shortcut, &ShortcutGraph)>(&mut self, edge_id: EdgeId, f: F) {
        let mut shortcut = Shortcut::new(None, self.original_graph);
        swap(&mut self.incoming[edge_id as usize], &mut shortcut);
        f(&mut shortcut, &self);
        swap(&mut self.incoming[edge_id as usize], &mut shortcut);
    }

    pub fn swap_out_outgoing(&mut self, edge_id: EdgeId) -> Shortcut {
        let mut shortcut = Shortcut::new(None, self.original_graph);
        swap(&mut self.outgoing[edge_id as usize], &mut shortcut);
        shortcut
    }

    pub fn swap_out_incoming(&mut self, edge_id: EdgeId) -> Shortcut {
        let mut shortcut = Shortcut::new(None, self.original_graph);
        swap(&mut self.incoming[edge_id as usize], &mut shortcut);
        shortcut
    }

    pub fn swap_in_outgoing(&mut self, edge_id: EdgeId, mut shortcut: Shortcut) {
        swap(&mut self.outgoing[edge_id as usize], &mut shortcut);
    }

    pub fn swap_in_incoming(&mut self, edge_id: EdgeId, mut shortcut: Shortcut) {
        swap(&mut self.incoming[edge_id as usize], &mut shortcut);
    }

    pub fn original_graph(&self) -> &TDGraph {
        self.original_graph
    }
}

#[derive(Debug)]
pub struct CustomizedGraph<'a> {
    pub original_graph: &'a TDGraph,
    first_out: &'a [EdgeId],
    head: &'a [NodeId],
    pub outgoing: CustomizedSingleDirGraph,
    pub incoming: CustomizedSingleDirGraph,
}

impl<'a> From<ShortcutGraph<'a>> for CustomizedGraph<'a> {
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
            outgoing_head.extend(shortcut_graph.head[range.clone()].iter().zip(shortcut_graph.outgoing[range.clone()].iter()).filter(|(_head, s)| s.required).map(|(head, _)| head));
            outgoing_first_out.push(outgoing_first_out.last().unwrap() + shortcut_graph.outgoing[range.clone()].iter().filter(|s| s.required).count() as u32);

            incoming_head.extend(shortcut_graph.head[range.clone()].iter().zip(shortcut_graph.incoming[range.clone()].iter()).filter(|(_head, s)| s.required).map(|(head, _)| head));
            incoming_first_out.push(incoming_first_out.last().unwrap() + shortcut_graph.incoming[range.clone()].iter().filter(|s| s.required).count() as u32);
        }

        let mut outgoing_constant = BitVec::new(outgoing_head.len());
        let mut incoming_constant = BitVec::new(incoming_head.len());

        let outgoing_iter = || { shortcut_graph.outgoing.iter().filter(|s| s.required) };
        let incoming_iter = || { shortcut_graph.incoming.iter().filter(|s| s.required) };

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
            for tail in &mut outgoing_tail[range[0] as usize .. range[1] as usize] {
                *tail = node as NodeId;
            }
        }

        let mut incoming_tail = vec![0 as NodeId; incoming_head.len()];
        for (node, range) in incoming_first_out.windows(2).enumerate() {
            for tail in &mut incoming_tail[range[0] as usize .. range[1] as usize] {
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
                sources: outgoing_iter().flat_map(|shortcut| shortcut.sources_iter().map(|(t, &s)| {
                    let s = if let ShortcutSource::Shortcut(down, up) = ShortcutSource::from(s) {
                        ShortcutSource::Shortcut(mapping_incoming.get(down as usize).unwrap() as EdgeId, mapping_outgoing.get(up as usize).unwrap() as EdgeId)
                    } else {
                        ShortcutSource::from(s)
                    };
                    (t, ShortcutSourceData::from(s))
                })).collect(),
            },

            incoming: CustomizedSingleDirGraph {
                first_out: incoming_first_out,
                head: incoming_head,
                tail: incoming_tail,

                bounds: incoming_iter().map(|shortcut| (shortcut.lower_bound, shortcut.upper_bound)).collect(),
                constant: incoming_constant,
                first_source: degrees_to_first_out(incoming_iter().map(|shortcut| shortcut.num_sources() as u32)).collect(),
                sources: incoming_iter().flat_map(|shortcut| shortcut.sources_iter().map(|(t, &s)| {
                    let s = if let ShortcutSource::Shortcut(down, up) = ShortcutSource::from(s) {
                        ShortcutSource::Shortcut(mapping_incoming.get(down as usize).unwrap() as EdgeId, mapping_outgoing.get(up as usize).unwrap() as EdgeId)
                    } else {
                        ShortcutSource::from(s)
                    };
                    (t, ShortcutSourceData::from(s))
                })).collect(),
            },
        }
    }
}

impl<'a> CustomizedGraph<'a> {
    pub fn upward_bounds_graph(&self) -> SingleDirBoundsGraph {
        SingleDirBoundsGraph {
            first_out: &self.outgoing.first_out[..],
            head: &self.outgoing.head[..],
            bounds: &self.outgoing.bounds[..]
        }
    }

    pub fn downward_bounds_graph(&self) -> SingleDirBoundsGraph {
        SingleDirBoundsGraph {
            first_out: &self.incoming.first_out[..],
            head: &self.incoming.head[..],
            bounds: &self.incoming.bounds[..]
        }
    }
}

impl<'a> Deconstruct for CustomizedGraph<'a> {
    fn store_each(&self, store: &Fn(&str, &dyn Store) -> std::io::Result<()>) -> std::io::Result<()> {
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
            for tail in &mut outgoing_tail[range[0] as usize .. range[1] as usize] {
                *tail = node as NodeId;
            }
        }

        let mut incoming_tail = vec![0 as NodeId; incoming_head.len()];
        for (node, range) in incoming_first_out.windows(2).enumerate() {
            for tail in &mut incoming_tail[range[0] as usize .. range[1] as usize] {
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
    pub fn degree(&self, node: NodeId) -> usize {
        (self.first_out[node as usize + 1] - self.first_out[node as usize]) as usize
    }

    pub fn bounds(&self) -> &[(FlWeight, FlWeight)] {
        &self.bounds[..]
    }

    pub fn head(&self) -> &[NodeId] {
        &self.head[..]
    }

    pub fn tail(&self) -> &[NodeId] {
        &self.tail[..]
    }

    pub fn evaluate<F>(&self, edge_id: EdgeId, t: Timestamp, customized_graph: &CustomizedGraph, f: &mut F) -> FlWeight
        where F: (FnMut(bool, EdgeId, Timestamp) -> bool)
    {
        let edge_idx = edge_id as usize;
        if self.constant.get(edge_idx) {
            debug_assert_eq!(self.bounds[edge_idx].0, self.edge_source_at(edge_idx, t).map(|&source| ShortcutSource::from(source).evaluate(t, customized_graph, &mut always)).unwrap_or(FlWeight::INFINITY),
                "{:?}, {:?}, {}", self.bounds[edge_idx], self.edge_sources(edge_idx), edge_id);
            return self.bounds[edge_idx].0;
        }

        self.edge_source_at(edge_idx, t).map(|&source| ShortcutSource::from(source).evaluate(t, customized_graph, f)).unwrap_or(FlWeight::INFINITY)
    }

    pub fn evaluate_next_segment_at<Dir: Bool, F>(&self, edge_id: EdgeId, t: Timestamp, lower_bound_target: FlWeight, customized_graph: &CustomizedGraph, lower_bounds_to_target: &mut ClearlistVector<FlWeight>, mark_upward: &mut F) -> Option<(FlWeight, NodeId, EdgeId)>
        where F: FnMut(EdgeId)
    {
        let edge_idx = edge_id as usize;

        if self.constant.get(edge_idx) {
            return Some((self.bounds[edge_idx].0, if Dir::VALUE { self.head[edge_idx] } else { self.tail[edge_idx] }, edge_id))
        }
        self.edge_source_at(edge_idx, t).map(|&source| {
            match source.into() {
                ShortcutSource::Shortcut(down, up) => {
                    mark_upward(up);
                    let lower_bound_to_middle = customized_graph.outgoing.bounds()[up as usize].0 + lower_bound_target;
                    lower_bounds_to_target[customized_graph.incoming.tail[down as usize] as usize] = min(lower_bounds_to_target[customized_graph.incoming.tail[down as usize] as usize], lower_bound_to_middle);
                    customized_graph.incoming.evaluate_next_segment_at::<False, _>(down, t, lower_bound_to_middle, customized_graph, lower_bounds_to_target, mark_upward).unwrap()
                },
                ShortcutSource::OriginalEdge(edge) => {
                    (customized_graph.original_graph.travel_time_function(edge).evaluate(t), if Dir::VALUE { self.head[edge_idx] } else { self.tail[edge_idx] }, edge_id)
                },
                ShortcutSource::None => (FlWeight::INFINITY, if Dir::VALUE { self.head[edge_idx] } else { self.tail[edge_idx] }, edge_id)
            }
        })
    }

    pub fn unpack_at(&self, edge_id: EdgeId, t: Timestamp, customized_graph: &CustomizedGraph, result: &mut Vec<(EdgeId, Timestamp)>) {
        self.edge_source_at(edge_id as usize, t).map(|&source| ShortcutSource::from(source).unpack_at(t, customized_graph, result)).expect("can't unpack empty shortcut");
    }

    fn edge_source_at(&self, edge_idx: usize, t: Timestamp) -> Option<&ShortcutSourceData> {
        let data = self.edge_sources(edge_idx);

        if data.is_empty() {
            return None
        }
        if data.len() == 1 {
            return Some(&data[0].1)
        }

        let (_, t_period) = t.split_of_period();
        debug_assert!(data.first().map(|&(t, _)| t == Timestamp::zero()).unwrap_or(true), "{:?}", data);
        match data.binary_search_by_key(&t_period, |(t, _)| *t) {
            Ok(i) => data.get(i),
            Err(i) => {
                debug_assert!(data.get(i-1).map(|&(t, _)| t < t_period).unwrap_or(true));
                if i < data.len() {
                    debug_assert!(t_period < data[i].0);
                }
                data.get(i-1)
            }
        }.map(|(_, s)| s)
    }

    pub fn edge_sources(&self, edge_idx: usize) -> &[(Timestamp, ShortcutSourceData)] {
        &self.sources[(self.first_source[edge_idx] as usize)..(self.first_source[edge_idx + 1] as usize)]
    }
}

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
        let edge_ids = range.start as EdgeId .. range.end as EdgeId;
        self.head[range.clone()].iter().cloned().zip(edge_ids).zip(self.bounds[range].iter())
    }
}


fn always(_up: bool, _shortcut_id: EdgeId, _t: Timestamp) -> bool {
    true
}
