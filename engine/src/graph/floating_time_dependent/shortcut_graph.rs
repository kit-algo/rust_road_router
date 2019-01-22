use super::*;
use crate::graph::first_out_graph::degrees_to_first_out;
use crate::rank_select_map::BitVec;
use crate::io::*;
use std::mem::swap;

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

    pub fn original_graph(&self) -> &TDGraph {
        self.original_graph
    }
}

#[derive(Debug)]
pub struct CustomizedGraph<'a> {
    pub original_graph: &'a TDGraph,
    first_out: &'a [EdgeId],
    head: &'a [NodeId],
    pub outgoing: CustomizedSingleDirGraph<'a>,
    pub incoming: CustomizedSingleDirGraph<'a>,
}

impl<'a> From<ShortcutGraph<'a>> for CustomizedGraph<'a> {
    fn from(shortcut_graph: ShortcutGraph<'a>) -> Self {
        let mut outgoing_constant = BitVec::new(shortcut_graph.head.len());
        let mut incoming_constant = BitVec::new(shortcut_graph.head.len());

        for (idx, shortcut) in shortcut_graph.outgoing.iter().enumerate() {
            if shortcut.is_constant() {
                outgoing_constant.set(idx);
            }
        }

        for (idx, shortcut) in shortcut_graph.incoming.iter().enumerate() {
            if shortcut.is_constant() {
                incoming_constant.set(idx);
            }
        }

        CustomizedGraph {
            original_graph: shortcut_graph.original_graph,
            first_out: shortcut_graph.first_out,
            head: shortcut_graph.head,

            outgoing: CustomizedSingleDirGraph {
                first_out: shortcut_graph.first_out,
                head: shortcut_graph.head,

                bounds: shortcut_graph.outgoing.iter().map(|shortcut| (shortcut.lower_bound, shortcut.upper_bound)).collect(),
                constant: outgoing_constant,
                first_source: degrees_to_first_out(shortcut_graph.outgoing.iter().map(|shortcut| shortcut.num_sources() as u32)).collect(),
                sources: shortcut_graph.outgoing.iter().map(|shortcut| shortcut.sources_iter().map(|(t, &s)| (t, s))).flatten().collect(),
            },

            incoming: CustomizedSingleDirGraph {
                first_out: shortcut_graph.first_out,
                head: shortcut_graph.head,

                bounds: shortcut_graph.incoming.iter().map(|shortcut| (shortcut.lower_bound, shortcut.upper_bound)).collect(),
                constant: incoming_constant,
                first_source: degrees_to_first_out(shortcut_graph.incoming.iter().map(|shortcut| shortcut.num_sources() as u32)).collect(),
                sources: shortcut_graph.incoming.iter().map(|shortcut| shortcut.sources_iter().map(|(t, &s)| (t, s))).flatten().collect(),
            },
        }
    }
}

impl<'a> CustomizedGraph<'a> {
    pub fn upward_bounds_graph(&self) -> SingleDirBoundsGraph {
        SingleDirBoundsGraph {
            first_out: self.first_out,
            head: self.head,
            bounds: &self.outgoing.bounds[..]
        }
    }

    pub fn downward_bounds_graph(&self) -> SingleDirBoundsGraph {
        SingleDirBoundsGraph {
            first_out: self.first_out,
            head: self.head,
            bounds: &self.incoming.bounds[..]
        }
    }
}

impl<'a> Deconstruct for CustomizedGraph<'a> {
    fn store_each(&self, store: &Fn(&str, &dyn Store) -> std::io::Result<()>) -> std::io::Result<()> {
        store("outgoing_bounds", &self.outgoing.bounds)?;
        store("outgoing_constant", &self.outgoing.constant)?;
        store("outgoing_first_source", &self.outgoing.first_source)?;
        store("outgoing_sources", &self.outgoing.sources)?;
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
        Ok(CustomizedGraph {
            original_graph: self.original_graph,
            first_out: self.first_out,
            head: self.head,

            outgoing: CustomizedSingleDirGraph {
                first_out: self.first_out,
                head: self.head,

                bounds: loader.load("outgoing_bounds")?,
                constant: loader.load("outgoing_constant")?,
                first_source: loader.load("outgoing_first_source")?,
                sources: loader.load("outgoing_sources")?,
            },

            incoming: CustomizedSingleDirGraph {
                first_out: self.first_out,
                head: self.head,

                bounds: loader.load("incoming_bounds")?,
                constant: loader.load("incoming_constant")?,
                first_source: loader.load("incoming_first_source")?,
                sources: loader.load("incoming_sources")?,
            },
        })
    }
}

#[derive(Debug)]
pub struct CustomizedSingleDirGraph<'a> {
    first_out: &'a [EdgeId], // TODO do not store here
    head: &'a [NodeId],

    bounds: Vec<(FlWeight, FlWeight)>,
    constant: BitVec,
    first_source: Vec<u32>,
    sources: Vec<(Timestamp, ShortcutSourceData)>,
}

impl<'a> CustomizedSingleDirGraph<'a> {
    pub fn bounds(&self) -> &[(FlWeight, FlWeight)] {
        &self.bounds[..]
    }

    pub fn evaluate<F>(&self, edge_id: EdgeId, t: Timestamp, customized_graph: &CustomizedGraph, f: &mut F) -> FlWeight
        where F: (FnMut(bool, EdgeId, Timestamp) -> bool)
    {
        let edge_idx = edge_id as usize;
        if self.constant.get(edge_idx) {
            debug_assert_eq!(self.bounds[edge_idx].0, self.edge_source_at(edge_idx, t).map(|&source| ShortcutSource::from(source).evaluate(t, customized_graph, &mut always)).unwrap_or(FlWeight::INFINITY),
                "{:?}, {:?}", self.bounds[edge_idx], self.edge_sources(edge_idx));
            return self.bounds[edge_idx].0;
        }

        self.edge_source_at(edge_idx, t).map(|&source| ShortcutSource::from(source).evaluate(t, customized_graph, f)).unwrap_or(FlWeight::INFINITY)
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

    fn edge_sources(&self, edge_idx: usize) -> &[(Timestamp, ShortcutSourceData)] {
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
