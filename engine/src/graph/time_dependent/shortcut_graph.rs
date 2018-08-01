use super::*;
use std::mem::swap;
use rank_select_map::BitVec;

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

    pub fn merge_upward(&mut self, shortcut_edge_id: EdgeId, alternative: Linked) {
        let mut shortcut = Shortcut::new(None);
        swap(&mut self.outgoing[shortcut_edge_id as usize], &mut shortcut);
        shortcut.merge(alternative, &self);
        swap(&mut self.outgoing[shortcut_edge_id as usize], &mut shortcut);
    }

    pub fn merge_downward(&mut self, shortcut_edge_id: EdgeId, alternative: Linked) {
        let mut shortcut = Shortcut::new(None);
        swap(&mut self.incoming[shortcut_edge_id as usize], &mut shortcut);
        shortcut.merge(alternative, &self);
        swap(&mut self.incoming[shortcut_edge_id as usize], &mut shortcut);
    }

    pub fn get_upward(&self, edge_id: EdgeId) -> &Shortcut {
        &self.outgoing[edge_id as usize]
    }

    pub fn remove_dominated_upward(&mut self, shortcut_edge_id: EdgeId) {
        let mut shortcut = Shortcut::new(None);
        swap(&mut self.outgoing[shortcut_edge_id as usize], &mut shortcut);
        shortcut.remove_dominated(&self);
        swap(&mut self.outgoing[shortcut_edge_id as usize], &mut shortcut);
    }

    pub fn remove_dominated_by_upward(&mut self, shortcut_edge_id: EdgeId, bound: Weight) {
        let mut shortcut = Shortcut::new(None);
        swap(&mut self.outgoing[shortcut_edge_id as usize], &mut shortcut);
        shortcut.remove_dominated_by(&self, bound);
        swap(&mut self.outgoing[shortcut_edge_id as usize], &mut shortcut);
    }

    pub fn get_downward(&self, edge_id: EdgeId) -> &Shortcut {
        &self.incoming[edge_id as usize]
    }

    pub fn remove_dominated_downward(&mut self, shortcut_edge_id: EdgeId) {
        let mut shortcut = Shortcut::new(None);
        swap(&mut self.incoming[shortcut_edge_id as usize], &mut shortcut);
        shortcut.remove_dominated(&self);
        swap(&mut self.incoming[shortcut_edge_id as usize], &mut shortcut);
    }

    pub fn remove_dominated_by_downward(&mut self, shortcut_edge_id: EdgeId, bound: Weight) {
        let mut shortcut = Shortcut::new(None);
        swap(&mut self.incoming[shortcut_edge_id as usize], &mut shortcut);
        shortcut.remove_dominated_by(&self, bound);
        swap(&mut self.incoming[shortcut_edge_id as usize], &mut shortcut);
    }

    pub fn original_graph(&self) -> &TDGraph {
        self.original_graph
    }

    pub fn total_num_segments(&self) -> usize {
        let a: usize = self.outgoing.iter().map(|shortcut| shortcut.num_path_segments()).sum();
        let b: usize = self.incoming.iter().map(|shortcut| shortcut.num_path_segments()).sum();
        a + b
    }

    pub fn print_segment_stats(&self) {
        let max = self.outgoing.iter()
            .chain(self.incoming.iter())
            .map(|shortcut| shortcut.num_path_segments())
            .max().unwrap();
        let mut histogramm = vec![0; max + 1];

        for shortcut in self.outgoing.iter().chain(self.incoming.iter()) {
            histogramm[shortcut.num_path_segments()] += 1;
        }

        println!("{:?}", histogramm);

        let mut original_edges = BitVec::new(self.original_graph.num_arcs());
        let mut shortcuts = BitVec::new(self.outgoing.len() + self.incoming.len());
        let m = self.outgoing.len();
        let max_search_space = self.outgoing[m-1000..].iter().chain(self.incoming[m-1000..].iter())
            .map(|shortcut| {
                original_edges.clear();
                shortcuts.clear();
                shortcut.unpack(self, &mut shortcuts, &mut original_edges);
                original_edges.count_ones()
            }).max().unwrap();

        println!("{:?}", max_search_space);
    }

    pub fn upward_graph<'s>(&'s self) -> SingleDirShortcutGraph<'s> {
        SingleDirShortcutGraph {
            first_out: self.first_out,
            head: self.head,
            shortcuts: &self.outgoing[..]
        }
    }

    pub fn downward_graph<'s>(&'s self) -> SingleDirShortcutGraph<'s> {
        SingleDirShortcutGraph {
            first_out: self.first_out,
            head: self.head,
            shortcuts: &self.incoming[..]
        }
    }
}

#[derive(Debug)]
pub struct SingleDirShortcutGraph<'a> {
    first_out: &'a [EdgeId],
    head: &'a [NodeId],
    shortcuts: &'a [Shortcut],
}

impl<'a> SingleDirShortcutGraph<'a> {
    pub fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }

    fn neighbor_edge_indices_usize(&self, node: NodeId) -> Range<usize> {
        (self.first_out[node as usize] as usize)..(self.first_out[(node + 1) as usize] as usize)
    }

    pub fn neighbor_iter(&self, node: NodeId) -> impl Iterator<Item = ((NodeId, EdgeId), &Shortcut)> {
        let range = self.neighbor_edge_indices_usize(node);
        let edge_ids = range.start as EdgeId .. range.end as EdgeId;
        self.head[range.clone()].iter().cloned().zip(edge_ids).zip(self.shortcuts[range].iter())
    }
}
