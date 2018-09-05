use super::*;
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
        let mut shortcut = Shortcut::new(None);
        swap(&mut self.outgoing[edge_id as usize], &mut shortcut);
        f(&mut shortcut, &self);
        swap(&mut self.outgoing[edge_id as usize], &mut shortcut);
    }

    pub fn borrow_mut_incoming<F: FnOnce(&mut Shortcut, &ShortcutGraph)>(&mut self, edge_id: EdgeId, f: F) {
        let mut shortcut = Shortcut::new(None);
        swap(&mut self.incoming[edge_id as usize], &mut shortcut);
        f(&mut shortcut, &self);
        swap(&mut self.incoming[edge_id as usize], &mut shortcut);
    }

    pub fn original_graph(&self) -> &TDGraph {
        self.original_graph
    }

    pub fn upward_graph(&self) -> SingleDirShortcutGraph {
        SingleDirShortcutGraph {
            first_out: self.first_out,
            head: self.head,
            shortcuts: &self.outgoing[..]
        }
    }

    pub fn downward_graph(&self) -> SingleDirShortcutGraph {
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
