use super::*;

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
        self.outgoing[shortcut_edge_id as usize] = {
            let shortcut = &self.outgoing[shortcut_edge_id as usize];
            shortcut.merge(alternative, &self)
        };
    }

    pub fn merge_downward(&mut self, shortcut_edge_id: EdgeId, alternative: Linked) {
        self.incoming[shortcut_edge_id as usize] = {
            let shortcut = &self.incoming[shortcut_edge_id as usize];
            shortcut.merge(alternative, &self)
        };
    }

    pub fn get_upward(&self, edge_id: EdgeId) -> &Shortcut {
        &self.outgoing[edge_id as usize]
    }

    pub fn get_downward(&self, edge_id: EdgeId) -> &Shortcut {
        &self.incoming[edge_id as usize]
    }

    pub fn original_graph(&self) -> &TDGraph {
        self.original_graph
    }
}
