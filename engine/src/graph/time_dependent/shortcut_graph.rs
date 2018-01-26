use super::*;

#[derive(Debug)]
pub struct ShortcutGraph {
    shortcuts: Vec<Shortcut>
}

impl ShortcutGraph {
    pub fn get(&self, edge_id: EdgeId) -> &Shortcut {
        &self.shortcuts[edge_id as usize]
    }
}
