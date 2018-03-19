use super::*;

#[derive(Debug)]
pub struct ShortcutGraph {
    shortcuts: Vec<Shortcut>
}

impl ShortcutGraph {
    pub fn new(shortcuts: Vec<Shortcut>) -> ShortcutGraph {
        ShortcutGraph { shortcuts }
    }

    pub fn get(&self, edge_id: EdgeId) -> &Shortcut {
        &self.shortcuts[edge_id as usize]
    }

    pub fn set(&mut self, edge_id: EdgeId, shortcut: Shortcut) {
        self.shortcuts[edge_id as usize] = shortcut;
    }
}
