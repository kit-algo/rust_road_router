use super::*;

#[derive(Debug, Clone)]
pub struct Shortcut {}

impl Shortcut {
    pub fn new(_source: Option<(EdgeId, PiecewiseLinearFunction)>) -> Self {
        unimplemented!();
    }

    pub fn merge(&mut self, _linked_ids: (EdgeId, EdgeId), _shortcut_graph: &ShortcutGraph) {
        unimplemented!()
    }
}
