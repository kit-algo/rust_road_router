use super::*;

#[derive(Debug, Clone)]
enum Sources {
    None,
    One(ShortcutSourceData),
    Multi(Vec<(Timestamp, ShortcutSourceData)>)
}

#[derive(Debug, Clone)]
pub struct Shortcut {
    sources: Sources,
    ttf: Option<Vec<Point>>,
}

impl Shortcut {
    pub fn new(source: Option<EdgeId>) -> Self {
        match source {
            Some(edge_id) => {
                Shortcut {
                    sources: Sources::One(ShortcutSource::OriginalEdge(edge_id).into()),
                    ttf: None
                }
            },
            None => Shortcut { sources: Sources::None, ttf: None },
        }
    }

    pub fn merge(&mut self, _linked_ids: (EdgeId, EdgeId), _shortcut_graph: &ShortcutGraph) {
        unimplemented!()
    }
}
