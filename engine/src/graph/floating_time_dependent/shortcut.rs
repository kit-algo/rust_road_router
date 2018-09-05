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
            Some(edge_id) => Shortcut {
                sources: Sources::One(ShortcutSource::OriginalEdge(edge_id).into()),
                ttf: None
            },
            None => Shortcut { sources: Sources::None, ttf: None },
        }
    }

    pub fn merge(&mut self, linked_ids: (EdgeId, EdgeId), shortcut_graph: &ShortcutGraph) {
        let other_data = ShortcutSource::Shortcut(linked_ids.0, linked_ids.1).into();

        if !(shortcut_graph.get_incoming(linked_ids.0).is_valid_path() && shortcut_graph.get_outgoing(linked_ids.1).is_valid_path()) { return; }

        let first_plf = shortcut_graph.get_incoming(linked_ids.0).plf(shortcut_graph);
        let second_plf = shortcut_graph.get_outgoing(linked_ids.1).plf(shortcut_graph);

        let linked_ipps = first_plf.link(&second_plf);

        if !self.is_valid_path() {
            self.ttf = Some(linked_ipps);
            self.sources = Sources::One(other_data);
            return;
        }

        let linked = PiecewiseLinearFunction::new(&linked_ipps);
        let other_lower_bound = linked.lower_bound();
        let other_upper_bound = linked.upper_bound();

        let self_plf = self.plf(shortcut_graph);
        let lower_bound = self_plf.lower_bound();
        let upper_bound = self_plf.upper_bound();

        if lower_bound >= other_upper_bound {
            self.ttf = Some(linked_ipps);
            self.sources = Sources::One(other_data);
            return;
        } else if other_lower_bound >= upper_bound {
            return;
        }

        let (merged, intersection_data) = self_plf.merge(&linked);
        self.ttf = Some(merged);
        let mut sources = Sources::None;
        std::mem::swap(&mut sources, &mut self.sources);
        self.sources = Shortcut::combine(sources, intersection_data);
    }

    fn plf(&self, _shortcut_graph: &ShortcutGraph) -> PiecewiseLinearFunction {
        unimplemented!();
    }

    fn is_valid_path(&self) -> bool {
        unimplemented!();
    }

    pub fn clear_plf(&mut self) {
        self.ttf = None;
    }

    fn combine(_sources: Sources, _intersection_data: Vec<(Timestamp, bool)>) -> Sources {
        unimplemented!();
    }
}
