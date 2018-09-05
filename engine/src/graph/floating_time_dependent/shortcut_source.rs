use super::*;
use in_range_option::InRangeOption;

#[derive(Debug, Clone, Copy)]
pub enum ShortcutSource {
    Shortcut(EdgeId, EdgeId),
    OriginalEdge(EdgeId),
}

#[derive(Debug, Clone, Copy, PartialEq)]
pub struct ShortcutSourceData {
    down_arc: InRangeOption<EdgeId>,
    up_arc: EdgeId
}

impl From<ShortcutSource> for ShortcutSourceData {
    fn from(source: ShortcutSource) -> Self {
        match source {
            ShortcutSource::Shortcut(down, up) => ShortcutSourceData { down_arc: InRangeOption::new(Some(down)), up_arc: up },
            ShortcutSource::OriginalEdge(edge) => ShortcutSourceData { down_arc: InRangeOption::new(None), up_arc: edge },
        }
    }
}
