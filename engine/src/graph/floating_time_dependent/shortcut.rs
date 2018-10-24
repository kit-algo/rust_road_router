use super::*;

#[derive(Debug, Clone)]
pub struct Shortcut {
    sources: Sources,
    ttf: Option<Vec<Point>>,
}

impl Shortcut {
    pub fn new(source: Option<EdgeId>) -> Self {
        match source {
            Some(edge_id) => {
                PATH_SOURCES_COUNT.with(|count| count.set(count.get() + 1));
                Shortcut {
                    sources: Sources::One(ShortcutSource::OriginalEdge(edge_id).into()),
                    ttf: None
                }
            },
            None => Shortcut { sources: Sources::None, ttf: None },
        }
    }

    pub fn merge(&mut self, linked_ids: (EdgeId, EdgeId), shortcut_graph: &ShortcutGraph) {
        IPP_COUNT.with(|count| count.set(count.get() - self.ttf.as_ref().map(|ipps| ipps.len()).unwrap_or(0)));
        PATH_SOURCES_COUNT.with(|count| count.set(count.get() - self.sources.len()));

        #[allow(clippy::redundant_closure_call)]
        (|| {
            let other_data = ShortcutSource::Shortcut(linked_ids.0, linked_ids.1).into();

            if !(shortcut_graph.get_incoming(linked_ids.0).is_valid_path() && shortcut_graph.get_outgoing(linked_ids.1).is_valid_path()) { return; }

            let first_plf = shortcut_graph.get_incoming(linked_ids.0).plf(shortcut_graph);
            let second_plf = shortcut_graph.get_outgoing(linked_ids.1).plf(shortcut_graph);

            if !self.is_valid_path() {
                self.ttf = Some(first_plf.link(&second_plf));
                self.sources = Sources::One(other_data);
                return;
            }

            let other_lower_bound = first_plf.lower_bound() + second_plf.lower_bound();

            let self_plf = self.plf(shortcut_graph);
            let lower_bound = self_plf.lower_bound();
            let upper_bound = self_plf.upper_bound();

            if other_lower_bound >= upper_bound {
                return;
            }

            let linked_ipps = first_plf.link(&second_plf);

            let linked = PiecewiseLinearFunction::new(&linked_ipps);
            let other_lower_bound = linked.lower_bound();
            let other_upper_bound = linked.upper_bound();

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
            self.sources = Shortcut::combine(sources, intersection_data, other_data);
        }) ();

        IPP_COUNT.with(|count| count.set(count.get() + self.ttf.as_ref().map(|ipps| ipps.len()).unwrap_or(0)));
        PATH_SOURCES_COUNT.with(|count| count.set(count.get() + self.sources.len()));
    }

    fn plf<'s>(&'s self, shortcut_graph: &'s ShortcutGraph) -> PiecewiseLinearFunction<'s> {
        if let Some(ipps) = &self.ttf {
            return PiecewiseLinearFunction::new(ipps)
        }

        match self.sources {
            Sources::One(source) => {
                match source.into() {
                    ShortcutSource::OriginalEdge(id) => shortcut_graph.original_graph().travel_time_function(id),
                    _ => panic!("invalid state of shortcut: ipps must be cached when shortcut not trivial"),
                }
            },
            _ => panic!("invalid state of shortcut: ipps must be cached when shortcut not trivial"),
        }
    }

    fn is_valid_path(&self) -> bool {
        match self.sources {
            Sources::None => false,
            _ => true,
        }
    }

    pub fn clear_plf(&mut self) {
        IPP_COUNT.with(|count| count.set(count.get() - self.ttf.as_ref().map(|ipps| ipps.len()).unwrap_or(0)));
        self.ttf = None;
    }

    fn combine(sources: Sources, intersection_data: Vec<(Timestamp, bool)>, other_data: ShortcutSourceData) -> Sources {
        if let [(_, is_self_better)] = &intersection_data[..] {
            if *is_self_better {
                return sources
            } else {
                return Sources::One(other_data)
            }
        }

        debug_assert!(intersection_data.len() >= 2);
        let mut intersection_iter = intersection_data.into_iter().peekable();
        let (zero, mut self_currently_better) = intersection_iter.next().unwrap();
        debug_assert!(zero == Timestamp::zero());

        let dummy = ShortcutSource::OriginalEdge(std::u32::MAX).into();
        let mut prev_source = &dummy;
        let mut new_sources = Vec::new();

        for (at, source) in sources.iter() {
            if intersection_iter.peek().is_none() || at < intersection_iter.peek().unwrap().0 {
                if self_currently_better {
                    new_sources.push((at, *source));
                }
            } else {
                self_currently_better = intersection_iter.peek().unwrap().1;

                if self_currently_better {
                    if at > intersection_iter.peek().unwrap().0 {
                        new_sources.push((intersection_iter.peek().unwrap().0, *prev_source));
                    }
                    new_sources.push((at, *source));
                } else {
                    new_sources.push((intersection_iter.peek().unwrap().0, other_data));
                }

                intersection_iter.next();
            }

            prev_source = source;
        }

        Sources::Multi(new_sources)
    }
}

#[derive(Debug, Clone)]
enum Sources {
    None,
    One(ShortcutSourceData),
    Multi(Vec<(Timestamp, ShortcutSourceData)>)
}

impl Sources {
    fn iter(&self) -> SourcesIter {
        match self {
            Sources::None => SourcesIter::None,
            Sources::One(source) => SourcesIter::One(std::iter::once(&source)),
            Sources::Multi(sources) => SourcesIter::Multi(sources.iter()),
        }
    }

    fn len(&self) -> usize {
        match self {
            Sources::None => 0,
            Sources::One(_) => 1,
            Sources::Multi(sources) => sources.len(),
        }
    }
}

#[derive(Debug)]
enum SourcesIter<'a> {
    None,
    One(std::iter::Once<&'a ShortcutSourceData>),
    Multi(std::slice::Iter<'a, (Timestamp, ShortcutSourceData)>),
}

impl<'a> Iterator for SourcesIter<'a> {
    type Item = (Timestamp, &'a ShortcutSourceData);

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            SourcesIter::None => None,
            SourcesIter::One(iter) => iter.next().map(|source| (Timestamp::zero(), source)),
            SourcesIter::Multi(iter) => iter.next().map(|(t, source)| (*t, source)),
        }
    }
}
