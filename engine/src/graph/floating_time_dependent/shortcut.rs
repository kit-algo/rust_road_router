use super::*;
use std::cmp::min;

#[derive(Debug, Clone)]
pub struct Shortcut {
    sources: Sources,
    ttf: Option<Vec<TTFPoint>>,
    pub lower_bound: FlWeight,
    pub upper_bound: FlWeight
}

impl Shortcut {
    pub fn new(source: Option<EdgeId>, original_graph: &TDGraph) -> Self {
        match source {
            Some(edge_id) => {
                PATH_SOURCES_COUNT.with(|count| count.set(count.get() + 1));
                Shortcut {
                    sources: Sources::One(ShortcutSource::OriginalEdge(edge_id).into()),
                    ttf: None,
                    lower_bound: original_graph.travel_time_function(edge_id).lower_bound(),
                    upper_bound: original_graph.travel_time_function(edge_id).upper_bound(),
                }
            },
            None => Shortcut { sources: Sources::None, ttf: None, lower_bound: FlWeight::new(f64::from(INFINITY)), upper_bound: FlWeight::new(f64::from(INFINITY)) },
        }
    }

    pub fn merge(&mut self, linked_ids: (EdgeId, EdgeId), shortcut_graph: &ShortcutGraph) {
        IPP_COUNT.with(|count| count.set(count.get() - self.ttf.as_ref().map(|ipps| ipps.len()).unwrap_or(0)));
        PATH_SOURCES_COUNT.with(|count| count.set(count.get() - self.sources.len()));
        if self.ttf.is_some() { ACTIVE_SHORTCUTS.with(|count| count.set(count.get() - 1)); }

        #[allow(clippy::redundant_closure_call)]
        (|| {
            let other_data = ShortcutSource::Shortcut(linked_ids.0, linked_ids.1).into();

            if !(shortcut_graph.get_incoming(linked_ids.0).is_valid_path() && shortcut_graph.get_outgoing(linked_ids.1).is_valid_path()) { return; }

            let first = shortcut_graph.get_incoming(linked_ids.0);
            let second = shortcut_graph.get_outgoing(linked_ids.1);

            let other_lower_bound = first.lower_bound + second.lower_bound;

            if other_lower_bound > self.upper_bound {
                return
            }

            let first_plf = first.plf(shortcut_graph);
            let second_plf = second.plf(shortcut_graph);

            if !self.is_valid_path() {
                ACTUALLY_LINKED.with(|count| count.set(count.get() + 1));
                let linked = first_plf.link(&second_plf);

                self.upper_bound = min(self.upper_bound, PiecewiseLinearFunction::new(&linked).upper_bound());
                debug_assert!(self.upper_bound >= self.lower_bound);
                self.ttf = Some(linked);
                self.sources = Sources::One(other_data);
                return;
            }

            let self_plf = self.plf(shortcut_graph);

            let linked_ipps = first_plf.link(&second_plf);
            ACTUALLY_LINKED.with(|count| count.set(count.get() + 1));

            let linked = PiecewiseLinearFunction::new(&linked_ipps);
            let other_lower_bound = linked.lower_bound();
            let other_upper_bound = linked.upper_bound();

            if self_plf.lower_bound() >= other_upper_bound {
                self.upper_bound = min(self.upper_bound, PiecewiseLinearFunction::new(&linked_ipps).upper_bound());
                debug_assert!(self.upper_bound >= self.lower_bound);
                self.ttf = Some(linked_ipps);
                self.sources = Sources::One(other_data);
                UNNECESSARY_LINKED.with(|count| count.set(count.get() + 1));
                return;
            } else if other_lower_bound > self.upper_bound {
                return;
            }

            ACTUALLY_MERGED.with(|count| count.set(count.get() + 1));
            let (merged, intersection_data) = self_plf.merge(&linked);

            self.upper_bound = min(self.upper_bound, PiecewiseLinearFunction::new(&merged).upper_bound());
            debug_assert!(self.upper_bound >= self.lower_bound);
            self.ttf = Some(merged);
            let mut sources = Sources::None;
            std::mem::swap(&mut sources, &mut self.sources);
            self.sources = Shortcut::combine(sources, intersection_data, other_data);
        }) ();

        IPP_COUNT.with(|count| count.set(count.get() + self.ttf.as_ref().map(|ipps| ipps.len()).unwrap_or(0)));
        PATH_SOURCES_COUNT.with(|count| count.set(count.get() + self.sources.len()));
        if self.ttf.is_some() { ACTIVE_SHORTCUTS.with(|count| count.set(count.get() + 1)); }
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

    pub fn finalize_lower_bound(&mut self) {
        if let Some(points) = &self.ttf {
            let new_lower_bound = PiecewiseLinearFunction::new(points).lower_bound();
            debug_assert!(!new_lower_bound.fuzzy_lt(self.lower_bound), "{:?}, {:?}", new_lower_bound, self);
            debug_assert!(new_lower_bound <= self.upper_bound, "{:?}, {:?}", new_lower_bound, self);
            self.lower_bound = new_lower_bound;
        }
    }

    pub fn clear_plf(&mut self) {
        IPP_COUNT.with(|count| count.set(count.get() - self.ttf.as_ref().map(|ipps| ipps.len()).unwrap_or(0)));
        if self.ttf.is_some() { ACTIVE_SHORTCUTS.with(|count| count.set(count.get() - 1)); }
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
        let &(zero, mut self_currently_better) = intersection_data.first().unwrap();
        debug_assert!(zero == Timestamp::zero());
        let debug_intersections = intersection_data.clone();
        let mut intersection_iter = intersection_data.into_iter().peekable();

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

        for (at, is_self_better) in intersection_iter {
            if is_self_better {
                new_sources.push((at, *prev_source));
            } else {
                new_sources.push((at, other_data));
            }
        }

        debug_assert!(new_sources.len() >= 2, "old: {:?}\nintersections: {:?}\nnew: {:?}", sources, debug_intersections, new_sources);
        debug_assert!(new_sources.first().unwrap().0 == Timestamp::zero());
        Sources::Multi(new_sources)
    }

    pub fn evaluate<F>(&self, t: Timestamp, shortcut_graph: &ShortcutGraph, f: &mut F) -> FlWeight
        where F: (FnMut(bool, EdgeId, Timestamp) -> bool)
    {
        // TODO benchmark
        if self.lower_bound == self.upper_bound {
            return self.lower_bound;
        }

        if let Some(ipps) = &self.ttf {
            return PiecewiseLinearFunction::new(ipps).evaluate(t)
        }

        match &self.sources {
            Sources::None => FlWeight::new(f64::from(INFINITY)),
            Sources::One(source) => ShortcutSource::from(*source).evaluate(t, shortcut_graph, f),
            Sources::Multi(data) => {
                let (_, t_period) = t.split_of_period();
                debug_assert!(data[0].0 == Timestamp::zero(), "{:?}", data);
                let (_, source) = match data.binary_search_by_key(&t_period, |(t, _)| *t) {
                    Ok(i) => data[i],
                    Err(i) => {
                        debug_assert!(data[i-1].0 < t_period);
                        if i < data.len() {
                            debug_assert!(t_period < data[i].0);
                        }
                        data[i-1]
                    }
                };
                ShortcutSource::from(source).evaluate(t, shortcut_graph, f)
            },
        }
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
