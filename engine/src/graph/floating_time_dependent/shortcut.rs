use super::*;
use std::sync::atomic::Ordering::Relaxed;
use std::cmp::{min, max, Ordering};

#[derive(Debug)]
enum TTFCache {
    Exact(Box<[TTFPoint]>),
    Approx(Box<[TTFPoint]>, Box<[TTFPoint]>),
}

impl TTFCache {
    fn num_points(&self) -> usize {
        use TTFCache::*;

        match &self {
            Exact(points) => points.len(),
            Approx(lower, upper) => lower.len() + upper.len(),
        }
    }
}

#[derive(Debug)]
enum TTF<'a> {
    Exact(PiecewiseLinearFunction<'a>),
    Approx(PiecewiseLinearFunction<'a>, PiecewiseLinearFunction<'a>),
}

impl<'a> From<&'a TTFCache> for TTF<'a> {
    fn from(cache: &'a TTFCache) -> Self {
        match cache {
            TTFCache::Exact(ipps) => TTF::Exact(PiecewiseLinearFunction::new(ipps)),
            TTFCache::Approx(lower_ipps, upper_ipps) => TTF::Approx(PiecewiseLinearFunction::new(lower_ipps), PiecewiseLinearFunction::new(upper_ipps)),
        }
    }
}

impl<'a> TTF<'a> {
    fn static_lower_bound(&self) -> FlWeight {
        use TTF::*;

        match self {
            Exact(plf) => plf.lower_bound(),
            Approx(lower_plf, _) => lower_plf.lower_bound(),
        }
    }

    fn static_upper_bound(&self) -> FlWeight {
        use TTF::*;

        match self {
            Exact(plf) => plf.upper_bound(),
            Approx(_, upper_plf) => upper_plf.upper_bound(),
        }
    }

    fn link(&self, second: &TTF) -> TTFCache {
        use TTF::*;

        if let (Exact(first), Exact(second)) = (self, second) {
            return TTFCache::Exact(first.link(second))
        }

        let (first_lower, first_upper) = self.bound_plfs();
        let (second_lower, second_upper) = second.bound_plfs();

        TTFCache::Approx(
            first_lower.link(&second_lower),
            first_upper.link(&second_upper)
        )
    }

    #[allow(clippy::collapsible_if)]
    #[allow(clippy::cyclomatic_complexity)]
    fn merge(&self, other: &TTF, merge_exact: impl Fn(Timestamp, Timestamp) -> (Box<[TTFPoint]>, Vec<(Timestamp, bool)>)) -> (TTFCache, Vec<(Timestamp, bool)>) {
        use TTF::*;

        if let (Exact(self_plf), Exact(other)) = (self, other) {
            let (plf, intersections) = self_plf.merge(other);
            return (TTFCache::Exact(plf), intersections);
        }

        let (self_lower, self_upper) = self.bound_plfs();
        let (other_lower, other_upper) = other.bound_plfs();

        let mut self_buffer = Vec::with_capacity(max(self_lower.len(), self_upper.len()));
        let mut other_buffer = Vec::with_capacity(max(other_lower.len(), other_upper.len()));

        let mut result_lower = Vec::with_capacity(2 * self_lower.len() + 2 * other_lower.len() + 2);
        let mut result_upper = Vec::with_capacity(2 * self_upper.len() + 2 * other_upper.len() + 2);

        let (_, self_dominating_intersections) = self_upper.merge(&other_lower);
        let (_, other_dominating_intersections) = other_upper.merge(&self_lower);

        let mut dominating = false;
        let mut start_of_segment = Timestamp::zero();
        let mut self_dominating_iter = self_dominating_intersections.iter().peekable();
        let mut other_dominating_iter = other_dominating_intersections.iter().peekable();
        let mut result = Vec::new();

        match (self_dominating_iter.peek().unwrap().1, other_dominating_iter.peek().unwrap().1) {
            (true, false) => {
                dominating = true;
                result.push((Timestamp::zero(), true));
            },
            (false, true) => {
                dominating = true;
                result.push((Timestamp::zero(), false));
            },
            _ => {} // in BOTH cases (especially the broken true true case) everything is unclear and we need to do exact merging
        }

        self_dominating_iter.next();
        other_dominating_iter.next();

        while self_dominating_iter.peek().is_some() || other_dominating_iter.peek().is_some() {
            let next_t_self = self_dominating_iter.peek().map(|(t, _)| *t).unwrap_or(Timestamp::NEVER);
            let next_t_other = other_dominating_iter.peek().map(|(t, _)| *t).unwrap_or(Timestamp::NEVER);

            if dominating {
                if next_t_self.fuzzy_lt(next_t_other) {
                    debug_assert!(!self_dominating_iter.peek().unwrap().1, "{:?}", dbg_each!(&self_dominating_intersections, &other_dominating_intersections, &self_lower, &self_upper, &other_lower, &other_upper));
                    debug_assert!(result.last().unwrap().1, "{:?}", dbg_each!(&self_dominating_intersections, &other_dominating_intersections, &self_lower, &self_upper, &other_lower, &other_upper));
                    dominating = false;

                    result_lower.pop();
                    self_lower.copy_range(start_of_segment, next_t_self, &mut result_lower);
                    result_upper.pop();
                    self_upper.copy_range(start_of_segment, next_t_self, &mut result_upper);

                    start_of_segment = next_t_self;
                    self_dominating_iter.next();
                } else if next_t_other.fuzzy_lt(next_t_self) {
                    debug_assert!(!other_dominating_iter.peek().unwrap().1, "{:?}", dbg_each!(&self_dominating_intersections, &other_dominating_intersections, &self_lower, &self_upper, &other_lower, &other_upper)); // <--
                    debug_assert!(!result.last().unwrap().1, "{:?}", dbg_each!(&self_dominating_intersections, &other_dominating_intersections, &self_lower, &self_upper, &other_lower, &other_upper));
                    dominating = false;

                    result_lower.pop();
                    other_lower.copy_range(start_of_segment, next_t_other, &mut result_lower);
                    result_upper.pop();
                    other_upper.copy_range(start_of_segment, next_t_other, &mut result_upper);

                    start_of_segment = next_t_other;
                    other_dominating_iter.next();
                } else {
                    debug_assert_ne!(self_dominating_iter.peek().unwrap().1, other_dominating_iter.peek().unwrap().1, "{:?}", dbg_each!(&self_dominating_intersections, &other_dominating_intersections, &self_lower, &self_upper, &other_lower, &other_upper));
                    result.push((next_t_self, self_dominating_iter.peek().unwrap().1));

                    result_lower.pop();
                    result_upper.pop();
                    if self_dominating_iter.peek().unwrap().1 {
                        other_lower.copy_range(start_of_segment, next_t_self, &mut result_lower);
                        other_upper.copy_range(start_of_segment, next_t_self, &mut result_upper);
                    } else {
                        self_lower.copy_range(start_of_segment, next_t_self, &mut result_lower);
                        self_upper.copy_range(start_of_segment, next_t_self, &mut result_upper);
                    }

                    start_of_segment = next_t_self;
                    self_dominating_iter.next();
                    other_dominating_iter.next();
                }
            } else {
                if next_t_self.fuzzy_lt(next_t_other) {

                    let (_, intersections) = merge_exact(start_of_segment, next_t_self);
                    let mut iter = intersections.into_iter();
                    let first_intersection = iter.next().unwrap();
                    if result.last().map(|(_, self_better)| *self_better != first_intersection.1).unwrap_or(true) {
                        result.push(first_intersection);
                    }
                    result.extend(iter);

                    if self_dominating_iter.peek().unwrap().1 {
                        if !result.last().unwrap().1 {
                            result.push((next_t_self, true));
                        }
                    } else {
                        if result.last().unwrap().1 {
                            result.push((next_t_self, false));
                        }
                    }

                    self_buffer.clear();
                    self_lower.copy_range(start_of_segment, next_t_self, &mut self_buffer);
                    other_buffer.clear();
                    other_lower.copy_range(start_of_segment, next_t_self, &mut other_buffer);
                    let (partial_lower, _) = PiecewiseLinearFunction::merge_partials(&self_buffer, &other_buffer, start_of_segment, next_t_self);
                    result_lower.pop();
                    result_lower.append(&mut partial_lower.into_vec());

                    self_buffer.clear();
                    self_upper.copy_range(start_of_segment, next_t_self, &mut self_buffer);
                    other_buffer.clear();
                    other_upper.copy_range(start_of_segment, next_t_self, &mut other_buffer);
                    let (partial_upper, _) = PiecewiseLinearFunction::merge_partials(&self_buffer, &other_buffer, start_of_segment, next_t_self);
                    result_upper.pop();
                    result_upper.append(&mut partial_upper.into_vec());

                    start_of_segment = next_t_self;
                    dominating = true;
                    self_dominating_iter.next();
                } else if next_t_other.fuzzy_lt(next_t_self) {

                    let (_, intersections) = merge_exact(start_of_segment, next_t_other);
                    let mut iter = intersections.into_iter();
                    let first_intersection = iter.next().unwrap();
                    if result.last().map(|(_, self_better)| *self_better != first_intersection.1).unwrap_or(true) {
                        result.push(first_intersection);
                    }
                    result.extend(iter);

                    if other_dominating_iter.peek().unwrap().1 {
                        if result.last().unwrap().1 {
                            result.push((next_t_other, false));
                        }
                    } else {
                        if !result.last().unwrap().1 {
                            result.push((next_t_other, true));
                        }
                    }

                    self_buffer.clear();
                    self_lower.copy_range(start_of_segment, next_t_other, &mut self_buffer);
                    other_buffer.clear();
                    other_lower.copy_range(start_of_segment, next_t_other, &mut other_buffer);
                    let (partial_lower, _) = PiecewiseLinearFunction::merge_partials(&self_buffer, &other_buffer, start_of_segment, next_t_other);
                    result_lower.pop();
                    result_lower.append(&mut partial_lower.into_vec());

                    self_buffer.clear();
                    self_upper.copy_range(start_of_segment, next_t_other, &mut self_buffer);
                    other_buffer.clear();
                    other_upper.copy_range(start_of_segment, next_t_other, &mut other_buffer);
                    let (partial_upper, _) = PiecewiseLinearFunction::merge_partials(&self_buffer, &other_buffer, start_of_segment, next_t_other);
                    result_upper.pop();
                    result_upper.append(&mut partial_upper.into_vec());

                    start_of_segment = next_t_other;
                    dominating = true;
                    other_dominating_iter.next();
                } else {
                    self_dominating_iter.next();
                    other_dominating_iter.next();
                }
            }
        }

        if !dominating {
            let (_, intersections) = merge_exact(start_of_segment, period());
            let mut iter = intersections.into_iter();
            let first_intersection = iter.next().unwrap();
            if result.last().map(|(_, self_better)| *self_better != first_intersection.1).unwrap_or(true) {
                result.push(first_intersection);
            }
            result.extend(iter);

            self_buffer.clear();
            self_lower.copy_range(start_of_segment, period(), &mut self_buffer);
            other_buffer.clear();
            other_lower.copy_range(start_of_segment, period(), &mut other_buffer);
            let (partial_lower, _) = PiecewiseLinearFunction::merge_partials(&self_buffer, &other_buffer, start_of_segment, period());
            result_lower.pop();
            result_lower.append(&mut partial_lower.into_vec());

            self_buffer.clear();
            self_upper.copy_range(start_of_segment, period(), &mut self_buffer);
            other_buffer.clear();
            other_upper.copy_range(start_of_segment, period(), &mut other_buffer);
            let (partial_upper, _) = PiecewiseLinearFunction::merge_partials(&self_buffer, &other_buffer, start_of_segment, period());
            result_upper.pop();
            result_upper.append(&mut partial_upper.into_vec());
        } else {
            result_lower.pop();
            result_upper.pop();
            if result.last().unwrap().1 {
                self_lower.copy_range(start_of_segment, period(), &mut result_lower);
                self_upper.copy_range(start_of_segment, period(), &mut result_upper);
            } else {
                other_lower.copy_range(start_of_segment, period(), &mut result_lower);
                other_upper.copy_range(start_of_segment, period(), &mut result_upper);
            }
        }

        debug_assert!(result.first().unwrap().0 == Timestamp::zero());
        for better in result.windows(2) {
            debug_assert!(better[0].0 < better[1].0, "{:?}", dbg_each!(&self_dominating_intersections, &other_dominating_intersections));
            debug_assert_ne!(better[0].1, better[1].1, "{:?}", dbg_each!(&self_dominating_intersections, &other_dominating_intersections));
        }

        (TTFCache::Approx(result_lower.into_boxed_slice(), result_upper.into_boxed_slice()), result)
    }

    fn approximate(&self) -> TTFCache {
        use TTF::*;

        match self {
            Exact(plf) => TTFCache::Approx(plf.lower_bound_ttf(), plf.upper_bound_ttf()),
            Approx(lower_plf, upper_plf) => TTFCache::Approx(lower_plf.lower_bound_ttf(), upper_plf.upper_bound_ttf()),
        }
    }

    fn bound_plfs(&self) -> (PiecewiseLinearFunction<'a>, PiecewiseLinearFunction<'a>) {
        use TTF::*;

        match self {
            Exact(plf) => (*plf, *plf),
            Approx(lower_plf, upper_plf) => (*lower_plf, *upper_plf),
        }
    }
}

#[derive(Debug)]
pub struct Shortcut {
    sources: Sources,
    cache: Option<TTFCache>,
    pub lower_bound: FlWeight,
    pub upper_bound: FlWeight,
    constant: bool,
    pub required: bool,
}

impl Shortcut {
    pub fn new(source: Option<EdgeId>, original_graph: &TDGraph) -> Self {
        match source {
            Some(edge_id) => {
                PATH_SOURCES_COUNT.fetch_add(1, Relaxed);
                Shortcut {
                    sources: Sources::One(ShortcutSource::OriginalEdge(edge_id).into()),
                    cache: None,
                    lower_bound: original_graph.travel_time_function(edge_id).lower_bound(),
                    upper_bound: original_graph.travel_time_function(edge_id).upper_bound(),
                    constant: false,
                    required: true,
                }
            },
            None => Shortcut { sources: Sources::None, cache: None, lower_bound: FlWeight::INFINITY, upper_bound: FlWeight::INFINITY, constant: false, required: true },
        }
    }

    pub fn merge(&mut self, linked_ids: (EdgeId, EdgeId), shortcut_graph: &ShortcutGraph) {
        if !self.required { return }

        IPP_COUNT.fetch_sub(self.cache.as_ref().map(TTFCache::num_points).unwrap_or(0), Relaxed);
        PATH_SOURCES_COUNT.fetch_sub(self.sources.len(), Relaxed);
        if self.cache.is_some() { ACTIVE_SHORTCUTS.fetch_sub(1, Relaxed); }

        #[allow(clippy::redundant_closure_call)]
        (|| {
            let other_data = ShortcutSource::Shortcut(linked_ids.0, linked_ids.1).into();

            if !(shortcut_graph.get_incoming(linked_ids.0).is_valid_path() && shortcut_graph.get_outgoing(linked_ids.1).is_valid_path()) { return; }

            let first = shortcut_graph.get_incoming(linked_ids.0);
            let second = shortcut_graph.get_outgoing(linked_ids.1);

            let other_lower_bound = first.lower_bound + second.lower_bound;

            if self.upper_bound.fuzzy_lt(other_lower_bound) {
                return
            }

            let first_plf = first.plf(shortcut_graph);
            let second_plf = second.plf(shortcut_graph);

            if !self.is_valid_path() {
                ACTUALLY_LINKED.fetch_add(1, Relaxed);
                let linked = first_plf.link(&second_plf);

                self.upper_bound = min(self.upper_bound, TTF::from(&linked).static_upper_bound());
                debug_assert!(!self.upper_bound.fuzzy_lt(self.lower_bound), "lower {:?} upper {:?}", self.lower_bound, self.upper_bound);
                self.cache = Some(linked);
                self.sources = Sources::One(other_data);
                return;
            }

            let self_plf = self.plf(shortcut_graph);

            let mut linked_ipps = first_plf.link(&second_plf);
            ACTUALLY_LINKED.fetch_add(1, Relaxed);

            let linked = TTF::from(&linked_ipps);
            let other_lower_bound = linked.static_lower_bound();
            let other_upper_bound = linked.static_upper_bound();

            if !self_plf.static_lower_bound().fuzzy_lt(other_upper_bound) {
                self.upper_bound = min(self.upper_bound, other_upper_bound);
                debug_assert!(!self.upper_bound.fuzzy_lt(self.lower_bound), "lower {:?} upper {:?}", self.lower_bound, self.upper_bound);
                if cfg!(feature = "tdcch-approx") && linked_ipps.num_points() > 250 {
                    let old = linked_ipps.num_points();
                    CONSIDERED_FOR_APPROX.fetch_add(old, Relaxed);
                    linked_ipps = linked.approximate();
                    SAVED_BY_APPROX.fetch_add(old as isize - linked_ipps.num_points() as isize, Relaxed);
                }
                self.cache = Some(linked_ipps);
                self.sources = Sources::One(other_data);
                UNNECESSARY_LINKED.fetch_add(1, Relaxed);
                return;
            } else if self.upper_bound.fuzzy_lt(other_lower_bound) {
                return;
            }

            ACTUALLY_MERGED.fetch_add(1, Relaxed);
            let (mut merged, intersection_data) = self_plf.merge(&linked, |start, end| {
                let self_ipps = self.exact_ttf_for(start, end, shortcut_graph);
                let other_ipps = ShortcutSource::from(other_data).exact_ttf_for(start, end, shortcut_graph);
                PiecewiseLinearFunction::merge_partials(&self_ipps, &other_ipps, start, end)
            });
            if cfg!(feature = "tdcch-approx") && merged.num_points() > 250 {
                let old = merged.num_points();
                CONSIDERED_FOR_APPROX.fetch_add(old, Relaxed);
                merged = TTF::from(&merged).approximate();
                SAVED_BY_APPROX.fetch_add(old as isize - merged.num_points() as isize, Relaxed);
            }

            self.upper_bound = min(self.upper_bound, TTF::from(&merged).static_upper_bound());
            debug_assert!(!self.upper_bound.fuzzy_lt(self.lower_bound), "lower {:?} upper {:?}", self.lower_bound, self.upper_bound);
            self.cache = Some(merged);
            let mut sources = Sources::None;
            std::mem::swap(&mut sources, &mut self.sources);
            self.sources = Shortcut::combine(sources, intersection_data, other_data);
        }) ();

        IPP_COUNT.fetch_add(self.cache.as_ref().map(TTFCache::num_points).unwrap_or(0), Relaxed);
        PATH_SOURCES_COUNT.fetch_add(self.sources.len(), Relaxed);
        if self.cache.is_some() { ACTIVE_SHORTCUTS.fetch_add(1, Relaxed); }
    }

    fn plf<'s>(&'s self, shortcut_graph: &'s ShortcutGraph) -> TTF<'s> {
        if let Some(cache) = &self.cache {
            return cache.into()
        }

        match self.sources {
            Sources::One(source) => {
                match source.into() {
                    ShortcutSource::OriginalEdge(id) => TTF::Exact(shortcut_graph.original_graph().travel_time_function(id)),
                    _ => panic!("invalid state of shortcut: ipps must be cached when shortcut not trivial {:?}", self),
                }
            },
            _ => panic!("invalid state of shortcut: ipps must be cached when shortcut not trivial {:?}", self),
        }
    }

    fn is_valid_path(&self) -> bool {
        match self.sources {
            Sources::None => false,
            _ => true,
        }
    }

    pub fn finalize_bounds(&mut self, shortcut_graph: &ShortcutGraph) {
        if !self.required { return }

        if let Sources::None = self.sources {
            self.required = false;
            self.lower_bound = FlWeight::INFINITY;
            self.upper_bound = FlWeight::INFINITY;
            return
        }

        let new_lower_bound = self.plf(shortcut_graph).static_lower_bound();

        if self.upper_bound.fuzzy_lt(new_lower_bound) {
            self.required = false;
            self.sources = Sources::None;
            self.lower_bound = FlWeight::INFINITY;
            self.upper_bound = FlWeight::INFINITY;
            return
        }

        debug_assert!(!new_lower_bound.fuzzy_lt(self.lower_bound), "{:?}, {:?}", new_lower_bound, self);
        debug_assert!(!self.upper_bound.fuzzy_lt(new_lower_bound), "{:?}, {:?}", new_lower_bound, self);
        self.lower_bound = new_lower_bound;
        // let new_upper_bound = self.plf(shortcut_graph).upper_bound();
        // debug_assert!(!new_upper_bound.fuzzy_lt(self.upper_bound), "{:?}, {:?}", new_upper_bound, self);
        // debug_assert!(!new_upper_bound.fuzzy_lt(self.lower_bound), "{:?}, {:?}", new_upper_bound, self);
        // self.upper_bound = new_upper_bound;
    }

    pub fn update_is_constant(&mut self) {
        self.constant = self.upper_bound == self.lower_bound;
    }

    pub fn clear_plf(&mut self) {
        IPP_COUNT.fetch_sub(self.cache.as_ref().map(TTFCache::num_points).unwrap_or(0), Relaxed);
        if self.cache.is_some() { ACTIVE_SHORTCUTS.fetch_sub(1, Relaxed); }
        self.cache = None;
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
                while let Some(&(next_change, better)) = intersection_iter.peek() {
                    if next_change > at {
                        break;
                    }

                    self_currently_better = better;

                    if self_currently_better {
                        if next_change < at {
                            new_sources.push((next_change, *prev_source));
                        }
                    } else {
                        new_sources.push((next_change, other_data));
                    }

                    intersection_iter.next();
                }

                if self_currently_better {
                    new_sources.push((at, *source));
                }
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
        for sources in new_sources.windows(2) {
            debug_assert!(sources[0].0.fuzzy_lt(sources[1].0), "old: {:?}\nintersections: {:?}\nnew: {:?}", sources, debug_intersections, new_sources);
            debug_assert!(sources[0].0.fuzzy_lt(period()), "old: {:?}\nintersections: {:?}\nnew: {:?}", sources, debug_intersections, new_sources);
        }
        Sources::Multi(new_sources.into_boxed_slice())
    }

    pub(super) fn exact_ttf_for(&self, start: Timestamp, end: Timestamp, shortcut_graph: &ShortcutGraph) -> Vec<TTFPoint> {
        debug_assert!(start.fuzzy_lt(end), "{:?} - {:?}", start, end);

        if self.constant {
            return vec![TTFPoint { at: start, val: self.lower_bound }, TTFPoint { at: end, val: self.lower_bound }]
        }

        match &self.sources {
            Sources::None => unreachable!("There are no TTFs for empty shortcuts"),
            Sources::One(source) => ShortcutSource::from(*source).exact_ttf_for(start, end, shortcut_graph),
            Sources::Multi(sources) => {
                let mut result = Vec::new();
                let mut c = SourceCursor::valid_at(sources, start);

                loop {
                    let prev_last = result.pop();
                    let mut ttf = ShortcutSource::from(c.cur().1).exact_ttf_for(max(start, c.cur().0), min(end, c.next().0), shortcut_graph);
                    if !(prev_last.as_ref().map(|p: &TTFPoint| p.at.fuzzy_eq(ttf.first().unwrap().at) && p.val.fuzzy_eq(ttf.first().unwrap().val)).unwrap_or(true)) {
                        eprintln!("######################## Broken Intersection #################################");
                        dbg!(prev_last);
                        dbg!(ttf.first());
                    }
                    result.append(&mut ttf);
                    c.advance();
                    if !c.cur().0.fuzzy_lt(end) {
                        break
                    }
                }

                for points in result.windows(2) {
                    debug_assert!(points[0].at.fuzzy_lt(points[1].at));
                }

                result
            },
        }
    }

    pub fn num_sources(&self) -> usize {
        self.sources.len()
    }

    pub fn sources_iter(&self) -> impl Iterator<Item = (Timestamp, &ShortcutSourceData)> {
        self.sources.iter()
    }

    pub fn is_constant(&self) -> bool {
        self.constant
    }
}

#[derive(Debug, Clone)]
enum Sources {
    None,
    One(ShortcutSourceData),
    Multi(Box<[(Timestamp, ShortcutSourceData)]>)
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

#[derive(Debug)]
pub struct SourceCursor<'a> {
    sources: &'a [(Timestamp, ShortcutSourceData)],
    current_index: usize,
    offset: FlWeight,
}

impl<'a> SourceCursor<'a> {
    fn valid_at(sources: &'a [(Timestamp, ShortcutSourceData)], t: Timestamp) -> Self {
        debug_assert!(sources.len() > 1);

        let (times_period, t) = t.split_of_period();
        let offset = times_period * FlWeight::from(period());

        let pos = sources.binary_search_by(|p| {
            if p.0.fuzzy_eq(t) {
                Ordering::Equal
            } else if p.0 < t {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        });


        match pos {
            Ok(i) => Self { sources, current_index: i, offset },
            Err(i) => Self { sources, current_index: i - 1, offset }
        }
    }

    fn cur(&self) -> (Timestamp, ShortcutSourceData) {
        (self.sources[self.current_index].0 + self.offset, self.sources[self.current_index].1)
    }

    fn next(&self) -> (Timestamp, ShortcutSourceData) {
        if self.current_index + 1 == self.sources.len() {
            (self.sources[0].0 + self.offset + FlWeight::from(period()), self.sources[0].1)
        } else {
            (self.sources[self.current_index + 1].0 + self.offset, self.sources[self.current_index + 1].1)
        }
    }

    fn advance(&mut self) {
        self.current_index += 1;
        if self.current_index == self.sources.len() {
            self.offset = self.offset + FlWeight::from(period());
            self.current_index = 0;
        }
    }
}
