use super::*;
use std::sync::atomic::Ordering::Relaxed;
use std::cmp::{min, max, Ordering};

#[cfg(not(override_tdcch_approx_threshold))]
pub const APPROX_THRESHOLD: usize = 1000;
#[cfg(override_tdcch_approx_threshold)]
pub const APPROX_THRESHOLD: usize = include!(concat!(env!("OUT_DIR"), "/TDCCH_APPROX_THRESHOLD"));

#[derive(Debug)]
enum TTFCache<D> {
    Exact(D),
    Approx(D, D),
}

impl<> TTFCache<Vec<TTFPoint>> {
    fn num_points(&self) -> usize {
        use TTFCache::*;

        match &self {
            Exact(points) => points.len(),
            Approx(lower, upper) => lower.len() + upper.len(),
        }
    }
}

impl TTFCache<Box<[TTFPoint]>> {
    fn num_points(&self) -> usize {
        use TTFCache::*;

        match &self {
            Exact(points) => points.len(),
            Approx(lower, upper) => lower.len() + upper.len(),
        }
    }
}

impl From<TTFCache<Vec<TTFPoint>>> for TTFCache<Box<[TTFPoint]>> {
    fn from(cache: TTFCache<Vec<TTFPoint>>) -> Self {
        match cache {
            TTFCache::Exact(ipps) => TTFCache::Exact(ipps.into_boxed_slice()),
            TTFCache::Approx(lower_ipps, upper_ipps) => TTFCache::Approx(lower_ipps.into_boxed_slice(), upper_ipps.into_boxed_slice()),
        }
    }
}

#[derive(Debug, Clone, Copy, PartialEq)]
enum BoundMergingState {
    First,
    Second,
    Merge
}

#[derive(Debug)]
enum TTF<'a> {
    Exact(PiecewiseLinearFunction<'a>),
    Approx(PiecewiseLinearFunction<'a>, PiecewiseLinearFunction<'a>),
}

impl<'a> From<&'a TTFCache<Box<[TTFPoint]>>> for TTF<'a> {
    fn from(cache: &'a TTFCache<Box<[TTFPoint]>>) -> Self {
        match cache {
            TTFCache::Exact(ipps) => TTF::Exact(PiecewiseLinearFunction::new(ipps)),
            TTFCache::Approx(lower_ipps, upper_ipps) => TTF::Approx(PiecewiseLinearFunction::new(lower_ipps), PiecewiseLinearFunction::new(upper_ipps)),
        }
    }
}

impl<'a> From<&'a TTFCache<Vec<TTFPoint>>> for TTF<'a> {
    fn from(cache: &'a TTFCache<Vec<TTFPoint>>) -> Self {
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

    fn link(&self, second: &TTF) -> TTFCache<Vec<TTFPoint>> {
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
    #[allow(clippy::cognitive_complexity)]
    fn merge(&self, other: &TTF, buffers: &mut MergeBuffers, merge_exact: impl Fn(Timestamp, Timestamp, &mut MergeBuffers) -> (Box<[TTFPoint]>, Vec<(Timestamp, bool)>)) -> (TTFCache<Box<[TTFPoint]>>, Vec<(Timestamp, bool)>) {
        use TTF::*;

        if let (Exact(self_plf), Exact(other)) = (self, other) {
            let (plf, intersections) = self_plf.merge(other, &mut buffers.buffer);
            return (TTFCache::Exact(plf), intersections);
        }

        let (self_lower, self_upper) = self.bound_plfs();
        let (other_lower, other_upper) = other.bound_plfs();

        let (_, self_dominating_intersections) = self_upper.merge(&other_lower, &mut buffers.buffer);
        let (_, other_dominating_intersections) = other_upper.merge(&self_lower, &mut buffers.buffer);

        let mut dominating = false;
        let mut start_of_segment = Timestamp::zero();
        let mut self_dominating_iter = self_dominating_intersections.iter().peekable();
        let mut other_dominating_iter = other_dominating_intersections.iter().peekable();
        let mut result = Vec::new();
        let mut bound_merge_state = Vec::new();

        match (self_dominating_iter.peek().unwrap().1, other_dominating_iter.peek().unwrap().1) {
            (true, false) => {
                dominating = true;
                result.push((Timestamp::zero(), true));
                bound_merge_state.push((Timestamp::zero(), BoundMergingState::First));
            },
            (false, true) => {
                dominating = true;
                result.push((Timestamp::zero(), false));
                bound_merge_state.push((Timestamp::zero(), BoundMergingState::Second));
            },
            _ => {
                // in BOTH cases (especially the broken true true case) everything is unclear and we need to do exact merging
            }
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

                    start_of_segment = next_t_self;
                    self_dominating_iter.next();
                } else if next_t_other.fuzzy_lt(next_t_self) {
                    debug_assert!(!other_dominating_iter.peek().unwrap().1, "{:?}", dbg_each!(&self_dominating_intersections, &other_dominating_intersections, &self_lower, &self_upper, &other_lower, &other_upper)); // <--
                    debug_assert!(!result.last().unwrap().1, "{:?}", dbg_each!(&self_dominating_intersections, &other_dominating_intersections, &self_lower, &self_upper, &other_lower, &other_upper));
                    dominating = false;

                    start_of_segment = next_t_other;
                    other_dominating_iter.next();
                } else {
                    debug_assert_ne!(self_dominating_iter.peek().unwrap().1, other_dominating_iter.peek().unwrap().1, "{:?}", dbg_each!(&self_dominating_intersections, &other_dominating_intersections, &self_lower, &self_upper, &other_lower, &other_upper));
                    result.push((next_t_self, self_dominating_iter.peek().unwrap().1));

                    debug_assert!(bound_merge_state.last().map(|&(prev_start, _)| prev_start.fuzzy_lt(next_t_self)).unwrap_or(true), "{:?}", dbg_each!(bound_merge_state));
                    if self_dominating_iter.peek().unwrap().1 {
                        debug_assert!(bound_merge_state.last().map(|&(_, prev_state)| prev_state != BoundMergingState::First).unwrap_or(true), "{:?}", dbg_each!(bound_merge_state));
                        bound_merge_state.push((next_t_self, BoundMergingState::First));
                    } else {
                        debug_assert!(bound_merge_state.last().map(|&(_, prev_state)| prev_state != BoundMergingState::Second).unwrap_or(true), "{:?}", dbg_each!(next_t_self, next_t_other, bound_merge_state, self_dominating_intersections, other_dominating_intersections, result));
                        bound_merge_state.push((next_t_self, BoundMergingState::Second));
                    }

                    start_of_segment = next_t_self;
                    self_dominating_iter.next();
                    other_dominating_iter.next();
                }
            } else {
                if next_t_self.fuzzy_lt(next_t_other) {

                    let (_, intersections) = merge_exact(start_of_segment, next_t_self, buffers);

                    if intersections.len() > 1 || result.last().map(|(_, self_better)| *self_better != self_dominating_iter.peek().unwrap().1).unwrap_or(true) {
                        debug_assert!(bound_merge_state.last().map(|&(prev_start, prev_state)| prev_start.fuzzy_lt(start_of_segment) && prev_state != BoundMergingState::Merge).unwrap_or(true), "{:?}", dbg_each!(bound_merge_state));
                        bound_merge_state.push((start_of_segment, BoundMergingState::Merge));
                        debug_assert!(start_of_segment.fuzzy_lt(next_t_self), "{:?}", dbg_each!(start_of_segment, next_t_self));
                        bound_merge_state.push((next_t_self, if self_dominating_iter.peek().unwrap().1 { BoundMergingState::First } else { BoundMergingState::Second }));
                    }

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

                    start_of_segment = next_t_self;
                    dominating = true;
                    self_dominating_iter.next();
                } else if next_t_other.fuzzy_lt(next_t_self) {

                    let (_, intersections) = merge_exact(start_of_segment, next_t_other, buffers);

                    if intersections.len() > 1 || result.last().map(|(_, self_better)| !*self_better != other_dominating_iter.peek().unwrap().1).unwrap_or(true) {
                        debug_assert!(bound_merge_state.last().map(|&(prev_start, prev_state)| prev_start.fuzzy_lt(start_of_segment) && prev_state != BoundMergingState::Merge).unwrap_or(true), "{:?}", dbg_each!(bound_merge_state));
                        bound_merge_state.push((start_of_segment, BoundMergingState::Merge));
                        debug_assert!(start_of_segment.fuzzy_lt(next_t_other), "{:?}", dbg_each!(start_of_segment, next_t_other));
                        bound_merge_state.push((next_t_other, if other_dominating_iter.peek().unwrap().1 { BoundMergingState::Second } else { BoundMergingState::First }));
                    }

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
            let (_, intersections) = merge_exact(start_of_segment, period(), buffers);

            if intersections.len() > 1 ||
                result.last().map(|(_, self_better)| *self_better != intersections[0].1).unwrap_or(true) ||
                bound_merge_state.first().map(|&(_, initial_state)| initial_state == BoundMergingState::Merge).unwrap_or(true)
            {
                debug_assert!(bound_merge_state.last().map(|&(prev_start, prev_state)| prev_start.fuzzy_lt(start_of_segment) && prev_state != BoundMergingState::Merge).unwrap_or(true), "{:?}", dbg_each!(bound_merge_state));
                bound_merge_state.push((start_of_segment, BoundMergingState::Merge));
            }

            let mut iter = intersections.into_iter();
            let first_intersection = iter.next().unwrap();
            if result.last().map(|(_, self_better)| *self_better != first_intersection.1).unwrap_or(true) {
                result.push(first_intersection);
            }
            result.extend(iter);
        }

        debug_assert!(result.first().unwrap().0 == Timestamp::zero());
        for better in result.windows(2) {
            debug_assert!(better[0].0 < better[1].0, "{:?}", dbg_each!(&self_dominating_intersections, &other_dominating_intersections));
            debug_assert_ne!(better[0].1, better[1].1, "{:?}", dbg_each!(&self_dominating_intersections, &other_dominating_intersections));
        }

        buffers.exact_self_buffer.reserve(max(self_lower.len(), self_upper.len()));
        buffers.exact_other_buffer.reserve(max(other_lower.len(), other_upper.len()));
        buffers.exact_result_lower.reserve(2 * self_lower.len() + 2 * other_lower.len() + 2);
        buffers.exact_result_upper.reserve(2 * self_upper.len() + 2 * other_upper.len() + 2);

        debug_assert_eq!(bound_merge_state[0].0, Timestamp::zero());

        let mut end_of_segment_iter = bound_merge_state.iter().map(|(t, _)| *t).chain(std::iter::once(period()));
        end_of_segment_iter.next();

        for (&(start_of_segment, state), end_of_segment) in bound_merge_state.iter().zip(end_of_segment_iter) {
            match state {
                BoundMergingState::First => {
                    self_lower.copy_append_to_partial(start_of_segment, end_of_segment, &mut buffers.exact_result_lower);
                    self_upper.copy_append_to_partial(start_of_segment, end_of_segment, &mut buffers.exact_result_upper);
                },
                BoundMergingState::Second => {
                    other_lower.copy_append_to_partial(start_of_segment, end_of_segment, &mut buffers.exact_result_lower);
                    other_upper.copy_append_to_partial(start_of_segment, end_of_segment, &mut buffers.exact_result_upper);
                },
                BoundMergingState::Merge => {
                    buffers.exact_self_buffer.clear();
                    self_lower.copy_range(start_of_segment, end_of_segment, &mut buffers.exact_self_buffer);
                    buffers.exact_other_buffer.clear();
                    other_lower.copy_range(start_of_segment, end_of_segment, &mut buffers.exact_other_buffer);
                    let (partial_lower, _) = PiecewiseLinearFunction::merge_partials(&buffers.exact_self_buffer, &buffers.exact_other_buffer, start_of_segment, end_of_segment, &mut buffers.buffer);
                    PiecewiseLinearFunction::append_partials(&mut buffers.exact_result_lower, &partial_lower, start_of_segment);

                    buffers.exact_self_buffer.clear();
                    self_upper.copy_range(start_of_segment, end_of_segment, &mut buffers.exact_self_buffer);
                    buffers.exact_other_buffer.clear();
                    other_upper.copy_range(start_of_segment, end_of_segment, &mut buffers.exact_other_buffer);
                    let (partial_upper, _) = PiecewiseLinearFunction::merge_partials(&buffers.exact_self_buffer, &buffers.exact_other_buffer, start_of_segment, end_of_segment, &mut buffers.buffer);
                    PiecewiseLinearFunction::append_partials(&mut buffers.exact_result_upper, &partial_upper, start_of_segment);
                }
            }
        }

        let ret = (TTFCache::Approx(buffers.exact_result_lower[..].to_vec().into_boxed_slice(), buffers.exact_result_upper[..].to_vec().into_boxed_slice()), result);

        buffers.exact_self_buffer.clear();
        buffers.exact_other_buffer.clear();
        buffers.exact_result_lower.clear();
        buffers.exact_result_upper.clear();

        ret
        // let (result_lower, _) = self_lower.merge(&other_lower, &mut buffers.buffer);
        // let (result_upper, _) = self_upper.merge(&other_upper, &mut buffers.buffer);
        // (TTFCache::Approx(result_lower, result_upper), result)
    }

    fn approximate(&self, buffers: &mut MergeBuffers) -> TTFCache<Box<[TTFPoint]>> {
        use TTF::*;

        match self {
            Exact(plf) => {
                let (lower, upper) = plf.bound_ttfs();
                TTFCache::Approx(lower, upper)
            },
            Approx(lower_plf, upper_plf) => TTFCache::Approx(lower_plf.lower_bound_ttf(&mut buffers.buffer), upper_plf.upper_bound_ttf(&mut buffers.buffer)),
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
    cache: Option<TTFCache<Box<[TTFPoint]>>>,
    pub lower_bound: FlWeight,
    pub upper_bound: FlWeight,
    constant: bool,
    pub required: bool,
}

impl Shortcut {
    pub fn new(source: Option<EdgeId>, original_graph: &TDGraph) -> Self {
        match source {
            Some(edge_id) => {
                if cfg!(feature = "detailed-stats") { PATH_SOURCES_COUNT.fetch_add(1, Relaxed); }
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

    pub fn merge(&mut self, linked_ids: (EdgeId, EdgeId), shortcut_graph: &PartialShortcutGraph, buffers: &mut MergeBuffers) {
        if !self.required { return }

        if cfg!(feature = "detailed-stats") {
            IPP_COUNT.fetch_sub(self.cache.as_ref().map(TTFCache::<Box<[TTFPoint]>>::num_points).unwrap_or(0), Relaxed);
            PATH_SOURCES_COUNT.fetch_sub(self.sources.len(), Relaxed);
            if self.cache.is_some() { ACTIVE_SHORTCUTS.fetch_sub(1, Relaxed); }
        }

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
                if cfg!(feature = "detailed-stats") { ACTUALLY_LINKED.fetch_add(1, Relaxed); }
                let linked = first_plf.link(&second_plf);

                self.upper_bound = min(self.upper_bound, TTF::from(&linked).static_upper_bound());
                debug_assert!(!self.upper_bound.fuzzy_lt(self.lower_bound), "lower {:?} upper {:?}", self.lower_bound, self.upper_bound);
                self.cache = Some(linked.into());
                self.sources = Sources::One(other_data);
                return;
            }

            let self_plf = self.plf(shortcut_graph);

            let linked_ipps = first_plf.link(&second_plf);
            if cfg!(feature = "detailed-stats") { ACTUALLY_LINKED.fetch_add(1, Relaxed); }

            let linked = TTF::from(&linked_ipps);
            let other_lower_bound = linked.static_lower_bound();
            let other_upper_bound = linked.static_upper_bound();

            if !self_plf.static_lower_bound().fuzzy_lt(other_upper_bound) {
                self.upper_bound = min(self.upper_bound, other_upper_bound);
                debug_assert!(!self.upper_bound.fuzzy_lt(self.lower_bound), "lower {:?} upper {:?}", self.lower_bound, self.upper_bound);
                if cfg!(feature = "tdcch-approx") && linked_ipps.num_points() > APPROX_THRESHOLD {
                    let old = linked_ipps.num_points();
                    if cfg!(feature = "detailed-stats") { CONSIDERED_FOR_APPROX.fetch_add(old, Relaxed); }
                    let linked_ipps = linked.approximate(buffers);
                    if cfg!(feature = "detailed-stats") { SAVED_BY_APPROX.fetch_add(old as isize - linked_ipps.num_points() as isize, Relaxed); }
                    self.cache = Some(linked_ipps);
                } else {
                    self.cache = Some(linked_ipps.into());
                }
                self.sources = Sources::One(other_data);
                if cfg!(feature = "detailed-stats") { UNNECESSARY_LINKED.fetch_add(1, Relaxed); }
                return;
            } else if self.upper_bound.fuzzy_lt(other_lower_bound) {
                return;
            }

            if cfg!(feature = "detailed-stats") { ACTUALLY_MERGED.fetch_add(1, Relaxed); }
            let (mut merged, intersection_data) = self_plf.merge(&linked, buffers, |start, end, buffers| {
                let mut self_target = buffers.unpacking_target.push_plf();
                self.exact_ttf_for(start, end, shortcut_graph, &mut self_target, &mut buffers.unpacking_tmp);

                let mut other_target = self_target.storage_mut().push_plf();
                ShortcutSource::from(other_data).exact_ttf_for(start, end, shortcut_graph, &mut other_target, &mut buffers.unpacking_tmp);

                let (self_ipps, other_ipps) = other_target.storage().top_plfs();
                PiecewiseLinearFunction::merge_partials(self_ipps, other_ipps, start, end, &mut buffers.buffer)
            });
            if cfg!(feature = "tdcch-approx") && merged.num_points() > APPROX_THRESHOLD {
                let old = merged.num_points();
                if cfg!(feature = "detailed-stats") { CONSIDERED_FOR_APPROX.fetch_add(old, Relaxed); }
                merged = TTF::from(&merged).approximate(buffers);
                if cfg!(feature = "detailed-stats") { SAVED_BY_APPROX.fetch_add(old as isize - merged.num_points() as isize, Relaxed); }
            }

            self.upper_bound = min(self.upper_bound, TTF::from(&merged).static_upper_bound());
            debug_assert!(!self.upper_bound.fuzzy_lt(self.lower_bound), "lower {:?} upper {:?}", self.lower_bound, self.upper_bound);
            self.cache = Some(merged);
            let mut sources = Sources::None;
            std::mem::swap(&mut sources, &mut self.sources);
            self.sources = Shortcut::combine(sources, intersection_data, other_data);
        }) ();

        if cfg!(feature = "detailed-stats") {
            IPP_COUNT.fetch_add(self.cache.as_ref().map(TTFCache::<Box<[TTFPoint]>>::num_points).unwrap_or(0), Relaxed);
            PATH_SOURCES_COUNT.fetch_add(self.sources.len(), Relaxed);
            if self.cache.is_some() { ACTIVE_SHORTCUTS.fetch_add(1, Relaxed); }
        }
    }

    fn plf<'s>(&'s self, shortcut_graph: &'s PartialShortcutGraph) -> TTF<'s> {
        if let Some(cache) = &self.cache {
            return cache.into()
        }

        match self.sources {
            Sources::One(source) => {
                match source.into() {
                    ShortcutSource::OriginalEdge(id) => TTF::Exact(shortcut_graph.original_graph.travel_time_function(id)),
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

    pub fn finalize_bounds(&mut self, shortcut_graph: &PartialShortcutGraph) {
        if !self.required { return }

        if let Sources::None = self.sources {
            self.required = false;
            self.lower_bound = FlWeight::INFINITY;
            self.upper_bound = FlWeight::INFINITY;
            return
        }

        let new_lower_bound = max(self.lower_bound, self.plf(shortcut_graph).static_lower_bound());

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

        let new_upper_bound = min(self.upper_bound, self.plf(shortcut_graph).static_upper_bound());
        debug_assert!(!new_upper_bound.fuzzy_lt(self.upper_bound), "{:?}, {:?}", new_upper_bound, self);
        debug_assert!(!new_upper_bound.fuzzy_lt(self.lower_bound), "{:?}, {:?}", new_upper_bound, self);
        self.upper_bound = new_upper_bound;
    }

    pub fn invalidate_unneccesary_sources(&mut self, shortcut_graph: &PartialShortcutGraph) {
        match &mut self.sources {
            Sources::None => {},
            Sources::One(source) => {
                if !ShortcutSource::from(*source).required(shortcut_graph) {
                    *source = ShortcutSource::None.into();
                    self.upper_bound = FlWeight::INFINITY;
                }
            },
            Sources::Multi(sources) => {
                for (_, source) in &mut sources[..] {
                    if !ShortcutSource::from(*source).required(shortcut_graph) {
                        *source = ShortcutSource::None.into();
                        self.upper_bound = FlWeight::INFINITY;
                    }
                }
            }
        }
    }

    pub fn update_is_constant(&mut self) {
        self.constant = self.upper_bound == self.lower_bound;
    }

    pub fn clear_plf(&mut self) {
        if cfg!(feature = "detailed-stats") {
            IPP_COUNT.fetch_sub(self.cache.as_ref().map(TTFCache::<Box<[TTFPoint]>>::num_points).unwrap_or(0), Relaxed);
            if self.cache.is_some() { ACTIVE_SHORTCUTS.fetch_sub(1, Relaxed); }
        }
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

        let dummy = ShortcutSource::None.into();
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

    pub(super) fn exact_ttf_for(&self, start: Timestamp, end: Timestamp, shortcut_graph: &PartialShortcutGraph, target: &mut MutTopPLF, tmp: &mut ReusablePLFStorage) {
        debug_assert!(start.fuzzy_lt(end), "{:?} - {:?}", start, end);

        if self.constant {
            target.push(TTFPoint { at: start, val: self.lower_bound });
            target.push(TTFPoint { at: end, val: self.lower_bound });
            return
        }

        match &self.sources {
            Sources::None => unreachable!("There are no TTFs for empty shortcuts"),
            Sources::One(source) => ShortcutSource::from(*source).exact_ttf_for(start, end, shortcut_graph, target, tmp),
            Sources::Multi(sources) => {
                let mut c = SourceCursor::valid_at(sources, start);

                while c.cur().0.fuzzy_lt(end) {
                    let mut inner_target = tmp.push_plf();
                    ShortcutSource::from(c.cur().1).exact_ttf_for(max(start, c.cur().0), min(end, c.next().0), shortcut_graph, &mut inner_target, target.storage_mut());
                    PiecewiseLinearFunction::append_partials(target, &inner_target, max(start, c.cur().0));

                    c.advance();
                }

                for points in target.windows(2) {
                    debug_assert!(points[0].at.fuzzy_lt(points[1].at));
                }
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

pub struct MergeBuffers {
    unpacking_target: ReusablePLFStorage,
    unpacking_tmp: ReusablePLFStorage,
    buffer: Vec<TTFPoint>,
    exact_self_buffer: Vec<TTFPoint>,
    exact_other_buffer: Vec<TTFPoint>,
    exact_result_lower: Vec<TTFPoint>,
    exact_result_upper: Vec<TTFPoint>,
}

impl Default for MergeBuffers {
    fn default() -> Self {
        Self::new()
    }
}

impl MergeBuffers {
    pub fn new() -> Self {
        MergeBuffers {
            unpacking_target: ReusablePLFStorage::new(),
            unpacking_tmp: ReusablePLFStorage::new(),
            buffer: Vec::new(),
            exact_self_buffer: Vec::new(),
            exact_other_buffer: Vec::new(),
            exact_result_lower: Vec::new(),
            exact_result_upper: Vec::new(),
        }
    }
}
