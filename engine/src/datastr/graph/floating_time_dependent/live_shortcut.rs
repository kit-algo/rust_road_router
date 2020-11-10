//! Logic and data structures for managing data associated TD-CCH edges.

use super::shortcut::Sources;
use super::shortcut_source::Sources as _;
use super::*;
use std::cmp::{max, min};
use std::sync::atomic::Ordering::Relaxed;

/// Shortcut data for a CCH edge.
///
/// Here, we use Shortcut as the name for all CCH edges -- probably TDCCHEdge would be a better name.
/// A shortcut may contain several `ShortcutSource`s which are valid (optimal) for different ranges of times.
/// `ShortcutSource`s may be edges from the original graph, real shortcuts - that is skipping over a lower triangle, or None, if edge is not necessary for some time.
///
/// A shortcut additionally stores upper and lower bounds, a flag if its function is constant and
/// one if the edge is necessary at all (unnecessary edges may be removed during perfect customization).
/// Also during customization we keep the corresponding travel time function around for as long as we need it.
#[derive(Debug)]
pub struct LiveShortcut {
    sources: Sources,
    cache: Option<ApproxTTFContainer<Box<[TTFPoint]>>>,
    pub lower_bound: FlWeight,
    pub upper_bound: FlWeight,
    constant: bool,
    /// Is this edge actually necessary in a CH? Set to `false` to mark for removal in perfect customization.
    pub required: bool,
    pub live_until: Option<Timestamp>,
    pub unpack: Option<Timestamp>,
}

impl LiveShortcut {
    /// Create new `LiveShortcut` referencing an original edge or set to Infinity.
    pub fn new(source: Option<EdgeId>, original_graph: &LiveGraph) -> Self {
        match source {
            Some(edge_id) => {
                if cfg!(feature = "detailed-stats") {
                    PATH_SOURCES_COUNT.fetch_add(1, Relaxed);
                }
                LiveShortcut {
                    sources: Sources::One(ShortcutSource::OriginalEdge(edge_id).into()),
                    cache: None,
                    lower_bound: original_graph.travel_time_function(edge_id).lower_bound(),
                    upper_bound: original_graph.travel_time_function(edge_id).upper_bound(),
                    constant: false,
                    required: true,
                    live_until: original_graph.travel_time_function(edge_id).t_switch(),
                    unpack: None,
                }
            }
            None => LiveShortcut {
                sources: Sources::None,
                cache: None,
                lower_bound: FlWeight::INFINITY,
                upper_bound: FlWeight::INFINITY,
                constant: false,
                required: true,
                live_until: None,
                unpack: None,
            },
        }
    }

    pub fn reconstruct_cache<'g>(
        &mut self,
        pred_shortcut: &Shortcut,
        shortcut_graph: &'g impl ShortcutGraphTrt<'g, OriginalGraph = LiveGraph, ApproxTTF = ApproxPartialTTF<'g>>,
        buffers: &mut MergeBuffers,
    ) {
        // We already know, we won't need this edge, so do nothing
        if !self.required {
            return;
        }

        let live = self.live_until.is_some();
        let unpack_end = if let Some(unpack_end) = self.unpack { unpack_end } else { return };
        let unpack_start = self.live_until.unwrap_or(shortcut_graph.original_graph().t_live());

        let exact_ttfs_available = pred_shortcut
            .sources_for(unpack_start, unpack_end)
            .all(|(_, source)| match ShortcutSource::from(source) {
                ShortcutSource::Shortcut(down, up) => {
                    shortcut_graph.ttf(ShortcutId::Incoming(down)).exact() && shortcut_graph.ttf(ShortcutId::Outgoing(up)).exact()
                }
                _ => true,
            });

        if exact_ttfs_available && (!live || self.travel_time_function(shortcut_graph).exact()) {
            let mut target = buffers.unpacking_target.push_plf();
            if live {
                if let ApproxPartialTTF::Exact(plf) = self.travel_time_function(shortcut_graph) {
                    plf.append(shortcut_graph.original_graph().t_live(), &mut target)
                } else {
                    unreachable!()
                }
            }
            pred_shortcut.exact_ttf_for(unpack_start, unpack_end, shortcut_graph, &mut target, &mut buffers.unpacking_tmp);
            self.cache = Some(ApproxTTFContainer::Exact(Box::<[TTFPoint]>::from(&target[..])));
            return;
        }

        let sources_iter = || {
            pred_shortcut.sources_for(unpack_start, unpack_end).zip(
                pred_shortcut
                    .sources_for(unpack_start, unpack_end)
                    .map(|(t_beg, _)| t_beg)
                    .skip(1)
                    .chain(std::iter::once(unpack_end)),
            )
        };

        let mut target = buffers.unpacking_target.push_plf();

        if live {
            self.travel_time_function(shortcut_graph)
                .bound_plfs()
                .0
                .append(shortcut_graph.original_graph().t_live(), &mut target);
        }

        for ((range_start, source), range_end) in sources_iter() {
            let mut inner_target = buffers.unpacking_tmp.push_plf();
            ShortcutSource::from(source).partial_lower_bound_from_partial(
                max(unpack_start, range_start),
                min(unpack_end, range_end),
                shortcut_graph,
                &mut inner_target,
                target.storage_mut(),
            );
            PeriodicPiecewiseLinearFunction::append_lower_bound_partials(&mut target, &inner_target, max(unpack_start, range_start));
        }

        let lower = Box::<[TTFPoint]>::from(&target[..]);
        drop(target);

        let mut target = buffers.unpacking_target.push_plf();

        if live {
            self.travel_time_function(shortcut_graph)
                .bound_plfs()
                .1
                .append(shortcut_graph.original_graph().t_live(), &mut target);
        }

        for ((range_start, source), range_end) in sources_iter() {
            let mut inner_target = buffers.unpacking_tmp.push_plf();
            ShortcutSource::from(source).partial_upper_bound_from_partial(
                max(unpack_start, range_start),
                min(unpack_end, range_end),
                shortcut_graph,
                &mut inner_target,
                target.storage_mut(),
            );
            PeriodicPiecewiseLinearFunction::append_upper_bound_partials(&mut target, &inner_target, max(unpack_start, range_start));
        }

        let upper = Box::<[TTFPoint]>::from(&target[..]);

        self.cache = Some(ApproxTTFContainer::Approx(lower, upper));
    }

    /// Merge this LiveShortcut with the lower triangle made up of the two EdgeIds (first down, then up).
    /// The `shortcut_graph` has to contain all the edges we may need to unpack.
    pub fn merge<'g>(
        &mut self,
        linked_ids: (EdgeId, EdgeId),
        shortcut_graph: &'g impl ShortcutGraphTrt<'g, OriginalGraph = LiveGraph, ApproxTTF = ApproxPartialTTF<'g>>,
        buffers: &mut MergeBuffers,
    ) {
        // We already know, we won't need this edge, so do nothing
        if !self.required {
            return;
        }

        let live_until = if let Some(live_until) = self.live_until { live_until } else { return };
        let live_from = shortcut_graph.original_graph().t_live();

        if cfg!(feature = "detailed-stats") {
            IPP_COUNT.fetch_sub(self.cache.as_ref().map(ApproxTTFContainer::<Box<[TTFPoint]>>::num_points).unwrap_or(0), Relaxed);
            PATH_SOURCES_COUNT.fetch_sub(self.sources.len(), Relaxed);
            if self.cache.is_some() {
                ACTIVE_SHORTCUTS.fetch_sub(1, Relaxed);
            }
        }

        // we have to update the stats once we're done merging, so wrap everything in a lambda so we can do early returns and only exit the lambda.
        (|| {
            let other_data = ShortcutSource::Shortcut(linked_ids.0, linked_ids.1).into();

            // if one of the edges of the triangle is an infinity edge we don't need to do anything
            if !(shortcut_graph.is_valid_path(ShortcutId::Incoming(linked_ids.0)) && shortcut_graph.is_valid_path(ShortcutId::Outgoing(linked_ids.1))) {
                return;
            }

            let other_lower_bound =
                shortcut_graph.lower_bound(ShortcutId::Incoming(linked_ids.0)) + shortcut_graph.lower_bound(ShortcutId::Outgoing(linked_ids.1));

            // current upper bound always better than linked lower bound - do nothing
            if self.upper_bound.fuzzy_lt(other_lower_bound) {
                return;
            }

            // get cached (possibly approximated) TTFs
            let first_plf = shortcut_graph.ttf(ShortcutId::Incoming(linked_ids.0));
            let second_plf = shortcut_graph.ttf(ShortcutId::Outgoing(linked_ids.1));

            // when the current shortcut is always infinity, the linked paths will always be better.
            if !self.is_valid_path() {
                if cfg!(feature = "detailed-stats") {
                    ACTUALLY_LINKED.fetch_add(1, Relaxed);
                }
                // link functions
                let linked = first_plf.link(&second_plf, live_from, live_until);

                self.upper_bound = min(self.upper_bound, ApproxPartialTTF::from(&linked).static_upper_bound());
                debug_assert!(
                    !cfg!(feature = "tdcch-precustomization") || !self.upper_bound.fuzzy_lt(self.lower_bound),
                    "lower {:?} upper {:?}",
                    self.lower_bound,
                    self.upper_bound
                );
                self.cache = Some(linked.into());
                self.sources = Sources::One(other_data);
                return;
            }

            // get own cached TTF
            let self_plf = self.travel_time_function(shortcut_graph);

            // link TTFs in triangle
            let linked_ipps = first_plf.link(&second_plf, live_from, live_until);
            if cfg!(feature = "detailed-stats") {
                ACTUALLY_LINKED.fetch_add(1, Relaxed);
            }

            let linked = ApproxPartialTTF::from(&linked_ipps);
            // these bounds are more tight than the previous ones
            let other_lower_bound = linked.static_lower_bound();
            let other_upper_bound = linked.static_upper_bound();

            // note that self_plf.static_lower_bound() >= self.lower_bound
            // this can be the case because we set self.lower_bound during precustomization as low as possible
            // if we would compare here to self.lower_bound we might never take that branch, even when we would have to.
            if !self_plf.static_lower_bound().fuzzy_lt(other_upper_bound) {
                // new linked function is always better than the current one
                self.upper_bound = min(self.upper_bound, other_upper_bound);
                debug_assert!(
                    !cfg!(feature = "tdcch-precustomization") || !self.upper_bound.fuzzy_lt(self.lower_bound),
                    "lower {:?} upper {:?}",
                    self.lower_bound,
                    self.upper_bound
                );
                if cfg!(feature = "tdcch-approx") && linked_ipps.num_points() > APPROX_THRESHOLD {
                    let old = linked_ipps.num_points();
                    if cfg!(feature = "detailed-stats") {
                        CONSIDERED_FOR_APPROX.fetch_add(old, Relaxed);
                    }
                    let linked_ipps = linked.approximate(buffers);
                    if cfg!(feature = "detailed-stats") {
                        SAVED_BY_APPROX.fetch_add(old as isize - linked_ipps.num_points() as isize, Relaxed);
                    }
                    self.cache = Some(linked_ipps);
                } else {
                    self.cache = Some(linked_ipps.into());
                }
                self.sources = Sources::One(other_data);
                if cfg!(feature = "detailed-stats") {
                    UNNECESSARY_LINKED.fetch_add(1, Relaxed);
                }
                return;
            } else if self.upper_bound.fuzzy_lt(other_lower_bound) {
                // current upper bound always better than linked lower bound - keep whatever we currently have
                return;
            }

            // all bound checking done, we need to actually merge
            if cfg!(feature = "detailed-stats") {
                ACTUALLY_MERGED.fetch_add(1, Relaxed);
            }

            // this function does exact merging, even when we have only approximate functions by unpacking exact functions for time ranges when bounds overlap.
            // the callback executes exact merging for small time ranges where the bounds overlap, the function takes care of all the rest around that.
            let (mut merged, intersection_data) = self_plf.merge(&linked, live_from, live_until, buffers, |start, end, buffers| {
                let mut self_target = buffers.unpacking_target.push_plf();
                self.exact_ttf_for(start, end, shortcut_graph, &mut self_target, &mut buffers.unpacking_tmp);

                let mut other_target = self_target.storage_mut().push_plf();
                ShortcutSource::from(other_data).exact_ttf_for(start, end, shortcut_graph, &mut other_target, &mut buffers.unpacking_tmp);

                let (self_ipps, other_ipps) = other_target.storage().top_plfs();
                PartialPiecewiseLinearFunction::new(self_ipps).merge(&PartialPiecewiseLinearFunction::new(other_ipps), start, end, &mut buffers.buffer)
            });
            if cfg!(feature = "tdcch-approx") && merged.num_points() > APPROX_THRESHOLD {
                let old = merged.num_points();
                if cfg!(feature = "detailed-stats") {
                    CONSIDERED_FOR_APPROX.fetch_add(old, Relaxed);
                }
                merged = ApproxTTF::from(&merged).approximate(buffers);
                if cfg!(feature = "detailed-stats") {
                    SAVED_BY_APPROX.fetch_add(old as isize - merged.num_points() as isize, Relaxed);
                }
            }

            // update upper bounds after merging, but not lower bounds.
            // lower bounds were already set during precustomization -- possibly lower than necessary.
            // We would like to increase the lower bound to make it tighter, but we can't take the max right now,
            // We might find a lower function during a later merge operation.
            // We can only set the lower bound as tight as possible, once we have the final travel time function.
            self.upper_bound = min(self.upper_bound, ApproxTTF::from(&merged).static_upper_bound());
            debug_assert!(
                !cfg!(feature = "tdcch-precustomization") || !self.upper_bound.fuzzy_lt(self.lower_bound),
                "lower {:?} upper {:?}",
                self.lower_bound,
                self.upper_bound
            );
            self.cache = Some(merged);
            let mut sources = Sources::None;
            std::mem::swap(&mut sources, &mut self.sources);
            // calculate new `ShortcutSource`s.
            self.sources = LiveShortcut::combine(sources, intersection_data, other_data);
        })();

        if cfg!(feature = "detailed-stats") {
            IPP_COUNT.fetch_add(self.cache.as_ref().map(ApproxTTFContainer::<Box<[TTFPoint]>>::num_points).unwrap_or(0), Relaxed);
            PATH_SOURCES_COUNT.fetch_add(self.sources.len(), Relaxed);
            if self.cache.is_some() {
                ACTIVE_SHORTCUTS.fetch_add(1, Relaxed);
            }
        }
    }

    pub fn travel_time_function<'s, 'g: 's>(&'s self, shortcut_graph: &'g impl ShortcutGraphTrt<'g, OriginalGraph = LiveGraph>) -> ApproxPartialTTF<'s> {
        if let Some(cache) = &self.cache {
            return cache.into();
        }

        match self.sources {
            Sources::One(source) => match source.into() {
                ShortcutSource::OriginalEdge(id) => ApproxPartialTTF::Exact(
                    shortcut_graph.original_graph().travel_time_function(id).update_plf().unwrap_or(
                        PartialPiecewiseLinearFunction::from(shortcut_graph.original_graph().travel_time_function(id).unmodified_plf())
                            .sub_plf(shortcut_graph.original_graph().t_live(), self.live_until.unwrap()),
                    ),
                ),
                _ => panic!("invalid state of shortcut: ipps must be cached when shortcut not trivial {:?}", self),
            },
            _ => panic!("invalid state of shortcut: ipps must be cached when shortcut not trivial {:?}", self),
        }
    }

    fn get_travel_time_function<'s, 'g: 's>(
        &'s self,
        shortcut_graph: &'g impl ShortcutGraphTrt<'g, OriginalGraph = LiveGraph>,
    ) -> Option<ApproxPartialTTF<'s>> {
        if let Some(cache) = &self.cache {
            return Some(cache.into());
        }

        match self.sources {
            Sources::One(source) => match source.into() {
                ShortcutSource::OriginalEdge(id) => Some(ApproxPartialTTF::Exact(
                    shortcut_graph.original_graph().travel_time_function(id).update_plf().unwrap_or(
                        PartialPiecewiseLinearFunction::from(shortcut_graph.original_graph().travel_time_function(id).unmodified_plf())
                            .sub_plf(shortcut_graph.original_graph().t_live(), self.live_until.unwrap()),
                    ),
                )),
                _ => None,
            },
            _ => None,
        }
    }

    pub fn is_valid_path(&self) -> bool {
        match self.sources {
            Sources::None => false,
            _ => true,
        }
    }

    /// Once the TTF of this Shortcut is final, we can tighten the lower bound.
    /// When we know or detect, that we don't need this shortcut, we set all bounds to infinity.
    pub fn finalize_bounds<'g>(&mut self, shortcut_graph: &'g impl ShortcutGraphTrt<'g, OriginalGraph = LiveGraph>) {
        if !self.required {
            return;
        }

        if let Some(ttf_until) = max(self.unpack, self.live_until) {
            let (lower, upper) = self.travel_time_function(shortcut_graph).bound_plfs();
            debug_assert!(!lower.last().unwrap().at.fuzzy_lt(ttf_until));
            debug_assert!(!upper.last().unwrap().at.fuzzy_lt(ttf_until));
        } else {
            return;
        }

        if let Sources::None = self.sources {
            self.required = false;
            self.lower_bound = FlWeight::INFINITY;
            self.upper_bound = FlWeight::INFINITY;
            return;
        }

        let new_lower_bound = if cfg!(feature = "tdcch-precustomization") {
            max(self.lower_bound, self.travel_time_function(shortcut_graph).static_lower_bound())
        } else {
            self.travel_time_function(shortcut_graph).static_lower_bound()
        };

        // The final functions lower bound is worse than the upper bound this shortcut got during pre/post/perfect customization
        // Thus, we will never need it.
        if self.upper_bound.fuzzy_lt(new_lower_bound) {
            self.required = false;
            self.sources = Sources::None;
            self.lower_bound = FlWeight::INFINITY;
            self.upper_bound = FlWeight::INFINITY;
            return;
        }

        debug_assert!(
            !cfg!(feature = "tdcch-precustomization") || !new_lower_bound.fuzzy_lt(self.lower_bound),
            "{:?}, {:?}",
            new_lower_bound,
            self
        );
        debug_assert!(!self.upper_bound.fuzzy_lt(new_lower_bound), "{:?}, {:?}", new_lower_bound, self);
        self.lower_bound = new_lower_bound;

        let new_upper_bound = min(self.upper_bound, self.travel_time_function(shortcut_graph).static_upper_bound());
        debug_assert!(!new_upper_bound.fuzzy_lt(self.lower_bound), "{:?}, {:?}", new_upper_bound, self);
        self.upper_bound = new_upper_bound;
    }

    /// If Shortcuts in skipped triangles are not required, the corresponding `Source` in this shortcut is also not required, so remove it
    pub fn disable_if_unneccesary(&mut self, shortcut_graph: &PartialShortcutGraph) {
        match &mut self.sources {
            Sources::None => {}
            Sources::One(source) => {
                if !ShortcutSource::from(*source).required(shortcut_graph) {
                    self.required = false;
                    self.lower_bound = FlWeight::INFINITY;
                    self.upper_bound = FlWeight::INFINITY;
                }
            }
            Sources::Multi(sources) => {
                let mut any_required = false;
                for (_, source) in &mut sources[..] {
                    if ShortcutSource::from(*source).required(shortcut_graph) {
                        any_required = true;
                    }
                }
                if !any_required {
                    self.required = false;
                    self.lower_bound = FlWeight::INFINITY;
                    self.upper_bound = FlWeight::INFINITY;
                }
            }
        }
    }

    pub fn reenable_required(&self, downward: &mut [Shortcut], upward: &mut [Shortcut]) {
        if self.required {
            for (_, source) in self.sources_iter() {
                if let ShortcutSource::Shortcut(down, up) = source.into() {
                    downward[down as usize].required = true;
                    upward[up as usize].required = true;
                }
            }
        }
    }

    pub fn update_is_constant(&mut self) {
        // exact check here probably overly pessimistic, but we only loose performance when an edge is unnecessarily marked as non-const
        self.constant = self.upper_bound == self.lower_bound;
    }

    /// Drop cached TTF to save memory.
    /// Should only be called once it is really not needed anymore.
    pub fn clear_plf(&mut self) {
        if cfg!(feature = "detailed-stats") {
            IPP_COUNT.fetch_sub(self.cache.as_ref().map(ApproxTTFContainer::<Box<[TTFPoint]>>::num_points).unwrap_or(0), Relaxed);
            if self.cache.is_some() {
                ACTIVE_SHORTCUTS.fetch_sub(1, Relaxed);
            }
        }
        self.cache = None;
    }

    pub fn set_cache(&mut self, ttf: Option<ApproxTTFContainer<Box<[TTFPoint]>>>) {
        self.cache = ttf;
    }

    pub fn set_sources(&mut self, sources: &[(Timestamp, ShortcutSourceData)]) {
        self.sources = match sources {
            &[] => Sources::None,
            &[(_, data)] => Sources::One(data),
            data => Sources::Multi(data.into()),
        };
    }

    // Combine current `Sources` and the result of a merge into new `Sources`
    fn combine(sources: Sources, intersection_data: Vec<(Timestamp, bool)>, other_data: ShortcutSourceData) -> Sources {
        // when just one is better all the time
        if let [(_, is_self_better)] = &intersection_data[..] {
            if *is_self_better {
                // stick with current sources
                return sources;
            } else {
                // just the new data
                return Sources::One(other_data);
            }
        }

        debug_assert!(intersection_data.len() >= 2);
        let &(zero, mut self_currently_better) = intersection_data.first().unwrap();
        debug_assert!(zero == Timestamp::zero());
        let debug_intersections = intersection_data.clone();
        let mut intersection_iter = intersection_data.into_iter().peekable();

        let dummy: ShortcutSourceData = ShortcutSource::None.into();
        let mut prev_source = dummy;
        let mut new_sources = Vec::new();

        // iterate over all old sources.
        // while self is better we need to copy these over
        // when other becomes better at an intersection we need to insert other_data at the intersection time
        // when self becomes better at an intersection we need to insert the source that was active at that time in the old sources at the new intersection time.
        for (at, source) in sources.iter() {
            if intersection_iter.peek().is_none() || at < intersection_iter.peek().unwrap().0 {
                if self_currently_better {
                    new_sources.push((at, source));
                }
            } else {
                while let Some(&(next_change, better)) = intersection_iter.peek() {
                    if next_change > at {
                        break;
                    }

                    self_currently_better = better;

                    if self_currently_better {
                        if next_change < at {
                            new_sources.push((next_change, prev_source));
                        }
                    } else {
                        new_sources.push((next_change, other_data));
                    }

                    intersection_iter.next();
                }

                if self_currently_better {
                    new_sources.push((at, source));
                }
            }

            prev_source = source;
        }

        // intersections after the last old source
        for (at, is_self_better) in intersection_iter {
            if is_self_better {
                new_sources.push((at, prev_source));
            } else {
                new_sources.push((at, other_data));
            }
        }

        debug_assert!(
            new_sources.len() >= 2,
            "old: {:?}\nintersections: {:?}\nnew: {:?}",
            sources,
            debug_intersections,
            new_sources
        );
        debug_assert!(new_sources.first().unwrap().0 == Timestamp::zero());
        for sources in new_sources.windows(2) {
            debug_assert!(
                sources[0].0.fuzzy_lt(sources[1].0),
                "old: {:?}\nintersections: {:?}\nnew: {:?}",
                sources,
                debug_intersections,
                new_sources
            );
            debug_assert!(
                sources[0].0.fuzzy_lt(period()),
                "old: {:?}\nintersections: {:?}\nnew: {:?}",
                sources,
                debug_intersections,
                new_sources
            );
        }
        Sources::Multi(new_sources.into_boxed_slice())
    }

    /// Recursively unpack the exact travel time function for a given time range.
    // Use two `ReusablePLFStorage`s to reduce allocations.
    // One storage will contain the functions for each source - the other the complete resulting function.
    // That means when fetching the functions for each source, we need to use the two storages with flipped roles.
    pub fn exact_ttf_for<'g>(
        &self,
        start: Timestamp,
        end: Timestamp,
        shortcut_graph: &'g impl ShortcutGraphTrt<'g, OriginalGraph = LiveGraph>,
        target: &mut MutTopPLF,
        tmp: &mut ReusablePLFStorage,
    ) {
        debug_assert!(start.fuzzy_lt(end), "{:?} - {:?}", start, end);

        if self.constant {
            target.push(TTFPoint {
                at: start,
                val: self.lower_bound,
            });
            target.push(TTFPoint {
                at: end,
                val: self.lower_bound,
            });
            return;
        }

        if let Some(ApproxPartialTTF::Exact(ttf)) = self.get_travel_time_function(shortcut_graph) {
            if self.unpack.map(|u| !u.fuzzy_lt(end)).unwrap_or(false) || self.live_until.map(|l| !l.fuzzy_lt(end)).unwrap_or(false) {
                ttf.sub_plf(start, end).append(start, target);
                return;
            }
        }
        match &self.sources {
            Sources::None => unreachable!("There are no TTFs for empty shortcuts"),
            Sources::One(source) => ShortcutSource::from(*source).exact_ttf_for(start, end, shortcut_graph, target, tmp),
            Sources::Multi(sources) => sources.exact_ttf_for(start, end, shortcut_graph, target, tmp),
        }

        debug_assert!(!target.last().unwrap().at.fuzzy_lt(end), "{:?}", dbg_each!(self, start, end));
    }

    pub fn num_sources(&self) -> usize {
        self.sources.len()
    }

    /// Returns an iterator over all the sources combined with a Timestamp for the time from which the corresponding source becomes valid.
    pub fn sources_iter<'s>(&'s self) -> impl Iterator<Item = (Timestamp, ShortcutSourceData)> + 's {
        self.sources.iter()
    }

    pub fn sources_for<'s>(&'s self, start: Timestamp, end: Timestamp) -> impl Iterator<Item = (Timestamp, ShortcutSourceData)> + 's {
        self.sources.wrapping_iter_for(start, end)
    }

    pub fn is_constant(&self) -> bool {
        self.constant
    }

    pub fn get_switchpoints<'g>(
        &self,
        start: Timestamp,
        end: Timestamp,
        shortcut_graph: &'g impl ShortcutGraphTrt<'g>,
        switchpoints: &mut Vec<Timestamp>,
    ) -> (FlWeight, FlWeight) {
        match &self.sources {
            Sources::None => unreachable!("There are no TTFs for empty shortcuts"),
            Sources::One(source) => ShortcutSource::from(*source).get_switchpoints(start, end, shortcut_graph, switchpoints),
            Sources::Multi(sources) => sources.get_switchpoints(start, end, shortcut_graph, switchpoints),
        }
    }

    pub fn unpack_at<'g>(&self, t: Timestamp, shortcut_graph: &'g impl ShortcutGraphTrt<'g>, result: &mut Vec<(EdgeId, Timestamp)>) {
        ShortcutSource::from(*match &self.sources {
            Sources::None => unreachable!("There are no paths for empty shortcuts"),
            Sources::One(source) => source,
            Sources::Multi(sources) => sources.edge_source_at(t).unwrap(),
        })
        .unpack_at(t, shortcut_graph, result)
    }

    pub fn evaluate<'g>(&self, t: Timestamp, shortcut_graph: &'g impl ShortcutGraphTrt<'g>) -> FlWeight {
        if self.constant {
            return self.lower_bound;
        }

        ShortcutSource::from(*match &self.sources {
            Sources::None => return FlWeight::INFINITY,
            Sources::One(source) => source,
            Sources::Multi(sources) => sources.edge_source_at(t).unwrap(),
        })
        .evaluate(t, shortcut_graph)
    }

    pub fn to_pre_shortcut(&self) -> PreLiveShortcut {
        PreLiveShortcut {
            lower_bound: self.lower_bound,
            upper_bound: self.upper_bound,
            live_until: self.live_until,
            unpack: self.unpack,
        }
    }

    pub fn update_with(&mut self, mut pre: PreLiveShortcut, _t_live: Timestamp) {
        if let Some(live) = pre.live_until {
            debug_assert!(_t_live.fuzzy_lt(live));
            if let Some(unpack) = pre.unpack {
                if !live.fuzzy_lt(unpack) {
                    pre.unpack = None;
                }
            }
        }
        if let Some(unpack) = pre.unpack {
            debug_assert!(_t_live.fuzzy_lt(unpack));
        }
        self.lower_bound = pre.lower_bound;
        self.upper_bound = pre.upper_bound;
        self.live_until = pre.live_until;
        self.unpack = pre.unpack;
    }
}

#[derive(Debug, Copy, Clone)]
pub struct PreLiveShortcut {
    pub lower_bound: FlWeight,
    pub upper_bound: FlWeight,
    pub live_until: Option<Timestamp>,
    pub unpack: Option<Timestamp>,
}

impl Default for PreLiveShortcut {
    fn default() -> Self {
        Self {
            lower_bound: FlWeight::INFINITY,
            upper_bound: FlWeight::INFINITY,
            live_until: None,
            unpack: None,
        }
    }
}
