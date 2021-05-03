//! Logic and data structures for managing data associated TD-CCH edges.

use super::shortcut::Sources;
use super::shortcut_source::Sources as _;
use super::*;
use std::cmp::{max, min};
use std::convert::TryFrom;
use std::sync::atomic::Ordering::Relaxed;

#[derive(Debug)]
pub struct PartialShortcut {
    sources: Sources,
    cache: Option<ATTFContainer<Box<[TTFPoint]>>>,
    pub start: Timestamp,
    pub end: Timestamp,
    pub lower_bound: FlWeight,
    pub upper_bound: FlWeight,
    constant: bool,
    /// Is this edge actually necessary in a CH? Set to `false` to mark for removal in perfect customization.
    pub required: bool,
}

impl PartialShortcut {
    /// Create new `PartialShortcut` referencing an original edge or set to Infinity.
    pub fn new(start: Timestamp, end: Timestamp, source: Option<EdgeId>, original_graph: &TDGraph) -> Self {
        match source {
            Some(edge_id) => {
                if cfg!(feature = "detailed-stats") {
                    PATH_SOURCES_COUNT.fetch_add(1, Relaxed);
                }
                PartialShortcut {
                    sources: Sources::One(ShortcutSource::OriginalEdge(edge_id).into()),
                    cache: None,
                    start,
                    end,
                    lower_bound: original_graph.travel_time_function(edge_id).lower_bound(),
                    upper_bound: original_graph.travel_time_function(edge_id).upper_bound(),
                    constant: false,
                    required: true,
                }
            }
            None => PartialShortcut {
                sources: Sources::None,
                cache: None,
                start,
                end,
                lower_bound: FlWeight::INFINITY,
                upper_bound: FlWeight::INFINITY,
                constant: false,
                required: true,
            },
        }
    }

    pub fn new_finished(start: Timestamp, end: Timestamp, sources: &[(Timestamp, ShortcutSourceData)], bounds: (FlWeight, FlWeight)) -> Self {
        let sources = match sources {
            &[] => Sources::None,
            &[(_, data)] => Sources::One(data),
            data => Sources::Multi(data.into()),
        };
        Self {
            cache: None,
            start,
            end,
            lower_bound: bounds.0,
            upper_bound: bounds.1,
            constant: bounds.0.fuzzy_eq(bounds.1),
            required: true,
            sources,
        }
    }

    /// Merge this Shortcut with the lower triangle made up of the two EdgeIds (first down, then up).
    /// The `shortcut_graph` has to contain all the edges we may need to unpack.
    pub fn merge(&mut self, linked_ids: (EdgeId, EdgeId), shortcut_graph: &impl ShortcutGraphTrt<OriginalGraph = TDGraph>, buffers: &mut MergeBuffers) {
        // We already know, we won't need this edge, so do nothing
        if !self.required {
            return;
        }

        if cfg!(feature = "detailed-stats") {
            IPP_COUNT.fetch_sub(self.cache.as_ref().map(ATTFContainer::<Box<[TTFPoint]>>::num_points).unwrap_or(0), Relaxed);
            PATH_SOURCES_COUNT.fetch_sub(self.sources.len(), Relaxed);
            if self.cache.is_some() {
                ACTIVE_SHORTCUTS.fetch_sub(1, Relaxed);
            }
        }

        // we have to update the stats once we're done merging, so wrap everything in a lambda so we can do early returns and only exit the lambda.
        (|| {
            let other_data = ShortcutSource::Shortcut(linked_ids.0, linked_ids.1).into();

            // if linked_ids == (126454, 127116) {
            //     dbg!();
            // }

            // if one of the edges of the triangle is an infinity edge we don't need to do anything
            if !(shortcut_graph.is_valid_path(ShortcutId::Incoming(linked_ids.0)) && shortcut_graph.is_valid_path(ShortcutId::Outgoing(linked_ids.1))) {
                return;
            }

            let other_lower_bound =
                shortcut_graph.lower_bound(ShortcutId::Incoming(linked_ids.0)) + shortcut_graph.lower_bound(ShortcutId::Outgoing(linked_ids.1));

            // current upper bound always better than linked lower bound - do nothing
            // if self.upper_bound.fuzzy_lt(other_lower_bound) {
            if self
                .partial_ttf(shortcut_graph, self.start, self.end)
                .map(|plf| plf.static_upper_bound().fuzzy_lt(other_lower_bound))
                .unwrap_or(false)
            {
                return;
            }

            // if linked_ids == (126454, 127116) {
            //     dbg!();
            // }

            // get cached (possibly approximated) TTFs
            let partial_first = shortcut_graph.partial_ttf(ShortcutId::Incoming(linked_ids.0), self.start, self.end);
            let mut first_lower_target = buffers.unpacking_target.push_plf();

            if partial_first.is_none() {
                if let PeriodicATTF::Approx(lower_plf, _) = shortcut_graph.periodic_ttf(ShortcutId::Incoming(linked_ids.0)).unwrap() {
                    lower_plf.append_range(self.start, self.end, &mut first_lower_target);
                }
            };

            let mut first_upper_target = first_lower_target.storage_mut().push_plf();

            let first_ttf = if let Some(ttf) = partial_first {
                ttf
            } else {
                match shortcut_graph.periodic_ttf(ShortcutId::Incoming(linked_ids.0)).unwrap() {
                    PeriodicATTF::Exact(plf) => {
                        plf.append_range(self.start, self.end, &mut first_upper_target);
                        PartialATTF::Exact(PartialPiecewiseLinearFunction::new(&first_upper_target[..]))
                    }
                    PeriodicATTF::Approx(_, upper_plf) => {
                        upper_plf.append_range(self.start, self.end, &mut first_upper_target);
                        let plfs = first_upper_target.storage().top_plfs();
                        PartialATTF::Approx(PartialPiecewiseLinearFunction::new(plfs.0), PartialPiecewiseLinearFunction::new(plfs.1))
                    }
                }
            };

            let second_start = first_ttf.bound_plfs().0.eval(self.start) + self.start;
            let second_end = first_ttf.bound_plfs().1.eval(self.end) + self.end;
            let partial_second = shortcut_graph.partial_ttf(ShortcutId::Outgoing(linked_ids.1), second_start, second_end);
            let mut second_lower_target = buffers.unpacking_tmp.push_plf();

            if partial_second.is_none() {
                if let PeriodicATTF::Approx(lower_plf, _) = shortcut_graph.periodic_ttf(ShortcutId::Outgoing(linked_ids.1)).unwrap() {
                    lower_plf.append_range(second_start, second_end, &mut second_lower_target);
                }
            };

            let mut second_upper_target = second_lower_target.storage_mut().push_plf();

            let second_ttf = if let Some(ttf) = partial_second {
                ttf
            } else {
                match shortcut_graph.periodic_ttf(ShortcutId::Outgoing(linked_ids.1)).unwrap() {
                    PeriodicATTF::Exact(plf) => {
                        plf.append_range(second_start, second_end, &mut second_upper_target);
                        PartialATTF::Exact(PartialPiecewiseLinearFunction::new(&second_upper_target[..]))
                    }
                    PeriodicATTF::Approx(_, upper_plf) => {
                        upper_plf.append_range(second_start, second_end, &mut second_upper_target);
                        let plfs = second_upper_target.storage().top_plfs();
                        PartialATTF::Approx(PartialPiecewiseLinearFunction::new(plfs.0), PartialPiecewiseLinearFunction::new(plfs.1))
                    }
                }
            };

            // if linked_ids == (126454, 127116) {
            //     dbg!();
            // }

            // when the current shortcut is always infinity, the linked paths will always be better.
            if !self.is_valid_path() {
                if cfg!(feature = "detailed-stats") {
                    ACTUALLY_LINKED.fetch_add(1, Relaxed);
                }
                // link functions
                let linked = first_ttf.link(&second_ttf, self.start, self.end);

                // if self.upper_bound.fuzzy_lt(PartialATTF::from(&linked).static_lower_bound()) {
                //     return;
                // }

                self.upper_bound = min(self.upper_bound, PartialATTF::from(&linked).static_upper_bound());
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

            // if linked_ids == (126454, 127116) {
            //     dbg!();
            // }

            // get own cached TTF
            let self_plf = self.partial_ttf(shortcut_graph, self.start, self.end).unwrap();

            // link TTFs in triangle
            let linked_ipps = first_ttf.link(&second_ttf, self.start, self.end);
            if cfg!(feature = "detailed-stats") {
                ACTUALLY_LINKED.fetch_add(1, Relaxed);
            }
            drop(first_ttf);
            drop(first_upper_target);
            drop(first_lower_target);
            drop(second_ttf);
            drop(second_upper_target);
            drop(second_lower_target);

            let linked = PartialATTF::from(&linked_ipps);
            // these bounds are more tight than the previous ones
            let other_lower_bound = linked.static_lower_bound();
            let other_upper_bound = linked.static_upper_bound();

            // note that self_plf.static_lower_bound() >= self.lower_bound
            // this can be the case because we set self.lower_bound during precustomization as low as possible
            // if we would compare here to self.lower_bound we might never take that branch, even when we would have to.
            if !self_plf.static_lower_bound().fuzzy_lt(other_upper_bound) {
                // if linked_ids == (126454, 127116) {
                //     dbg!();
                // }
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
            } else if self_plf.static_upper_bound().fuzzy_lt(other_lower_bound) {
                // if linked_ids == (126454, 127116) {
                //     dbg!(linked);
                // }
                // current upper bound always better than linked lower bound - keep whatever we currently have
                return;
            }

            // all bound checking done, we need to actually merge
            if cfg!(feature = "detailed-stats") {
                ACTUALLY_MERGED.fetch_add(1, Relaxed);
            }

            // this function does exact merging, even when we have only approximate functions by unpacking exact functions for time ranges when bounds overlap.
            // the callback executes exact merging for small time ranges where the bounds overlap, the function takes care of all the rest around that.
            let (mut merged, intersection_data) = self_plf.merge(&linked, self.start, self.end, buffers, |start, end, buffers| {
                let mut self_target = buffers.unpacking_target.push_plf();
                self.reconstruct_exact_ttf(start, end, shortcut_graph, &mut self_target, &mut buffers.unpacking_tmp);

                let mut other_target = self_target.storage_mut().push_plf();
                ShortcutSource::from(other_data).reconstruct_exact_ttf(start, end, shortcut_graph, &mut other_target, &mut buffers.unpacking_tmp);

                let (self_ipps, other_ipps) = other_target.storage().top_plfs();
                PartialPiecewiseLinearFunction::new(self_ipps).merge(&PartialPiecewiseLinearFunction::new(other_ipps), start, end, &mut buffers.buffer)
            });
            if cfg!(feature = "tdcch-approx") && merged.num_points() > APPROX_THRESHOLD {
                let old = merged.num_points();
                if cfg!(feature = "detailed-stats") {
                    CONSIDERED_FOR_APPROX.fetch_add(old, Relaxed);
                }
                merged = PartialATTF::from(&merged).approximate(buffers);
                if cfg!(feature = "detailed-stats") {
                    SAVED_BY_APPROX.fetch_add(old as isize - merged.num_points() as isize, Relaxed);
                }
            }

            // update upper bounds after merging, but not lower bounds.
            // lower bounds were already set during precustomization -- possibly lower than necessary.
            // We would like to increase the lower bound to make it tighter, but we can't take the max right now,
            // We might find a lower function during a later merge operation.
            // We can only set the lower bound as tight as possible, once we have the final travel time function.
            self.upper_bound = min(self.upper_bound, PartialATTF::from(&merged).static_upper_bound());
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
            self.sources = sources.combine(intersection_data, other_data, self.start, self.end);
        })();

        if cfg!(feature = "detailed-stats") {
            IPP_COUNT.fetch_add(self.cache.as_ref().map(ATTFContainer::<Box<[TTFPoint]>>::num_points).unwrap_or(0), Relaxed);
            PATH_SOURCES_COUNT.fetch_add(self.sources.len(), Relaxed);
            if self.cache.is_some() {
                ACTIVE_SHORTCUTS.fetch_add(1, Relaxed);
            }
        }
    }

    pub fn periodic_ttf<'s, 'g: 's>(&'s self, shortcut_graph: &'g impl ShortcutGraphTrt<OriginalGraph = TDGraph>) -> Option<PeriodicATTF<'s>> {
        if let Some(cache) = &self.cache {
            if self.start.fuzzy_eq(Timestamp::ZERO) && self.end.fuzzy_eq(period()) {
                return Some(cache.into());
            }
        }

        match self.sources {
            Sources::One(source) => match source.into() {
                ShortcutSource::OriginalEdge(id) => return Some(PeriodicATTF::Exact(shortcut_graph.original_graph().travel_time_function(id))),
                _ => (),
            },
            _ => (),
        }

        None
    }

    pub fn partial_ttf<'s, 'g: 's>(
        &'s self,
        shortcut_graph: &'g impl ShortcutGraphTrt<OriginalGraph = TDGraph>,
        start: Timestamp,
        end: Timestamp,
    ) -> Option<PartialATTF<'s>> {
        if start.fuzzy_lt(self.start) || self.end.fuzzy_lt(end) {
            return None;
        }
        if let Some(cache) = &self.cache {
            return Some(PartialATTF::from(cache).sub_ttf(start, end));
        }

        match self.sources {
            Sources::One(source) => match source.into() {
                ShortcutSource::OriginalEdge(id) => {
                    return PartialPiecewiseLinearFunction::try_from(shortcut_graph.original_graph().travel_time_function(id))
                        .ok()
                        .and_then(|plf| plf.get_sub_plf(start, end))
                        .map(PartialATTF::Exact)
                }
                _ => (),
            },
            _ => (),
        }

        None
    }

    pub fn is_valid_path(&self) -> bool {
        match self.sources {
            Sources::None => false,
            _ => true,
        }
    }

    /// Once the TTF of this Shortcut is final, we can tighten the lower bound.
    /// When we know or detect, that we don't need this shortcut, we set all bounds to infinity.
    pub fn finalize_bounds(&mut self, shortcut_graph: &impl ShortcutGraphTrt<OriginalGraph = TDGraph>) {
        if !self.required {
            return;
        }

        if let Sources::None = self.sources {
            self.required = false;
            self.lower_bound = FlWeight::INFINITY;
            self.upper_bound = FlWeight::INFINITY;
            return;
        }

        self.lower_bound = self.partial_ttf(shortcut_graph, self.start, self.end).unwrap().static_lower_bound();
        self.upper_bound = self.partial_ttf(shortcut_graph, self.start, self.end).unwrap().static_upper_bound();

        // let new_lower_bound = if cfg!(feature = "tdcch-precustomization") {
        //     max(
        //         self.lower_bound,
        //         self.partial_ttf(shortcut_graph, self.start, self.end).unwrap().static_lower_bound(),
        //     )
        // } else {
        //     self.partial_ttf(shortcut_graph, self.start, self.end).unwrap().static_lower_bound()
        // };

        // // The final functions lower bound is worse than the upper bound this shortcut got during pre/post/perfect customization
        // // Thus, we will never need it.
        // if self.upper_bound.fuzzy_lt(new_lower_bound) {
        //     self.required = false;
        //     self.sources = Sources::None;
        //     self.lower_bound = FlWeight::INFINITY;
        //     self.upper_bound = FlWeight::INFINITY;
        //     return;
        // }

        // debug_assert!(
        //     !cfg!(feature = "tdcch-precustomization") || !new_lower_bound.fuzzy_lt(self.lower_bound),
        //     "{:?}, {:?}",
        //     new_lower_bound,
        //     self
        // );
        // debug_assert!(!self.upper_bound.fuzzy_lt(new_lower_bound), "{:?}, {:?}", new_lower_bound, self);
        // self.lower_bound = new_lower_bound;

        // // upper bound was already set as tight as possible during merging.
        // debug_assert!(
        //     self.upper_bound
        //         .fuzzy_leq(self.partial_ttf(shortcut_graph, self.start, self.end).unwrap().static_upper_bound()),
        //     "{:#?}",
        //     (
        //         self.lower_bound,
        //         self.upper_bound,
        //         &self.sources,
        //         self.partial_ttf(shortcut_graph, self.start, self.end).unwrap().static_upper_bound()
        //     )
        // );
    }

    /// If Shortcuts in skipped triangles are not required, the corresponding `Source` in this shortcut is also not required, so remove it
    pub fn disable_if_unneccesary(&mut self, shortcut_graph: &PartialShortcutGraph) {
        match &mut self.sources {
            Sources::None => {}
            Sources::One(source) => {
                if !ShortcutSource::from(*source).required(shortcut_graph) {
                    self.required = false;
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
            IPP_COUNT.fetch_sub(self.cache.as_ref().map(ATTFContainer::<Box<[TTFPoint]>>::num_points).unwrap_or(0), Relaxed);
            if self.cache.is_some() {
                ACTIVE_SHORTCUTS.fetch_sub(1, Relaxed);
            }
        }
        self.cache = None;
    }

    pub fn set_cache(&mut self, ttf: Option<ATTFContainer<Box<[TTFPoint]>>>) {
        self.cache = ttf;
    }

    pub fn set_sources(&mut self, sources: &[(Timestamp, ShortcutSourceData)]) {
        self.sources = match sources {
            &[] => Sources::None,
            &[(_, data)] => Sources::One(data),
            data => Sources::Multi(data.into()),
        };
    }

    /// Recursively unpack the exact travel time function for a given time range.
    // Use two `ReusablePLFStorage`s to reduce allocations.
    // One storage will contain the functions for each source - the other the complete resulting function.
    // That means when fetching the functions for each source, we need to use the two storages with flipped roles.
    pub fn reconstruct_exact_ttf(
        &self,
        start: Timestamp,
        end: Timestamp,
        shortcut_graph: &impl ShortcutGraphTrt,
        target: &mut MutTopPLF,
        tmp: &mut ReusablePLFStorage,
    ) {
        debug_assert!(start.fuzzy_lt(end), "{:?} - {:?}", start, end);
        debug_assert!(self.start.fuzzy_leq(start));
        debug_assert!(end.fuzzy_leq(self.end));

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

        match &self.sources {
            Sources::None => unreachable!("There are no TTFs for empty shortcuts"),
            Sources::One(source) => ShortcutSource::from(*source).reconstruct_exact_ttf(start, end, shortcut_graph, target, tmp),
            Sources::Multi(sources) => PartialSources(sources, self.end).reconstruct_exact_ttf(start, end, shortcut_graph, target, tmp),
        }

        debug_assert!(!target.last().unwrap().at.fuzzy_lt(end), "{:?}", dbg_each!(self, start, end));
    }

    pub fn num_sources(&self) -> usize {
        self.sources.len()
    }

    // Returns an iterator over all the sources combined with a Timestamp for the time from which the corresponding source becomes valid.
    pub fn sources_iter<'s>(&'s self) -> impl Iterator<Item = (Timestamp, ShortcutSourceData)> + 's {
        self.sources.iter(self.start)
    }

    pub fn is_constant(&self) -> bool {
        self.constant
    }

    pub fn get_switchpoints(
        &self,
        start: Timestamp,
        end: Timestamp,
        shortcut_graph: &impl ShortcutGraphTrt,
    ) -> (Vec<(Timestamp, Vec<EdgeId>, FlWeight)>, FlWeight) {
        debug_assert!(self.start.fuzzy_leq(start));
        debug_assert!(end.fuzzy_leq(self.end));
        match &self.sources {
            Sources::None => unreachable!("There are no TTFs for empty shortcuts"),
            Sources::One(source) => ShortcutSource::from(*source).get_switchpoints(start, end, shortcut_graph),
            Sources::Multi(sources) => PartialSources(sources, self.end).get_switchpoints(start, end, shortcut_graph),
        }
    }

    pub fn unpack_at(&self, t: Timestamp, shortcut_graph: &impl ShortcutGraphTrt, result: &mut Vec<(EdgeId, Timestamp)>) {
        debug_assert!(self.start.fuzzy_leq(t));
        debug_assert!(t.fuzzy_leq(self.end));
        ShortcutSource::from(match &self.sources {
            Sources::None => unreachable!("There are no paths for empty shortcuts"),
            Sources::One(source) => *source,
            Sources::Multi(sources) => PartialSources(sources, self.end).edge_source_at(t).unwrap(),
        })
        .unpack_at(t, shortcut_graph, result)
    }

    pub fn evaluate(&self, t: Timestamp, shortcut_graph: &impl ShortcutGraphTrt) -> FlWeight {
        debug_assert!(self.start.fuzzy_leq(t));
        debug_assert!(t.fuzzy_leq(self.end));

        if self.constant {
            return self.lower_bound;
        }

        ShortcutSource::from(match &self.sources {
            Sources::None => return FlWeight::INFINITY,
            Sources::One(source) => *source,
            Sources::Multi(sources) => PartialSources(sources, self.end).edge_source_at(t).unwrap(),
        })
        .evaluate(t, shortcut_graph)
    }
}

impl PartialTrt for PartialShortcut {
    fn start(&self) -> Timestamp {
        self.start
    }
    fn end(&self) -> Timestamp {
        self.end
    }
    fn combine(a: &[Self], b: &[Self]) -> Self {
        if b[0].start < a[0].start {
            return Self::combine(b, a);
        }
        let mut new_sources: Vec<_> = crate::util::interleave(a.iter().map(|s| s.sources_iter()), b.iter().map(|s| s.sources_iter()))
            .flatten()
            .collect();
        new_sources.dedup_by_key(|(_, source)| *source);
        let lower = a.iter().chain(b.iter()).map(|s| s.lower_bound).min().unwrap();
        let upper = a.iter().chain(b.iter()).map(|s| s.upper_bound).max().unwrap();

        let a: Vec<_> = a
            .iter()
            .map(|s: &PartialShortcut| Partial {
                start: s.start,
                end: s.end,
                ttf: s.cache.as_ref().unwrap().borrow(),
            })
            .collect();
        let b: Vec<_> = b
            .iter()
            .map(|s: &PartialShortcut| Partial {
                start: s.start,
                end: s.end,
                ttf: s.cache.as_ref().unwrap().borrow(),
            })
            .collect();

        let Partial { start, end, ttf } = Partial::combine(&a, &b);

        let mut s = PartialShortcut::new_finished(start, end, &new_sources, (lower, upper));
        s.set_cache(Some(ttf.into()));
        s
    }
}
