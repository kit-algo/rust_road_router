//! Logic and data structures for managing data associated TD-CCH edges.

use super::*;
use std::cmp::{max, min, Ordering};
use std::sync::atomic::Ordering::Relaxed;

/// Number of points that a PLF is allowed to have before reduction by approximation is triggered.
/// Can be overriden through the TDCCH_APPROX_THRESHOLD env var
#[cfg(not(override_tdcch_approx_threshold))]
pub const APPROX_THRESHOLD: usize = 1000;
#[cfg(override_tdcch_approx_threshold)]
pub const APPROX_THRESHOLD: usize = include!(concat!(env!("OUT_DIR"), "/TDCCH_APPROX_THRESHOLD"));

// During customization we need to store PLFs.
// For each shortcut we either have the exact function (`Exact`) or an approximation through less complex upper and lower bounds (`Approx`).
#[derive(Debug)]
pub enum TTFCache<D> {
    Exact(D),
    Approx(D, D),
}

impl TTFCache<Vec<TTFPoint>> {
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

// When merging approximated functions, either it is clear which one is better by the bounds alone, or we need to do exact merging by unpacking the exact functions,
#[derive(Debug, Clone, Copy, PartialEq)]
enum BoundMergingState {
    First,
    Second,
    Merge,
}

// Similar to `TTFCache`, though this one is for actually working with the functions, `TTFCache` is for storing them.
#[derive(Debug)]
pub enum TTF<'a> {
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

    // Link to TTFs, creating a new function
    fn link(&self, second: &TTF) -> TTFCache<Vec<TTFPoint>> {
        use TTF::*;

        // if both TTFs are exact, we can link exact
        if let (Exact(first), Exact(second)) = (self, second) {
            return TTFCache::Exact(first.link(second));
        }
        // else the result will be approximated anyway

        let (first_lower, first_upper) = self.bound_plfs();
        let (second_lower, second_upper) = second.bound_plfs();

        // linking two upper bounds is a valid upper bound, same for lower bounds
        TTFCache::Approx(first_lower.link(&second_lower), first_upper.link(&second_upper))
    }

    // this ones a bit ugly...
    // exactly merging two TTFs, even when we only have approximations by lazily calculating exact functions for time ranges where the approximated bounds overlap.
    // beside the two TTFs we take buffers to reduce allocations
    // and a callback which does lazy exact function retrieval and merging when we really need it.
    #[allow(clippy::collapsible_if)]
    #[allow(clippy::cognitive_complexity)]
    #[allow(clippy::type_complexity)]
    fn merge(
        &self,
        other: &TTF,
        buffers: &mut MergeBuffers,
        merge_exact: impl Fn(Timestamp, Timestamp, &mut MergeBuffers) -> (Box<[TTFPoint]>, Vec<(Timestamp, bool)>),
    ) -> (TTFCache<Box<[TTFPoint]>>, Vec<(Timestamp, bool)>) {
        use TTF::*;

        // easy case, both functions are exact, we can just do actual function mering and are done
        if let (Exact(self_plf), Exact(other)) = (self, other) {
            let (plf, intersections) = self_plf.merge(other, &mut buffers.buffer);
            return (TTFCache::Exact(plf), intersections);
        }

        // get bound functions
        let (self_lower, self_upper) = self.bound_plfs();
        let (other_lower, other_upper) = other.bound_plfs();

        // merge lower with upper bounds to check when one function completely dominates the other one
        // and when bounds overlap
        let (_, self_dominating_intersections) = self_upper.merge(&other_lower, &mut buffers.buffer);
        let (_, other_dominating_intersections) = other_upper.merge(&self_lower, &mut buffers.buffer);

        let mut dominating = false; // does currently one function completely dominate the other
        let mut start_of_segment = Timestamp::zero(); // where does the current dominance segment start
        let mut self_dominating_iter = self_dominating_intersections.iter().peekable();
        let mut other_dominating_iter = other_dominating_intersections.iter().peekable();
        let mut result = Vec::new(); // Will contain final (Timestamp, bool) pairs which indicate which function is better when
        let mut bound_merge_state = Vec::new(); // track `BoundMergingState` for constructing TTFs in the end

        match (self_dominating_iter.peek().unwrap().1, other_dominating_iter.peek().unwrap().1) {
            (true, false) => {
                // first function is currently better
                dominating = true;
                result.push((Timestamp::zero(), true));
                bound_merge_state.push((Timestamp::zero(), BoundMergingState::First));
            }
            (false, true) => {
                // second function is currently better
                dominating = true;
                result.push((Timestamp::zero(), false));
                bound_merge_state.push((Timestamp::zero(), BoundMergingState::Second));
            }
            _ => {
                // false false -> bounds overlap
                // in BOTH cases (especially the broken true true case) everything is unclear and we need to do exact merging
            }
        }

        self_dominating_iter.next();
        other_dominating_iter.next();

        // while there are still more intersections of the bound functions
        while self_dominating_iter.peek().is_some() || other_dominating_iter.peek().is_some() {
            // get timestamps of next intersections
            let next_t_self = self_dominating_iter.peek().map(|(t, _)| *t).unwrap_or(Timestamp::NEVER);
            let next_t_other = other_dominating_iter.peek().map(|(t, _)| *t).unwrap_or(Timestamp::NEVER);

            if dominating {
                // currently one function dominates
                if next_t_self.fuzzy_lt(next_t_other) {
                    // next intersection is self upper with other lower
                    debug_assert!(
                        !self_dominating_iter.peek().unwrap().1,
                        "{:?}",
                        dbg_each!(
                            &self_dominating_intersections,
                            &other_dominating_intersections,
                            &self_lower,
                            &self_upper,
                            &other_lower,
                            &other_upper
                        )
                    );
                    debug_assert!(
                        result.last().unwrap().1,
                        "{:?}",
                        dbg_each!(
                            &self_dominating_intersections,
                            &other_dominating_intersections,
                            &self_lower,
                            &self_upper,
                            &other_lower,
                            &other_upper
                        )
                    );
                    dominating = false; // no clear dominance by bounds

                    start_of_segment = next_t_self;
                    self_dominating_iter.next();
                } else if next_t_other.fuzzy_lt(next_t_self) {
                    // next intersection is other upper with self lower
                    debug_assert!(
                        !other_dominating_iter.peek().unwrap().1,
                        "{:?}",
                        dbg_each!(
                            &self_dominating_intersections,
                            &other_dominating_intersections,
                            &self_lower,
                            &self_upper,
                            &other_lower,
                            &other_upper
                        )
                    ); // <--
                    debug_assert!(
                        !result.last().unwrap().1,
                        "{:?}",
                        dbg_each!(
                            &self_dominating_intersections,
                            &other_dominating_intersections,
                            &self_lower,
                            &self_upper,
                            &other_lower,
                            &other_upper
                        )
                    );
                    dominating = false; // no clear dominance by bounds

                    start_of_segment = next_t_other;
                    other_dominating_iter.next();
                } else {
                    // both bounds intersect at the same time - still clear dominance but with switched roles
                    debug_assert_ne!(
                        self_dominating_iter.peek().unwrap().1,
                        other_dominating_iter.peek().unwrap().1,
                        "{:?}",
                        dbg_each!(
                            &self_dominating_intersections,
                            &other_dominating_intersections,
                            &self_lower,
                            &self_upper,
                            &other_lower,
                            &other_upper
                        )
                    );
                    result.push((next_t_self, self_dominating_iter.peek().unwrap().1));

                    debug_assert!(
                        bound_merge_state
                            .last()
                            .map(|&(prev_start, _)| prev_start.fuzzy_lt(next_t_self))
                            .unwrap_or(true),
                        "{:?}",
                        dbg_each!(bound_merge_state)
                    );
                    if self_dominating_iter.peek().unwrap().1 {
                        debug_assert!(
                            bound_merge_state
                                .last()
                                .map(|&(_, prev_state)| prev_state != BoundMergingState::First)
                                .unwrap_or(true),
                            "{:?}",
                            dbg_each!(bound_merge_state)
                        );
                        bound_merge_state.push((next_t_self, BoundMergingState::First));
                    } else {
                        debug_assert!(
                            bound_merge_state
                                .last()
                                .map(|&(_, prev_state)| prev_state != BoundMergingState::Second)
                                .unwrap_or(true),
                            "{:?}",
                            dbg_each!(
                                next_t_self,
                                next_t_other,
                                bound_merge_state,
                                self_dominating_intersections,
                                other_dominating_intersections,
                                result
                            )
                        );
                        bound_merge_state.push((next_t_self, BoundMergingState::Second));
                    }

                    start_of_segment = next_t_self;
                    self_dominating_iter.next();
                    other_dominating_iter.next();
                }
            } else {
                // currently no dominance, bounds overlap
                if next_t_self.fuzzy_lt(next_t_other) {
                    // next intersection is self upper with other lower
                    // overlap ends, do exact merging in the overlapping range by unpacking
                    let (_, intersections) = merge_exact(start_of_segment, next_t_self, buffers);

                    // if there actually is a real intersection
                    // (either something happens in the range,
                    // or the better function at the beginning is not the one, that we currently think is the better)
                    if intersections.len() > 1
                        || result
                            .last()
                            .map(|(_, self_better)| *self_better != self_dominating_iter.peek().unwrap().1)
                            .unwrap_or(true)
                    {
                        debug_assert!(
                            bound_merge_state
                                .last()
                                .map(|&(prev_start, prev_state)| prev_start.fuzzy_lt(start_of_segment) && prev_state != BoundMergingState::Merge)
                                .unwrap_or(true),
                            "{:?}",
                            dbg_each!(bound_merge_state)
                        );
                        bound_merge_state.push((start_of_segment, BoundMergingState::Merge));
                        debug_assert!(start_of_segment.fuzzy_lt(next_t_self), "{:?}", dbg_each!(start_of_segment, next_t_self));
                        // setup bound merge state for next segment
                        bound_merge_state.push((
                            next_t_self,
                            if self_dominating_iter.peek().unwrap().1 {
                                BoundMergingState::First
                            } else {
                                BoundMergingState::Second
                            },
                        ));
                    }

                    // append intersections to `result`
                    let mut iter = intersections.into_iter();
                    let first_intersection = iter.next().unwrap();
                    if result.last().map(|(_, self_better)| *self_better != first_intersection.1).unwrap_or(true) {
                        result.push(first_intersection);
                    }
                    result.extend(iter);

                    // when our bound merge iterators say, we continue different, than we currently are
                    // there has to be an intersection at the end of the segment we are currently mering
                    // so push that to result
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
                    // next intersection is other upper with self lower
                    // overlap ends, do exact merging in the overlapping range by unpacking
                    let (_, intersections) = merge_exact(start_of_segment, next_t_other, buffers);

                    // if there actually is a real intersection
                    // (either something happens in the range,
                    // or the better function at the beginning is not the one, that we currently think is the better)
                    if intersections.len() > 1
                        || result
                            .last()
                            .map(|(_, self_better)| !*self_better != other_dominating_iter.peek().unwrap().1)
                            .unwrap_or(true)
                    {
                        debug_assert!(
                            bound_merge_state
                                .last()
                                .map(|&(prev_start, prev_state)| prev_start.fuzzy_lt(start_of_segment) && prev_state != BoundMergingState::Merge)
                                .unwrap_or(true),
                            "{:?}",
                            dbg_each!(bound_merge_state)
                        );
                        bound_merge_state.push((start_of_segment, BoundMergingState::Merge));
                        debug_assert!(start_of_segment.fuzzy_lt(next_t_other), "{:?}", dbg_each!(start_of_segment, next_t_other));
                        // setup bound merge state for next segment
                        bound_merge_state.push((
                            next_t_other,
                            if other_dominating_iter.peek().unwrap().1 {
                                BoundMergingState::Second
                            } else {
                                BoundMergingState::First
                            },
                        ));
                    }

                    // append intersections to `result`
                    let mut iter = intersections.into_iter();
                    let first_intersection = iter.next().unwrap();
                    if result.last().map(|(_, self_better)| *self_better != first_intersection.1).unwrap_or(true) {
                        result.push(first_intersection);
                    }
                    result.extend(iter);

                    // when our bound merge iterators say, we continue different, than we currently are
                    // there has to be an intersection at the end of the segment we are currently mering
                    // so push that to result
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
                    // were currently not dominating and both bounds intersect at the same time, so we're still not dominating.
                    // just merge it when we finally start dominating.
                    self_dominating_iter.next();
                    other_dominating_iter.next();
                }
            }
        }
        // all intersections processed

        // we were not dominating in the end, so we need to merge the rest
        if !dominating {
            let (_, intersections) = merge_exact(start_of_segment, period(), buffers);

            if intersections.len() > 1
                || result.last().map(|(_, self_better)| *self_better != intersections[0].1).unwrap_or(true)
                || bound_merge_state
                    .first()
                    .map(|&(_, initial_state)| initial_state == BoundMergingState::Merge)
                    .unwrap_or(true)
            {
                debug_assert!(
                    bound_merge_state
                        .last()
                        .map(|&(prev_start, prev_state)| prev_start.fuzzy_lt(start_of_segment) && prev_state != BoundMergingState::Merge)
                        .unwrap_or(true),
                    "{:?}",
                    dbg_each!(bound_merge_state)
                );
                bound_merge_state.push((start_of_segment, BoundMergingState::Merge));
            }

            let mut iter = intersections.into_iter();
            let first_intersection = iter.next().unwrap();
            if result.last().map(|(_, self_better)| *self_better != first_intersection.1).unwrap_or(true) {
                result.push(first_intersection);
            }
            result.extend(iter);
        }
        // `result` now is finalized

        debug_assert!(result.first().unwrap().0 == Timestamp::zero());
        for better in result.windows(2) {
            debug_assert!(
                better[0].0 < better[1].0,
                "{:?}",
                dbg_each!(&self_dominating_intersections, &other_dominating_intersections)
            );
            debug_assert_ne!(
                better[0].1,
                better[1].1,
                "{:?}",
                dbg_each!(&self_dominating_intersections, &other_dominating_intersections)
            );
        }

        // we still need new upper and lower bounds for the merged function
        // do some preallocation
        buffers.exact_self_buffer.reserve(max(self_lower.len(), self_upper.len()));
        buffers.exact_other_buffer.reserve(max(other_lower.len(), other_upper.len()));
        buffers.exact_result_lower.reserve(2 * self_lower.len() + 2 * other_lower.len() + 2);
        buffers.exact_result_upper.reserve(2 * self_upper.len() + 2 * other_upper.len() + 2);

        debug_assert_eq!(bound_merge_state[0].0, Timestamp::zero());

        let mut end_of_segment_iter = bound_merge_state.iter().map(|(t, _)| *t).chain(std::iter::once(period()));
        end_of_segment_iter.next();

        // go over all segments, either copy the better one, or merge bounds (this time lower with lower and upper with upper) and append these
        for (&(start_of_segment, state), end_of_segment) in bound_merge_state.iter().zip(end_of_segment_iter) {
            match state {
                BoundMergingState::First => {
                    self_lower.copy_append_to_partial(start_of_segment, end_of_segment, &mut buffers.exact_result_lower);
                    self_upper.copy_append_to_partial(start_of_segment, end_of_segment, &mut buffers.exact_result_upper);
                }
                BoundMergingState::Second => {
                    other_lower.copy_append_to_partial(start_of_segment, end_of_segment, &mut buffers.exact_result_lower);
                    other_upper.copy_append_to_partial(start_of_segment, end_of_segment, &mut buffers.exact_result_upper);
                }
                BoundMergingState::Merge => {
                    buffers.exact_self_buffer.clear();
                    self_lower.copy_range(start_of_segment, end_of_segment, &mut buffers.exact_self_buffer);
                    buffers.exact_other_buffer.clear();
                    other_lower.copy_range(start_of_segment, end_of_segment, &mut buffers.exact_other_buffer);
                    let (partial_lower, _) = PiecewiseLinearFunction::merge_partials(
                        &buffers.exact_self_buffer,
                        &buffers.exact_other_buffer,
                        start_of_segment,
                        end_of_segment,
                        &mut buffers.buffer,
                    );
                    PiecewiseLinearFunction::append_partials(&mut buffers.exact_result_lower, &partial_lower, start_of_segment);

                    buffers.exact_self_buffer.clear();
                    self_upper.copy_range(start_of_segment, end_of_segment, &mut buffers.exact_self_buffer);
                    buffers.exact_other_buffer.clear();
                    other_upper.copy_range(start_of_segment, end_of_segment, &mut buffers.exact_other_buffer);
                    let (partial_upper, _) = PiecewiseLinearFunction::merge_partials(
                        &buffers.exact_self_buffer,
                        &buffers.exact_other_buffer,
                        start_of_segment,
                        end_of_segment,
                        &mut buffers.buffer,
                    );
                    PiecewiseLinearFunction::append_partials(&mut buffers.exact_result_upper, &partial_upper, start_of_segment);
                }
            }
        }

        let ret = (
            TTFCache::Approx(
                buffers.exact_result_lower[..].to_vec().into_boxed_slice(),
                buffers.exact_result_upper[..].to_vec().into_boxed_slice(),
            ),
            result,
        );

        buffers.exact_self_buffer.clear();
        buffers.exact_other_buffer.clear();
        buffers.exact_result_lower.clear();
        buffers.exact_result_upper.clear();

        ret
        // alternatively just merge the complete lower and upper bounds, but the other variant turned out to be faster.
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
            }
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
pub struct Shortcut {
    sources: Sources,
    cache: Option<TTFCache<Box<[TTFPoint]>>>,
    pub lower_bound: FlWeight,
    pub upper_bound: FlWeight,
    constant: bool,
    /// Is this edge actually necessary in a CH? Set to `false` to mark for removal in perfect customization.
    pub required: bool,
}

impl Shortcut {
    /// Create new `Shortcut` referencing an original edge or set to Infinity.
    pub fn new(source: Option<EdgeId>, original_graph: &TDGraph) -> Self {
        match source {
            Some(edge_id) => {
                if cfg!(feature = "detailed-stats") {
                    PATH_SOURCES_COUNT.fetch_add(1, Relaxed);
                }
                Shortcut {
                    sources: Sources::One(ShortcutSource::OriginalEdge(edge_id).into()),
                    cache: None,
                    lower_bound: original_graph.travel_time_function(edge_id).lower_bound(),
                    upper_bound: original_graph.travel_time_function(edge_id).upper_bound(),
                    constant: false,
                    required: true,
                }
            }
            None => Shortcut {
                sources: Sources::None,
                cache: None,
                lower_bound: FlWeight::INFINITY,
                upper_bound: FlWeight::INFINITY,
                constant: false,
                required: true,
            },
        }
    }

    pub fn new_finished(sources: &[(Timestamp, ShortcutSourceData)], bounds: (FlWeight, FlWeight), constant: bool) -> Self {
        let sources = match sources {
            &[] => Sources::None,
            &[(_, data)] => Sources::One(data),
            data => Sources::Multi(data.into()),
        };
        Self {
            cache: None,
            lower_bound: bounds.0,
            upper_bound: bounds.1,
            constant,
            required: true,
            sources,
        }
    }

    /// Merge this Shortcut with the lower triangle made up of the two EdgeIds (first down, then up).
    /// The `shortcut_graph` has to contain all the edges we may need to unpack.
    pub fn merge(&mut self, linked_ids: (EdgeId, EdgeId), shortcut_graph: &impl ShortcutGraphTrt, buffers: &mut MergeBuffers) {
        // We already know, we won't need this edge, so do nothing
        if !self.required {
            return;
        }

        if cfg!(feature = "detailed-stats") {
            IPP_COUNT.fetch_sub(self.cache.as_ref().map(TTFCache::<Box<[TTFPoint]>>::num_points).unwrap_or(0), Relaxed);
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
                let linked = first_plf.link(&second_plf);

                self.upper_bound = min(self.upper_bound, TTF::from(&linked).static_upper_bound());
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
            let self_plf = self.plf(shortcut_graph);

            // link TTFs in triangle
            let linked_ipps = first_plf.link(&second_plf);
            if cfg!(feature = "detailed-stats") {
                ACTUALLY_LINKED.fetch_add(1, Relaxed);
            }

            let linked = TTF::from(&linked_ipps);
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
                if cfg!(feature = "detailed-stats") {
                    CONSIDERED_FOR_APPROX.fetch_add(old, Relaxed);
                }
                merged = TTF::from(&merged).approximate(buffers);
                if cfg!(feature = "detailed-stats") {
                    SAVED_BY_APPROX.fetch_add(old as isize - merged.num_points() as isize, Relaxed);
                }
            }

            // update upper bounds after merging, but not lower bounds.
            // lower bounds were already set during precustomization -- possibly lower than necessary.
            // We would like to increase the lower bound to make it tighter, but we can't take the max right now,
            // We might find a lower function during a later merge operation.
            // We can only set the lower bound as tight as possible, once we have the final travel time function.
            self.upper_bound = min(self.upper_bound, TTF::from(&merged).static_upper_bound());
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
            self.sources = Shortcut::combine(sources, intersection_data, other_data);
        })();

        if cfg!(feature = "detailed-stats") {
            IPP_COUNT.fetch_add(self.cache.as_ref().map(TTFCache::<Box<[TTFPoint]>>::num_points).unwrap_or(0), Relaxed);
            PATH_SOURCES_COUNT.fetch_add(self.sources.len(), Relaxed);
            if self.cache.is_some() {
                ACTIVE_SHORTCUTS.fetch_add(1, Relaxed);
            }
        }
    }

    pub fn plf<'s>(&'s self, shortcut_graph: &'s impl ShortcutGraphTrt) -> TTF<'s> {
        if let Some(cache) = &self.cache {
            return cache.into();
        }

        match self.sources {
            Sources::One(source) => match source.into() {
                ShortcutSource::OriginalEdge(id) => TTF::Exact(shortcut_graph.original_graph().travel_time_function(id)),
                _ => panic!("invalid state of shortcut: ipps must be cached when shortcut not trivial {:?}", self),
            },
            _ => panic!("invalid state of shortcut: ipps must be cached when shortcut not trivial {:?}", self),
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
    pub fn finalize_bounds(&mut self, shortcut_graph: &impl ShortcutGraphTrt) {
        if !self.required {
            return;
        }

        if let Sources::None = self.sources {
            self.required = false;
            self.lower_bound = FlWeight::INFINITY;
            self.upper_bound = FlWeight::INFINITY;
            return;
        }

        let new_lower_bound = if cfg!(feature = "tdcch-precustomization") {
            max(self.lower_bound, self.plf(shortcut_graph).static_lower_bound())
        } else {
            self.plf(shortcut_graph).static_lower_bound()
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

        let new_upper_bound = min(self.upper_bound, self.plf(shortcut_graph).static_upper_bound());
        debug_assert!(!new_upper_bound.fuzzy_lt(self.upper_bound), "{:?}, {:?}", new_upper_bound, self);
        debug_assert!(!new_upper_bound.fuzzy_lt(self.lower_bound), "{:?}, {:?}", new_upper_bound, self);
        self.upper_bound = new_upper_bound;
    }

    /// If Shortcuts in skipped triangles are not required, the corresponding `Source` in this shortcut is also not required, so remove it
    pub fn invalidate_unneccesary_sources(&mut self, shortcut_graph: &PartialShortcutGraph) {
        match &mut self.sources {
            Sources::None => {}
            Sources::One(source) => {
                if !ShortcutSource::from(*source).required(shortcut_graph) {
                    *source = ShortcutSource::None.into();
                    self.upper_bound = FlWeight::INFINITY;
                    self.required = false;
                }
            }
            Sources::Multi(sources) => {
                let mut any_required = false;
                for (_, source) in &mut sources[..] {
                    if !ShortcutSource::from(*source).required(shortcut_graph) {
                        *source = ShortcutSource::None.into();
                        self.upper_bound = FlWeight::INFINITY;
                    } else {
                        any_required = true;
                    }
                }
                if !any_required {
                    self.required = false;
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
            IPP_COUNT.fetch_sub(self.cache.as_ref().map(TTFCache::<Box<[TTFPoint]>>::num_points).unwrap_or(0), Relaxed);
            if self.cache.is_some() {
                ACTIVE_SHORTCUTS.fetch_sub(1, Relaxed);
            }
        }
        self.cache = None;
    }

    pub fn set_cache(&mut self, ttf: Option<TTFCache<Box<[TTFPoint]>>>) {
        self.cache = ttf;
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

        let dummy = ShortcutSource::None.into();
        let mut prev_source = &dummy;
        let mut new_sources = Vec::new();

        // iterate over all old sources.
        // while self is better we need to copy these over
        // when other becomes better at an intersection we need to insert other_data at the intersection time
        // when self becomes better at an intersection we need to insert the source that was active at that time in the old sources at the new intersection time.
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

        // intersections after the last old source
        for (at, is_self_better) in intersection_iter {
            if is_self_better {
                new_sources.push((at, *prev_source));
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
    pub fn exact_ttf_for(
        &self,
        start: Timestamp,
        end: Timestamp,
        shortcut_graph: &impl ShortcutGraphTrt,
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

        match &self.sources {
            Sources::None => unreachable!("There are no TTFs for empty shortcuts"),
            Sources::One(source) => ShortcutSource::from(*source).exact_ttf_for(start, end, shortcut_graph, target, tmp),
            Sources::Multi(sources) => Self::exact_ttf_sources(sources, start, end, shortcut_graph, target, tmp),
        }
    }

    pub fn exact_ttf_sources(
        sources: &[(Timestamp, ShortcutSourceData)],
        start: Timestamp,
        end: Timestamp,
        shortcut_graph: &impl ShortcutGraphTrt,
        target: &mut MutTopPLF,
        tmp: &mut ReusablePLFStorage,
    ) {
        // when we have multiple source, we need to do unpacking (and append the results) for all sources which are relevant for the given time range.
        let mut c = SourceCursor::valid_at(sources, start);

        while c.cur().0.fuzzy_lt(end) {
            let mut inner_target = tmp.push_plf();
            ShortcutSource::from(c.cur().1).exact_ttf_for(
                max(start, c.cur().0),
                min(end, c.next().0),
                shortcut_graph,
                &mut inner_target,
                target.storage_mut(),
            );
            PiecewiseLinearFunction::append_partials(target, &inner_target, max(start, c.cur().0));

            // TODO what about wrapping holes??
            c.advance();
        }

        for points in target.windows(2) {
            debug_assert!(points[0].at.fuzzy_lt(points[1].at));
        }
    }

    pub fn num_sources(&self) -> usize {
        self.sources.len()
    }

    /// Returns an iterator over all the sources combined with a Timestamp for the time from which the corresponding source becomes valid.
    pub fn sources_iter(&self) -> impl Iterator<Item = (Timestamp, &ShortcutSourceData)> {
        self.sources.iter()
    }

    pub fn is_constant(&self) -> bool {
        self.constant
    }
}

// Enum to catch the common no source or just one source cases without allocations.
#[derive(Debug, Clone)]
enum Sources {
    None,
    One(ShortcutSourceData),
    Multi(Box<[(Timestamp, ShortcutSourceData)]>),
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

// Helper struct to iterate over sources.
// Allows to get sources valid for times > period().
// Handles all the ugly wraparound logic.
#[derive(Debug)]
struct SourceCursor<'a> {
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
            Ok(i) => Self {
                sources,
                current_index: i,
                offset,
            },
            Err(i) => Self {
                sources,
                current_index: i - 1,
                offset,
            },
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

/// Container struct which bundles all the reusable buffers we need during the customization for merging.
pub struct MergeBuffers {
    pub unpacking_target: ReusablePLFStorage,
    pub unpacking_tmp: ReusablePLFStorage,
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
