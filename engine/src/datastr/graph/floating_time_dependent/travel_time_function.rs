use super::*;
use std::cmp::max;

// During customization we need to store PLFs.
// For each shortcut we either have the exact function (`Exact`) or an approximation through less complex upper and lower bounds (`Approx`).
#[derive(Debug)]
pub enum ApproxTTFContainer<D> {
    Exact(D),
    Approx(D, D),
}

impl ApproxTTFContainer<Vec<TTFPoint>> {
    pub fn num_points(&self) -> usize {
        use ApproxTTFContainer::*;

        match &self {
            Exact(points) => points.len(),
            Approx(lower, upper) => lower.len() + upper.len(),
        }
    }
}

impl ApproxTTFContainer<Box<[TTFPoint]>> {
    pub fn num_points(&self) -> usize {
        use ApproxTTFContainer::*;

        match &self {
            Exact(points) => points.len(),
            Approx(lower, upper) => lower.len() + upper.len(),
        }
    }
}

impl From<ApproxTTFContainer<Vec<TTFPoint>>> for ApproxTTFContainer<Box<[TTFPoint]>> {
    fn from(cache: ApproxTTFContainer<Vec<TTFPoint>>) -> Self {
        match cache {
            ApproxTTFContainer::Exact(ipps) => ApproxTTFContainer::Exact(ipps.into_boxed_slice()),
            ApproxTTFContainer::Approx(lower_ipps, upper_ipps) => ApproxTTFContainer::Approx(lower_ipps.into_boxed_slice(), upper_ipps.into_boxed_slice()),
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

// Similar to `ApproxTTFContainer`, though this one is for actually working with the functions, `ApproxTTFContainer` is for storing them.
#[derive(Debug)]
pub enum ApproxTTF<'a> {
    Exact(PeriodicPiecewiseLinearFunction<'a>),
    Approx(PeriodicPiecewiseLinearFunction<'a>, PeriodicPiecewiseLinearFunction<'a>),
}

impl<'a> From<&'a ApproxTTFContainer<Box<[TTFPoint]>>> for ApproxTTF<'a> {
    fn from(cache: &'a ApproxTTFContainer<Box<[TTFPoint]>>) -> Self {
        match cache {
            ApproxTTFContainer::Exact(ipps) => ApproxTTF::Exact(PeriodicPiecewiseLinearFunction::new(ipps)),
            ApproxTTFContainer::Approx(lower_ipps, upper_ipps) => ApproxTTF::Approx(
                PeriodicPiecewiseLinearFunction::new(lower_ipps),
                PeriodicPiecewiseLinearFunction::new(upper_ipps),
            ),
        }
    }
}

impl<'a> From<&'a ApproxTTFContainer<Vec<TTFPoint>>> for ApproxTTF<'a> {
    fn from(cache: &'a ApproxTTFContainer<Vec<TTFPoint>>) -> Self {
        match cache {
            ApproxTTFContainer::Exact(ipps) => ApproxTTF::Exact(PeriodicPiecewiseLinearFunction::new(ipps)),
            ApproxTTFContainer::Approx(lower_ipps, upper_ipps) => ApproxTTF::Approx(
                PeriodicPiecewiseLinearFunction::new(lower_ipps),
                PeriodicPiecewiseLinearFunction::new(upper_ipps),
            ),
        }
    }
}

impl<'a> ApproxTTF<'a> {
    pub fn exact(&self) -> bool {
        use ApproxTTF::*;

        match &self {
            Exact(_) => true,
            Approx(_, _) => false,
        }
    }

    pub fn static_lower_bound(&self) -> FlWeight {
        use ApproxTTF::*;

        match self {
            Exact(plf) => plf.lower_bound(),
            Approx(lower_plf, _) => lower_plf.lower_bound(),
        }
    }

    pub fn static_upper_bound(&self) -> FlWeight {
        use ApproxTTF::*;

        match self {
            Exact(plf) => plf.upper_bound(),
            Approx(_, upper_plf) => upper_plf.upper_bound(),
        }
    }

    // Link to TTFs, creating a new function
    pub fn link(&self, second: &Self) -> ApproxTTFContainer<Vec<TTFPoint>> {
        use ApproxTTF::*;

        // if both TTFs are exact, we can link exact
        if let (Exact(first), Exact(second)) = (self, second) {
            return ApproxTTFContainer::Exact(first.link(second));
        }
        // else the result will be approximated anyway

        let (first_lower, first_upper) = self.bound_plfs();
        let (second_lower, second_upper) = second.bound_plfs();

        // linking two upper bounds is a valid upper bound, same for lower bounds
        ApproxTTFContainer::Approx(first_lower.link(&second_lower), first_upper.link(&second_upper))
    }

    // this ones a bit ugly...
    // exactly merging two TTFs, even when we only have approximations by lazily calculating exact functions for time ranges where the approximated bounds overlap.
    // beside the two TTFs we take buffers to reduce allocations
    // and a callback which does lazy exact function retrieval and merging when we really need it.
    #[allow(clippy::collapsible_if)]
    #[allow(clippy::cognitive_complexity)]
    #[allow(clippy::type_complexity)]
    pub fn merge(
        &self,
        other: &Self,
        buffers: &mut MergeBuffers,
        merge_exact: impl Fn(Timestamp, Timestamp, &mut MergeBuffers) -> (Box<[TTFPoint]>, Vec<(Timestamp, bool)>),
    ) -> (ApproxTTFContainer<Box<[TTFPoint]>>, Vec<(Timestamp, bool)>) {
        use ApproxTTF::*;

        // easy case, both functions are exact, we can just do actual function mering and are done
        if let (Exact(self_plf), Exact(other)) = (self, other) {
            let (plf, intersections) = self_plf.merge(other, &mut buffers.buffer);
            return (ApproxTTFContainer::Exact(plf), intersections);
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
        // debug::debug(self, other, &bound_merge_state);
        for (&(start_of_segment, state), end_of_segment) in bound_merge_state.iter().zip(end_of_segment_iter) {
            match state {
                BoundMergingState::First => {
                    self_lower.append_range(start_of_segment, end_of_segment, &mut buffers.exact_result_lower);
                    self_upper.append_range(start_of_segment, end_of_segment, &mut buffers.exact_result_upper);
                }
                BoundMergingState::Second => {
                    other_lower.append_range(start_of_segment, end_of_segment, &mut buffers.exact_result_lower);
                    other_upper.append_range(start_of_segment, end_of_segment, &mut buffers.exact_result_upper);
                }
                BoundMergingState::Merge => {
                    // TODO why cant we just take a subslice?
                    buffers.exact_self_buffer.clear();
                    self_lower.append_range(start_of_segment, end_of_segment, &mut buffers.exact_self_buffer);
                    buffers.exact_other_buffer.clear();
                    other_lower.append_range(start_of_segment, end_of_segment, &mut buffers.exact_other_buffer);
                    let (partial_lower, _) = PartialPiecewiseLinearFunction::new(&buffers.exact_self_buffer).merge(
                        &PartialPiecewiseLinearFunction::new(&buffers.exact_other_buffer),
                        start_of_segment,
                        end_of_segment,
                        &mut buffers.buffer,
                    );
                    PartialPiecewiseLinearFunction::new(&partial_lower).append(start_of_segment, &mut buffers.exact_result_lower);

                    buffers.exact_self_buffer.clear();
                    self_upper.append_range(start_of_segment, end_of_segment, &mut buffers.exact_self_buffer);
                    buffers.exact_other_buffer.clear();
                    other_upper.append_range(start_of_segment, end_of_segment, &mut buffers.exact_other_buffer);
                    let (partial_upper, _) = PartialPiecewiseLinearFunction::new(&buffers.exact_self_buffer).merge(
                        &PartialPiecewiseLinearFunction::new(&buffers.exact_other_buffer),
                        start_of_segment,
                        end_of_segment,
                        &mut buffers.buffer,
                    );
                    PartialPiecewiseLinearFunction::new(&partial_upper).append(start_of_segment, &mut buffers.exact_result_upper);
                }
            }
        }

        let ret = (
            ApproxTTFContainer::Approx(Box::from(&buffers.exact_result_lower[..]), Box::from(&buffers.exact_result_upper[..])),
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
        // (ApproxTTFContainer::Approx(result_lower, result_upper), result)
    }

    pub fn approximate(&self, buffers: &mut MergeBuffers) -> ApproxTTFContainer<Box<[TTFPoint]>> {
        use ApproxTTF::*;

        match self {
            Exact(plf) => {
                let (lower, upper) = plf.bound_ttfs();
                ApproxTTFContainer::Approx(lower, upper)
            }
            Approx(lower_plf, upper_plf) => {
                ApproxTTFContainer::Approx(lower_plf.lower_bound_ttf(&mut buffers.buffer), upper_plf.upper_bound_ttf(&mut buffers.buffer))
            }
        }
    }

    pub fn bound_plfs(&self) -> (PeriodicPiecewiseLinearFunction<'a>, PeriodicPiecewiseLinearFunction<'a>) {
        use ApproxTTF::*;

        match self {
            Exact(plf) => (*plf, *plf),
            Approx(lower_plf, upper_plf) => (*lower_plf, *upper_plf),
        }
    }
}

mod debug {
    use super::*;

    use std::env;
    use std::fs::File;
    use std::io::{Error, Write};
    use std::process::{Command, Stdio};

    #[allow(dead_code)]
    pub(super) fn debug(f: &ApproxTTF, g: &ApproxTTF, state: &[(Timestamp, BoundMergingState)]) {
        if let Ok(mut file) = File::create("debug.py") {
            write_python(&mut file, f, g, state).unwrap_or_else(|_| eprintln!("failed to write debug script to file"));
        }

        if env::var("TDCCH_INTERACTIVE_DEBUG").is_ok() {
            if let Ok(mut child) = Command::new("python3").stdin(Stdio::piped()).spawn() {
                if let Some(mut stdin) = child.stdin.as_mut() {
                    write_python(&mut stdin, f, g, state).unwrap_or_else(|_| eprintln!("failed to execute debug script"));
                }
            }
        }
    }

    fn write_python(output: &mut dyn Write, f: &ApproxTTF, g: &ApproxTTF, state: &[(Timestamp, BoundMergingState)]) -> Result<(), Error> {
        writeln!(
            output,
            "
import numpy as np
import matplotlib as mpl
import matplotlib.pyplot as plt
import seaborn

def plot_coords(coords, *args, **kwargs):
    x, y = zip(*coords)
    plt.plot(list(x), list(y), *args, **kwargs)

"
        )?;
        write!(output, "plot_coords([")?;
        for p in f.bound_plfs().0.iter() {
            write!(output, "({}, {}), ", f64::from(p.at), f64::from(p.val))?;
        }
        writeln!(output, "], 'r+-', label='f', linewidth=1, markersize=5)")?;

        write!(output, "plot_coords([")?;
        for p in f.bound_plfs().1.iter() {
            write!(output, "({}, {}), ", f64::from(p.at), f64::from(p.val))?;
        }
        writeln!(output, "], 'r+-', label='f', linewidth=1, markersize=5)")?;

        write!(output, "plot_coords([")?;
        for p in g.bound_plfs().0.iter() {
            write!(output, "({}, {}), ", f64::from(p.at), f64::from(p.val))?;
        }
        writeln!(output, "], 'gx-', label='g', linewidth=1, markersize=5)")?;

        write!(output, "plot_coords([")?;
        for p in g.bound_plfs().1.iter() {
            write!(output, "({}, {}), ", f64::from(p.at), f64::from(p.val))?;
        }
        writeln!(output, "], 'gx-', label='g', linewidth=1, markersize=5)")?;

        let max_val = f.bound_plfs().1.iter().map(|p| p.val).max().unwrap();
        let max_val = max(g.bound_plfs().1.iter().map(|p| p.val).max().unwrap(), max_val);

        let min_val = f.bound_plfs().0.iter().map(|p| p.val).min().unwrap();
        let min_val = std::cmp::min(g.bound_plfs().0.iter().map(|p| p.val).min().unwrap(), min_val);

        for &(t, state) in state {
            writeln!(
                output,
                "plt.vlines({}, {}, {}, '{}', linewidth=1)",
                f64::from(t),
                f64::from(min_val),
                f64::from(max_val),
                match state {
                    BoundMergingState::First => 'r',
                    BoundMergingState::Second => 'g',
                    BoundMergingState::Merge => 'b',
                }
            )?;
        }

        writeln!(output, "plt.legend()")?;
        writeln!(output, "plt.show()")?;
        Ok(())
    }
}
