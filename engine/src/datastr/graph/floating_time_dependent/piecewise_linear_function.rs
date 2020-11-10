//! Actual algorithms for linking, merging and copying around `PiecewiseLinearFunctions`.
//!
//! Code tuned for maximum performance, not for readability or maintainability.
//! Whatever happens in here should stay in here, theres quite a bit of Type abuse,
//! a lot of code with subtle implications and many ugly floating point arithmetic things.
//! Many things which might seem confusing or like they should be refactored have subtle reasons for being exactly like that.
//! The git history might be interesting for some of that.
//! The initial implementation of linking and merging took quite a few inspirations from https://github.com/GVeitBatz/KaTCH/blob/master/datastr/base/pwl_ttf.h

use self::debug::debug_merge;
use super::*;
use crate::util::*;
use std::cmp::{max, min, Ordering};

pub mod cursor;
use cursor::*;

pub trait PLF {
    fn evaluate(&self, t: Timestamp) -> FlWeight;
    fn append_range(&self, start: Timestamp, end: Timestamp, target: &mut impl PLFTarget);
}

// append point to a PLF making sure, that its not too close to the previous point
fn append_point(points: &mut Vec<TTFPoint>, point: TTFPoint) {
    debug_assert!(point.val >= FlWeight::new(0.0), "{:?}", point);
    if let Some(p) = points.last() {
        if p.at.fuzzy_eq(point.at) && p.val.fuzzy_eq(point.val) {
            return;
        }
    }
    debug_assert!(
        points.last().map(|p| p.at.fuzzy_lt(point.at)).unwrap_or(true),
        "last: {:?}, append: {:?}",
        points.last(),
        point
    );

    points.push(point)
}

/// A struct borrowing a slice of points which implements all sorts of operations and algorithms for PPLFs.
#[derive(Debug, Clone, Copy)]
pub struct PeriodicPiecewiseLinearFunction<'a> {
    ipps: &'a [TTFPoint],
}

impl<'a> PLF for PeriodicPiecewiseLinearFunction<'a> {
    fn evaluate(&self, t: Timestamp) -> FlWeight {
        let (_, t) = t.split_of_period();
        PartialPiecewiseLinearFunction::from(self).eval(t)
    }
    fn append_range(&self, start: Timestamp, end: Timestamp, target: &mut impl PLFTarget) {
        PeriodicPiecewiseLinearFunction::append_range(self, start, end, target)
    }
}

impl<'a> std::ops::Deref for PeriodicPiecewiseLinearFunction<'a> {
    type Target = [TTFPoint];

    fn deref(&self) -> &Self::Target {
        self.ipps
    }
}

impl<'a> PeriodicPiecewiseLinearFunction<'a> {
    /// New PLF from slice of points.
    /// In debug will validate the invariants we need from the function.
    pub fn new(ipps: &'a [TTFPoint]) -> Self {
        debug_assert!(ipps.first().unwrap().at == Timestamp::zero(), "{:?}", ipps);
        debug_assert!(ipps.first().unwrap().val.fuzzy_eq(ipps.last().unwrap().val), "{:?}", ipps);
        debug_assert!(ipps.len() == 1 || ipps.last().unwrap().at == period(), "{:?}", ipps);

        for points in ipps.windows(2) {
            debug_assert!(points[0].at.fuzzy_lt(points[1].at), "{:?}", ipps);
            debug_assert!(!(points[1].val - points[0].val).fuzzy_lt(points[0].at - points[1].at), "{:#?}", points);
        }

        Self { ipps }
    }

    pub fn lower_bound(&self) -> FlWeight {
        PartialPiecewiseLinearFunction::from(self).lower_bound()
    }

    pub fn upper_bound(&self) -> FlWeight {
        PartialPiecewiseLinearFunction::from(self).upper_bound()
    }

    /// Copy range of points to target such that [start, end] is completely covered but target may already contain points.
    /// When target already covers start, restrict those points to the range up to start, insert a point by linear interpolation
    /// and then insert points to cover everything up to (including) end.
    pub(super) fn append_range(&self, start: Timestamp, end: Timestamp, target: &mut impl PLFTarget) {
        debug_assert!(start.fuzzy_lt(end), "{:?} - {:?}", start, end);

        let mut f = Cursor::starting_at_or_after(&self.ipps, start);

        if target.is_empty() {
            if start.fuzzy_lt(f.cur().at) {
                target.push(f.prev());
            }
        } else {
            let target_last = target.pop().unwrap();

            let switchover_val = if target_last.at.fuzzy_eq(start) {
                target_last.val
            } else {
                interpolate_linear(target.last().unwrap(), &target_last, start)
            };

            if f.cur().at.fuzzy_eq(start) {
                debug_assert!(switchover_val.fuzzy_eq(f.cur().val), "{:?}", dbg_each!(switchover_val, f.cur()));
            } else {
                let second_switchover_val = interpolate_linear(&f.prev(), &f.cur(), start);
                debug_assert!(
                    switchover_val.fuzzy_eq(second_switchover_val),
                    "{:?}",
                    dbg_each!(switchover_val, second_switchover_val, start)
                );

                if target.last() != Some(&f.prev()) {
                    target.push(TTFPoint {
                        at: start,
                        val: switchover_val,
                    });
                }
            }
        }

        while f.cur().at.fuzzy_lt(end) {
            target.push(f.cur());
            f.advance();
        }

        // this ons is on or after end
        target.push(f.cur());

        for points in target.windows(2) {
            debug_assert!(points[0].at.fuzzy_lt(points[1].at));
        }

        debug_assert!(target[target.len() - 2].at.fuzzy_lt(end));
        debug_assert!(!target[target.len() - 1].at.fuzzy_lt(end));
    }

    pub(super) fn append_lower_bound_partials(first: &mut impl PLFTarget, second: &[TTFPoint], switchover: Timestamp) {
        debug_assert!(second.len() > 1);
        if let Some(&TTFPoint { at, .. }) = first.split_last().map(|(_, rest)| rest.last()).unwrap_or(None) {
            debug_assert!(at.fuzzy_lt(switchover));
        }
        if let Some(&TTFPoint { at, .. }) = first.last() {
            debug_assert!(!at.fuzzy_lt(switchover));
        }
        debug_assert!(!switchover.fuzzy_lt(second[0].at));
        debug_assert!(switchover.fuzzy_lt(second[1].at));

        if first.is_empty() {
            first.extend(second.iter().cloned());
            return;
        }

        let first_last = first.pop().unwrap();

        let first_switchover_val = if first_last.at.fuzzy_eq(switchover) {
            first_last.val
        } else {
            interpolate_linear(first.last().unwrap(), &first_last, switchover)
        };

        let second_switchover_val = if second[0].at.fuzzy_eq(switchover) {
            second[0].val
        } else {
            interpolate_linear(&second[0], &second[1], switchover)
        };

        // ignore point when segments colinear
        if first.last() != Some(&second[0]) {
            first.push(TTFPoint {
                at: switchover,
                val: min(first_switchover_val, second_switchover_val),
            });
        }
        let debug_start = first.len().saturating_sub(2);
        first.extend(second[1..].iter().cloned());

        for points in first[debug_start..].windows(2).take(2) {
            debug_assert!(points[0].at.fuzzy_lt(points[1].at));
            // debug_assert!(
            //     !(points[1].at - points[0].at).fuzzy_lt(points[0].val - points[1].val),
            //     "FiFo broken {:?}",
            //     points
            // );
            // if (points[1].at - points[0].at).fuzzy_lt(points[0].val - points[1].val) {
            //     eprintln!("FiFo broken {:?}", points);
            // }
        }
    }

    pub(super) fn append_upper_bound_partials(first: &mut impl PLFTarget, second: &[TTFPoint], switchover: Timestamp) {
        debug_assert!(second.len() > 1);
        if let Some(&TTFPoint { at, .. }) = first.split_last().map(|(_, rest)| rest.last()).unwrap_or(None) {
            debug_assert!(at.fuzzy_lt(switchover));
        }
        if let Some(&TTFPoint { at, .. }) = first.last() {
            debug_assert!(!at.fuzzy_lt(switchover));
        }
        debug_assert!(!switchover.fuzzy_lt(second[0].at));
        debug_assert!(switchover.fuzzy_lt(second[1].at));

        if first.is_empty() {
            first.extend(second.iter().cloned());
            return;
        }

        let first_last = first.pop().unwrap();

        let first_switchover_val = if first_last.at.fuzzy_eq(switchover) {
            first_last.val
        } else {
            interpolate_linear(first.last().unwrap(), &first_last, switchover)
        };

        let second_switchover_val = if second[0].at.fuzzy_eq(switchover) {
            second[0].val
        } else {
            interpolate_linear(&second[0], &second[1], switchover)
        };

        // ignore point when segments colinear
        if first.last() != Some(&second[0]) {
            first.push(TTFPoint {
                at: switchover,
                val: max(first_switchover_val, second_switchover_val),
            });
        }
        let debug_start = first.len().saturating_sub(2);
        first.extend(second[1..].iter().cloned());

        for points in first[debug_start..].windows(2).take(2) {
            debug_assert!(points[0].at.fuzzy_lt(points[1].at));
            // debug_assert!(
            //     !(points[1].at - points[0].at).fuzzy_lt(points[0].val - points[1].val),
            //     "FiFo broken {:?}",
            //     points
            // );
            // if (points[1].at - points[0].at).fuzzy_lt(points[0].val - points[1].val) {
            //     eprintln!("FiFo broken {:?}", points);
            // }
        }
    }

    /// Link two complete and valid PLFs.
    /// The result is also a complete and valid PLF, but since PLF is just a borrow we return a `Vec<TTFPoint>`
    pub fn link(&self, other: &Self) -> Vec<TTFPoint> {
        if let [TTFPoint { val, .. }] = &self.ipps {
            if let [TTFPoint { val: other, .. }] = &other.ipps {
                return vec![TTFPoint {
                    at: Timestamp::zero(),
                    val: val + other,
                }];
            } else {
                let zero_val = other.evaluate(val.into());
                let (_, val_offset) = Timestamp::from(val).split_of_period();
                let mut result = std::iter::once(TTFPoint {
                    at: Timestamp::zero(),
                    val: zero_val + val,
                })
                .chain(other.ipps.iter().filter(|p| p.at > val_offset).map(|p| TTFPoint {
                    at: p.at - FlWeight::from(val_offset),
                    val: p.val + val,
                }))
                .chain(other.ipps.iter().filter(|p| p.at < val_offset).map(|p| TTFPoint {
                    at: p.at + FlWeight::from(period()) - FlWeight::from(val_offset),
                    val: p.val + val,
                }))
                .chain(std::iter::once(TTFPoint {
                    at: period(),
                    val: zero_val + val,
                }))
                .fold(Vec::with_capacity(other.ipps.len() + 2), |mut acc, p| {
                    append_point(&mut acc, p);
                    acc
                });

                result.last_mut().unwrap().at = period();

                return result;
            }
        }
        if let [TTFPoint { val, .. }] = &other.ipps {
            return self.ipps.iter().map(|p| TTFPoint { at: p.at, val: p.val + val }).collect();
        }

        let mut result = Vec::with_capacity(self.ipps.len() + other.ipps.len() + 1);

        let mut f = PartialPlfLinkCursor::new(&self.ipps);
        let mut g = Cursor::starting_at_or_after(&other.ipps, Timestamp::zero() + self.ipps[0].val);

        loop {
            let mut x;
            let y;

            if g.cur().at.fuzzy_eq(f.cur().at + f.cur().val) {
                x = f.cur().at;
                y = g.cur().val + f.cur().val;

                g.advance();
                f.advance();
            } else if g.cur().at < f.cur().at + f.cur().val {
                debug_assert!(g.cur().at.fuzzy_lt(f.cur().at + f.cur().val));

                let m_arr_f_inverse = (f.cur().at - f.prev().at) / (f.cur().at + f.cur().val - f.prev().at - f.prev().val);
                x = m_arr_f_inverse * (g.cur().at - f.prev().at - f.prev().val) + f.prev().at;
                y = g.cur().at + g.cur().val - x;

                g.advance();
            } else {
                debug_assert!((f.cur().at + f.cur().val).fuzzy_lt(g.cur().at));

                x = f.cur().at;
                let m_g = (g.cur().val - g.prev().val) / (g.cur().at - g.prev().at);
                y = g.prev().val + m_g * (f.cur().at + f.cur().val - g.prev().at) + f.cur().val;

                f.advance();
            }

            if !x.fuzzy_lt(period()) {
                break;
            }
            debug_assert!(!x.fuzzy_lt(Timestamp::zero()), "{:?} {:?}", x, y);

            x = min(x, period());
            x = max(x, Timestamp::zero());

            append_point(&mut result, TTFPoint { at: x, val: y });
        }

        let zero_val = result[0].val;
        append_point(&mut result, TTFPoint { at: period(), val: zero_val });
        result.last_mut().unwrap().at = period();

        debug_assert!(result.len() <= self.ipps.len() + other.ipps.len() + 1);

        result
    }

    // Merge two complete and valid PLFs in the range between 0 and period and store the result in buffer.
    pub fn merge(&self, other: &Self, buffer: &mut Vec<TTFPoint>) -> (Box<[TTFPoint]>, Vec<(Timestamp, bool)>) {
        PartialPiecewiseLinearFunction::from(self).merge_in_bounds::<Cursor, True>(
            &PartialPiecewiseLinearFunction::from(other),
            Timestamp::zero(),
            period(),
            buffer,
        )
    }

    // douglas peuker approximation implementaion

    /// Approximate a PLF
    #[cfg(not(feature = "tdcch-approx-imai-iri"))]
    pub fn approximate(&self, buffer: &mut Vec<TTFPoint>) -> Box<[TTFPoint]> {
        buffer.reserve(self.ipps.len());
        PartialPiecewiseLinearFunction::from(self).douglas_peuker(buffer);
        let result = Box::<[TTFPoint]>::from(&buffer[..]);
        buffer.clear();
        result
    }

    /// Generate an approximated function which is always less or equal to the original function
    #[cfg(not(feature = "tdcch-approx-imai-iri"))]
    pub fn lower_bound_ttf(&self, buffer: &mut Vec<TTFPoint>) -> Box<[TTFPoint]> {
        buffer.reserve(self.ipps.len());
        PartialPiecewiseLinearFunction::from(self).douglas_peuker_lower(buffer);

        let wrap_min = min(buffer.first().unwrap().val, buffer.last().unwrap().val);

        buffer.first_mut().unwrap().val = wrap_min;
        buffer.last_mut().unwrap().val = wrap_min;

        let mut result = Box::<[TTFPoint]>::from(&buffer[..]);
        Self::fifoize_down(&mut result);
        buffer.clear();
        result
    }

    /// Generate an approximated function which is always greater or equal to the original function
    #[cfg(not(feature = "tdcch-approx-imai-iri"))]
    pub fn upper_bound_ttf(&self, buffer: &mut Vec<TTFPoint>) -> Box<[TTFPoint]> {
        buffer.reserve(self.ipps.len());
        PartialPiecewiseLinearFunction::from(self).douglas_peuker_upper(buffer);

        let wrap_max = max(buffer.first().unwrap().val, buffer.last().unwrap().val);

        buffer.first_mut().unwrap().val = wrap_max;
        buffer.last_mut().unwrap().val = wrap_max;

        let mut result = Box::<[TTFPoint]>::from(&buffer[..]);
        Self::fifoize_up(&mut result);
        buffer.clear();
        result
    }

    /// Same result as `(lower_bound_ttf(), upper_bound_ttf())` but with just one call to DP
    #[cfg(not(feature = "tdcch-approx-imai-iri"))]
    pub fn bound_ttfs(&self) -> (Box<[TTFPoint]>, Box<[TTFPoint]>) {
        let mut result_lower = Vec::with_capacity(self.ipps.len());
        let mut result_upper = Vec::with_capacity(self.ipps.len());
        PartialPiecewiseLinearFunction::from(self).douglas_peuker_combined(&mut result_lower, &mut result_upper);

        let wrap_min = min(result_lower.first().unwrap().val, result_lower.last().unwrap().val);
        let wrap_max = max(result_upper.first().unwrap().val, result_upper.last().unwrap().val);

        result_lower.first_mut().unwrap().val = wrap_min;
        result_lower.last_mut().unwrap().val = wrap_min;
        result_upper.first_mut().unwrap().val = wrap_max;
        result_upper.last_mut().unwrap().val = wrap_max;

        Self::fifoize_down(&mut result_lower);
        Self::fifoize_up(&mut result_upper);

        (result_lower.into(), result_upper.into())
    }

    #[cfg(feature = "tdcch-approx-imai-iri")]
    pub fn approximate(&self) -> Box<[TTFPoint]> {
        Imai::new(self.ipps, APPROX.into(), APPROX.into(), true, true).compute().into_boxed_slice()
    }

    #[cfg(feature = "tdcch-approx-imai-iri")]
    pub fn lower_bound_ttf(&self) -> Box<[TTFPoint]> {
        let mut lower = Imai::new(self.ipps, 0.0, APPROX.into(), true, true).compute();

        let wrap = min(lower.first().unwrap().val, lower.last().unwrap().val);
        lower.first_mut().unwrap().val = wrap;
        lower.last_mut().unwrap().val = wrap;

        Self::fifoize_down(&mut lower);

        lower.into_boxed_slice()
    }

    #[cfg(feature = "tdcch-approx-imai-iri")]
    pub fn upper_bound_ttf(&self) -> Box<[TTFPoint]> {
        let mut upper = Imai::new(self.ipps, APPROX.into(), 0.0, true, true).compute();

        let wrap = max(upper.first().unwrap().val, upper.last().unwrap().val);
        upper.first_mut().unwrap().val = wrap;
        upper.last_mut().unwrap().val = wrap;

        Self::fifoize_up(&mut upper);

        upper.into_boxed_slice()
    }

    fn fifoize_down(plf: &mut [TTFPoint]) {
        PartialPiecewiseLinearFunction::fifoize_down(plf);
        if !plf.last_mut().unwrap().val.fuzzy_eq(plf[0].val) {
            plf.last_mut().unwrap().val = plf[0].val;
            PartialPiecewiseLinearFunction::fifoize_down(plf);
        }
    }

    fn fifoize_up(plf: &mut [TTFPoint]) {
        PartialPiecewiseLinearFunction::fifoize_up(plf);
        if !plf[0].val.fuzzy_eq(plf.last().unwrap().val) {
            plf[0].val = plf.last().unwrap().val;
            PartialPiecewiseLinearFunction::fifoize_up(plf);
        }
    }
}

/// A struct borrowing a slice of points which implements all sorts of operations and algorithms for Partial PLFs (nonperiodic).
#[derive(Debug, Clone, Copy)]
pub struct PartialPiecewiseLinearFunction<'a> {
    ipps: &'a [TTFPoint],
}

impl<'a, 'b> From<&'b PeriodicPiecewiseLinearFunction<'a>> for PartialPiecewiseLinearFunction<'a> {
    fn from(pplf: &'b PeriodicPiecewiseLinearFunction<'a>) -> Self {
        PartialPiecewiseLinearFunction { ipps: pplf.ipps }
    }
}

impl<'a> From<PeriodicPiecewiseLinearFunction<'a>> for PartialPiecewiseLinearFunction<'a> {
    fn from(pplf: PeriodicPiecewiseLinearFunction<'a>) -> Self {
        PartialPiecewiseLinearFunction { ipps: pplf.ipps }
    }
}

impl<'a> std::ops::Deref for PartialPiecewiseLinearFunction<'a> {
    type Target = [TTFPoint];

    fn deref(&self) -> &Self::Target {
        self.ipps
    }
}

impl<'a> PartialPiecewiseLinearFunction<'a> {
    /// New PartialPLF from slice of points.
    /// In debug will validate the invariants we need from the function.
    pub fn new(ipps: &'a [TTFPoint]) -> Self {
        debug_assert!(!ipps.is_empty(), "{:?}", ipps);

        for points in ipps.windows(2) {
            debug_assert!(points[0].at.fuzzy_lt(points[1].at), "{:?}", ipps);
            debug_assert!(!(points[1].val - points[0].val).fuzzy_lt(points[0].at - points[1].at), "{:#?}", points);
        }

        Self { ipps }
    }

    pub fn lower_bound(&self) -> FlWeight {
        self.ipps.iter().map(|p| p.val).min().unwrap()
    }

    pub fn upper_bound(&self) -> FlWeight {
        self.ipps.iter().map(|p| p.val).max().unwrap()
    }

    pub(super) fn eval(&self, t: Timestamp) -> FlWeight {
        if self.ipps.len() == 1 {
            return self.ipps.first().unwrap().val;
        }

        debug_assert!(!self.last().unwrap().at.fuzzy_lt(t));
        debug_assert!(!t.fuzzy_lt(self.first().unwrap().at));

        let pos = self.ipps.binary_search_by(|p| {
            if p.at.fuzzy_eq(t) {
                Ordering::Equal
            } else if p.at < t {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        });

        match pos {
            Ok(i) => unsafe { self.ipps.get_unchecked(i).val },
            Err(i) => {
                let prev = unsafe { self.ipps.get_unchecked(i - 1) };
                let next = unsafe { self.ipps.get_unchecked(i) };

                interpolate_linear(prev, next, t)
            }
        }
    }

    pub fn sub_plf(&self, start: Timestamp, end: Timestamp) -> Self {
        debug_assert!(!start.fuzzy_eq(end));
        if self.len() == 1 {
            return *self;
        }
        debug_assert!(!start.fuzzy_lt(self.first().unwrap().at));
        debug_assert!(
            !self.last().unwrap().at.fuzzy_lt(end),
            "{:?} - {:?}, start {:?} end {:?}",
            self.first(),
            self.last(),
            start,
            end
        );

        let pos = self.ipps.binary_search_by(|p| {
            if p.at.fuzzy_eq(start) {
                Ordering::Equal
            } else if p.at < start {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        });
        let p_start = match pos {
            Ok(i) => i,
            Err(i) => i - 1,
        };

        let pos = self.ipps.binary_search_by(|p| {
            if p.at.fuzzy_eq(end) {
                Ordering::Equal
            } else if p.at < end {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        });
        let p_end = match pos {
            Ok(i) => i,
            Err(i) => i,
        };

        PartialPiecewiseLinearFunction::new(&self.ipps[p_start..=p_end])
    }

    /// Copy full slice of points to target/first.
    /// The difference here to the other copy/append methods is that we don't need the Cursor logig but can copy the entire slice.
    /// When target already covers switchover, restrict those points to the range up to start, insert a point by linear interpolation
    /// and then insert points to cover everything up to (including) end.
    pub(super) fn append(&self, switchover: Timestamp, target: &mut impl PLFTarget) {
        debug_assert!(self.len() > 1);
        if let Some(&TTFPoint { at, .. }) = target.split_last().map(|(_, rest)| rest.last()).unwrap_or(None) {
            debug_assert!(at.fuzzy_lt(switchover));
        }
        if let Some(&TTFPoint { at, .. }) = target.last() {
            debug_assert!(!at.fuzzy_lt(switchover));
        }
        debug_assert!(!switchover.fuzzy_lt(self[0].at));
        debug_assert!(switchover.fuzzy_lt(self[1].at), "{:?}", dbg_each!(&self[0..2], switchover));

        if target.is_empty() {
            target.extend(self.iter().cloned());
            return;
        }

        let target_last = target.pop().unwrap();

        let switchover_val = if target_last.at.fuzzy_eq(switchover) {
            target_last.val
        } else {
            interpolate_linear(target.last().unwrap(), &target_last, switchover)
        };

        if self[0].at.fuzzy_eq(switchover) {
            debug_assert!(switchover_val.fuzzy_eq(self[0].val), "{:?}", dbg_each!(switchover_val, self[0].val));
        } else {
            let self_switchover_val = interpolate_linear(&self[0], &self[1], switchover);
            debug_assert!(
                switchover_val.fuzzy_eq(self_switchover_val),
                "{:?}",
                dbg_each!(target.last(), target_last, switchover_val, self_switchover_val, &self[..=1], switchover)
            );
        }

        // ignore point when segments colinear
        if target.last() != Some(&self[0]) {
            target.push(TTFPoint {
                at: switchover,
                val: switchover_val,
            });
        }
        target.extend(self[1..].iter().cloned());

        for points in target.windows(2) {
            debug_assert!(points[0].at.fuzzy_lt(points[1].at));
        }
    }

    /// Link to partial PLFs and append the result to target, taking care of overlap
    #[allow(clippy::cognitive_complexity)]
    pub(super) fn link(&self, other: &Self, start: Timestamp, end: Timestamp, target: &mut impl PLFTarget) {
        let mut f = PartialPlfLinkCursor::new(self.ipps);
        let mut g = PartialPlfLinkCursor::new(other.ipps);

        loop {
            let x;
            let y;

            if g.cur().at.fuzzy_eq(f.cur().at + f.cur().val) {
                x = f.cur().at;
                y = g.cur().val + f.cur().val;

                f.advance();
                g.advance();
            } else if g.cur().at < f.cur().at + f.cur().val {
                debug_assert!(g.cur().at.fuzzy_lt(f.cur().at + f.cur().val));

                let m_arr_f_inverse = (f.cur().at - f.prev().at) / (f.cur().at + f.cur().val - f.prev().at - f.prev().val);
                x = m_arr_f_inverse * (g.cur().at - f.prev().at - f.prev().val) + f.prev().at;
                y = g.cur().at + g.cur().val - x;

                g.advance();
            } else {
                debug_assert!((f.cur().at + f.cur().val).fuzzy_lt(g.cur().at));

                x = f.cur().at;
                let m_g = (g.cur().val - g.prev().val) / (g.cur().at - g.prev().at);
                y = g.prev().val + m_g * (f.cur().at + f.cur().val - g.prev().at) + f.cur().val;

                f.advance();
            }

            // x <= start, target already has a point -> we got overlap at the start, pop last point from target
            if !start.fuzzy_lt(x) && !target.is_empty() {
                target.pop();
            }

            let mut point = TTFPoint { at: x, val: y };
            debug_assert!(point.val >= FlWeight::new(0.0), "{:?}", point);
            if let Some(p) = target.last() {
                if p.at.fuzzy_eq(point.at) && p.val.fuzzy_eq(point.val) {
                    continue;
                }
                if !p.at.fuzzy_lt(point.at) {
                    point.at = point.at + FlWeight::new(EPSILON);
                }
            }
            debug_assert!(
                target.last().map(|p| p.at.fuzzy_lt(point.at)).unwrap_or(true),
                "last: {:?}, append: {:?}",
                target.last(),
                point
            );

            target.push(point);

            if (f.done() && g.done()) || !x.fuzzy_lt(end) {
                break;
            }
        }

        if let [TTFPoint { val, .. }] = &target[..] {
            let val = *val;
            target.push(TTFPoint {
                at: self.last().unwrap().at,
                val,
            });
        }

        debug_assert!(target.len() > 1);
        debug_assert!(
            target.len() <= self.len() + other.len() + 1,
            "{:?}",
            dbg_each!(
                self.len(),
                other.len(),
                target.len(),
                start,
                end,
                self.first(),
                self.last(),
                other.first(),
                other.last()
            )
        );
        for points in target.windows(2) {
            debug_assert!(points[0].at.fuzzy_lt(points[1].at));
        }
        debug_assert!(!start.fuzzy_lt(target[0].at));
        debug_assert!(start.fuzzy_lt(target[1].at));
        debug_assert!(target[target.len() - 2].at.fuzzy_lt(end));
        debug_assert!(!target[target.len() - 1].at.fuzzy_lt(end));
    }

    // Merge two partial plfs in the range between start and end and store the result in buffer.
    pub fn merge(self, other: &Self, start: Timestamp, end: Timestamp, buffer: &mut Vec<TTFPoint>) -> (Box<[TTFPoint]>, Vec<(Timestamp, bool)>) {
        debug_assert!(start >= Timestamp::zero());
        debug_assert!(end <= period());

        self.merge_in_bounds::<PartialPlfMergeCursor, False>(other, start, end, buffer)
    }

    // Actual merging logic. Here be dragons.
    #[allow(clippy::cognitive_complexity)]
    fn merge_in_bounds<C: MergeCursor<'a>, FullRange: Bool>(
        &self,
        other: &Self,
        start: Timestamp,
        end: Timestamp,
        result: &mut Vec<TTFPoint>,
    ) -> (Box<[TTFPoint]>, Vec<(Timestamp, bool)>) {
        // easy cases
        if self.upper_bound() < other.lower_bound() {
            return (Box::from(self.ipps), vec![(start, true)]);
        } else if other.upper_bound() < self.lower_bound() {
            return (Box::from(other.ipps), vec![(start, false)]);
        }

        // setup
        result.reserve(2 * self.ipps.len() + 2 * other.ipps.len() + 2);
        let mut better = Vec::new();

        debug_assert!(!self.ipps.is_empty());
        debug_assert!(!other.ipps.is_empty());
        let mut f = C::new(&self.ipps);
        let mut g = C::new(&other.ipps);

        let self_start_val = if FullRange::VALUE || f.cur().at.fuzzy_eq(start) {
            f.cur().val
        } else {
            debug_assert!(f.cur().at.fuzzy_lt(start));
            interpolate_linear(&f.cur(), &f.next(), start)
        };

        let other_start_val = if FullRange::VALUE || g.cur().at.fuzzy_eq(start) {
            g.cur().val
        } else {
            debug_assert!(g.cur().at.fuzzy_lt(start));
            interpolate_linear(&g.cur(), &g.next(), start)
        };

        let mut needs_merging = false;
        let intersect_fuzzy_on_start =
            intersect(&f.cur(), &f.next(), &g.cur(), &g.next()) && intersection_point(&f.cur(), &f.next(), &g.cur(), &g.next()).at.fuzzy_eq(start);
        if self_start_val.fuzzy_eq(other_start_val) || intersect_fuzzy_on_start {
            better.push((start, !clockwise(&f.cur(), &f.next(), &g.next())));

            if intersect(&f.cur(), &f.next(), &g.cur(), &g.next()) {
                let intersection = intersection_point(&f.cur(), &f.next(), &g.cur(), &g.next());
                if start.fuzzy_lt(intersection.at) && intersection.at.fuzzy_lt(end) {
                    better[0].1 = !better[0].1;
                    needs_merging = true;
                }
            }
        } else {
            better.push((start, self_start_val < other_start_val));
        }

        f.advance();
        g.advance();

        // parallel sweep over both functions with linear_interpolation to check if one completely dominates the other,
        while !needs_merging && (!end.fuzzy_lt(f.prev().at) || !end.fuzzy_lt(g.prev().at)) {
            if f.cur().at.fuzzy_eq(g.cur().at) {
                if !f.cur().val.fuzzy_eq(g.cur().val) && (f.cur().val < g.cur().val) != better.last().unwrap().1 {
                    needs_merging = true;
                }

                f.advance();
                g.advance();
            } else if f.cur().at < g.cur().at {
                let delta = f.cur().val - interpolate_linear(&g.prev(), &g.cur(), f.cur().at);

                if !delta.fuzzy_eq(FlWeight::zero()) && (delta < FlWeight::zero()) != better.last().unwrap().1 {
                    needs_merging = true;
                }

                f.advance();
            } else {
                let delta = g.cur().val - interpolate_linear(&f.prev(), &f.cur(), g.cur().at);

                if !delta.fuzzy_eq(FlWeight::zero()) && (delta > FlWeight::zero()) != better.last().unwrap().1 {
                    needs_merging = true;
                }

                g.advance();
            }
        }

        if !needs_merging && !intersect_fuzzy_on_start {
            return ((Box::from(if better.last().unwrap().1 { self.ipps } else { other.ipps })), better);
        }

        let mut f = C::new(&self.ipps);
        let mut g = C::new(&other.ipps);

        if intersect_fuzzy_on_start {
            append_point(
                result,
                TTFPoint {
                    at: start,
                    val: min(self_start_val, other_start_val),
                },
            );
            let p_after_start = |f: &C| TTFPoint {
                at: start + FlWeight::new(EPSILON * 1.1),
                val: interpolate_linear(&f.cur(), &f.next(), start + FlWeight::new(EPSILON * 1.1)),
            };
            append_point(result, p_after_start(if better.last().unwrap().1 { &f } else { &g }));
        } else {
            append_point(result, if better.last().unwrap().1 { f.cur() } else { g.cur() });
        }

        f.advance();
        g.advance();

        // main loop
        while f.cur().at < end || g.cur().at < end {
            if intersect(&f.prev(), &f.cur(), &g.prev(), &g.cur()) {
                let intersection = intersection_point(&f.prev(), &f.cur(), &g.prev(), &g.cur());
                if start.fuzzy_lt(intersection.at) && intersection.at.fuzzy_lt(end) {
                    debug_assert_ne!(
                        better.last().unwrap().1,
                        counter_clockwise(&g.prev(), &f.cur(), &g.cur()),
                        "{:?} {:?}",
                        debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better),
                        dbg_each!(start, end, intersection)
                    );
                    better.push((intersection.at, counter_clockwise(&g.prev(), &f.cur(), &g.cur())));
                    append_point(result, intersection);
                }
            }

            if f.cur().at.fuzzy_eq(g.cur().at) {
                if f.cur().val.fuzzy_eq(g.cur().val) {
                    if better.last().unwrap().1 {
                        append_point(result, f.cur());
                    } else {
                        append_point(result, g.cur());
                    }

                    if !better.last().unwrap().1 && counter_clockwise(&f.cur(), &f.next(), &g.next()) {
                        debug_assert!(
                            !counter_clockwise(&g.prev(), &f.prev(), &f.cur()),
                            "{:?}",
                            debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
                        );
                        better.push((f.cur().at, true));
                    }

                    if better.last().unwrap().1 && clockwise(&f.cur(), &f.next(), &g.next()) {
                        debug_assert!(
                            !clockwise(&g.prev(), &f.prev(), &f.cur()),
                            "{:?}",
                            debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
                        );
                        better.push((f.cur().at, false));
                    }
                } else if f.cur().val < g.cur().val {
                    append_point(result, f.cur());
                    debug_assert!(
                        f.cur().val.fuzzy_lt(g.cur().val),
                        "{:?}",
                        debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
                    );
                    debug_assert!(
                        better.last().unwrap().1,
                        "{:?}",
                        debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
                    );

                    if !better.last().unwrap().1 {
                        eprintln!("Missed Intersection!");
                        let intersection = intersection_point(&f.prev(), &f.cur(), &g.prev(), &g.cur());
                        better.push((intersection.at, true));
                    }
                } else {
                    append_point(result, g.cur());
                    debug_assert!(
                        g.cur().val < f.cur().val,
                        "{:?}",
                        debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
                    );
                    debug_assert!(
                        !better.last().unwrap().1,
                        "{:?}",
                        debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
                    );

                    if better.last().unwrap().1 {
                        eprintln!("Missed Intersection!");
                        let intersection = intersection_point(&f.prev(), &f.cur(), &g.prev(), &g.cur());
                        better.push((intersection.at, false));
                    }
                }

                f.advance();
                g.advance();
            } else if f.cur().at < g.cur().at {
                debug_assert!(f.cur().at.fuzzy_lt(g.cur().at), "f {:?} g {:?}", f.cur().at, g.cur().at);

                if counter_clockwise(&g.prev(), &f.cur(), &g.cur()) {
                    append_point(result, f.cur());
                    debug_assert!(
                        better.last().unwrap().1,
                        "{:?}",
                        debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
                    );

                    if !better.last().unwrap().1 {
                        eprintln!("Missed Intersection!");
                        let intersection = intersection_point(&f.prev(), &f.cur(), &g.prev(), &g.cur());
                        better.push((intersection.at, true));
                    }
                } else if colinear_ordered(&g.prev(), &f.cur(), &g.cur()) {
                    if !better.last().unwrap().1 && counter_clockwise(&f.cur(), &f.next(), &g.cur()) {
                        better.push((f.cur().at, true))
                    }

                    if counter_clockwise(&g.prev(), &f.prev(), &f.cur()) || counter_clockwise(&f.cur(), &f.next(), &g.cur()) {
                        append_point(result, f.cur());
                        debug_assert!(
                            better.last().unwrap().1,
                            "{:?}",
                            debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
                        );
                    }

                    if better.last().unwrap().1 && clockwise(&f.cur(), &f.next(), &g.cur()) {
                        better.push((f.cur().at, false))
                    }
                }

                f.advance();
            } else {
                debug_assert!(g.cur().at < f.cur().at);

                if counter_clockwise(&f.prev(), &g.cur(), &f.cur()) {
                    append_point(result, g.cur());
                    debug_assert!(
                        !better.last().unwrap().1,
                        "{:?}",
                        debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
                    );

                    if better.last().unwrap().1 {
                        eprintln!("Missed Intersection!");
                        let intersection = intersection_point(&f.prev(), &f.cur(), &g.prev(), &g.cur());
                        better.push((intersection.at, false));
                    }
                } else if colinear_ordered(&f.prev(), &g.cur(), &f.cur()) {
                    if better.last().unwrap().1 && counter_clockwise(&g.cur(), &g.next(), &f.cur()) {
                        better.push((g.cur().at, false))
                    }

                    if counter_clockwise(&g.prev(), &g.cur(), &f.prev()) || counter_clockwise(&g.cur(), &g.next(), &f.cur()) {
                        append_point(result, g.cur());
                        debug_assert!(
                            !better.last().unwrap().1,
                            "{:?}",
                            debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
                        );
                    }

                    if !better.last().unwrap().1 && clockwise(&g.cur(), &g.next(), &f.cur()) {
                        better.push((g.cur().at, true))
                    }
                }

                g.advance();
            }
        }

        debug_assert!(
            !f.cur().at.fuzzy_lt(end),
            "{:?}",
            debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
        );
        debug_assert!(
            !g.cur().at.fuzzy_lt(end),
            "{:?}",
            debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
        );

        // intersection of final segments?
        if intersect(&f.prev(), &f.cur(), &g.prev(), &g.cur()) {
            let intersection = intersection_point(&f.prev(), &f.cur(), &g.prev(), &g.cur());
            if start.fuzzy_lt(intersection.at) && intersection.at.fuzzy_lt(end) {
                debug_assert_ne!(
                    better.last().unwrap().1,
                    counter_clockwise(&g.prev(), &f.cur(), &g.cur()),
                    "{:?}",
                    debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
                );
                better.push((intersection.at, counter_clockwise(&g.prev(), &f.cur(), &g.cur())));
                append_point(result, intersection);
            }
        }

        // last point
        if FullRange::VALUE {
            if result.len() > 1 {
                let p = TTFPoint { at: end, val: result[0].val };
                append_point(result, p);
            }
        } else if result.last().unwrap().at.fuzzy_lt(end) {
            append_point(result, if better.last().unwrap().1 { f.cur() } else { g.cur() });
        }

        debug_assert!(result.len() <= 2 * self.ipps.len() + 2 * other.ipps.len() + 2);
        for better_fns in better.windows(2) {
            debug_assert!(
                better_fns[0].0 < better_fns[1].0,
                "{:?}",
                debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
            );
            debug_assert_ne!(
                better_fns[0].1,
                better_fns[1].1,
                "{:?}",
                debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
            );
        }
        for &(at, _) in &better {
            debug_assert!(!at.fuzzy_lt(start));
            debug_assert!(!end.fuzzy_lt(at));
        }
        if !f.cur().val.fuzzy_eq(g.cur().val) && start == Timestamp::zero() && end == period() {
            debug_assert_eq!(
                better.first().map(|(_, better_fn)| better_fn),
                better.last().map(|(_, better_fn)| better_fn),
                "{:?}",
                debug_merge(&self.ipps, f.cur(), &other.ipps, g.cur(), &result, &better)
            );
        }
        debug_assert!(!start.fuzzy_lt(result[0].at));
        debug_assert!(start.fuzzy_lt(result[1].at));
        debug_assert!(!result[result.len() - 1].at.fuzzy_lt(end));
        debug_assert!(result[result.len() - 2].at.fuzzy_lt(end));

        let ret = (Box::from(&result[..]), better);
        result.clear();
        ret
    }

    // calculate approximated function
    #[cfg(not(feature = "tdcch-approx-imai-iri"))]
    fn douglas_peuker(&self, result: &mut Vec<TTFPoint>) {
        if self.ipps.len() <= 2 {
            result.extend_from_slice(self.ipps);
            return;
        }

        let first = self.ipps.first().unwrap();
        let last = self.ipps.last().unwrap();

        let (i, delta) = self.ipps[1..self.ipps.len() - 1]
            .iter()
            .enumerate()
            .map(|(i, p)| (i + 1, (p.val - interpolate_linear(first, last, p.at)).abs()))
            .max_by_key(|&(_, delta)| delta)
            .unwrap();

        if delta > APPROX {
            Self { ipps: &self.ipps[0..=i] }.douglas_peuker(result);
            result.pop();
            Self {
                ipps: &self.ipps[i..self.ipps.len()],
            }
            .douglas_peuker(result);
        } else {
            result.push(first.clone());
            result.push(last.clone());
        }
    }

    // calculate approximated bound functions and make them as tight as possible
    #[cfg(not(feature = "tdcch-approx-imai-iri"))]
    fn douglas_peuker_combined(&self, result_lower: &mut Vec<TTFPoint>, result_upper: &mut Vec<TTFPoint>) {
        if self.ipps.len() <= 2 {
            result_lower.extend_from_slice(self.ipps);
            result_upper.extend_from_slice(self.ipps);
            return;
        }

        let first = self.ipps.first().unwrap();
        let last = self.ipps.last().unwrap();

        let deltas = self.ipps[1..self.ipps.len() - 1]
            .iter()
            .enumerate()
            .map(|(i, p)| (i + 1, (p.val - interpolate_linear(first, last, p.at))));

        let (i_min, min_delta) = deltas.clone().min_by_key(|&(_, delta)| delta).unwrap();
        let (i_max, max_delta) = deltas.max_by_key(|&(_, delta)| delta).unwrap();

        let (i, delta) = if min_delta.abs() > max_delta.abs() {
            (i_min, min_delta.abs())
        } else {
            (i_max, max_delta.abs())
        };

        if delta > APPROX {
            Self { ipps: &self.ipps[0..=i] }.douglas_peuker_combined(result_lower, result_upper);
            let prev_min = result_lower.pop().map(|p| p.val).unwrap_or_else(FlWeight::zero);
            let prev_max = result_upper.pop().map(|p| p.val).unwrap_or_else(FlWeight::zero);
            let prev_len = result_lower.len();
            Self {
                ipps: &self.ipps[i..self.ipps.len()],
            }
            .douglas_peuker_combined(result_lower, result_upper);
            result_lower[prev_len].val = min(result_lower[prev_len].val, prev_min);
            result_upper[prev_len].val = max(result_upper[prev_len].val, prev_max);
        } else {
            result_lower.push(TTFPoint {
                at: first.at,
                val: first.val + min(FlWeight::zero(), min_delta),
            });
            result_upper.push(TTFPoint {
                at: first.at,
                val: first.val + max(FlWeight::zero(), max_delta),
            });
            result_lower.push(TTFPoint {
                at: last.at,
                val: last.val + min(FlWeight::zero(), min_delta),
            });
            result_upper.push(TTFPoint {
                at: last.at,
                val: last.val + max(FlWeight::zero(), max_delta),
            });
        }
    }

    // calculate approximated lower bound function and make it as tight as possible
    #[cfg(not(feature = "tdcch-approx-imai-iri"))]
    fn douglas_peuker_lower(&self, result_lower: &mut Vec<TTFPoint>) {
        if self.ipps.len() <= 2 {
            result_lower.extend_from_slice(self.ipps);
            return;
        }

        let first = self.ipps.first().unwrap();
        let last = self.ipps.last().unwrap();

        let deltas = self.ipps[1..self.ipps.len() - 1]
            .iter()
            .enumerate()
            .map(|(i, p)| (i + 1, (p.val - interpolate_linear(first, last, p.at))));

        let (i_min, min_delta) = deltas.clone().min_by_key(|&(_, delta)| delta).unwrap();
        let (i_max, max_delta) = deltas.max_by_key(|&(_, delta)| delta).unwrap();

        let (i, delta) = if min_delta.abs() > max_delta.abs() {
            (i_min, min_delta.abs())
        } else {
            (i_max, max_delta.abs())
        };

        if delta > APPROX {
            Self { ipps: &self.ipps[0..=i] }.douglas_peuker_lower(result_lower);
            let prev_min = result_lower.pop().map(|p| p.val).unwrap_or_else(FlWeight::zero);
            let prev_len = result_lower.len();
            Self {
                ipps: &self.ipps[i..self.ipps.len()],
            }
            .douglas_peuker_lower(result_lower);
            result_lower[prev_len].val = min(result_lower[prev_len].val, prev_min);
        } else {
            result_lower.push(TTFPoint {
                at: first.at,
                val: first.val + min(FlWeight::zero(), min_delta),
            });
            result_lower.push(TTFPoint {
                at: last.at,
                val: last.val + min(FlWeight::zero(), min_delta),
            });
        }
    }

    // calculate approximated upper bound function and make it as tight as possible
    #[cfg(not(feature = "tdcch-approx-imai-iri"))]
    fn douglas_peuker_upper(&self, result_upper: &mut Vec<TTFPoint>) {
        if self.ipps.len() <= 2 {
            result_upper.extend_from_slice(self.ipps);
            return;
        }

        let first = self.ipps.first().unwrap();
        let last = self.ipps.last().unwrap();

        let deltas = self.ipps[1..self.ipps.len() - 1]
            .iter()
            .enumerate()
            .map(|(i, p)| (i + 1, (p.val - interpolate_linear(first, last, p.at))));

        let (i_min, min_delta) = deltas.clone().min_by_key(|&(_, delta)| delta).unwrap();
        let (i_max, max_delta) = deltas.max_by_key(|&(_, delta)| delta).unwrap();

        let (i, delta) = if min_delta.abs() > max_delta.abs() {
            (i_min, min_delta.abs())
        } else {
            (i_max, max_delta.abs())
        };

        if delta > APPROX {
            Self { ipps: &self.ipps[0..=i] }.douglas_peuker_upper(result_upper);
            let prev_max = result_upper.pop().map(|p| p.val).unwrap_or_else(FlWeight::zero);
            let prev_len = result_upper.len();
            Self {
                ipps: &self.ipps[i..self.ipps.len()],
            }
            .douglas_peuker_upper(result_upper);
            result_upper[prev_len].val = max(result_upper[prev_len].val, prev_max);
        } else {
            result_upper.push(TTFPoint {
                at: first.at,
                val: first.val + max(FlWeight::zero(), max_delta),
            });
            result_upper.push(TTFPoint {
                at: last.at,
                val: last.val + max(FlWeight::zero(), max_delta),
            });
        }
    }

    /// Generate an approximated function which is always less or equal to the original function
    #[cfg(not(feature = "tdcch-approx-imai-iri"))]
    pub fn lower_bound_ttf(&self, buffer: &mut Vec<TTFPoint>) -> Box<[TTFPoint]> {
        buffer.reserve(self.ipps.len());
        self.douglas_peuker_lower(buffer);

        let mut result = Box::<[TTFPoint]>::from(&buffer[..]);
        Self::fifoize_down(&mut result);
        buffer.clear();
        result
    }

    /// Generate an approximated function which is always greater or equal to the original function
    #[cfg(not(feature = "tdcch-approx-imai-iri"))]
    pub fn upper_bound_ttf(&self, buffer: &mut Vec<TTFPoint>) -> Box<[TTFPoint]> {
        buffer.reserve(self.ipps.len());
        self.douglas_peuker_upper(buffer);

        let mut result = Box::<[TTFPoint]>::from(&buffer[..]);
        Self::fifoize_up(&mut result);
        buffer.clear();
        result
    }

    /// Same result as `(lower_bound_ttf(), upper_bound_ttf())` but with just one call to DP
    #[cfg(not(feature = "tdcch-approx-imai-iri"))]
    pub fn bound_ttfs(&self) -> (Box<[TTFPoint]>, Box<[TTFPoint]>) {
        let mut result_lower = Vec::with_capacity(self.ipps.len());
        let mut result_upper = Vec::with_capacity(self.ipps.len());
        self.douglas_peuker_combined(&mut result_lower, &mut result_upper);

        Self::fifoize_down(&mut result_lower);
        Self::fifoize_up(&mut result_upper);

        (result_lower.into(), result_upper.into())
    }

    fn fifoize_down(plf: &mut [TTFPoint]) {
        for i in (0..plf.len() - 1).rev() {
            plf[i].val = min(plf[i].val, plf[i + 1].val + plf[i + 1].at - plf[i].at);
        }
    }

    fn fifoize_up(plf: &mut [TTFPoint]) {
        for i in 1..plf.len() {
            plf[i].val = max(plf[i].val, plf[i - 1].val + plf[i - 1].at - plf[i].at);
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_fn_cursor() {
        run_test_with_periodicity(Timestamp::new(10.0), || {
            let ipps = [TTFPoint {
                at: Timestamp::zero(),
                val: FlWeight::new(5.0),
            }];
            let mut cursor = Cursor::new(&ipps);
            assert_eq!(
                cursor.cur(),
                TTFPoint {
                    at: Timestamp::zero(),
                    val: FlWeight::new(5.0)
                }
            );
            assert_eq!(
                cursor.next(),
                TTFPoint {
                    at: period(),
                    val: FlWeight::new(5.0)
                }
            );
            assert_eq!(
                cursor.prev(),
                TTFPoint {
                    at: Timestamp::zero() - FlWeight::from(period()),
                    val: FlWeight::new(5.0)
                }
            );
            cursor.advance();
            assert_eq!(
                cursor.cur(),
                TTFPoint {
                    at: period(),
                    val: FlWeight::new(5.0)
                }
            );
            assert_eq!(
                cursor.next(),
                TTFPoint {
                    at: Timestamp::new(20.0),
                    val: FlWeight::new(5.0)
                }
            );
            assert_eq!(
                cursor.prev(),
                TTFPoint {
                    at: Timestamp::zero(),
                    val: FlWeight::new(5.0)
                }
            );
        });
    }

    #[test]
    fn test_dyn_fn_cursor() {
        run_test_with_periodicity(Timestamp::new(10.0), || {
            let ipps = [
                TTFPoint {
                    at: Timestamp::zero(),
                    val: FlWeight::new(5.0),
                },
                TTFPoint {
                    at: Timestamp::new(5.0),
                    val: FlWeight::new(7.0),
                },
                TTFPoint {
                    at: period(),
                    val: FlWeight::new(5.0),
                },
            ];
            let mut cursor = Cursor::new(&ipps);
            assert_eq!(
                cursor.cur(),
                TTFPoint {
                    at: Timestamp::zero(),
                    val: FlWeight::new(5.0)
                }
            );
            assert_eq!(
                cursor.next(),
                TTFPoint {
                    at: Timestamp::new(5.0),
                    val: FlWeight::new(7.0)
                }
            );
            assert_eq!(
                cursor.prev(),
                TTFPoint {
                    at: Timestamp::new(-5.0),
                    val: FlWeight::new(7.0)
                }
            );
            cursor.advance();
            assert_eq!(
                cursor.cur(),
                TTFPoint {
                    at: Timestamp::new(5.0),
                    val: FlWeight::new(7.0)
                }
            );
            assert_eq!(
                cursor.next(),
                TTFPoint {
                    at: period(),
                    val: FlWeight::new(5.0)
                }
            );
            assert_eq!(
                cursor.prev(),
                TTFPoint {
                    at: Timestamp::zero(),
                    val: FlWeight::new(5.0)
                }
            );
            cursor.advance();
            assert_eq!(
                cursor.cur(),
                TTFPoint {
                    at: period(),
                    val: FlWeight::new(5.0)
                }
            );
            assert_eq!(
                cursor.next(),
                TTFPoint {
                    at: Timestamp::new(15.0),
                    val: FlWeight::new(7.0)
                }
            );
            assert_eq!(
                cursor.prev(),
                TTFPoint {
                    at: Timestamp::new(5.0),
                    val: FlWeight::new(7.0)
                }
            );
        });
    }

    #[test]
    fn test_linking_with_period_crossing() {
        run_test_with_periodicity(Timestamp::new(100.0), || {
            let ipps1 = [
                TTFPoint {
                    at: Timestamp::zero(),
                    val: FlWeight::new(105.0),
                },
                TTFPoint {
                    at: Timestamp::new(50.0),
                    val: FlWeight::new(95.0),
                },
                TTFPoint {
                    at: period(),
                    val: FlWeight::new(105.0),
                },
            ];

            let ipps2 = [
                TTFPoint {
                    at: Timestamp::zero(),
                    val: FlWeight::new(10.0),
                },
                TTFPoint {
                    at: Timestamp::new(60.0),
                    val: FlWeight::new(15.0),
                },
                TTFPoint {
                    at: period(),
                    val: FlWeight::new(10.0),
                },
            ];

            let linked = PeriodicPiecewiseLinearFunction::new(&ipps1).link(&PeriodicPiecewiseLinearFunction::new(&ipps2));
            assert_eq!(5, linked.len())
        });
    }

    #[test]
    fn test_linking_with_period_crossing_and_first_static() {
        run_test_with_periodicity(Timestamp::new(100.0), || {
            let ipps1 = [TTFPoint {
                at: Timestamp::zero(),
                val: FlWeight::new(110.0),
            }];

            let ipps2 = [
                TTFPoint {
                    at: Timestamp::zero(),
                    val: FlWeight::new(10.0),
                },
                TTFPoint {
                    at: Timestamp::new(60.0),
                    val: FlWeight::new(15.0),
                },
                TTFPoint {
                    at: period(),
                    val: FlWeight::new(10.0),
                },
            ];

            let linked = PeriodicPiecewiseLinearFunction::new(&ipps1).link(&PeriodicPiecewiseLinearFunction::new(&ipps2));
            assert_eq!(4, linked.len())
        });
    }

    #[test]
    fn test_copy_range_for_constant_plf() {
        run_test_with_periodicity(Timestamp::new(100.0), || {
            let ipps = [TTFPoint {
                at: Timestamp::zero(),
                val: FlWeight::new(10.0),
            }];
            let mut result = Vec::new();
            PeriodicPiecewiseLinearFunction::new(&ipps).append_range(Timestamp::new(40.0), Timestamp::new(50.0), &mut result);
            assert_eq!(
                result,
                vec![
                    TTFPoint {
                        at: Timestamp::zero(),
                        val: FlWeight::new(10.0)
                    },
                    TTFPoint {
                        at: Timestamp::new(100.0),
                        val: FlWeight::new(10.0)
                    }
                ]
            );
        });
    }

    #[test]
    fn test_partial_merging_with_intersection_fuzzy_on_start() {
        run_test_with_periodicity(Timestamp::new(86400.0), || {
            let first = [
                TTFPoint {
                    at: Timestamp::new(52074.519796162815),
                    val: FlWeight::new(135.4043214842386),
                },
                TTFPoint {
                    at: Timestamp::new(52079.684629900694),
                    val: FlWeight::new(168.51889019081864),
                },
                TTFPoint {
                    at: Timestamp::new(52120.84202396357),
                    val: FlWeight::new(165.15049612794246),
                },
            ];
            let first = PartialPiecewiseLinearFunction::new(&first);
            let second = [
                TTFPoint {
                    at: Timestamp::new(52078.159999999996),
                    val: FlWeight::new(169.1520000000022),
                },
                TTFPoint {
                    at: Timestamp::new(52082.479999999996),
                    val: FlWeight::new(166.49300000000252),
                },
            ];
            let second = PartialPiecewiseLinearFunction::new(&second);
            let (result, _) = first.merge(&second, Timestamp::new(52079.64118104493), Timestamp::new(52082.0), &mut Vec::new());
            assert_eq!(*result.last().unwrap(), second[1]);
        });
    }
}

/// Utilities for debugging PLF ops.
/// Will dump functions into a python file which when executed uses matplotlib to plot stuff.
mod debug {
    use super::*;

    use std::env;
    use std::fs::File;
    use std::io::{Error, Write};
    use std::process::{Command, Stdio};

    pub(super) fn debug_merge(f: &[TTFPoint], f_cur: TTFPoint, g: &[TTFPoint], g_cur: TTFPoint, merged: &[TTFPoint], better: &[(Timestamp, bool)]) {
        if let Ok(mut file) = File::create(format!("debug-{}-{}.py", f64::from(f_cur.at), f64::from(g_cur.at))) {
            write_python(&mut file, f, f_cur, g, g_cur, merged, better).unwrap_or_else(|_| eprintln!("failed to write debug script to file"));
        }

        if env::var("TDCCH_INTERACTIVE_DEBUG").is_ok() {
            if let Ok(mut child) = Command::new("python3").stdin(Stdio::piped()).spawn() {
                if let Some(mut stdin) = child.stdin.as_mut() {
                    write_python(&mut stdin, f, f_cur, g, g_cur, merged, better).unwrap_or_else(|_| eprintln!("failed to execute debug script"));
                }
            }
        }
    }

    fn write_python(
        output: &mut dyn Write,
        f: &[TTFPoint],
        f_cur: TTFPoint,
        g: &[TTFPoint],
        g_cur: TTFPoint,
        merged: &[TTFPoint],
        better: &[(Timestamp, bool)],
    ) -> Result<(), Error> {
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
        for p in f {
            write!(output, "({}, {}), ", f64::from(p.at), f64::from(p.val))?;
        }
        writeln!(output, "], 'r+-', label='f', linewidth=1, markersize=5)")?;

        write!(output, "plot_coords([")?;
        for p in g {
            write!(output, "({}, {}), ", f64::from(p.at), f64::from(p.val))?;
        }
        writeln!(output, "], 'gx-', label='g', linewidth=1, markersize=5)")?;

        writeln!(
            output,
            "plot_coords([({}, {})], 'rs', markersize=10)",
            f64::from(f_cur.at),
            f64::from(f_cur.val)
        )?;
        writeln!(
            output,
            "plot_coords([({}, {})], 'gs', markersize=10)",
            f64::from(g_cur.at),
            f64::from(g_cur.val)
        )?;

        if !merged.is_empty() {
            write!(output, "plot_coords([")?;
            for p in merged {
                write!(output, "({}, {}), ", f64::from(p.at), f64::from(p.val))?;
            }
            writeln!(output, "], 'bo-', label='merged', linewidth=1, markersize=1)")?;
        }

        let max_val = f.iter().map(|p| p.val).max().unwrap();
        let max_val = max(g.iter().map(|p| p.val).max().unwrap(), max_val);

        let min_val = f.iter().map(|p| p.val).min().unwrap();
        let min_val = min(g.iter().map(|p| p.val).min().unwrap(), min_val);

        for &(t, f_better) in better {
            writeln!(
                output,
                "plt.vlines({}, {}, {}, '{}', linewidth=1)",
                f64::from(t),
                f64::from(min_val),
                f64::from(max_val),
                if f_better { 'r' } else { 'g' }
            )?;
        }

        writeln!(output, "plt.legend()")?;
        writeln!(output, "plt.show()")?;
        Ok(())
    }
}

#[derive(Debug)]
pub struct UpdatedPiecewiseLinearFunction<'a> {
    plf: PeriodicPiecewiseLinearFunction<'a>,
    update: &'a [TTFPoint],
}

impl<'a> UpdatedPiecewiseLinearFunction<'a> {
    pub fn new(plf: PeriodicPiecewiseLinearFunction<'a>, update: &'a [TTFPoint]) -> Self {
        debug_assert_ne!(update.len(), 1);
        if let Some(p) = update.last() {
            debug_assert!(plf.evaluate(p.at).fuzzy_eq(p.val));
        }
        Self { plf, update }
    }

    pub fn update_plf(&self) -> Option<PartialPiecewiseLinearFunction<'a>> {
        if self.update.is_empty() {
            None
        } else {
            Some(PartialPiecewiseLinearFunction::new(self.update))
        }
    }

    pub fn unmodified_plf(&self) -> PeriodicPiecewiseLinearFunction<'a> {
        self.plf
    }

    pub fn lower_bound(&self) -> FlWeight {
        min(
            self.plf.lower_bound(),
            self.update_plf().map(|plf| plf.lower_bound()).unwrap_or(FlWeight::INFINITY),
        )
    }

    pub fn upper_bound(&self) -> FlWeight {
        max(
            self.plf.upper_bound(),
            self.update_plf().map(|plf| plf.upper_bound()).unwrap_or(FlWeight::zero()),
        )
    }

    pub fn t_switch(&self) -> Option<Timestamp> {
        self.update.last().map(|p| p.at)
    }
}

impl<'a> PLF for UpdatedPiecewiseLinearFunction<'a> {
    fn evaluate(&self, t: Timestamp) -> FlWeight {
        debug_assert!(!t.fuzzy_lt(self.update[0].at));
        if self.update.last().map(|p| t <= p.at).unwrap_or(false) {
            PartialPiecewiseLinearFunction::new(self.update).eval(t)
        } else {
            self.plf.evaluate(t)
        }
    }
    fn append_range(&self, start: Timestamp, end: Timestamp, target: &mut impl PLFTarget) {
        if self.t_switch().map(|l| !start.fuzzy_lt(l)).unwrap_or(true) {
            self.plf.append_range(start, end, target)
        } else {
            let live_until = self.t_switch().unwrap();
            if !live_until.fuzzy_lt(end) {
                PartialPiecewiseLinearFunction::new(self.update).sub_plf(start, end).append(start, target);
            } else {
                PartialPiecewiseLinearFunction::new(self.update)
                    .sub_plf(start, live_until)
                    .append(start, target);
                self.plf.append_range(live_until, end, target);
            }
        }
    }
}
