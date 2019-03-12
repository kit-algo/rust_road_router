use super::*;
use std::cmp::{min, max, Ordering};
use self::debug::debug_merge;

#[derive(Debug, Clone, Copy)]
pub struct PiecewiseLinearFunction<'a> {
    ipps: &'a [TTFPoint],
}

impl<'a> PiecewiseLinearFunction<'a> {
    pub fn new(ipps: &'a [TTFPoint]) -> PiecewiseLinearFunction<'a> {
        debug_assert!(ipps.first().unwrap().at == Timestamp::zero(), "{:?}", ipps);
        debug_assert!(ipps.first().unwrap().val.fuzzy_eq(ipps.last().unwrap().val), "{:?}", ipps);
        debug_assert!(ipps.len() == 1 || ipps.last().unwrap().at == period(), "{:?}", ipps);

        for points in ipps.windows(2) {
            debug_assert!(points[0].at < points[1].at, "{:?}", ipps);
        }

        PiecewiseLinearFunction { ipps }
    }

    pub fn len(&self) -> usize {
        self.ipps.len()
    }

    pub fn ipps(&self) -> &'a [TTFPoint] {
        self.ipps
    }

    pub fn lower_bound(&self) -> FlWeight {
        self.ipps.iter().map(|p| p.val).min().unwrap()
    }

    pub fn upper_bound(&self) -> FlWeight {
        self.ipps.iter().map(|p| p.val).max().unwrap()
    }

    pub fn evaluate(&self, t: Timestamp) -> FlWeight {
        let (_, t) = t.split_of_period();
        self.eval(t)
    }

    pub(super) fn eval(&self, t: Timestamp) -> FlWeight {
        debug_assert!(t < period());

        if self.ipps.len() == 1 {
            return self.ipps.first().unwrap().val
        }

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
                let prev = unsafe { self.ipps.get_unchecked(i-1) };
                let next = unsafe { self.ipps.get_unchecked(i) };

                interpolate_linear(prev, next, t)
            },
        }
    }

    pub(super) fn copy_range(&self, start: Timestamp, end: Timestamp, target: &mut impl PLFTarget) {
        debug_assert!(start.fuzzy_lt(end), "{:?} - {:?}", start, end);

        let mut f = Cursor::starting_at_or_after(&self.ipps, start);

        if start.fuzzy_lt(f.cur().at) {
            target.push(f.prev());
        }

        while f.cur().at.fuzzy_lt(end) {
            target.push(f.cur());
            f.advance();
        }

        target.push(f.cur());

        debug_assert!(target.len() > 1);

        for points in target.windows(2) {
            debug_assert!(points[0].at.fuzzy_lt(points[1].at), "{:?}", dbg_each!(&points[0], &target[..], start, end));
        }

        debug_assert!(!start.fuzzy_lt(target[0].at));
        debug_assert!(start.fuzzy_lt(target[1].at));
        debug_assert!(target[target.len() - 2].at.fuzzy_lt(end));
        debug_assert!(!target[target.len() - 1].at.fuzzy_lt(end));
    }

    pub(super) fn copy_append_to_partial(&self, start: Timestamp, end: Timestamp, target: &mut Vec<TTFPoint>) {
        debug_assert!(start.fuzzy_lt(end), "{:?} - {:?}", start, end);

        let mut f = Cursor::starting_at_or_after(&self.ipps, start);

        if target.is_empty() {
            debug_assert_eq!(start, Timestamp::zero());
            debug_assert_eq!(f.cur().at, Timestamp::zero());

            if start.fuzzy_lt(f.cur().at) {
                target.push(f.prev());
            }
        } else {
            let target_last = target.pop().unwrap();

            let switchover_val = if target_last.at.fuzzy_eq(start) {
                target_last.val
            } else {
                interpolate_linear(&target[target.len() - 1], &target_last, start)
            };

            if f.cur().at.fuzzy_eq(start) {
                debug_assert!(switchover_val.fuzzy_eq(f.cur().val), "{:?}", dbg_each!(switchover_val, f.cur()));
            } else {
                let second_switchover_val = interpolate_linear(&f.prev(), &f.cur(), start);
                debug_assert!(switchover_val.fuzzy_eq(second_switchover_val), "{:?}", dbg_each!(switchover_val, second_switchover_val, start));

                target.push(TTFPoint { at: start, val: switchover_val });
                f.advance();
            }
        }


        while f.cur().at.fuzzy_lt(end) {
            target.push(f.cur());
            f.advance();
        }

        target.push(f.cur());

        for points in target.windows(2) {
            debug_assert!(points[0].at.fuzzy_lt(points[1].at));
        }
    }

    pub(super) fn append_partials(first: &mut impl PLFTarget, second: &[TTFPoint], switchover: Timestamp) {
        debug_assert!(second.len() > 1);
        if let Some(&TTFPoint { at, .. }) = first.split_last().map(|(_, rest)| rest.last()).unwrap_or(None) { debug_assert!(at.fuzzy_lt(switchover)); }
        if let Some(&TTFPoint { at, .. }) = first.last() { debug_assert!(!at.fuzzy_lt(switchover)); }
        if let Some(&TTFPoint { at, .. }) = second.first() { debug_assert!(!switchover.fuzzy_lt(at)); }
        if let Some(&TTFPoint { at, .. }) = second.split_first().map(|(_, rest)| rest.first()).unwrap_or(None) { debug_assert!(switchover.fuzzy_lt(at)); }

        if first.is_empty() {
            first.extend(second.iter().cloned());
            return
        }

        let first_last = first.pop().unwrap();
        let switchover_val = if first_last.at.fuzzy_eq(switchover) {
            first_last.val
        } else {
            interpolate_linear(first.last().unwrap(), &first_last, switchover)
        };

        if second[0].at.fuzzy_eq(switchover) {
            debug_assert!(switchover_val.fuzzy_eq(second[0].val), "{:?}", dbg_each!(switchover_val, second[0].val));
        } else {
            let second_switchover_val = interpolate_linear(&second[0], &second[1], switchover);
            debug_assert!(switchover_val.fuzzy_eq(second_switchover_val), "{:?}", dbg_each!(first.last(), first_last, second_switchover_val, &second[..=1], switchover));
        }

        first.push(TTFPoint { at: switchover, val: switchover_val });
        first.extend(second[1..].iter().cloned());

        for points in first.windows(2) {
            debug_assert!(points[0].at.fuzzy_lt(points[1].at));
        }
    }

    pub fn link(&self, other: &Self) -> Vec<TTFPoint> {
        if let [TTFPoint { val, .. }] = &self.ipps {
            if let [TTFPoint { val: other, .. }] = &other.ipps {
                return vec![TTFPoint { at: Timestamp::zero(), val: val + other }]
            } else {
                let zero_val = other.evaluate(val.into());
                let (_, val_offset) = Timestamp::from(val).split_of_period();
                let mut result = std::iter::once(TTFPoint { at: Timestamp::zero(), val: zero_val + val })
                    .chain(
                        other.ipps.iter().filter(|p| p.at > val_offset).map(|p| TTFPoint { at: p.at - FlWeight::from(val_offset), val: p.val + val })
                    ).chain(
                        other.ipps.iter().filter(|p| p.at < val_offset).map(|p| TTFPoint { at: p.at + FlWeight::from(period()) - FlWeight::from(val_offset), val: p.val + val })
                    ).chain(std::iter::once(TTFPoint { at: period(), val: zero_val + val }))
                    .fold(Vec::with_capacity(other.ipps.len() + 2), |mut acc, p| {
                        Self::append_point(&mut acc, p);
                        acc
                    });

                result.last_mut().unwrap().at = period();

                return result
            }
        }
        if let [TTFPoint { val, .. }] = &other.ipps {
            return self.ipps.iter().map(|p| TTFPoint { at: p.at, val: p.val + val }).collect()
        }

        let mut result = Vec::with_capacity(self.ipps.len() + other.ipps.len() + 1);

        let mut f = Cursor::new(&self.ipps);
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

            if !x.fuzzy_lt(period()) { break }
            debug_assert!(!x.fuzzy_lt(Timestamp::zero()), "{:?} {:?} {:?}", x, y, debug_merge(&f, &g, &result, &[]));

            x = min(x, period());
            x = max(x, Timestamp::zero());

            Self::append_point(&mut result, TTFPoint { at: x, val: y });
        }

        let zero_val = result[0].val;
        Self::append_point(&mut result, TTFPoint { at: period(), val: zero_val });
        result.last_mut().unwrap().at = period();

        debug_assert!(result.len() <= self.ipps.len() + other.ipps.len() + 1);

        result
    }

    #[allow(clippy::cognitive_complexity)]
    pub(super) fn link_partials(first: &[TTFPoint], second: &[TTFPoint], start: Timestamp, end: Timestamp, target: &mut MutTopPLF) {
        let mut f_iter = first.iter();
        let mut g_iter = second.iter();

        let mut f_done = false;
        let mut g_done = false;

        let mut f_cur = f_iter.next().cloned().unwrap();
        let mut g_cur = g_iter.next().cloned().unwrap();

        let mut f_prev = TTFPoint { at: f_cur.at - FlWeight::new(1.0), val: f_cur.val };
        let mut g_prev = TTFPoint { at: g_cur.at - FlWeight::new(1.0), val: g_cur.val };

        loop {
            let x;
            let y;

            if g_cur.at.fuzzy_eq(f_cur.at + f_cur.val) {
                x = f_cur.at;
                y = g_cur.val + f_cur.val;

                if let Some(next_f) = f_iter.next() {
                    f_prev = f_cur;
                    f_cur = next_f.clone();
                } else {
                    f_prev.at = f_prev.at + FlWeight::new(1.0);
                    f_cur.at = f_cur.at + FlWeight::new(1.0);
                    f_done = true;
                }

                if let Some(next_g) = g_iter.next() {
                    g_prev = g_cur;
                    g_cur = next_g.clone();
                } else {
                    g_prev.at = g_prev.at + FlWeight::new(1.0);
                    g_cur.at = g_cur.at + FlWeight::new(1.0);
                    g_done = true;
                }
            } else if g_cur.at < f_cur.at + f_cur.val {
                debug_assert!(g_cur.at.fuzzy_lt(f_cur.at + f_cur.val));

                let m_arr_f_inverse = (f_cur.at - f_prev.at) / (f_cur.at + f_cur.val - f_prev.at - f_prev.val);
                x = m_arr_f_inverse * (g_cur.at - f_prev.at - f_prev.val) + f_prev.at;
                y = g_cur.at + g_cur.val - x;

                if let Some(next_g) = g_iter.next() {
                    g_prev = g_cur;
                    g_cur = next_g.clone();
                } else {
                    g_prev.at = g_prev.at + FlWeight::new(1.0);
                    g_cur.at = g_cur.at + FlWeight::new(1.0);
                    g_done = true;
                }
            } else {
                debug_assert!((f_cur.at + f_cur.val).fuzzy_lt(g_cur.at));

                x = f_cur.at;
                let m_g = (g_cur.val - g_prev.val) / (g_cur.at - g_prev.at);
                y = g_prev.val + m_g * (f_cur.at + f_cur.val - g_prev.at) + f_cur.val;

                if let Some(next_f) = f_iter.next() {
                    f_prev = f_cur;
                    f_cur = next_f.clone();
                } else {
                    f_prev.at = f_prev.at + FlWeight::new(1.0);
                    f_cur.at = f_cur.at + FlWeight::new(1.0);
                    f_done = true;
                }
            }

            if !start.fuzzy_lt(x) && !target.is_empty() {
                target.pop();
            }

            let point = TTFPoint { at: x, val: y };
            debug_assert!(point.val >= FlWeight::new(0.0), "{:?}", point);
            if let Some(p) = target.last() {
                if p.at.fuzzy_eq(point.at) && p.val.fuzzy_eq(point.val) { return }
            }
            debug_assert!(target.last().map(|p| p.at.fuzzy_lt(point.at)).unwrap_or(true), "last: {:?}, append: {:?}", target.last(), point);

            target.push(point);

            if (f_done && g_done) || !x.fuzzy_lt(end) {
                break;
            }
        }

        if let [TTFPoint { val, .. }] = &target[..] {
            target.push(TTFPoint { at: first.last().unwrap().at, val: *val });
        }

        debug_assert!(target.len() > 1);
        debug_assert!(target.len() <= first.len() + second.len() + 1);
        for points in target.windows(2) {
            debug_assert!(points[0].at.fuzzy_lt(points[1].at));
        }
        debug_assert!(!start.fuzzy_lt(target[0].at));
        debug_assert!(start.fuzzy_lt(target[1].at));
        debug_assert!(target[target.len() - 2].at.fuzzy_lt(end));
        debug_assert!(!target[target.len() - 1].at.fuzzy_lt(end));
    }

    pub fn merge_partials(first: &[TTFPoint], second: &[TTFPoint], start: Timestamp, end: Timestamp, buffer: &mut Vec<TTFPoint>) -> (Box<[TTFPoint]>, Vec<(Timestamp, bool)>) {
        debug_assert!(start >= Timestamp::zero());
        debug_assert!(end <= period());

        PiecewiseLinearFunction { ipps: first }.merge_in_bounds::<PartialPlfCursor>(&PiecewiseLinearFunction { ipps: second }, start, end, buffer)
    }

    pub fn merge(&self, other: &Self, buffer: &mut Vec<TTFPoint>) -> (Box<[TTFPoint]>, Vec<(Timestamp, bool)>) {
        self.merge_in_bounds::<Cursor>(other, Timestamp::zero(), period(), buffer)
    }

    #[allow(clippy::cognitive_complexity)]
    fn merge_in_bounds<C: MergeCursor<'a>>(&self, other: &Self, start: Timestamp, end: Timestamp, result: &mut Vec<TTFPoint>) -> (Box<[TTFPoint]>, Vec<(Timestamp, bool)>) {
        if self.upper_bound() < other.lower_bound() {
            return (self.ipps.to_vec().into_boxed_slice(), vec![(start, true)])
        } else if other.upper_bound() < self.lower_bound() {
            return (other.ipps.to_vec().into_boxed_slice(), vec![(start, false)])
        }

        result.reserve(2 * self.ipps.len() + 2 * other.ipps.len() + 2);
        let mut better = Vec::new();

        let mut f = C::new(&self.ipps);
        let mut g = C::new(&other.ipps);

        let self_start_val = if f.cur().at.fuzzy_eq(start) {
            f.cur().val
        } else {
            debug_assert!(f.cur().at.fuzzy_lt(start));
            interpolate_linear(&f.cur(), &f.next(), start)
        };

        let other_start_val = if g.cur().at.fuzzy_eq(start) {
            g.cur().val
        } else {
            debug_assert!(g.cur().at.fuzzy_lt(start));
            interpolate_linear(&g.cur(), &g.next(), start)
        };

        let mut needs_merging = false;
        if self_start_val.fuzzy_eq(other_start_val) {
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

        if !needs_merging {
            return ((if better.last().unwrap().1 { self.ipps } else { other.ipps }).to_vec().into_boxed_slice(), better)
        }

        let mut f = C::new(&self.ipps);
        let mut g = C::new(&other.ipps);

        Self::append_point(result, if better.last().unwrap().1 { f.cur() } else { g.cur() });
        f.advance();
        g.advance();

        while f.cur().at < end || g.cur().at < end {

            if intersect(&f.prev(), &f.cur(), &g.prev(), &g.cur()) {
                let intersection = intersection_point(&f.prev(), &f.cur(), &g.prev(), &g.cur());
                if start.fuzzy_lt(intersection.at) && intersection.at.fuzzy_lt(end) {
                    debug_assert_ne!(better.last().unwrap().1, counter_clockwise(&g.prev(), &f.cur(), &g.cur()), "{:?} {:?}", debug_merge(&f, &g, &result, &better), dbg_each!(start, end, intersection));
                    better.push((intersection.at, counter_clockwise(&g.prev(), &f.cur(), &g.cur())));
                    Self::append_point(result, intersection);
                }
            }

            if f.cur().at.fuzzy_eq(g.cur().at) {
                if f.cur().val.fuzzy_eq(g.cur().val) {
                    if better.last().unwrap().1 {
                        Self::append_point(result, f.cur());
                    } else {
                        Self::append_point(result, g.cur());
                    }

                    if !better.last().unwrap().1 && counter_clockwise(&f.cur(), &f.next(), &g.next()) {
                        debug_assert!(!counter_clockwise(&g.prev(), &f.prev(), &f.cur()), "{:?}", debug_merge(&f, &g, &result, &better));
                        better.push((f.cur().at, true));
                    }

                    if better.last().unwrap().1 && clockwise(&f.cur(), &f.next(), &g.next()) {
                        debug_assert!(!clockwise(&g.prev(), &f.prev(), &f.cur()), "{:?}", debug_merge(&f, &g, &result, &better));
                        better.push((f.cur().at, false));
                    }

                } else if f.cur().val < g.cur().val {

                    Self::append_point(result, f.cur());
                    debug_assert!(f.cur().val.fuzzy_lt(g.cur().val), "{:?}", debug_merge(&f, &g, &result, &better));
                    debug_assert!(better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));

                    if !better.last().unwrap().1 {
                        eprintln!("Missed Intersection!");
                        let intersection = intersection_point(&f.prev(), &f.cur(), &g.prev(), &g.cur());
                        better.push((intersection.at, true));
                    }

                } else {

                    Self::append_point(result, g.cur());
                    debug_assert!(g.cur().val < f.cur().val, "{:?}", debug_merge(&f, &g, &result, &better));
                    debug_assert!(!better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));

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
                    Self::append_point(result, f.cur());
                    debug_assert!(better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));

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
                        Self::append_point(result, f.cur());
                        debug_assert!(better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));
                    }

                    if better.last().unwrap().1 && clockwise(&f.cur(), &f.next(), &g.cur()) {
                        better.push((f.cur().at, false))
                    }
                }

                f.advance();

            } else {

                debug_assert!(g.cur().at < f.cur().at);

                if counter_clockwise(&f.prev(), &g.cur(), &f.cur()) {
                    Self::append_point(result, g.cur());
                    debug_assert!(!better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));

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
                        Self::append_point(result, g.cur());
                        debug_assert!(!better.last().unwrap().1, "{:?}", debug_merge(&f, &g, &result, &better));
                    }

                    if !better.last().unwrap().1 && clockwise(&g.cur(), &g.next(), &f.cur()) {
                        better.push((g.cur().at, true))
                    }
                }

                g.advance();
            }
        };

        debug_assert!(!f.cur().at.fuzzy_lt(end), "{:?}", debug_merge(&f, &g, &result, &better));
        debug_assert!(!g.cur().at.fuzzy_lt(end), "{:?}", debug_merge(&f, &g, &result, &better));

        if intersect(&f.prev(), &f.cur(), &g.prev(), &g.cur()) {
            let intersection = intersection_point(&f.prev(), &f.cur(), &g.prev(), &g.cur());
            if start.fuzzy_lt(intersection.at) && intersection.at.fuzzy_lt(end) {
                debug_assert_ne!(better.last().unwrap().1, counter_clockwise(&g.prev(), &f.cur(), &g.cur()), "{:?}", debug_merge(&f, &g, &result, &better));
                better.push((intersection.at, counter_clockwise(&g.prev(), &f.cur(), &g.cur())));
                Self::append_point(result, intersection);
            }
        }

        if start == Timestamp::zero() && end == period() {
            if result.len() > 1 {
                let p = TTFPoint { at: end, val: result[0].val };
                Self::append_point(result, p);
            }
        } else if result.last().unwrap().at.fuzzy_lt(end) {
            Self::append_point(result, if better.last().unwrap().1 { f.cur() } else { g.cur() });
        }

        debug_assert!(result.len() <= 2 * self.ipps.len() + 2 * other.ipps.len() + 2);
        for better_fns in better.windows(2) {
            debug_assert!(better_fns[0].0 < better_fns[1].0, "{:?}", debug_merge(&f, &g, &result, &better));
            debug_assert_ne!(better_fns[0].1, better_fns[1].1, "{:?}", debug_merge(&f, &g, &result, &better));
        }
        for &(at, _) in &better {
            debug_assert!(!at.fuzzy_lt(start));
            debug_assert!(!end.fuzzy_lt(at));
        }
        if !f.cur().val.fuzzy_eq(g.cur().val) && start == Timestamp::zero() && end == period() {
            debug_assert_eq!(better.first().map(|(_, better_fn)| better_fn), better.last().map(|(_, better_fn)| better_fn), "{:?}", debug_merge(&f, &g, &result, &better));
        }
        debug_assert!(!start.fuzzy_lt(result[0].at));
        debug_assert!(start.fuzzy_lt(result[1].at));
        debug_assert!(!result[result.len() - 1].at.fuzzy_lt(end));
        debug_assert!(result[result.len() - 2].at.fuzzy_lt(end));

        let ret = (result[..].to_vec().into_boxed_slice(), better);
        result.clear();
        ret
    }

    fn append_point(points: &mut Vec<TTFPoint>, point: TTFPoint) {
        debug_assert!(point.val >= FlWeight::new(0.0), "{:?}", point);
        if let Some(p) = points.last() {
            if p.at.fuzzy_eq(point.at) && p.val.fuzzy_eq(point.val) { return }
        }
        debug_assert!(points.last().map(|p| p.at.fuzzy_lt(point.at)).unwrap_or(true), "last: {:?}, append: {:?}", points.last(), point);

        points.push(point)
    }

    #[cfg(not(feature = "tdcch-approx"))]
    pub fn approximate(&self) -> Box<[TTFPoint]> {
        unimplemented!()
    }

    #[cfg(not(feature = "tdcch-approx"))]
    pub fn lower_bound_ttf(&self) -> Box<[TTFPoint]> {
        unimplemented!()
    }

    #[cfg(not(feature = "tdcch-approx"))]
    pub fn upper_bound_ttf(&self) -> Box<[TTFPoint]> {
        unimplemented!()
    }

    #[cfg(not(feature = "tdcch-approx"))]
    pub fn bound_ttfs(&self) -> (Box<[TTFPoint]>, Box<[TTFPoint]>) {
        unimplemented!()
    }

    #[cfg(all(feature = "tdcch-approx", not(feature = "tdcch-approx-imai-iri")))]
    pub fn approximate(&self) -> Box<[TTFPoint]> {
        let mut result = Vec::with_capacity(self.ipps.len());
        self.douglas_peuker(&mut result);
        result.into_boxed_slice()
    }

    #[cfg(all(feature = "tdcch-approx", not(feature = "tdcch-approx-imai-iri")))]
    pub fn lower_bound_ttf(&self) -> Box<[TTFPoint]> {
        let mut result = Vec::with_capacity(self.ipps.len());
        self.douglas_peuker_lower(&mut result);

        let wrap_min = min(result.first().unwrap().val, result.last().unwrap().val);

        result.first_mut().unwrap().val = wrap_min;
        result.last_mut().unwrap().val = wrap_min;

        result.into_boxed_slice()
    }

    #[cfg(all(feature = "tdcch-approx", not(feature = "tdcch-approx-imai-iri")))]
    pub fn upper_bound_ttf(&self) -> Box<[TTFPoint]> {
        let mut result = Vec::with_capacity(self.ipps.len());
        self.douglas_peuker_upper(&mut result);

        let wrap_max = max(result.first().unwrap().val, result.last().unwrap().val);

        result.first_mut().unwrap().val = wrap_max;
        result.last_mut().unwrap().val = wrap_max;

        result.into_boxed_slice()
    }

    #[cfg(all(feature = "tdcch-approx", not(feature = "tdcch-approx-imai-iri")))]
    pub fn bound_ttfs(&self) -> (Box<[TTFPoint]>, Box<[TTFPoint]>) {
        let mut result_lower = Vec::with_capacity(self.ipps.len());
        let mut result_upper = Vec::with_capacity(self.ipps.len());
        self.douglas_peuker_combined(&mut result_lower, &mut result_upper);

        let wrap_min = min(result_lower.first().unwrap().val, result_lower.last().unwrap().val);
        let wrap_max = max(result_upper.first().unwrap().val, result_upper.last().unwrap().val);

        result_lower.first_mut().unwrap().val = wrap_min;
        result_lower.last_mut().unwrap().val = wrap_min;
        result_upper.first_mut().unwrap().val = wrap_max;
        result_upper.last_mut().unwrap().val = wrap_max;

        (result_lower.into_boxed_slice(), result_upper.into_boxed_slice())
    }

    #[cfg(all(feature = "tdcch-approx", not(feature = "tdcch-approx-imai-iri")))]
    fn douglas_peuker(&self, result: &mut Vec<TTFPoint>) {
        if self.ipps.len() <= 2 {
            result.extend_from_slice(self.ipps);
            return
        }

        let first = self.ipps.first().unwrap();
        let last = self.ipps.last().unwrap();

        let (i, delta) = self.ipps[1..self.ipps.len()-1].iter()
            .enumerate()
            .map(|(i, p)| (i+1, (p.val - interpolate_linear(first, last, p.at)).abs()))
            .max_by_key(|&(_, delta)| delta)
            .unwrap();

        if delta > APPROX {
            PiecewiseLinearFunction { ipps: &self.ipps[0..=i] }.douglas_peuker(result);
            result.pop();
            PiecewiseLinearFunction { ipps: &self.ipps[i..self.ipps.len()] }.douglas_peuker(result);
        } else {
            result.push(first.clone());
            result.push(last.clone());
        }
    }

    #[cfg(all(feature = "tdcch-approx", not(feature = "tdcch-approx-imai-iri")))]
    fn douglas_peuker_combined(&self, result_lower: &mut Vec<TTFPoint>, result_upper: &mut Vec<TTFPoint>) {
        if self.ipps.len() <= 2 {
            result_lower.extend_from_slice(self.ipps);
            result_upper.extend_from_slice(self.ipps);
            return
        }

        let first = self.ipps.first().unwrap();
        let last = self.ipps.last().unwrap();

        let deltas = self.ipps[1..self.ipps.len()-1].iter()
            .enumerate()
            .map(|(i, p)| (i+1, (p.val - interpolate_linear(first, last, p.at))));

        let (i_min, min_delta) = deltas.clone().min_by_key(|&(_, delta)| delta).unwrap();
        let (i_max, max_delta) = deltas.max_by_key(|&(_, delta)| delta).unwrap();

        let (i, delta) = if min_delta.abs() > max_delta.abs() {
            (i_min, min_delta.abs())
        } else {
            (i_max, max_delta.abs())
        };

        if delta > APPROX {
            PiecewiseLinearFunction { ipps: &self.ipps[0..=i] }.douglas_peuker_combined(result_lower, result_upper);
            let prev_min = result_lower.pop().map(|p| p.val).unwrap_or_else(FlWeight::zero);
            let prev_max = result_upper.pop().map(|p| p.val).unwrap_or_else(FlWeight::zero);
            let prev_len = result_lower.len();
            PiecewiseLinearFunction { ipps: &self.ipps[i..self.ipps.len()] }.douglas_peuker_combined(result_lower, result_upper);
            result_lower[prev_len].val = min(result_lower[prev_len].val, prev_min);
            result_upper[prev_len].val = max(result_upper[prev_len].val, prev_max);
        } else {
            result_lower.push(TTFPoint { at: first.at, val: first.val + min(FlWeight::zero(), min_delta) });
            result_upper.push(TTFPoint { at: first.at, val: first.val + max(FlWeight::zero(), max_delta) });
            result_lower.push(TTFPoint { at: last.at, val: last.val + min(FlWeight::zero(), min_delta) });
            result_upper.push(TTFPoint { at: last.at, val: last.val + max(FlWeight::zero(), max_delta) });
        }
    }

    #[cfg(all(feature = "tdcch-approx", not(feature = "tdcch-approx-imai-iri")))]
    fn douglas_peuker_lower(&self, result_lower: &mut Vec<TTFPoint>) {
        if self.ipps.len() <= 2 {
            result_lower.extend_from_slice(self.ipps);
            return
        }

        let first = self.ipps.first().unwrap();
        let last = self.ipps.last().unwrap();

        let deltas = self.ipps[1..self.ipps.len()-1].iter()
            .enumerate()
            .map(|(i, p)| (i+1, (p.val - interpolate_linear(first, last, p.at))));

        let (i_min, min_delta) = deltas.clone().min_by_key(|&(_, delta)| delta).unwrap();
        let (i_max, max_delta) = deltas.max_by_key(|&(_, delta)| delta).unwrap();

        let (i, delta) = if min_delta.abs() > max_delta.abs() {
            (i_min, min_delta.abs())
        } else {
            (i_max, max_delta.abs())
        };

        if delta > APPROX {
            PiecewiseLinearFunction { ipps: &self.ipps[0..=i] }.douglas_peuker_lower(result_lower);
            let prev_min = result_lower.pop().map(|p| p.val).unwrap_or_else(FlWeight::zero);
            let prev_len = result_lower.len();
            PiecewiseLinearFunction { ipps: &self.ipps[i..self.ipps.len()] }.douglas_peuker_lower(result_lower);
            result_lower[prev_len].val = min(result_lower[prev_len].val, prev_min);
        } else {
            result_lower.push(TTFPoint { at: first.at, val: first.val + min(FlWeight::zero(), min_delta) });
            result_lower.push(TTFPoint { at: last.at, val: last.val + min(FlWeight::zero(), min_delta) });
        }
    }

    #[cfg(all(feature = "tdcch-approx", not(feature = "tdcch-approx-imai-iri")))]
    fn douglas_peuker_upper(&self, result_upper: &mut Vec<TTFPoint>) {
        if self.ipps.len() <= 2 {
            result_upper.extend_from_slice(self.ipps);
            return
        }

        let first = self.ipps.first().unwrap();
        let last = self.ipps.last().unwrap();

        let deltas = self.ipps[1..self.ipps.len()-1].iter()
            .enumerate()
            .map(|(i, p)| (i+1, (p.val - interpolate_linear(first, last, p.at))));

        let (i_min, min_delta) = deltas.clone().min_by_key(|&(_, delta)| delta).unwrap();
        let (i_max, max_delta) = deltas.max_by_key(|&(_, delta)| delta).unwrap();

        let (i, delta) = if min_delta.abs() > max_delta.abs() {
            (i_min, min_delta.abs())
        } else {
            (i_max, max_delta.abs())
        };

        if delta > APPROX {
            PiecewiseLinearFunction { ipps: &self.ipps[0..=i] }.douglas_peuker_upper(result_upper);
            let prev_max = result_upper.pop().map(|p| p.val).unwrap_or_else(FlWeight::zero);
            let prev_len = result_upper.len();
            PiecewiseLinearFunction { ipps: &self.ipps[i..self.ipps.len()] }.douglas_peuker_upper(result_upper);
            result_upper[prev_len].val = max(result_upper[prev_len].val, prev_max);
        } else {
            result_upper.push(TTFPoint { at: first.at, val: first.val + max(FlWeight::zero(), max_delta) });
            result_upper.push(TTFPoint { at: last.at, val: last.val + max(FlWeight::zero(), max_delta) });
        }
    }

    #[cfg(feature = "tdcch-approx-imai-iri")]
    pub fn approximate(&self) -> Box<[TTFPoint]> {
        Imai::new(self.ipps, APPROX.into(), APPROX.into(), true, true).compute().into_boxed_slice()
    }

    #[cfg(feature = "tdcch-approx-imai-iri")]
    pub fn lower_bound_ttf(&self) -> Box<[TTFPoint]> {
        let mut lower = Imai::new(self.ipps, 0.0, APPROX.into(), true, true).compute();

        for i in (1..lower.len()).rev() {
            if lower[i-1].val - lower[i].val > lower[i].at - lower[i-1].at {
                lower[i-1].val = lower[i].val + (lower[i].at - lower[i-1].at)
            }
        }

        // TODO this may break FIFO again
        let wrap = min(lower.first().unwrap().val, lower.last().unwrap().val);
        lower.first_mut().unwrap().val = wrap;
        lower.last_mut().unwrap().val = wrap;

        lower.into_boxed_slice()
    }

    #[cfg(feature = "tdcch-approx-imai-iri")]
    pub fn upper_bound_ttf(&self) -> Box<[TTFPoint]> {
        let mut upper = Imai::new(self.ipps, APPROX.into(), 0.0, true, true).compute();

        for i in 1..upper.len() {
            if upper[i-1].val - upper[i].val > upper[i].at - upper[i-1].at {
                upper[i].val = upper[i-1].val - (upper[i].at - upper[i-1].at)
            }
        }

        // TODO this may break FIFO again
        let wrap = max(upper.first().unwrap().val, upper.last().unwrap().val);
        upper.first_mut().unwrap().val = wrap;
        upper.last_mut().unwrap().val = wrap;

        upper.into_boxed_slice()
    }
}

trait MergeCursor<'a> {
    fn new(ipps: &'a [TTFPoint]) -> Self;
    fn cur(&self) -> TTFPoint;
    fn next(&self) -> TTFPoint;
    fn prev(&self) -> TTFPoint;
    fn advance(&mut self);

    fn ipps(&self) -> &'a [TTFPoint];
}

#[derive(Debug)]
pub struct Cursor<'a> {
    ipps: &'a [TTFPoint],
    current_index: usize,
    offset: FlWeight,
}

impl<'a> Cursor<'a> {
    fn starting_at_or_after(ipps: &'a [TTFPoint], t: Timestamp) -> Self {
        let (times_period, t) = t.split_of_period();
        let offset = times_period * FlWeight::from(period());

        if ipps.len() == 1 {
            return if t > Timestamp::zero() {
                Cursor { ipps, current_index: 0, offset: (period() + offset).into() }
            } else {
                Cursor { ipps, current_index: 0, offset }
            }
        }

        let pos = ipps.binary_search_by(|p| {
            if p.at.fuzzy_eq(t) {
                Ordering::Equal
            } else if p.at < t {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        });


        let i = match pos {
            Ok(i) => i,
            Err(i) => i,
        };
        if i == ipps.len() - 1 {
            Cursor { ipps, current_index: 0, offset: (period() + offset).into() }
        } else {
            Cursor { ipps, current_index: i, offset }
        }
    }
}

impl<'a> MergeCursor<'a> for Cursor<'a> {
    fn new(ipps: &'a [TTFPoint]) -> Cursor<'a> {
        Cursor { ipps, current_index: 0, offset: FlWeight::new(0.0) }
    }

    fn cur(&self) -> TTFPoint {
        self.ipps[self.current_index].shifted(self.offset)
    }

    fn next(&self) -> TTFPoint {
        if self.ipps.len() == 1 {
            self.ipps.first().unwrap().shifted(self.offset + FlWeight::from(period()))
        } else {
            self.ipps[self.current_index + 1].shifted(self.offset)
        }
    }

    fn prev(&self) -> TTFPoint {
        if self.ipps.len() == 1 {
            self.ipps.first().unwrap().shifted(self.offset - FlWeight::from(period()))
        } else if self.current_index == 0 {
            let offset = self.offset - FlWeight::from(period());
            self.ipps[self.ipps.len() - 2].shifted(offset)
        } else {
            self.ipps[self.current_index - 1].shifted(self.offset)
        }
    }

    fn advance(&mut self) {
        self.current_index += 1;
        if self.current_index % self.ipps.len() == self.ipps.len() - 1 || self.ipps.len() == 1 {
            self.offset = self.offset + FlWeight::from(period());
            self.current_index = 0;
        }
    }

    fn ipps(&self) -> &'a [TTFPoint] {
        self.ipps
    }
}

#[derive(Debug)]
pub struct PartialPlfCursor<'a> {
    ipps: &'a [TTFPoint],
    current_index: usize,
}

impl<'a> MergeCursor<'a> for PartialPlfCursor<'a> {
    fn new(ipps: &'a [TTFPoint]) -> Self {
        PartialPlfCursor { ipps, current_index: 0 }
    }

    fn cur(&self) -> TTFPoint {
        if self.current_index < self.ipps.len() {
            self.ipps[self.current_index].clone()
        } else {
            let offset = FlWeight::new((self.current_index - self.ipps.len() + 1) as f64);
            self.ipps.last().unwrap().shifted(offset)
        }
    }

    fn next(&self) -> TTFPoint {
        if self.current_index + 1 < self.ipps.len() {
            self.ipps[self.current_index + 1].clone()
        } else {
            let offset = FlWeight::new((self.current_index + 2 - self.ipps.len()) as f64);
            self.ipps.last().unwrap().shifted(offset)
        }
    }

    fn prev(&self) -> TTFPoint {
        if self.current_index == 0 {
            self.ipps[0].shifted(FlWeight::from(period()) * FlWeight::new(-1.0))
        } else if self.current_index - 1 < self.ipps.len() {
            self.ipps[self.current_index - 1].clone()
        } else {
            let offset = FlWeight::new((self.current_index - self.ipps.len()) as f64);
            self.ipps.last().unwrap().shifted(offset)
        }
    }

    fn advance(&mut self) {
        self.current_index += 1;
    }

    fn ipps(&self) -> &'a [TTFPoint] {
        self.ipps
    }
}

impl<'a> PartialPlfCursor<'a> {
    fn done(&self) -> bool {
        self.current_index >= self.ipps.len()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_static_fn_cursor() {
        run_test_with_periodicity(Timestamp::new(10.0), || {
            let ipps = [TTFPoint { at: Timestamp::zero(), val: FlWeight::new(5.0) }];
            let mut cursor = Cursor::new(&ipps);
            assert_eq!(cursor.cur(), TTFPoint { at: Timestamp::zero(), val: FlWeight::new(5.0) });
            assert_eq!(cursor.next(), TTFPoint { at: period(), val: FlWeight::new(5.0) });
            assert_eq!(cursor.prev(), TTFPoint { at: Timestamp::zero() - FlWeight::from(period()), val: FlWeight::new(5.0) });
            cursor.advance();
            assert_eq!(cursor.cur(), TTFPoint { at: period(), val: FlWeight::new(5.0) });
            assert_eq!(cursor.next(), TTFPoint { at: Timestamp::new(20.0), val: FlWeight::new(5.0) });
            assert_eq!(cursor.prev(), TTFPoint { at: Timestamp::zero(), val: FlWeight::new(5.0) });
        });
    }

    #[test]
    fn test_dyn_fn_cursor() {
        run_test_with_periodicity(Timestamp::new(10.0), || {
            let ipps = [TTFPoint { at: Timestamp::zero(), val: FlWeight::new(5.0) }, TTFPoint { at: Timestamp::new(5.0), val: FlWeight::new(7.0) }, TTFPoint { at: period(), val: FlWeight::new(5.0) }];
            let mut cursor = Cursor::new(&ipps);
            assert_eq!(cursor.cur(), TTFPoint { at: Timestamp::zero(), val: FlWeight::new(5.0) });
            assert_eq!(cursor.next(), TTFPoint { at: Timestamp::new(5.0), val: FlWeight::new(7.0) });
            assert_eq!(cursor.prev(), TTFPoint { at: Timestamp::new(-5.0), val: FlWeight::new(7.0) });
            cursor.advance();
            assert_eq!(cursor.cur(), TTFPoint { at: Timestamp::new(5.0), val: FlWeight::new(7.0) });
            assert_eq!(cursor.next(), TTFPoint { at: period(), val: FlWeight::new(5.0) });
            assert_eq!(cursor.prev(), TTFPoint { at: Timestamp::zero(), val: FlWeight::new(5.0) });
            cursor.advance();
            assert_eq!(cursor.cur(), TTFPoint { at: period(), val: FlWeight::new(5.0) });
            assert_eq!(cursor.next(), TTFPoint { at: Timestamp::new(15.0), val: FlWeight::new(7.0) });
            assert_eq!(cursor.prev(), TTFPoint { at: Timestamp::new(5.0), val: FlWeight::new(7.0) });
        });
    }

    #[test]
    fn test_linking_with_period_crossing() {
        run_test_with_periodicity(Timestamp::new(100.0), || {
            let ipps1 = [
                TTFPoint { at: Timestamp::zero(), val: FlWeight::new(105.0) },
                TTFPoint { at: Timestamp::new(50.0), val: FlWeight::new(95.0) },
                TTFPoint { at: period(), val: FlWeight::new(105.0) }];

            let ipps2 = [
                TTFPoint { at: Timestamp::zero(), val: FlWeight::new(10.0) },
                TTFPoint { at: Timestamp::new(60.0), val: FlWeight::new(15.0) },
                TTFPoint { at: period(), val: FlWeight::new(10.0) }];

            let linked = PiecewiseLinearFunction::new(&ipps1).link(&PiecewiseLinearFunction::new(&ipps2));
            assert_eq!(5, linked.len())
        });
    }

    #[test]
    fn test_linking_with_period_crossing_and_first_static() {
        run_test_with_periodicity(Timestamp::new(100.0), || {
            let ipps1 = [TTFPoint { at: Timestamp::zero(), val: FlWeight::new(110.0) }];

            let ipps2 = [
                TTFPoint { at: Timestamp::zero(), val: FlWeight::new(10.0) },
                TTFPoint { at: Timestamp::new(60.0), val: FlWeight::new(15.0) },
                TTFPoint { at: period(), val: FlWeight::new(10.0) }];

            let linked = PiecewiseLinearFunction::new(&ipps1).link(&PiecewiseLinearFunction::new(&ipps2));
            assert_eq!(4, linked.len())
        });
    }

    #[test]
    fn test_copy_range_for_constant_plf() {
        run_test_with_periodicity(Timestamp::new(100.0), || {
            let ipps = [TTFPoint { at: Timestamp::zero(), val: FlWeight::new(10.0) }];
            let mut result = Vec::new();
            PiecewiseLinearFunction::new(&ipps).copy_range(Timestamp::new(40.0), Timestamp::new(50.0), &mut result);
            assert_eq!(
                result,
                vec![TTFPoint { at: Timestamp::zero(), val: FlWeight::new(10.0) }, TTFPoint { at: Timestamp::new(100.0), val: FlWeight::new(10.0) }]);
        });
    }
}

mod debug {
    use super::*;

    use std::fs::File;
    use std::io::{Write, Error};
    use std::process::{Command, Stdio};
    use std::env;

    pub(super) fn debug_merge<'a, C: MergeCursor<'a>>(f: &C, g: &C, merged: &[TTFPoint], better: &[(Timestamp, bool)]) {
        if let Ok(mut file) = File::create(format!("debug-{}-{}.py", f64::from(f.cur().at), f64::from(g.cur().at))) {
            write_python(&mut file, f, g, merged, better).unwrap_or_else(|_| eprintln!("failed to write debug script to file"));
        }

        if env::var("TDCCH_INTERACTIVE_DEBUG").is_ok() {
            if let Ok(mut child) = Command::new("python3").stdin(Stdio::piped()).spawn() {
                if let Some(mut stdin) = child.stdin.as_mut() {
                    write_python(&mut stdin, f, g, merged, better).unwrap_or_else(|_| eprintln!("failed to execute debug script"));
                }
            }
        }
    }

    fn write_python<'a, C: MergeCursor<'a>, O: Write>(output: &mut O, f: &C, g: &C, merged: &[TTFPoint], better: &[(Timestamp, bool)]) -> Result<(), Error> {
        writeln!(output, "
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
        for p in f.ipps() {
            write!(output, "({}, {}), ", f64::from(p.at), f64::from(p.val))?;
        }
        writeln!(output, "], 'r+-', label='f', linewidth=1, markersize=5)")?;

        write!(output, "plot_coords([")?;
        for p in g.ipps() {
            write!(output, "({}, {}), ", f64::from(p.at), f64::from(p.val))?;
        }
        writeln!(output, "], 'gx-', label='g', linewidth=1, markersize=5)")?;

        writeln!(output, "plot_coords([({}, {})], 'rs', markersize=10)", f64::from(f.cur().at), f64::from(f.cur().val))?;
        writeln!(output, "plot_coords([({}, {})], 'gs', markersize=10)", f64::from(g.cur().at), f64::from(g.cur().val))?;

        if !merged.is_empty() {
            write!(output, "plot_coords([")?;
            for p in merged {
                write!(output, "({}, {}), ", f64::from(p.at), f64::from(p.val))?;
            }
            writeln!(output, "], 'bo-', label='merged', linewidth=1, markersize=1)")?;
        }

        let max_val = f.ipps().iter().map(|p| p.val).max().unwrap();
        let max_val = max(g.ipps().iter().map(|p| p.val).max().unwrap(), max_val);

        let min_val = f.ipps().iter().map(|p| p.val).min().unwrap();
        let min_val = min(g.ipps().iter().map(|p| p.val).min().unwrap(), min_val);

        for &(t, f_better) in better {
            writeln!(output, "plt.vlines({}, {}, {}, '{}', linewidth=1)", f64::from(t), f64::from(min_val), f64::from(max_val), if f_better { 'r' } else { 'g' })?;
        }

        writeln!(output, "plt.legend()")?;
        writeln!(output, "plt.show()")?;
        Ok(())
    }
}
