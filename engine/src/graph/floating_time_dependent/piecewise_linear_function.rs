use super::*;
use std::cmp::{min, max};

#[derive(Debug)]
pub struct PiecewiseLinearFunction<'a> {
    ipps: &'a [Point],
}

impl<'a> PiecewiseLinearFunction<'a> {
    pub fn new(ipps: &'a [Point]) -> PiecewiseLinearFunction<'a> {
        debug_assert!(ipps.first().unwrap().at == Timestamp::zero());
        debug_assert!(ipps.first().unwrap().val == ipps.last().unwrap().val);
        debug_assert!(ipps.len() == 1 || ipps.last().unwrap().at == period());
        PiecewiseLinearFunction { ipps }
    }

    pub fn lower_bound(&self) -> FlWeight {
        self.ipps.iter().map(|p| p.val).min().unwrap()
    }

    pub fn upper_bound(&self) -> FlWeight {
        self.ipps.iter().map(|p| p.val).max().unwrap()
    }

    pub fn eval(&self, _t: Timestamp) -> FlWeight {
        unimplemented!();
    }

    pub fn link(&self, other: &Self) -> Vec<Point> {
        if let [Point { val, .. }] = &self.ipps {
            if let [Point { val: other, .. }] = &other.ipps {
                return vec![Point { at: Timestamp::zero(), val: val + other }]
            } else {
                let zero_val = other.eval(val.into());
                return other.ipps.iter().filter(|p| p.at > val.into()).map(|p| Point { at: p.at - val, val: p.val + val })
                    .chain(std::iter::once(Point { at: Timestamp::zero(), val: zero_val + val }))
                    .chain(
                        other.ipps.iter().filter(|p| p.at < val.into()).map(|p| Point { at: p.at + FlWeight::from(period()) - val, val: p.val + val })
                    ).chain(std::iter::once(Point { at: period(), val: zero_val + val })).collect()
            }
        }
        if let [Point { val, .. }] = &other.ipps {
            return self.ipps.iter().map(|p| Point { at: p.at, val: p.val + val }).collect()
        }

        let mut result = Vec::with_capacity(self.ipps.len() + other.ipps.len() + 1);

        let f = &self.ipps;
        let g = &other.ipps;

        let mut i = other.first_index_greater_or_equal(self.ipps[0].val);
        let mut j = 0;

        loop {
            let mut x;
            let y;

            if g[i].at.fuzzy_eq(f[j].at + f[j].val) {
                x = f[j].at;
                y = g[i].val + f[j].val;

                i += 1;
                j += 1;
            }
            else if g[i].at < f[j].at + f[j].val {
                debug_assert!(g[i].at.fuzzy_lt(f[j].at + f[j].val));

                let m_arr_f_inverse = (f[j].at - f[j-1].at) / (f[j].at + f[j].val - f[j-1].at - f[j-1].val);
                x = m_arr_f_inverse * (g[i].at - f[j-1].at - f[j-1].val) + f[j-1].at;
                y = g[i].at + g[i].val - x;

                i += 1;
            } else {
                debug_assert!((f[j].at + f[j].val).fuzzy_lt(g[i].at));

                x = f[j].at;
                let m_g = (g[i].val - g[i-1].val) / (g[i].at - g[i-1].at);
                y = g[i-1].val + m_g * (f[j].at + f[j].val - g[i-1].at) + f[j].val;

                j += 1;
            }

            if period().fuzzy_lt(x) { break }

            x = min(x, period());
            x = max(x, Timestamp::zero());

            Self::append_point(&mut result, Point { at: x, val: y });
        }

        debug_assert!(result.len() <= self.ipps.len() + other.ipps.len() + 1);
        result
    }

    pub fn merge(&self, _other: &Self) -> (Vec<Point>, Vec<(Timestamp, bool)>) {
        unimplemented!();
    }

    fn first_index_greater_or_equal(&self, _val: FlWeight) -> usize {
        unimplemented!();
    }

    fn append_point(points: &mut Vec<Point>, point: Point) {
        points.push(point)
    }
}
