use super::*;

#[derive(Debug)]
pub struct PiecewiseLinearFunction<'a> {
    ipps: &'a [Point],
}

impl<'a> PiecewiseLinearFunction<'a> {
    pub fn new(ipps: &'a [Point]) -> PiecewiseLinearFunction<'a> {
        debug_assert!(ipps.first().unwrap().at == Timestamp::zero());
        debug_assert!(ipps.first().unwrap().val == ipps.last().unwrap().val);
        debug_assert!(ipps.len() == 1 || ipps.last().unwrap().at == period().into());
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
                        other.ipps.iter().filter(|p| p.at < val.into()).map(|p| Point { at: p.at + period() - val, val: p.val + val })
                    ).chain(std::iter::once(Point { at: period().into(), val: zero_val + val })).collect()
            }
        }
        if let [Point { val, .. }] = &other.ipps {
            return self.ipps.iter().map(|p| Point { at: p.at, val: p.val + val }).collect()
        }

        unimplemented!();
    }

    pub fn merge(&self, _other: &Self) -> (Vec<Point>, Vec<(Timestamp, bool)>) {
        unimplemented!();
    }
}
