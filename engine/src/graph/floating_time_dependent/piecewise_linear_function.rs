use super::*;

#[derive(Debug)]
pub struct PiecewiseLinearFunction<'a> {
    ipps: &'a [Point],
}

impl<'a> PiecewiseLinearFunction<'a> {
    pub fn new(ipps: &'a [Point]) -> PiecewiseLinearFunction<'a> {
        PiecewiseLinearFunction { ipps }
    }

    pub fn lower_bound(&self) -> Weight {
        self.ipps.iter().map(|p| p.val).min().unwrap()
    }

    pub fn upper_bound(&self) -> Weight {
        self.ipps.iter().map(|p| p.val).max().unwrap()
    }

    pub fn link(&self, _other: &Self) -> Vec<Point> {
        unimplemented!();
    }

    pub fn merge(&self, _other: &Self) -> (Vec<Point>, Vec<(Timestamp, bool)>) {
        unimplemented!();
    }
}
