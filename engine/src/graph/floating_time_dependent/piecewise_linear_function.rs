use super::*;

#[derive(Debug)]
pub struct PiecewiseLinearFunction<'a> {
    ipps: &'a [Point],
}

impl<'a> PiecewiseLinearFunction<'a> {
    pub fn new(_ipps: &'a [Point]) -> PiecewiseLinearFunction<'a> {
        unimplemented!();
    }

    pub fn lower_bound(&self) -> Weight {
        unimplemented!();
    }

    pub fn upper_bound(&self) -> Weight {
        unimplemented!();
    }

    pub fn link(&self, _other: &Self) -> Vec<Point> {
        unimplemented!();
    }

    pub fn merge(&self, _other: &Self) -> (Vec<Point>, Vec<(Timestamp, bool)>) {
        unimplemented!();
    }
}
