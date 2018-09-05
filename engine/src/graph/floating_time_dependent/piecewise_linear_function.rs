use super::*;

#[derive(Debug)]
pub struct PiecewiseLinearFunction<'a> {
    ipps: &'a [Point],
}

impl<'a> PiecewiseLinearFunction<'a> {
    pub fn new(_ipps: &'a [Point]) -> PiecewiseLinearFunction<'a> {
        unimplemented!();
    }
}
