use super::*;

#[derive(Debug)]
pub struct PiecewiseLinearFunction<'a> {
    points: &'a [Point],
}
