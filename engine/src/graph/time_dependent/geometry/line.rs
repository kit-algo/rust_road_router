use super::*;

#[derive(Debug, Clone, PartialEq)]
pub struct Line<Point> {
    pub from: Point,
    pub to: Point,
}

impl<Point> Line<Point> {
    pub fn new(from: Point, to: Point) -> Self {
        Line { from, to }
    }
}

impl Line<TTIpp> {
    pub fn monotonize(self) -> MonotoneLine<TTIpp> {
        let Line { from, mut to } = self;
        if from.at > to.at {
            to.at += period();
        }
        MonotoneLine::<TTIpp>::new(Line { from, to })
    }
}

#[derive(Debug, Clone, PartialEq)]
pub struct MonotoneLine<Point>(Line<Point>);

impl<Point> MonotoneLine<Point> {
    pub fn line(&self) -> &Line<Point> {
        &self.0
    }
}

impl MonotoneLine<TTIpp> {
    pub fn new(line: Line<TTIpp>) -> Self {
        debug_assert!(line.from.at <= line.to.at);
        MonotoneLine(line)
    }

    #[inline]
    pub fn into_monotone_at_line(self) -> MonotoneLine<ATIpp> {
        let MonotoneLine(Line { from, to, .. }) = self;
        MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(from.at, from.at + from.val), ATIpp::new(to.at, to.at + to.val)))
    }
}

impl MonotoneLine<ATIpp> {
    pub fn new(line: Line<ATIpp>) -> Self {
        debug_assert!(line.from.at < line.to.at);
        debug_assert!(line.from.val <= line.to.val);
        MonotoneLine(line)
    }

    #[inline]
    pub fn interpolate_tt_in_range(&self, x: Timestamp) -> Weight {
        self.interpolate_at_in_range(x) - x
    }

    // no modulo period here!
    #[inline]
    pub fn interpolate_at_in_range(&self, x: Timestamp) -> Weight {
        let line = &self.0;
        debug_assert!(line.from.at < line.to.at, "self: {:?}", self);
        debug_assert!(line.from.val <= line.to.val, "self: {:?}", self);
        let delta_x = line.to.at - line.from.at;
        let delta_y = line.to.val - line.from.val;
        let relative_x = x - line.from.at;
        let result = u64::from(line.from.val) + (u64::from(relative_x) * u64::from(delta_y)) / u64::from(delta_x);
        debug_assert!(result < u64::from(INFINITY));
        result as Weight
    }
}
