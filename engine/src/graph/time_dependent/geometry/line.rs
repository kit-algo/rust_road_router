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
    pub fn into_monotone_tt_line(self) -> MonotoneLine<TTIpp> {
        let Line { from, mut to, .. } = self;
        if to.at < from.at {
            to.at += period();
        }
        MonotoneLine(Line::new(from, to))
    }

    pub fn into_monotone_at_line(self) -> MonotoneLine<ATIpp> {
        let Line { from, mut to, .. } = self;
        if to.at < from.at {
            to.at += period();
        }
        MonotoneLine(Line::new(ATIpp::new(from.at, from.at + from.val), ATIpp::new(to.at, to.at + to.val)))
    }

    #[cfg(test)]
    fn delta_x(&self) -> Weight {
        self.to.at - self.from.at
    }

    #[cfg(test)]
    fn delta_y(&self) -> Weight {
        self.to.val - self.from.val
    }
}

impl Line<ATIpp> {
    fn delta_x(&self) -> Weight {
        self.to.at - self.from.at
    }

    fn delta_y(&self) -> Weight {
        self.to.val - self.from.val
    }

    fn shift(&mut self) {
        self.from.shift();
        self.to.shift();
    }
}



#[derive(Debug, Clone, PartialEq)]
pub struct MonotoneLine<Point>(Line<Point>);

impl<Point> MonotoneLine<Point> {
    pub fn into_line(self) -> Line<Point> {
        self.0
    }

    pub fn line(&self) -> &Line<Point> {
        &self.0
    }
}

impl MonotoneLine<TTIpp> {
    pub fn new(line: Line<TTIpp>) -> Self {
        debug_assert!(line.from.at <= line.to.at);
        MonotoneLine(line)
    }

    // TODO wrong results because rounding up for negative slopes
    #[inline]
    pub fn interpolate_tt(&self, x: Timestamp) -> Weight {
        debug_assert!(self.0.from.at < self.0.to.at, "self: {:?}", self);
        let delta_x = self.0.to.at - self.0.from.at;
        let delta_y = i64::from(self.0.to.val) - i64::from(self.0.from.val);
        let relative_x = i64::from(x) - i64::from(self.0.from.at);
        let result = i64::from(self.0.from.val) + (relative_x * delta_y / i64::from(delta_x)); // TODO div_euc
        debug_assert!(result >= 0);
        debug_assert!(result <= i64::from(INFINITY));
        result as Weight
    }

    pub fn into_monotone_at_line(self) -> MonotoneLine<ATIpp> {
        let MonotoneLine(Line { from, to, .. }) = self;
        MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(from.at, from.at + from.val), ATIpp::new(to.at, to.at + to.val)))
    }

    // TODO WTF?
    pub fn apply_periodicity(self, period: Timestamp) -> Line<TTIpp> {
        let MonotoneLine(Line { mut from, mut to }) = self;
        from.at %= period;
        from.val %= period;
        to.at %= period;
        to.val %= period;
        Line { from, to }
    }

    #[cfg(test)]
    pub fn delta_x(&self) -> Weight {
        self.0.delta_x()
    }

    #[cfg(test)]
    pub fn delta_y(&self) -> Weight {
        self.0.delta_y()
    }
}

impl MonotoneLine<ATIpp> {
    pub fn new(line: Line<ATIpp>) -> Self {
        debug_assert!(line.from.at <= line.to.at);
        debug_assert!(line.from.val <= line.to.val);
        MonotoneLine(line)
    }

    #[inline]
    pub fn interpolate_tt(&self, x: Timestamp) -> Weight {
        debug_assert!(x >= self.0.from.at, "self: {:?}, x: {}", self, x);
        debug_assert!(x <= self.0.to.at, "self: {:?}, x: {}", self, x);
        debug_assert!(self.0.from.at < self.0.to.at, "self: {:?}", self);
        debug_assert!(self.0.from.val <= self.0.to.val, "self: {:?}", self);
        let delta_x = self.0.to.at - self.0.from.at;
        let delta_y = self.0.to.val - self.0.from.val;
        let relative_x = x - self.0.from.at;
        // TODO will round wrong for negative relative_x -> div_euc but expensive...
        let result = u64::from(self.0.from.val) + (u64::from(relative_x) * u64::from(delta_y) / u64::from(delta_x));
        result as Weight - x
    }

    pub fn delta_x(&self) -> Weight {
        self.0.delta_x()
    }

    pub fn delta_y(&self) -> Weight {
        self.0.delta_y()
    }

    pub fn into_monotone_tt_line(self) -> MonotoneLine<TTIpp> {
        let MonotoneLine(Line { from, to }) = self;
        let from = from.into_ttipp();
        let to = to.into_ttipp();
        MonotoneLine(Line { from, to })
    }

    pub fn shift(&mut self) {
        self.0.shift();
    }
}


#[cfg(test)]
mod tests {
    use graph::time_dependent::run_test_with_periodicity;
    use super::*;

    #[test]
    fn test_tt_line_to_monotone_at_line() {
        run_test_with_periodicity(10, || {
            assert_eq!(Line { from: TTIpp::new(2, 2), to: TTIpp::new(4, 5) }.into_monotone_at_line(),
                MonotoneLine(Line { from: ATIpp::new(2, 4), to: ATIpp::new(4, 9) }));
            assert_eq!(Line { from: TTIpp::new(4, 5), to: TTIpp::new(2, 2) }.into_monotone_at_line(),
                MonotoneLine(Line { from: ATIpp::new(4, 9), to: ATIpp::new(12, 14) }));
            assert_eq!(Line { from: TTIpp::new(8, 3), to: TTIpp::new(2, 2) }.into_monotone_at_line(),
                MonotoneLine(Line { from: ATIpp::new(8, 11), to: ATIpp::new(12, 14) }));
        });
    }
}
