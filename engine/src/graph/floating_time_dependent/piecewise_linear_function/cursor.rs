use super::*;

pub trait MergeCursor<'a> {
    fn new(ipps: &'a [TTFPoint]) -> Self;
    fn cur(&self) -> TTFPoint;
    fn next(&self) -> TTFPoint;
    fn prev(&self) -> TTFPoint;
    fn advance(&mut self);

    fn ipps(&self) -> &'a [TTFPoint];
}

// #[derive(Debug)]
// pub struct NewMergeCursor<'a> {
//     ipps: &'a [TTFPoint],
//     iter: std::slice::Iter<'a, TTFPoint>,
//     next: TTFPoint,
//     cur: TTFPoint,
//     prev: TTFPoint,
// }

// impl<'a> MergeCursor<'a> for NewMergeCursor<'a> {
//     fn new(ipps: &'a [TTFPoint]) -> Self {
//         let ipps_without_period_point = if ipps.len() > 1 {
//             &ipps[..ipps.len() - 1]
//         } else {
//             ipps
//         };
//         let mut iter = ipps_without_period_point.iter();
//         let next = iter.next().unwrap().clone();
//         let cur = ipps_without_period_point.last().unwrap().shifted(FlWeight::zero() - FlWeight::from(period()));
//         let mut cursor = NewMergeCursor { prev: TTFPoint::default(), cur, next, iter, ipps: ipps_without_period_point };
//         cursor.advance();
//         cursor
//     }

//     fn cur(&self) -> &TTFPoint {
//         &self.cur
//     }

//     fn next(&self) -> &TTFPoint {
//         &self.next
//     }

//     fn prev(&self) -> &TTFPoint {
//         &self.prev
//     }

//     fn advance(&mut self) {
//         self.prev = self.cur.clone();
//         self.cur = self.next.clone();
//         if let Some(next) = self.iter.next() {
//             let (times_period, _) = self.next.at.split_of_period();
//             self.next = TTFPoint { at: times_period * FlWeight::from(period()) + next.at, val: next.val };
//         } else {
//             self.iter = self.ipps.iter();
//             let next = self.iter.next().unwrap();
//             let (times_period, _) = self.next.at.split_of_period();
//             self.next = TTFPoint { at: (times_period + FlWeight::new(1.0)) * FlWeight::from(period()) + next.at, val: next.val };
//         }
//     }

//     fn ipps(&self) -> &'a [TTFPoint] {
//         self.ipps
//     }
// }

// impl<'a> NewMergeCursor<'a> {
//     pub fn starting_at_or_after(ipps: &'a [TTFPoint], t: Timestamp) -> Self {
//         let (times_period, t) = t.split_of_period();
//         let mut offset = times_period * FlWeight::from(period());

//         if ipps.len() == 1 {
//             let mut iter = ipps.iter();
//             let val = iter.next().unwrap().val;

//             if t > Timestamp::zero() {
//                 offset = (period() + offset).into();
//             }
//             let prev = TTFPoint { at: (offset - period().into()).into(), val };
//             let cur = TTFPoint { at: offset.into(), val };
//             let next = TTFPoint { at: offset + period(), val };

//             return NewMergeCursor { iter, prev, cur, next, ipps }
//         }

//         let pos = ipps.binary_search_by(|p| {
//             if p.at.fuzzy_eq(t) {
//                 Ordering::Equal

//             } else if p.at < t {
//                 Ordering::Less
//             } else {
//                 Ordering::Greater
//             }
//         });

//         let i = match pos {
//             Ok(i) => i,
//             Err(i) => i,
//         };

//         if i > 0 {
//             let mut iter = ipps[..ipps.len() - 1].iter();
//             let prev = iter.nth(i - 1).unwrap().shifted(offset);
//             let mut cursor = NewMergeCursor { iter, next: prev, cur: TTFPoint::default(), prev: TTFPoint::default(), ipps: &ipps[..ipps.len() - 1] };
//             cursor.advance();
//             cursor.advance();
//             cursor
//         } else {
//             let mut cursor = NewMergeCursor::new(ipps);
//             cursor.prev = cursor.prev.shifted(offset);
//             cursor.cur = cursor.cur.shifted(offset);
//             cursor.next = cursor.next.shifted(offset);
//             cursor
//         }
//     }
// }

#[derive(Debug)]
pub struct Cursor<'a> {
    ipps: &'a [TTFPoint],
    current_index: usize,
    offset: FlWeight,
}

impl<'a> Cursor<'a> {
    pub fn starting_at_or_after(ipps: &'a [TTFPoint], t: Timestamp) -> Self {
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
pub struct PartialPlfMergeCursor<'a> {
    iter: std::slice::Iter<'a, TTFPoint>,
    next: TTFPoint,
    cur: TTFPoint,
    prev: TTFPoint,
}

impl<'a> MergeCursor<'a> for PartialPlfMergeCursor<'a> {
    fn new(ipps: &'a [TTFPoint]) -> Self {
        let mut iter = ipps.iter();
        let next = iter.next().unwrap().clone();
        let cur = TTFPoint { at: next.at - FlWeight::new(1.0), val: next.val };
        let mut cursor = PartialPlfMergeCursor { prev: TTFPoint::default(), cur, next, iter };
        cursor.advance();
        cursor
    }

    fn cur(&self) -> TTFPoint {
        self.cur.clone()
    }

    fn next(&self) -> TTFPoint {
        self.next.clone()
    }

    fn prev(&self) -> TTFPoint {
        self.prev.clone()
    }

    fn advance(&mut self) {
        self.prev = self.cur.clone();
        self.cur = self.next.clone();
        if let Some(next) = self.iter.next() {
            self.next = next.clone();
        } else {
            self.next.at = self.next.at + FlWeight::new(1.0);
        }
    }

    fn ipps(&self) -> &'a [TTFPoint] {
        self.iter.as_slice()
    }
}

#[derive(Debug)]
pub struct PartialPlfLinkCursor<'a> {
    iter: std::slice::Iter<'a, TTFPoint>,
    cur: TTFPoint,
    prev: TTFPoint,
    done: bool,
}

impl<'a> PartialPlfLinkCursor<'a> {
    pub fn new(ipps: &'a [TTFPoint]) -> Self {
        let mut iter = ipps.iter();
        let cur = iter.next().unwrap().clone();
        let prev = TTFPoint { at: cur.at - FlWeight::new(1.0), val: cur.val };
        PartialPlfLinkCursor { prev, cur, iter, done: false }
    }

    pub fn cur(&self) -> &TTFPoint {
        &self.cur
    }

    pub fn prev(&self) -> &TTFPoint {
        &self.prev
    }

    pub fn advance(&mut self) {
        self.prev = self.cur.clone();
        if let Some(next) = self.iter.next() {
            self.cur = next.clone();
        } else {
            self.cur.at = self.cur.at + FlWeight::new(1.0);
            self.done = true;
        }
    }

    pub fn done(&self) -> bool {
        self.done
    }
}
