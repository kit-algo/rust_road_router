//! Data structures to efficiently iterate over TTFPoints.
//! Allows to get points valid for times > period().
//! Handling all the ugly shifting and wrapping logic.

use super::*;

/// All the ops we need during merging
pub trait MergeCursor<'a> {
    fn new(ipps: &'a [TTFPoint]) -> Self;
    fn cur(&self) -> TTFPoint;
    fn next(&self) -> TTFPoint;
    fn prev(&self) -> TTFPoint;
    fn advance(&mut self);

    fn ipps(&self) -> &'a [TTFPoint];
}

/// Cursor for complete TTFs
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
            return if t > Timestamp::ZERO {
                Cursor {
                    ipps,
                    current_index: 0,
                    offset: (period() + offset).into(),
                }
            } else {
                Cursor {
                    ipps,
                    current_index: 0,
                    offset,
                }
            };
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
            Cursor {
                ipps,
                current_index: 0,
                offset: (period() + offset).into(),
            }
        } else {
            Cursor {
                ipps,
                current_index: i,
                offset,
            }
        }
    }
}

impl<'a> MergeCursor<'a> for Cursor<'a> {
    fn new(ipps: &'a [TTFPoint]) -> Cursor<'a> {
        Cursor {
            ipps,
            current_index: 0,
            offset: FlWeight::new(0.0),
        }
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

/// Cursor for partial TTFs.
/// Needing no wrapping here.
/// When we move beyond end, we just move in steps and keep the last value.
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
        let cur = TTFPoint {
            at: next.at - FlWeight::from(period()),
            val: next.val,
        };
        let mut cursor = PartialPlfMergeCursor {
            prev: TTFPoint::default(),
            cur,
            next,
            iter,
        };
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
            self.next.at = self.next.at + FlWeight::from(period());
        }
    }

    fn ipps(&self) -> &'a [TTFPoint] {
        self.iter.as_slice()
    }
}

/// Similar to PartialPlfMergeCursor but for linking.
/// Without lookahead, and we need to know when we're done.
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
        let prev = TTFPoint {
            at: cur.at - FlWeight::new(1.0),
            val: cur.val,
        };
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
