use super::*;
use math::RangeExtensions;

#[derive(Debug)]
enum TwoTypeIter<T, X: Iterator<Item = T>, Y: Iterator<Item = T>> {
    First(X),
    Second(Y),
}

impl<T, X: Iterator<Item = T>, Y: Iterator<Item = T>> Iterator for TwoTypeIter<T, X, Y> {
    type Item = T;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            TwoTypeIter::First(iterator) => iterator.next(),
            TwoTypeIter::Second(iterator) => iterator.next(),
        }
    }
}

#[derive(Debug)]
pub(super) enum BetterSegment {
    Shortcut(MATSeg),
    Linked(MATSeg),
    Equal(MATSeg, MATSeg)
}

pub(super) fn merge(iter: impl Iterator<Item = (MATSeg, MATSeg)>) -> impl Iterator<Item = BetterSegment> {
    use std::cmp::Ordering;

    iter.flat_map(|(mut shortcut_seg, mut linked_seg)| {
        let common_valid = shortcut_seg.valid.intersection(&linked_seg.valid);
        shortcut_seg.valid = common_valid.clone();
        linked_seg.valid = common_valid;

        if let Some(intersection) = shortcut_seg.intersect(&linked_seg) {
            if intersection != shortcut_seg.valid.start && intersection != shortcut_seg.valid.end
                && intersection != linked_seg.valid.start && intersection != linked_seg.valid.end {

                let mut cloned_shortcut_seg = shortcut_seg.clone();
                let mut cloned_linked_seg = linked_seg.clone();
                shortcut_seg.valid.end = intersection;
                linked_seg.valid.end = intersection;
                cloned_shortcut_seg.valid.start = intersection;
                cloned_linked_seg.valid.start = intersection;

                return TwoTypeIter::First(once((shortcut_seg, linked_seg)).chain(once((cloned_shortcut_seg, cloned_linked_seg))))
            }
        }
        TwoTypeIter::Second(once((shortcut_seg, linked_seg)))
    }).map(|(shortcut_seg, linked_seg)| {
        debug_assert_eq!(shortcut_seg.valid, linked_seg.valid);
        let &Range { start, end } = &shortcut_seg.valid;
        match shortcut_seg.eval(start).cmp(&linked_seg.eval(start)) {
            Ordering::Less => BetterSegment::Shortcut(shortcut_seg),
            Ordering::Greater => BetterSegment::Linked(linked_seg),
            Ordering::Equal => {
                match shortcut_seg.eval(end).cmp(&linked_seg.eval(end)) {
                    Ordering::Less => BetterSegment::Shortcut(shortcut_seg),
                    Ordering::Greater => BetterSegment::Linked(linked_seg),
                    Ordering::Equal => {
                        BetterSegment::Equal(shortcut_seg, linked_seg)
                    }
                }
            }
        }
    })
}


#[derive(Debug)]
pub(super) struct SegmentAggregator<'a> {
    pub shortcut_path_segments: Peekable<PathSegmentIter<'a>>,
    pub merged_path_segments: Vec<(Timestamp, ShortcutData)>,
    pub merged_atf_segments: Vec<MATSeg>,
    pub linked_shortcut_data: ShortcutData
}

impl<'a> SegmentAggregator<'a> {
    pub fn integrate_segment(&mut self, segment: BetterSegment) {
        match self.merged_atf_segments.last_mut() {
            Some(prev_segment) => {
                match segment {
                    BetterSegment::Shortcut(segment) => {
                        if self.merged_path_segments.last().unwrap().1 == self.linked_shortcut_data {
                            let mut prev = if self.merged_path_segments.len() > 1 {
                                Some(self.merged_path_segments[self.merged_path_segments.len() - 2].1)
                            } else {
                                None
                            };
                            while self.shortcut_path_segments.peek().unwrap().0 < segment.valid.start {
                                prev = Some(*self.shortcut_path_segments.peek().unwrap().1);
                                self.shortcut_path_segments.next();
                            }

                            self.merged_path_segments.push((segment.valid.start, prev.unwrap()));
                        }

                        while let Some(next) = self.shortcut_path_segments.peek() {
                            if next.0 < segment.valid.end {
                                self.merged_path_segments.push((next.0, *next.1));
                                self.shortcut_path_segments.next();
                            } else {
                                break;
                            }
                        }

                        if !prev_segment.combine(&segment) {
                            self.merged_atf_segments.push(segment);
                        }
                    },
                    BetterSegment::Linked(segment) => {
                        if self.merged_path_segments.last().unwrap().1 != self.linked_shortcut_data {
                            self.merged_path_segments.push((segment.valid.start, self.linked_shortcut_data));
                        }
                        if !prev_segment.combine(&segment) {
                            self.merged_atf_segments.push(segment);
                        }
                    },
                    BetterSegment::Equal(shortcut_seg, linked_seg) => {
                        if self.merged_path_segments.last().unwrap().1 == self.linked_shortcut_data {
                            if !prev_segment.combine(&linked_seg) {
                                self.merged_atf_segments.push(linked_seg);
                            }
                        } else {
                            if self.merged_path_segments.last().unwrap().1 == self.linked_shortcut_data {
                                let mut prev = if self.merged_path_segments.len() > 1 {
                                    Some(self.merged_path_segments[self.merged_path_segments.len() - 2].1)
                                } else {
                                    None
                                };
                                while self.shortcut_path_segments.peek().unwrap().0 < shortcut_seg.valid.start {
                                    prev = Some(*self.shortcut_path_segments.peek().unwrap().1);
                                    self.shortcut_path_segments.next();
                                }

                                self.merged_path_segments.push((shortcut_seg.valid.start, prev.unwrap()));
                            }

                            while let Some(next) = self.shortcut_path_segments.peek() {
                                if next.0 < shortcut_seg.valid.end {
                                    self.merged_path_segments.push((next.0, *next.1));
                                    self.shortcut_path_segments.next();
                                } else {
                                    break;
                                }
                            }

                            if !prev_segment.combine(&shortcut_seg) {
                                self.merged_atf_segments.push(shortcut_seg);
                            }
                        }
                    },
                }
            },
            None => {
                match segment {
                    BetterSegment::Shortcut(segment) => {
                        self.merged_atf_segments.push(segment);
                        self.merged_path_segments.push(self.shortcut_path_segments.next().map(|(at, data)| (at, *data)).unwrap());
                    },
                    BetterSegment::Linked(segment) => {
                        self.merged_atf_segments.push(segment);
                        self.merged_path_segments.push((0, self.linked_shortcut_data));
                    },
                    BetterSegment::Equal(shortcut_seg, _linked_seg) => {
                        self.merged_atf_segments.push(shortcut_seg);
                        self.merged_path_segments.push(self.shortcut_path_segments.next().map(|(at, data)| (at, *data)).unwrap());
                    },
                }
            },
        }
    }
}

pub(super) struct CooccuringSegIter<SegmentIter: Iterator<Item = MATSeg>, LinkedSegIter: Iterator<Item = MATSeg>> {
    pub shortcut_iter: Peekable<SegmentIter>,
    pub linked_iter: Peekable<LinkedSegIter>
}

impl<'a, SegmentIter: Iterator<Item = MATSeg>, LinkedSegIter: Iterator<Item = MATSeg>> Iterator for CooccuringSegIter<SegmentIter, LinkedSegIter> {
    type Item = (MATSeg, MATSeg);

    fn next(&mut self) -> Option<Self::Item> {
        match (self.shortcut_iter.peek().cloned(), self.linked_iter.peek().cloned()) {
            (Some(shortcut_next_seg), Some(linked_next_seg)) => {
                if shortcut_next_seg.valid.end == linked_next_seg.valid.end {
                    self.shortcut_iter.next();
                    self.linked_iter.next();
                } else if shortcut_next_seg.valid.end <= linked_next_seg.valid.end {
                    self.shortcut_iter.next();
                } else {
                    self.linked_iter.next();
                }
                Some((shortcut_next_seg, linked_next_seg))
            },
            (None, None) => None,
            _ => panic!("broken valid ranges in parallel iteration")
        }
    }
}
