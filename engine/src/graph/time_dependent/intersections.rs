use super::*;

fn intersect_segments(first_segment: &MATSeg, second_segment: &MATSeg) -> Option<Timestamp> {
    let x1 = i128::from(first_segment.line().from.at);
    let x2 = i128::from(first_segment.line().to.at);
    let x3 = i128::from(second_segment.line().from.at);
    let x4 = i128::from(second_segment.line().to.at);
    let y1 = i128::from(first_segment.line().from.val);
    let y2 = i128::from(first_segment.line().to.val);
    let y3 = i128::from(second_segment.line().from.val);
    let y4 = i128::from(second_segment.line().to.val);

    let numerator = (x1 * y2 - y1 * x2) * (x3 - x4) - (x1 - x2) * (x3 * y4 - y3 * x4);
    let denominator = (x1 - x2) * (y3 - y4) - (y1 - y2) * (x3 - x4);

    if denominator == 0 {
        return None;
    }

    let result = if numerator >= 0 {
        (numerator + denominator - 1) / denominator
    } else {
        (numerator + denominator + 1) / denominator
    };
    let unrounded = numerator / denominator;

    if result >= i128::from(INFINITY) {
        return None
    }

    let result = result as Timestamp;
    let unrounded = unrounded as Timestamp;

    if !first_segment.valid.contains(&result) || !second_segment.valid.contains(&result) || unrounded < first_segment.valid.start || unrounded < second_segment.valid.start {
        return None
    }

    Some(result as Timestamp)
}

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

use math::RangeExtensions;

pub(super) fn merge(iter: impl Iterator<Item = (MATSeg, MATSeg)>) -> impl Iterator<Item = BetterSegment> {
    use std::cmp::Ordering;

    iter.flat_map(|(mut shortcut_seg, mut linked_seg)| {
        let common_valid = shortcut_seg.valid.intersection(&linked_seg.valid);
        shortcut_seg.valid = common_valid.clone();
        linked_seg.valid = common_valid;

        if let Some(intersection) = intersect_segments(&shortcut_seg, &linked_seg) {
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

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_intersect() {
        assert_eq!(intersect_segments(&MATSeg::from_point_tuples((0,0), (5,5)),     &MATSeg::from_point_tuples((0,8), (8,16))), None);
        assert_eq!(intersect_segments(&MATSeg::from_point_tuples((0,0), (5,5)),     &MATSeg::from_point_tuples((0,8), (8,15))), None);
        assert_eq!(intersect_segments(&MATSeg::from_point_tuples((0,0), (2,4)),     &MATSeg::from_point_tuples((0,2), (2,2))), Some(1));
        assert_eq!(intersect_segments(&MATSeg::from_point_tuples((0,0), (3,5)),     &MATSeg::from_point_tuples((0,2), (3,3))), Some(2));
        assert_eq!(intersect_segments(&MATSeg::from_point_tuples((0,2), (3,3)),     &MATSeg::from_point_tuples((0,0), (3,5))), Some(2));
        assert_eq!(intersect_segments(&MATSeg::from_point_tuples((7,7+8), (9,9+6)), &MATSeg::from_point_tuples((6,8), (16,16+12))), None);
        assert_eq!(intersect_segments(&MATSeg::from_point_tuples((9,9+6), (10,20)), &MATSeg::from_point_tuples((6,8), (16,16+12))), None);
    }
}
