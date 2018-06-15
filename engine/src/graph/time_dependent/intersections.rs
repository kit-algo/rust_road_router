use super::*;

#[derive(Debug, Clone, Copy)]
pub enum IppSource {
    First(Weight),
    Second(Weight),
    Both(Weight, Weight)
}

pub fn intersections<I, F, G>(iter: I, eval_first: F, eval_second: G, period: Timestamp) -> Vec<(Timestamp, bool)> where
    I: Iterator<Item = (Timestamp, IppSource)>,
    F: FnOnce() -> Timestamp,
    G: FnOnce() -> Timestamp,
{
    let mut found_intersections: Vec<((Option<(Timestamp, Weight)>, (Timestamp, Weight)), (Option<(Timestamp, Weight)>, (Timestamp, Weight)))> = vec![];
    let mut missing: Vec<((Option<(Timestamp, Weight)>, Option<(Timestamp, Weight)>), (Option<(Timestamp, Weight)>, Option<(Timestamp, Weight)>))> = vec![];

    let mut first_first = None;
    let mut first_second: Option<(Timestamp, Weight)> = None;

    let mut prev_first = None;
    let mut prev_second = None;

    for (time, values) in iter {
        match values {
            IppSource::First(value) => {
                let ipp = (time, value);

                if missing.last().map(|&((_, next_first), _)| next_first.is_none()).unwrap_or(false) {
                    for ((prev_first, next_first), (prev_second, next_second)) in missing.drain(..) {
                        debug_assert!(next_first.is_none());
                        debug_assert!(next_second.is_some());
                        found_intersections.push(((prev_first, ipp), (prev_second, next_second.unwrap())));
                    }
                }
                missing.push(((prev_first, Some(ipp)), (prev_second, None)));

                prev_first = Some(ipp);
                if first_first.is_none() {
                    first_first = Some(ipp);
                }
            }
            IppSource::Second(value) => {
                let ipp = (time, value);

                if missing.last().map(|&(_, (_, next_second))| next_second.is_none()).unwrap_or(false) {
                    for ((prev_first, next_first), (prev_second, next_second)) in missing.drain(..) {
                        debug_assert!(next_first.is_some());
                        debug_assert!(next_second.is_none());
                        found_intersections.push(((prev_first, next_first.unwrap()), (prev_second, ipp)));
                    }
                }
                missing.push(((prev_first, None), (prev_second, Some(ipp))));

                prev_second = Some(ipp);
                if first_second.is_none() {
                    first_second = Some(ipp);
                }
            }
            IppSource::Both(first_value, second_value) => {
                let first_ipp = (time, first_value);
                let second_ipp = (time, second_value);

                if missing.last().map(|&((_, next_first), _)| next_first.is_none()).unwrap_or(false) {
                    for ((prev_first, next_first), (prev_second, next_second)) in missing.drain(..) {
                        debug_assert!(next_first.is_none());
                        debug_assert!(next_second.is_some());
                        found_intersections.push(((prev_first, first_ipp), (prev_second, next_second.unwrap())));
                    }
                }
                if missing.last().map(|&(_, (_, next_second))| next_second.is_none()).unwrap_or(false) {
                    for ((prev_first, next_first), (prev_second, next_second)) in missing.drain(..) {
                        debug_assert!(next_first.is_some());
                        debug_assert!(next_second.is_none());
                        found_intersections.push(((prev_first, next_first.unwrap()), (prev_second, second_ipp)));
                    }
                }
                found_intersections.push(((prev_first, first_ipp), (prev_second, second_ipp)));

                prev_first = Some(first_ipp);
                if first_first.is_none() {
                    first_first = Some(first_ipp);
                }
                prev_second = Some(second_ipp);
                if first_second.is_none() {
                    first_second = Some(second_ipp);
                }
            }
        }
    }

    if first_first.is_none() {
        first_first = Some((0, eval_first()));
        prev_first = first_first;
    }
    if first_second.is_none() {
        first_second = Some((0, eval_second()));
        prev_second = first_second;
    }

    for ((prev_first, next_first), (prev_second, next_second)) in missing {
        debug_assert!((next_first.is_none() && next_second.is_some()) || (next_first.is_some() && next_second.is_none()));
        if next_first.is_none() {
            let (first_first_at, first_first_value) = first_first.unwrap();
            found_intersections.push(((prev_first, (first_first_at + period, first_first_value)), (prev_second, next_second.unwrap())))
        } else if next_second.is_none() {
            let (first_second_at, first_second_value) = first_second.unwrap();
            found_intersections.push(((prev_first, next_first.unwrap()), (prev_second, (first_second_at + period, first_second_value))));
        }
    }

    for &mut ((ref mut first_first_ipp, (ref mut first_second_ipp_at, _)), (ref mut second_first_ipp, (ref mut second_second_ipp_at, _))) in &mut found_intersections {
        if first_first_ipp.is_some() && second_first_ipp.is_some() {
            break;
        } else {
            if first_first_ipp.is_none() {
                *first_first_ipp = prev_first; // this is the last
            } else {
                first_first_ipp.as_mut().unwrap().0 += period;
            }
            if second_first_ipp.is_none() {
                *second_first_ipp = prev_second; // this is the last
            } else {
                second_first_ipp.as_mut().unwrap().0 += period;
            }
            *first_second_ipp_at += period;
            *second_second_ipp_at += period;
        }
    }

    let found_intersections: Vec<(Timestamp, bool)> = found_intersections.into_iter().map(|((first_first_ipp, first_second_ipp), (second_first_ipp, second_second_ipp))| {
        ((first_first_ipp.unwrap(), first_second_ipp), (second_first_ipp.unwrap(), second_second_ipp))
    }).map(|(first_line, second_line)| {
        let dx1 = (first_line.1).0 - (first_line.0).0;
        let dx2 = (second_line.1).0 - (second_line.0).0;
        let dy1 = i64::from((first_line.1).1) - i64::from((first_line.0).1);
        let dy2 = i64::from((second_line.1).1) - i64::from((second_line.0).1);
        (intersect(first_line, second_line), dy1 * i64::from(dx2) <= dy2 * i64::from(dx1))
    }).filter(|(intersection, _)| {
        intersection.is_some()
    }).map(|(intersection, first_better)| {
        (intersection.unwrap() % period, first_better)
    }).collect();

    let mut final_intersections = Vec::with_capacity(found_intersections.len());
    let last = found_intersections.last().cloned();

    for (at, first_better) in found_intersections {
        let (prev_at, prev_first_better) = final_intersections.last().cloned().unwrap_or_else(|| last.unwrap());
        if prev_at != at {
            if prev_first_better != first_better {
                final_intersections.push((at, first_better))
            }
        } else if prev_first_better != first_better {
            final_intersections.pop();
        }
    }

    final_intersections
}

fn intersect(((x1, y1), (x2, y2)): ((Timestamp, Weight), (Timestamp, Weight)), ((x3, y3), (x4, y4)): ((Timestamp, Weight), (Timestamp, Weight))) -> Option<Timestamp> {
    debug_assert!(x2 > x1);
    debug_assert!(x4 > x3);
    let x1 = i128::from(x1);
    let x2 = i128::from(x2);
    let x3 = i128::from(x3);
    let x4 = i128::from(x4);
    let y1 = i128::from(y1);
    let y2 = i128::from(y2);
    let y3 = i128::from(y3);
    let y4 = i128::from(y4);

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

    if result < x1 || result < x3 || result > x2 || result > x4 || unrounded < x1 || unrounded < x3 {
        return None
    }

    Some(result as Timestamp)
}

#[cfg(test)]
mod tests {
    use super::*;
    use self::IppSource::*;

    #[test]
    fn test_intersect() {
        assert_eq!(intersect(((0,0), (5,0)), ((0,8), (8,8))), None);
        assert_eq!(intersect(((0,0), (5,0)), ((0,8), (8,7))), None);
        assert_eq!(intersect(((0,0), (2,2)), ((0,2), (2,0))), Some(1));
        assert_eq!(intersect(((0,0), (3,2)), ((0,2), (3,0))), Some(2));
        assert_eq!(intersect(((0,2), (3,0)), ((0,0), (3,2))), Some(2));
        assert_eq!(intersect(((7, 8), (9, 6)), ((6, 2), (16, 12))), None);
        assert_eq!(intersect(((9,6), (10,10)), ((6,2), (16,12))), None);
    }

    #[test]
    fn test_empty_intersections() {
        assert_eq!(intersections(vec![].into_iter(), || { 42 }, || { 23 }, 24), vec![]);
    }

    #[test]
    fn test_one_empty_intersections() {
        assert_eq!(intersections(vec![(0, First(3)), (12, First(7))].into_iter(), || { 5 }, || { 5 }, 24), vec![(18, true), (6, false)]);
    }

    #[test]
    fn test_one_empty_with_more_points_intersections() {
        assert_eq!(
            intersections(vec![(5, First(3)), (7, First(7)), (17, First(7)), (19, First(3))].into_iter(), || { 5 }, || { 5 }, 24),
            vec![(6, false), (18, true)]);
    }

    #[test]
    fn test_intersections_with_two_functions() {
        assert_eq!(
            intersections(vec![(6, Both(3, 7)), (18, Both(7,3))].into_iter(), || { 5 }, || { 5 }, 24),
            vec![(0, true), (12, false)]);
    }

    #[test]
    fn test_intersections_with_two_complex_functions() {
        assert_eq!(
            intersections(vec![(0, First(3)), (5, First(8)), (6, Both(7, 2)), (7, First(8)), (9, First(6)), (10, First(10)), (16, Second(12))].into_iter(), || { 5 }, || { 5 }, 24),
            vec![(2, false), (13, true)]);
    }

    #[test]
    fn test_intersections_with_two_complex_functions_2() {
        assert_eq!(
            intersections(vec![(0, First(3)), (5, First(8)), (6, Both(7, 2)), (7, First(8)), (9, First(4)), (10, First(10)), (16, Second(12))].into_iter(), || { 5 }, || { 5 }, 24),
            vec![(2, false), (9, true), (10, false), (13, true)]);
    }

    #[test]
    fn test_overlapping_segments() {
        assert_eq!(
            intersections(vec![(0, Both(1, 2)), (3, First(1)), (4, Both(4, 2)), (5, Second(4)), (6, Second(4)), (7, Both(4, 5)), (8, Both(1, 2))].into_iter(), || { 1 }, || { 2 }, 24),
            vec![(4, false), (5, true)]);

        assert_eq!(
            intersections(vec![(0, Both(2, 1)), (3, Second(1)), (4, Both(2, 4)), (5, First(4)), (6, First(4)), (7, Both(5, 4)), (8, Both(2, 1))].into_iter(), || { 1 }, || { 2 }, 24),
            vec![(4, true), (5, false)]);

        assert_eq!(
            intersections(vec![(0, Both(1, 2)), (3, Second(2)), (4, Both(1, 4)), (5, First(4)), (6, First(4)), (7, Both(1, 4)), (8, Both(1, 2))].into_iter(), || { 1 }, || { 2 }, 24),
            vec![(5, false), (6, true)]); // TODO kill unneccessary

        assert_eq!(
            intersections(vec![(0, Both(2, 1)), (3, First(2)), (4, Both(4, 1)), (5, Second(4)), (6, Second(4)), (7, Both(4, 1)), (8, Both(2, 1))].into_iter(), || { 1 }, || { 2 }, 24),
            vec![(5, true), (6, false)]); // TODO kill unneccessary
    }

    #[test]
    fn test_intersection_on_ipps() {
        assert_eq!(
            intersections(vec![(0, First(1)), (2, First(2)), (3, First(1)), (5, First(3)), (7, First(1))].into_iter(), || { 1 }, || { 2 }, 24),
            vec![(4, false), (6, true)]);

        assert_eq!(
            intersections(vec![(0, Second(1)), (2, Second(2)), (3, Second(1))].into_iter(), || { 2 }, || { 1 }, 24),
            vec![]);

        assert_eq!(
            intersections(vec![(0, First(1)), (2, First(2)), (3, First(4)), (4, First(2)), (5, First(1))].into_iter(), || { 1 }, || { 2 }, 24),
            vec![(2, false), (4, true)]);

        assert_eq!(
            intersections(vec![(0, Second(1)), (2, Second(2)), (3, Second(4)), (4, Second(2)), (5, Second(1))].into_iter(), || { 2 }, || { 1 }, 24),
            vec![(2, true), (4, false)]);
    }
}
