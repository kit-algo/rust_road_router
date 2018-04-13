use super::*;

#[derive(Debug)]
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
        println!("");
        println!("{} {:?}", time, values);
        match values {
            IppSource::First(value) => {
                let ipp = (time, value);

                if let Some(prev_first) = prev_first {
                    if let Some(prev_second_check) = prev_second {
                        if missing.last().map(|&((_, next_first), _)| next_first.is_none()).unwrap_or(false) {
                            for ((prev_first, next_first), (prev_second, next_second)) in missing.drain(..) {
                                debug_assert!(prev_first.is_some());
                                debug_assert!(next_first.is_none());
                                debug_assert!(prev_second.is_some());
                                debug_assert_eq!(prev_second_check, prev_second.unwrap());
                                debug_assert!(next_second.is_some());
                                found_intersections.push(((prev_first, ipp), (prev_second, next_second.unwrap())));
                            }
                        }
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

                if let Some(prev_second) = prev_second {
                    if let Some(prev_first_check) = prev_first {
                        if missing.last().map(|&(_, (_, next_second))| next_second.is_none()).unwrap_or(false) {
                            for ((prev_first, next_first), (prev_second, next_second)) in missing.drain(..) {
                                debug_assert!(prev_first.is_some());
                                debug_assert_eq!(prev_first_check, prev_first.unwrap());
                                debug_assert!(next_first.is_some());
                                debug_assert!(prev_second.is_some());
                                debug_assert!(next_second.is_none());
                                found_intersections.push(((prev_first, next_first.unwrap()), (prev_second, ipp)));
                            }
                        }
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

                if let Some(prev_first_check) = prev_first {
                    if let Some(prev_second_check) = prev_second {
                        if missing.last().map(|&((_, next_first), _)| next_first.is_none()).unwrap_or(false) {
                            for ((prev_first, next_first), (prev_second, next_second)) in missing.drain(..) {
                                debug_assert!(prev_first.is_some());
                                debug_assert!(next_first.is_none());
                                debug_assert!(prev_second.is_some());
                                debug_assert_eq!(prev_second_check, prev_second.unwrap());
                                debug_assert!(next_second.is_some());
                                found_intersections.push(((prev_first, first_ipp), (prev_second, next_second.unwrap())));
                            }
                        }
                        if missing.last().map(|&(_, (_, next_second))| next_second.is_none()).unwrap_or(false) {
                            for ((prev_first, next_first), (prev_second, next_second)) in missing.drain(..) {
                                debug_assert!(prev_first.is_some());
                                debug_assert_eq!(prev_first_check, prev_first.unwrap());
                                debug_assert!(next_first.is_some());
                                debug_assert!(prev_second.is_some());
                                debug_assert!(next_second.is_none());
                                found_intersections.push(((prev_first, next_first.unwrap()), (prev_second, second_ipp)));
                            }
                        }
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

        println!("missing: {:?}", missing);
        println!("found: {:?}", found_intersections);
    }

    if first_first.is_none() {
        first_first = Some((0, eval_first()));
        prev_first = first_first;
    }
    if first_second.is_none() {
        first_second = Some((0, eval_second()));
        prev_second = first_second;
    }

    for ((prev_first, next_first), (prev_second, next_second)) in missing.into_iter() {
        debug_assert!((next_first.is_none() && next_second.is_some()) || (next_first.is_some() && next_second.is_none()));
        if next_first.is_none() {
            let (first_first_at, first_first_value) = first_first.unwrap();
            found_intersections.push(((prev_first, (first_first_at + period, first_first_value)), (prev_second, next_second.unwrap())))
        } else if next_second.is_none() {
            let (first_second_at, first_second_value) = first_second.unwrap();
            found_intersections.push(((prev_first, next_first.unwrap()), (prev_second, (first_second_at + period, first_second_value))));
        }
    }

    for &mut ((ref mut first_first_ipp, (ref mut first_second_ipp_at, _)), (ref mut second_first_ipp, (ref mut second_second_ipp_at, _))) in found_intersections.iter_mut() {
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

    found_intersections.into_iter().map(|((first_first_ipp, first_second_ipp), (second_first_ipp, second_second_ipp))| {
        ((first_first_ipp.unwrap(), first_second_ipp), (second_first_ipp.unwrap(), second_second_ipp))
    }).map(|(first_line, second_line)| {
        (intersect(first_line, second_line), (first_line.1).1 <= (second_line.1).1)
    }).filter(|(intersection, _)| {
        intersection.is_some()
    }).map(|(intersection, first_better)| {
        (intersection.unwrap() % period, first_better)
    }).collect()
}

fn intersect(((x1, y1), (x2, y2)): ((Timestamp, Weight), (Timestamp, Weight)), ((x3, y3), (x4, y4)): ((Timestamp, Weight), (Timestamp, Weight))) -> Option<Timestamp> {
    debug_assert!(x2 > x1);
    debug_assert!(x4 > x3);
    let x1 = x1 as i64;
    let x2 = x2 as i64;
    let x3 = x3 as i64;
    let x4 = x4 as i64;
    let y1 = y1 as i64;
    let y2 = y2 as i64;
    let y3 = y3 as i64;
    let y4 = y4 as i64;

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

    if result < x1 || result < x3 || result > x2 || result > x4 {
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
            vec![(3, false), (9, true), (10, false), (10, true)]); // or so
    }
}
