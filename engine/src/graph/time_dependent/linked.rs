use super::*;
use ::math::*;

#[derive(Debug, Clone, Copy)]
pub struct Linked {
    first: EdgeId,
    second: EdgeId
}

impl Linked {
    pub fn new(first: EdgeId, second: EdgeId) -> Linked {
        Linked { first, second }
    }

    pub fn evaluate(self, departure: Timestamp, shortcut_graph: &ShortcutGraph) -> Weight {
        debug_assert!(departure < period());
        let first_edge = shortcut_graph.get_downward(self.first);
        let second_edge = shortcut_graph.get_upward(self.second);
        let first_edge_value = first_edge.evaluate(departure, shortcut_graph);
        first_edge_value + second_edge.evaluate((departure + first_edge_value) % period(), shortcut_graph)
    }

    pub fn bounds(self, shortcut_graph: &ShortcutGraph) -> (Weight, Weight) {
        let (first_min, first_max) = shortcut_graph.get_downward(self.first).bounds(shortcut_graph);
        let (second_min, second_max) = shortcut_graph.get_upward(self.second).bounds(shortcut_graph);
        // INFINITY????
        (first_min + second_min, first_max + second_max)
    }

    pub(super) fn non_wrapping_seg_iter<'a>(self, range: Range<Timestamp>, shortcut_graph: &'a ShortcutGraph) -> impl Iterator<Item = MATSeg> + 'a {
        let first_edge = shortcut_graph.get_downward(self.first);
        let second_edge = shortcut_graph.get_upward(self.second);

        let progress = range.start;
        let mut first_iter = first_edge.non_wrapping_seg_iter(range, shortcut_graph).peekable();
        let start_of_second = first_iter.peek().map(|seg| seg.start_of_valid_at_val()).unwrap_or(0);

        let (first_range, mut second_range) = (start_of_second..(start_of_second+period()+1)).split(period());
        second_range.start -= period();
        second_range.end -= period();
        let second_iter = second_edge.non_wrapping_seg_iter(first_range, shortcut_graph).chain(second_edge.non_wrapping_seg_iter(second_range, shortcut_graph)).peekable();

        SegmentIter { first_iter, second_iter, progress }
    }

    pub fn as_shortcut_data(self) -> ShortcutData {
        ShortcutData::new(ShortcutSource::Shortcut(self.first, self.second))
    }

    pub fn is_valid_path(self, shortcut_graph: &ShortcutGraph) -> bool {
        shortcut_graph.get_downward(self.first).is_valid_path() && shortcut_graph.get_upward(self.second).is_valid_path()
    }

    pub fn debug_to_s(self, shortcut_graph: &ShortcutGraph, indent: usize) -> String {
        let first_edge = shortcut_graph.get_downward(self.first);
        let second_edge = shortcut_graph.get_upward(self.second);
        format!("Linked:\n{}first: {}\n{}second: {}", String::from(" ").repeat(indent * 2), first_edge.debug_to_s(shortcut_graph, indent + 1), String::from(" ").repeat(indent * 2), second_edge.debug_to_s(shortcut_graph, indent + 1))
    }
}

struct SegmentIter<Shortcut1SegIter: Iterator<Item = MATSeg>, Shortcut2SegIter: Iterator<Item = MATSeg>> {
    first_iter: Peekable<Shortcut1SegIter>,
    second_iter: Peekable<Shortcut2SegIter>,
    progress: Timestamp
}

impl<'a, Shortcut1SegIter: Iterator<Item = MATSeg>, Shortcut2SegIter: Iterator<Item = MATSeg>> Iterator for SegmentIter<Shortcut1SegIter, Shortcut2SegIter> {
    type Item = MATSeg;

    fn next(&mut self) -> Option<Self::Item> {
        let first_iter = &mut self.first_iter;
        let second_iter = &mut self.second_iter;
        let progress = self.progress;

        let linked = first_iter.peek().map(|first_segment| {
            match link_segments(first_segment, second_iter.peek().unwrap(), progress) {
                Some(linked) => linked,
                None => {
                    second_iter.next();
                    link_segments(first_segment, second_iter.peek().unwrap(), progress).unwrap()
                }
            }
        });

        if let Some(linked) = &linked {
            self.progress = linked.valid.end;

            if linked.valid.end == first_iter.peek().unwrap().valid.end {
                first_iter.next();
            } else {
                second_iter.next();
            }

            if linked.valid.start == linked.valid.end {
                return self.next()
            }
        }

        linked
    }
}

fn link_segments(first_segment: &MATSeg, second_segment: &MATSeg, progress: Timestamp) -> Option<MATSeg> {
    let mut second_segment = second_segment.clone();
    let first_value_range = first_segment.valid_value_range(progress); // not necessarily exclusive, non wrapping

    if (first_value_range.end > period() || first_value_range.start > period()) && second_segment.valid.end < first_value_range.start {
        second_segment.shift();
    }

    if first_value_range.start > second_segment.valid.end || (first_value_range.start == first_value_range.end && first_value_range.start == second_segment.valid.end) {
        println!("Skipping second:\n{:?}\n{:?}\n{:?}, {}", first_segment, second_segment, first_value_range, progress);
        return None
    }

    debug_assert!(!first_value_range.is_intersection_empty(&second_segment.valid) ||
        (first_value_range.start == first_value_range.end && second_segment.valid.contains(&first_value_range.start)) ||
        (first_value_range.start == second_segment.valid.end && first_value_range.end != second_segment.valid.end),
        "Linking non fitting segments:\n{:?}\n{:?}\n{:?}, {}", first_segment, second_segment, first_value_range, progress);

    let end_of_range = if let Some(end_of_second) = first_segment.line.invert(second_segment.valid.end) { // TODO all
        if first_segment.valid.contains(&end_of_second) && first_value_range.end > second_segment.valid.end {
            end_of_second
        } else {
            first_segment.valid.end
        }
    } else {
        first_segment.valid.end
    };

    let line = link_lines(&first_segment.line, &second_segment.line).unwrap_or_else(|| {
        // println!("approximating");
        let first_val = second_segment.line.interpolate_at(first_segment.line.interpolate_at(progress));
        let second_val = second_segment.line.interpolate_at(first_segment.line.interpolate_at(end_of_range));
        MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(progress, first_val), ATIpp::new(end_of_range, second_val)))
    });

    Some(MATSeg::new(line, progress..end_of_range))
}


fn link_lines(first_line: &MonotoneLine<ATIpp>, second_line: &MonotoneLine<ATIpp>) -> Option<MonotoneLine<ATIpp>> {
    let linked_delta_x = i128::from(first_line.delta_x()) * i128::from(second_line.delta_x());
    let linked_delta_y = i128::from(first_line.delta_y()) * i128::from(second_line.delta_y());

    let rest_first_term = i128::from(first_line.line().from.at) * linked_delta_y;
    let rest_second_term = i128::from(second_line.line().from.at) * i128::from(second_line.delta_y()) * i128::from(first_line.delta_x());
    let rest_third_term = i128::from(first_line.line().from.val) * i128::from(second_line.delta_y()) * i128::from(first_line.delta_x());
    let rest_fourth_term = i128::from(second_line.line().from.val) * linked_delta_x;
    let rest = rest_first_term + rest_second_term - rest_third_term - rest_fourth_term;
    let result = solve_linear_congruence(linked_delta_y, rest, linked_delta_x)?;

    let min_x = if linked_delta_y == 0 { 0 } else { (rest + linked_delta_y - 1) / linked_delta_y };
    let first_at = max(result.solution, result.solution + (min_x - result.solution + i128::from(result.modulus) - 1) / i128::from(result.modulus) * i128::from(result.modulus));

    let first_val = (linked_delta_y * first_at - rest) / linked_delta_x;
    debug_assert_eq!((linked_delta_y * first_at - rest) % linked_delta_x, 0);
    let second_at = first_at + i128::from(result.modulus);
    let second_val = (linked_delta_y * second_at - rest) / linked_delta_x;
    debug_assert_eq!((linked_delta_y * second_at - rest) % linked_delta_x, 0);

    debug_assert!(first_at >= 0, "linked: from at {} val {} to at {} val {}", first_at, first_val, second_at, second_val);
    // disabled as this may be false outside the valid range
    // debug_assert!(first_val >= first_at, "linked: from at {} val {} to at {} val {}", first_at, first_val, second_at, second_val);
    debug_assert!(second_at > first_at, "linked: from at {} val {} to at {} val {}", first_at, first_val, second_at, second_val);
    // disabled as this may be false outside the valid range
    // debug_assert!(first_at + i128::from(period()) > second_at, "linked: from at {} val {} to at {} val {}", first_at, first_val, second_at, second_val);
    if first_at >= i128::from(INFINITY) { return None }
    if first_val >= i128::from(INFINITY) { return None }
    if second_at >= i128::from(INFINITY) { return None }
    if second_val >= i128::from(INFINITY) { return None }
    debug_assert!(first_at < i128::from(INFINITY), "linked: from at {} val {} to at {} val {}", first_at, first_val, second_at, second_val);
    debug_assert!(first_val < i128::from(INFINITY), "linked: from at {} val {} to at {} val {}", first_at, first_val, second_at, second_val);
    debug_assert!(second_at < i128::from(INFINITY), "linked: from at {} val {} to at {} val {}", first_at, first_val, second_at, second_val);
    debug_assert!(second_val < i128::from(INFINITY), "linked: from at {} val {} to at {} val {}", first_at, first_val, second_at, second_val);

    Some(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(first_at as Timestamp, first_val as Timestamp), ATIpp::new(second_at as Timestamp, second_val as Timestamp))))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linking_static_segments() {
        run_test_with_periodicity(10, || {
            assert_eq!(link_segments(&MATSeg::from_point_tuples((1, 3), (4, 7)), &MATSeg::from_point_tuples((3, 5), (7, 9)), 1),
                Some(MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(1, 5), ATIpp::new(4, 9))), 1..4)));
            assert_eq!(link_segments(&MATSeg::from_point_tuples((0, 1), (10, 11)), &MATSeg::from_point_tuples((1, 3), (10, 12)), 0),
                Some(MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(0, 3), ATIpp::new(1, 4))), 0..9)));
            assert_eq!(link_segments(&MATSeg::from_point_tuples((0, 1), (10, 11)), &MATSeg::from_point_tuples((0, 2), (1, 3)), 9),
                Some(MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(0, 3), ATIpp::new(1, 4))), 9..10)));
            assert_eq!(link_segments(&MATSeg::from_point_tuples((9, 10), (10, 11)), &MATSeg::from_point_tuples((0, 2), (1, 3)), 9),
                Some(MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(0, 3), ATIpp::new(1, 4))), 9..10)));
        });
    }

    #[test]
    fn test_linking_one_static_one_var_segments() {
        run_test_with_periodicity(10, || {
            assert_eq!(link_segments(&MATSeg::from_point_tuples((0, 2), (5, 7)), &MATSeg::from_point_tuples((2, 4), (7, 10)), 0),
                Some(MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(0, 4), ATIpp::new(5, 10))), 0..5)));
            assert_eq!(link_segments(&MATSeg::from_point_tuples((0, 2), (5, 8)), &MATSeg::from_point_tuples((2, 4), (8, 10)), 0),
                Some(MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(0, 4), ATIpp::new(5, 10))), 0..5)));
        });
    }

    #[test]
    fn test_linking_non_fitting_length_segments() {
        run_test_with_periodicity(10, || {
            for s in 0..10 {
                let linked = link_segments(
                    &MATSeg::from_point_tuples((s, s+2), (s+5, s+5+2)),
                    &MATSeg::from_point_tuples((s+2, s+2+2), (s+6, s+6+3)), s).unwrap();
                let expect = MATSeg::from_point_tuples((s, s+4), (s+4, s+4+5));
                assert!(linked.is_equivalent_to(&expect), "Not equivalent!\nGot:      {:?}\nExpected: {:?}", linked, expect);

                let linked = link_segments(
                    &MATSeg::from_point_tuples((s, s+2), (s+4, s+4+2)),
                    &MATSeg::from_point_tuples((s+3, s+3+2), (s+5, s+5+3)), s+1).unwrap();
                let expect = MATSeg::from_point_tuples((s+1, s+1+4), (s+3, s+3+5));
                assert!(linked.is_equivalent_to(&expect), "Not equivalent!\nGot:      {:?}\nExpected: {:?}", linked, expect);
            }
        });
    }

    #[test]
    fn test_linking_different_slopes() {
        run_test_with_periodicity(10, || {
            assert_eq!(link_segments(
                    &MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(0, 2), ATIpp::new(4, 8))), 1..3),
                    &MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(2, 4), ATIpp::new(8, 9))), 3..7), 1),
                Some(MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(0, 4), ATIpp::new(4, 9))), 1..3)));
        });
    }

    #[test]
    fn test_linking_segnents_with_empty_borderline_overlap() {
        run_test_with_periodicity(10, || {
            assert_eq!(link_segments(
                    &MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(0, 2), ATIpp::new(1, 3))), 4..6),
                    &MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(0, 1), ATIpp::new(1, 2))), 4..6), 4),
                Some(MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(0, 3), ATIpp::new(1, 4))), 4..4)));
        });
    }

    #[test]
    fn test_bounds() {
        run_test_with_periodicity(10, || {
            let graph = TDGraph::new(
                vec![0, 1, 2, 2],
                vec![2, 0],
                vec![0, 4, 9],
                vec![0, 5, 8, 10,  0, 1, 3, 9, 10],
                vec![1, 2, 1, 1,   2, 2, 5, 3, 2],
            );

            let cch_first_out = vec![0, 1, 3, 3];
            let cch_head =      vec![2, 0, 2];

            let outgoing = vec![Shortcut::new(Some(0)), Shortcut::new(None), Shortcut::new(None)];
            let incoming = vec![Shortcut::new(None), Shortcut::new(Some(1)), Shortcut::new(None)];

            let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);
            let linked = Linked::new(1, 0);

            assert_eq!(linked.bounds(&shortcut_graph), (3, 7));
        });
    }

    #[test]
    fn test_eval_on_ipp() {
        run_test_with_periodicity(10, || {
            let graph = TDGraph::new(
                vec![0, 1, 2, 2],
                vec![2, 0],
                vec![0, 5, 9],
                vec![0, 1, 3, 9, 10,  0, 5, 8, 10],
                vec![2, 2, 5, 3, 2,   1, 2, 1, 1],
            );

            let cch_first_out = vec![0, 1, 3, 3];
            let cch_head =      vec![2, 0, 2];

            let outgoing = vec![Shortcut::new(Some(0)), Shortcut::new(None), Shortcut::new(None)];
            let incoming = vec![Shortcut::new(None), Shortcut::new(Some(1)), Shortcut::new(None)];

            let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);
            let linked = Linked::new(1, 0);

            assert_eq!(linked.evaluate(0, &shortcut_graph), 3);
        });
    }

    #[test]
    fn test_interpolating_eval() {
        run_test_with_periodicity(10, || {
            let graph = TDGraph::new(
                vec![0, 1, 2, 2],
                vec![2, 0],
                vec![0, 5, 9],
                vec![0, 1, 3, 9, 10,  0, 5, 8, 10],
                vec![2, 2, 5, 3, 2,   1, 2, 1, 1],
            );

            let cch_first_out = vec![0, 1, 3, 3];
            let cch_head =      vec![2, 0, 2];

            let outgoing = vec![Shortcut::new(Some(0)), Shortcut::new(None), Shortcut::new(None)];
            let incoming = vec![Shortcut::new(None), Shortcut::new(Some(1)), Shortcut::new(None)];

            let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);
            let linked = Linked::new(1, 0);

            assert_eq!(linked.evaluate(1, &shortcut_graph), 4);
        });
    }

    #[test]
    fn test_segment_iter() {
        run_test_with_periodicity(8, || {
            let graph = TDGraph::new(
                vec![0, 1,    3, 3],
                vec![2, 0, 2],
                vec![0,   2,       6,      10],
                vec![0,8, 0,2,6,8, 0,2,6,8],
                vec![1,1, 3,1,5,3, 4,6,2,4],
            );

            let cch_first_out = vec![0, 1, 3, 3];
            let cch_head =      vec![2, 0, 2];

            let outgoing = vec![Shortcut::new(Some(0)), Shortcut::new(None), Shortcut::new(Some(2))];
            let incoming = vec![Shortcut::new(None), Shortcut::new(Some(1)), Shortcut::new(None)];

            let shortcut_graph = ShortcutGraph::new(&graph, &cch_first_out, &cch_head, outgoing, incoming);

            let all_ipps: Vec<_> = Linked::new(1, 0).non_wrapping_seg_iter(0..8, &shortcut_graph).collect();
            assert_eq!(all_ipps.len(), 4);
            assert!(all_ipps[0].is_equivalent_to(&MATSeg::from_point_tuples((0,4),(2,4))));
            assert!(all_ipps[1].is_equivalent_to(&MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(2,4), ATIpp::new(6,12))), 2..5)));
            assert!(all_ipps[2].is_equivalent_to(&MATSeg::new(MonotoneLine::<ATIpp>::new(Line::new(ATIpp::new(2,4), ATIpp::new(6,12))), 5..6)));
            assert!(all_ipps[3].is_equivalent_to(&MATSeg::from_point_tuples((6,12), (8,12))));
        });
    }
}
