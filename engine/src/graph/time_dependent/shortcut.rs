use super::*;
use rank_select_map::BitVec;

#[derive(Debug, Clone)]
struct Bounds {
    lower: Weight,
    upper: Weight,
}

impl Default for Bounds {
    fn default() -> Self {
        Bounds { lower: INFINITY, upper: INFINITY }
    }
}

#[derive(Debug, Clone)]
pub struct Shortcut {
    data: ShortcutPaths,
}

const NUM_WINDOWS: usize = 2;
#[inline]
pub fn window_size() -> Weight {
    period() / NUM_WINDOWS as u32
}

#[derive(Debug, Clone)]
enum ShortcutPaths {
    None,
    One(Bounds, ShortcutData),
    Multi([(Bounds, Vec<ShortcutData>); NUM_WINDOWS])
}

impl Shortcut {
    pub fn new(source: Option<(EdgeId, PiecewiseLinearFunction)>) -> Shortcut {
        match source {
            Some((edge, plf)) => {
                let (lower, upper) = plf.bounds();
                Shortcut { data: ShortcutPaths::One(Bounds { lower, upper }, ShortcutData::new(ShortcutSource::OriginalEdge(edge))) }
            },
            None => Shortcut { data: ShortcutPaths::None },
        }
    }

    pub fn merge(&mut self, linked_ids: (EdgeId, EdgeId), shortcut_graph: &ShortcutGraph) {
        // TODO clean up times
        let shortcut_data = ShortcutData::new(ShortcutSource::Shortcut(linked_ids.0, linked_ids.1));
        let linked = Linked::new(shortcut_graph.get_incoming(linked_ids.0), shortcut_graph.get_outgoing(linked_ids.1));
        if !linked.is_valid_path() {
            return
        }
        let (other_lower_bound, other_upper_bound) = linked.bounds();

        if !self.is_valid_path() {
            self.data = ShortcutPaths::One(Bounds { lower: other_lower_bound, upper: other_upper_bound }, shortcut_data);
            return
        }

        let (current_lower_bound, current_upper_bound) = self.bounds();
        if current_lower_bound >= other_upper_bound {
            self.data = ShortcutPaths::One(Bounds { lower: other_lower_bound, upper: other_upper_bound }, shortcut_data);
            return
        } else if other_lower_bound >= current_upper_bound {
            return
        }

        debug_assert!(current_lower_bound < INFINITY);
        debug_assert!(current_upper_bound < INFINITY);
        debug_assert!(other_lower_bound < INFINITY);
        debug_assert!(other_upper_bound < INFINITY);

        let data = match self.data {
            ShortcutPaths::None => {
                self.data = ShortcutPaths::Multi(Default::default());
                if let ShortcutPaths::Multi(new_data) = &mut self.data { new_data } else { panic!("not happening") }
            },
            ShortcutPaths::One(_, data) => {
                self.data = ShortcutPaths::Multi(Default::default());
                if let ShortcutPaths::Multi(new_data) = &mut self.data {
                    for (i, window) in new_data.iter_mut().enumerate() {
                        let range = Shortcut::window_time_range(i);
                        let (lower, upper) = data.bounds_for(&range, shortcut_graph);
                        *window = (Bounds { lower, upper }, vec![data])
                    }
                    new_data
                } else { panic!("not happening") }
            },
            ShortcutPaths::Multi(ref mut data) => {
                data
            }
        };

        for (i, (bounds, data)) in data.iter_mut().enumerate() {
            let range = Shortcut::window_time_range(i);
            let (other_lower_bound, other_upper_bound) = linked.bounds_for(&range);

            if bounds.lower >= other_upper_bound {
                *bounds = Bounds { lower: other_lower_bound, upper: other_upper_bound };
                *data = vec![shortcut_data];
            } else if other_lower_bound >= bounds.upper {
                // only self - do nothing
            } else {
                bounds.lower = min(bounds.lower, other_lower_bound);
                bounds.upper = min(bounds.upper, other_upper_bound);
                data.push(shortcut_data);
            }
        }

        // TODO maybe check for stuff which fell back to single?
    }

    pub fn evaluate(&self, departure: Timestamp, shortcut_graph: &ShortcutGraph) -> Weight {
        debug_assert!(departure < period());
        match self.data {
            ShortcutPaths::None => INFINITY,
            ShortcutPaths::One(_, data) => data.evaluate(departure, shortcut_graph),
            ShortcutPaths::Multi(ref data) => {
                data[(departure / window_size()) as usize].1.iter().map(|path| path.evaluate(departure, shortcut_graph)).min().unwrap()
            }
        }
    }

    pub fn bounds(&self) -> (Weight, Weight) {
        match &self.data {
            ShortcutPaths::None => (INFINITY, INFINITY),
            ShortcutPaths::One(bounds, _) => (bounds.lower, bounds.upper),
            ShortcutPaths::Multi(ref data) => {
                data.iter()
                    .map(|(bounds, _)| bounds)
                    .fold((INFINITY, 0), |(acc_min, acc_max), bounds| (min(acc_min, bounds.lower), max(acc_max, bounds.upper)))
            }
        }
    }

    pub fn bounds_for(&self, range: &Range<Timestamp>) -> (Weight, Weight) {
        match &self.data {
            ShortcutPaths::None => (INFINITY, INFINITY),
            ShortcutPaths::One(bounds, _) => (bounds.lower, bounds.upper),
            ShortcutPaths::Multi(ref data) => {
                data[Shortcut::time_range_to_window_range(range)].iter()
                    .map(|(bounds, _)| bounds)
                    .fold((INFINITY, 0), |(acc_min, acc_max), bounds| (min(acc_min, bounds.lower), max(acc_max, bounds.upper)))
            }
        }
    }

    pub fn remove_dominated(&mut self, shortcut_graph: &ShortcutGraph) {
        if let ShortcutPaths::Multi(data) = &mut self.data {
            for (i, (Bounds { upper, .. }, window_data)) in data.iter_mut().enumerate() {
                window_data.retain(|path| path.bounds_for(&Shortcut::window_time_range(i), shortcut_graph).0 <= *upper);
            }
        }
    }

    pub fn remove_dominated_by(&mut self, shortcut_graph: &ShortcutGraph, other: &Linked) {
        match self.data {
            ShortcutPaths::One(ref bounds, _) => {
                let upper_bound = other.bounds().1;
                if bounds.lower > upper_bound {
                    self.data = ShortcutPaths::None;
                }
            },
            ShortcutPaths::Multi(ref mut data) => {
                for (i, (bounds, data)) in data.iter_mut().enumerate() {
                    let range = Shortcut::window_time_range(i);
                    let upper_bound = other.bounds_for(&range).1;
                    data.retain(|path| path.bounds_for(&range, shortcut_graph).0 <= upper_bound);
                    *bounds = data.iter().map(|path| path.bounds_for(&range, shortcut_graph))
                        .fold(Bounds { lower: INFINITY, upper: INFINITY }, |acc, bounds|
                            Bounds { lower: min(acc.lower, bounds.0), upper: min(acc.upper, bounds.1) })
                }
            }
            _ => (),
        }
    }

    pub fn num_path_segments(&self) -> usize {
        match self.data {
            ShortcutPaths::None => 0,
            ShortcutPaths::One(_, _) => 1,
            ShortcutPaths::Multi(ref data) => data.iter().map(|(_, data)| data.len()).sum()
        }
    }

    pub fn is_valid_path(&self) -> bool {
        self.num_path_segments() > 0
    }

    // TODO for range
    pub fn unpack(&self, shortcut_graph: &ShortcutGraph, unpacked_shortcuts: &mut BitVec, original_edges: &mut BitVec) {
        match self.data {
            ShortcutPaths::None => (),
            ShortcutPaths::One(_, source) => source.unpack(shortcut_graph, unpacked_shortcuts, original_edges),
            ShortcutPaths::Multi(ref data) => {
                for source in data.iter().flat_map(|(_, data)| data.iter()) {
                    source.unpack(shortcut_graph, unpacked_shortcuts, original_edges);
                }
            }
        }
    }

    fn time_range_to_window_range(range: &Range<Timestamp>) -> Range<usize> {
        (range.start / window_size()) as usize .. ((range.end + window_size() - 1) / window_size()) as usize
    }

    fn window_time_range(window: usize) -> Range<Timestamp> {
        window as Timestamp * window_size() .. (window + 1) as Timestamp * window_size()
    }

    pub fn debug_to_s<'a>(&self, shortcut_graph: &'a ShortcutGraph, indent: usize) -> String {
        println!("{:?}", self.data);
        let mut s = String::from("Shortcut: ");

        match self.data {
            ShortcutPaths::None => s += "NONE",
            ShortcutPaths::One(_, source) => {
                s += "ONE";
                s.push('\n');
                for _ in 0..indent {
                    s.push(' ');
                    s.push(' ');
                }
                s = s + &source.debug_to_s(shortcut_graph, indent + 1);
            },
            ShortcutPaths::Multi(ref data) => {
                for (i, window_data) in data.iter().enumerate() {
                    s.push('\n');
                    for _ in 0..indent {
                        s.push(' ');
                        s.push(' ');
                    }
                    s = s + &format!("{}: ", i);
                    for source in &window_data.1 {
                        s.push('\n');
                        for _ in 0..indent+1 {
                            s.push(' ');
                            s.push(' ');
                        }
                        s = s + &source.debug_to_s(shortcut_graph, indent + 2);
                    }
                }
            }
        }

        s
    }
}
