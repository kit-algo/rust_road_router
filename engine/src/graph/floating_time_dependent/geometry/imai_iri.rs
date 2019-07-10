use super::*;

pub const INVALID_INDEX: usize = std::usize::MAX;

#[derive(Debug)]
struct ImaiVertex {
    pub upper_coord: TTFPoint,
    pub lower_coord: TTFPoint,
    pub succ_upper: usize,
    pub succ_lower: usize,
    pub pred_upper: usize,
    pub pred_lower: usize,
}

impl ImaiVertex {
    fn new(p: &TTFPoint, upper_bound: f64, lower_bound: f64, upper_absolute: bool, lower_absolute: bool) -> Self {
        ImaiVertex {
            upper_coord: TTFPoint {
                at: p.at,
                val: if upper_absolute { p.val + FlWeight::new(upper_bound) } else { (1. + upper_bound) * p.val }
            },
            lower_coord: TTFPoint {
                at: p.at,
                val: if lower_absolute { p.val - FlWeight::new(lower_bound) } else { (1. - lower_bound) * p.val }
            },
            succ_upper: INVALID_INDEX,
            succ_lower: INVALID_INDEX,
            pred_upper: INVALID_INDEX,
            pred_lower: INVALID_INDEX,
        }
    }
}

#[derive(Debug)]
pub struct Imai {
    tunnel: Vec<ImaiVertex>,
    window: Vec<TTFPoint>,
    p_upper: usize, // index of upper point of current window
    p_lower: usize, // index of lower point of current window
    r_upper: usize, // index of right extreme point of the upper convex hull
    r_lower: usize, // index of right extreme point of the lower convex hull
    l_upper: usize, // index of left extreme point of the upper convex hull
    l_lower: usize, // index of left extreme point of the lower convex hull

    i: usize, // control variable for dealing with the points of the given function successively
    j: usize, // count determined sampling points of approximated function
    n: usize, // number of sampling points of the given piecewise linear function

    upper_bound: f64,
    lower_bound: f64,
    upper_absolute: bool,
    lower_absolute: bool,
}

impl Imai {
    pub fn new(points: &[TTFPoint], upper_bound: f64, lower_bound: f64, upper_absolute: bool, lower_absolute: bool) -> Self {
        // use std::iter::once;

        debug_assert!(upper_bound > 0.0 || lower_bound > 0.0);

        // let (first, points) = points.split_first().unwrap();
        // let (last, points) = points.split_last().unwrap();

        debug_assert!(!points.is_empty());

        // let tunnel = once(ImaiVertex::new(first, EPSILON / 2.0, EPSILON / 2.0, true, true))
        //     .chain(points.iter().map(|p| ImaiVertex::new(p, upper_bound, lower_bound, lower_absolute, upper_absolute)))
        //     .chain(once(ImaiVertex::new(last, EPSILON / 2.0, EPSILON / 2.0, true, true)))
        //     .collect();

        let tunnel = points.iter().map(|p| ImaiVertex::new(p, upper_bound, lower_bound, lower_absolute, upper_absolute)).collect();

        Imai {
            tunnel,
            window: Vec::new(),
            n: points.len(),
            p_upper: INVALID_INDEX,
            p_lower: INVALID_INDEX,
            r_upper: INVALID_INDEX,
            r_lower: INVALID_INDEX,
            l_upper: INVALID_INDEX,
            l_lower: INVALID_INDEX,
            i: INVALID_INDEX,
            j: INVALID_INDEX,
            upper_bound, lower_bound, upper_absolute, lower_absolute,
        }
    }

    pub fn compute(mut self) -> Vec<TTFPoint> {
        let mut approximated_points = Vec::new();

        self.init();

        while self.i < self.n - 1 {
            self.i += 1;

            /*
             * Updating CH+ and CH-
             */
            self.update_ch_upper();
            self.update_ch_lower();

            /*
             * checking whether CH+ and CH- intersect or not
             */
            if angle_upper_lt(&self.tunnel[self.i].upper_coord, &self.tunnel[self.l_upper].upper_coord, &self.tunnel[self.r_lower].lower_coord) {
                self.intersection_upper(&mut approximated_points);
            } else if angle_lower_lt(&self.tunnel[self.i].lower_coord, &self.tunnel[self.l_lower].lower_coord, &self.tunnel[self.r_upper].upper_coord) {
                self.intersection_lower(&mut approximated_points);
            } else {
                /*
                 * Updating two separating lines and the supporting points
                 */
                if angle_upper_lt(&self.tunnel[self.i].upper_coord, &self.tunnel[self.l_lower].lower_coord, &self.tunnel[self.r_upper].upper_coord) {
                    self.update_lines_upper();
                }
                if angle_lower_lt(&self.tunnel[self.i].lower_coord, &self.tunnel[self.l_upper].upper_coord, &self.tunnel[self.r_lower].lower_coord) {
                    self.update_lines_lower();
                }
            }
        }

        /*
         * Computing the last two points
         */
        let penult_coord = intersection_point(&self.tunnel[self.l_lower].lower_coord,
                                              &self.tunnel[self.r_upper].upper_coord,
                                              &self.tunnel[self.p_lower].lower_coord,
                                              &self.tunnel[self.p_upper].upper_coord);
        approximated_points.push(penult_coord);
        self.j += 1;

        let last_coord = intersection_point(&self.tunnel[self.l_lower].lower_coord,
                                            &self.tunnel[self.r_upper].upper_coord,
                                            &self.tunnel[self.n-1].lower_coord,
                                            &self.tunnel[self.n-1].upper_coord);

        if last_coord.at > approximated_points.last().unwrap().at {
            approximated_points.push(last_coord);
        }
        self.j += 1;

        if approximated_points.last().unwrap().at < period() {
            approximated_points.push(TTFPoint { at: period(), val: approximated_points.first().unwrap().val });
        }

        if approximated_points.last().unwrap().val != approximated_points.first().unwrap().val {
            approximated_points.last_mut().unwrap().val = approximated_points.first().unwrap().val;
        }

        approximated_points
    }

    fn init(&mut self) {
        self.tunnel[0].succ_upper = 1;
        self.tunnel[1].pred_upper = 0;

        self.p_upper = 0;
        self.l_upper = 0;
        self.r_upper = 1;

        self.tunnel[0].succ_lower = 1;
        self.tunnel[1].pred_lower = 0;

        self.p_lower = 0;
        self.l_lower = 0;
        self.r_lower = 1;

        self.i = 1;
        self.j = 0;
    }

    fn update_ch_lower(&mut self) {
        let mut k = self.i - 1;

        while self.tunnel[k].pred_lower != INVALID_INDEX &&
              !self.tunnel[k].lower_coord.at.fuzzy_eq(self.tunnel[self.p_lower].lower_coord.at) &&
              angle_lower_gt(&self.tunnel[self.i].lower_coord, &self.tunnel[k].lower_coord, &self.tunnel[self.tunnel[k].pred_lower].lower_coord) {
            k = self.tunnel[k].pred_lower;
        }

        self.tunnel[k].succ_lower = self.i;
        self.tunnel[self.i].pred_lower = k;
    }

    fn update_ch_upper(&mut self) {
        let mut k = self.i - 1;

        while self.tunnel[k].pred_upper != INVALID_INDEX &&
              !self.tunnel[k].upper_coord.at.fuzzy_eq(self.tunnel[self.p_upper].upper_coord.at) &&
              angle_upper_gt(&self.tunnel[self.i].upper_coord, &self.tunnel[k].upper_coord, &self.tunnel[self.tunnel[k].pred_upper].upper_coord) {
            k = self.tunnel[k].pred_upper;
        }

        self.tunnel[k].succ_upper = self.i;
        self.tunnel[self.i].pred_upper = k;
    }

    fn update_lines_lower(&mut self) {
        self.r_lower = self.i;
        while self.tunnel[self.l_upper].succ_upper != INVALID_INDEX &&
              angle_lower_lt(&self.tunnel[self.i].lower_coord,
                             &self.tunnel[self.l_upper].upper_coord,
                             &self.tunnel[self.tunnel[self.l_upper].succ_upper].upper_coord) {
            self.l_upper = self.tunnel[self.l_upper].succ_upper;
        }
    }

    fn update_lines_upper(&mut self) {
        self.r_upper = self.i;
        while self.tunnel[self.l_lower].succ_lower != INVALID_INDEX &&
              angle_upper_lt(&self.tunnel[self.i].upper_coord,
                             &self.tunnel[self.l_lower].lower_coord,
                             &self.tunnel[self.tunnel[self.l_lower].succ_lower].lower_coord) {
            self.l_lower = self.tunnel[self.l_lower].succ_lower;
        }
    }

    fn intersection_lower(&mut self, approximated_points: &mut Vec<TTFPoint>) {
        let coord = intersection_point(&self.tunnel[self.l_lower].lower_coord,
                                       &self.tunnel[self.r_upper].upper_coord,
                                       &self.tunnel[self.p_lower].lower_coord,
                                       &self.tunnel[self.p_upper].upper_coord);
        approximated_points.push(coord);

        debug_assert!(self.j == 0 || approximated_points[self.j-1].at <= approximated_points[self.j].at);
        self.j += 1;
        self.p_upper = self.r_upper;

        let mut p_lower_vertex_coord = intersection_point(&self.tunnel[self.l_lower].lower_coord,
                                                   &self.tunnel[self.r_upper].upper_coord,
                                                   &self.tunnel[self.i-1].lower_coord,
                                                   &self.tunnel[self.i].lower_coord);

        // this is appearently negating the effect of the ImaiVertex constructor... might be possible to otimize that away
        if self.lower_absolute {
            p_lower_vertex_coord.val = p_lower_vertex_coord.val + FlWeight::new(self.lower_bound);
        } else {
            p_lower_vertex_coord.val = (1. / (1. - self.lower_bound)) * p_lower_vertex_coord.val;
        }
        let mut p_lower_vertex = ImaiVertex::new(&p_lower_vertex_coord, self.upper_bound, self.lower_bound, self.upper_absolute, self.lower_absolute);
        p_lower_vertex.succ_lower = self.i;
        self.tunnel.push(p_lower_vertex);

        debug_assert!(self.tunnel.last().unwrap().lower_coord.val < FlWeight::INFINITY);
        debug_assert!(self.tunnel.last().unwrap().lower_coord.val > FlWeight::new(-f64::from(INFINITY)));

        self.p_lower = self.tunnel.len()-1;
        self.tunnel[self.i].pred_lower = self.tunnel.len()-1;
        self.r_lower = self.i;
        self.r_upper = self.i;
        self.l_lower = self.p_lower;
        self.l_upper = self.p_upper;
        while self.tunnel[self.l_upper].succ_upper != INVALID_INDEX &&
              angle_upper_lt(&self.tunnel[self.l_upper].upper_coord,
                             &self.tunnel[self.r_lower].lower_coord,
                             &self.tunnel[self.tunnel[self.l_upper].succ_upper].upper_coord) {
            self.l_upper = self.tunnel[self.l_upper].succ_upper;
        }

        self.window.push(self.tunnel[self.p_upper].upper_coord.clone());
        self.window.push(self.tunnel[self.p_lower].lower_coord.clone());
    }

    fn intersection_upper(&mut self, approximated_points: &mut Vec<TTFPoint>) {
        let coord = intersection_point(&self.tunnel[self.l_upper].upper_coord,
                                       &self.tunnel[self.r_lower].lower_coord,
                                       &self.tunnel[self.p_upper].upper_coord,
                                       &self.tunnel[self.p_lower].lower_coord);
        approximated_points.push(coord);
        debug_assert!(self.j == 0 || approximated_points[self.j - 1].at <= approximated_points[self.j].at);
        self.j += 1;
        self.p_lower = self.r_lower;

        let mut p_upper_vertex_coord = intersection_point(&self.tunnel[self.l_upper].upper_coord,
                                                   &self.tunnel[self.r_lower].lower_coord,
                                                   &self.tunnel[self.i-1].upper_coord,
                                                   &self.tunnel[self.i].upper_coord);

        // this is appearently negating the effect of the ImaiVertex constructor... might be possible to otimize that away
        if self.upper_absolute {
            p_upper_vertex_coord.val = p_upper_vertex_coord.val - FlWeight::new(self.upper_bound);
        } else {
            p_upper_vertex_coord.val = (1. / (1. + self.upper_bound)) * p_upper_vertex_coord.val;
        }
        let mut p_upper_vertex = ImaiVertex::new(&p_upper_vertex_coord, self.upper_bound, self.lower_bound, self.upper_absolute, self.lower_absolute);
        p_upper_vertex.succ_upper = self.i;
        self.tunnel.push(p_upper_vertex);

        debug_assert!(self.tunnel.last().unwrap().lower_coord.val < FlWeight::INFINITY);
        debug_assert!(self.tunnel.last().unwrap().lower_coord.val > FlWeight::new(-f64::from(INFINITY)));

        self.p_upper = self.tunnel.len() - 1;
        self.tunnel[self.i].pred_upper = self.tunnel.len() - 1;
        self.r_upper = self.i;
        self.r_lower = self.i;
        self.l_upper = self.p_upper;
        self.l_lower = self.p_lower;

        while self.tunnel[self.l_lower].succ_lower != INVALID_INDEX &&
              angle_lower_lt(&self.tunnel[self.l_lower].lower_coord,
                             &self.tunnel[self.r_upper].upper_coord,
                             &self.tunnel[self.tunnel[self.l_lower].succ_lower].lower_coord) {
            self.l_lower = self.tunnel[self.l_lower].succ_lower;
        }

        self.window.push(self.tunnel[self.p_upper].upper_coord.clone());
        self.window.push(self.tunnel[self.p_lower].lower_coord.clone());
    }
}

fn angle_upper_lt(u: &TTFPoint, v: &TTFPoint, w: &TTFPoint) -> bool {
    counter_clockwise(v, u, w)
}

fn angle_upper_gt(u: &TTFPoint, v: &TTFPoint, w: &TTFPoint) -> bool {
    clockwise(v, u, w)
}

fn angle_lower_lt(u: &TTFPoint, v: &TTFPoint, w: &TTFPoint) -> bool {
    clockwise(v, u, w)
}

fn angle_lower_gt(u: &TTFPoint, v: &TTFPoint, w: &TTFPoint) -> bool {
    counter_clockwise(v, u, w)
}
