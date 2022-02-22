use super::piecewise_linear_function::cursor::*;
use super::*;
use crate::datastr::graph::time_dependent::period as int_period;
use crate::datastr::graph::Graph as GraphTrait;
use crate::io::*;
use crate::report::*;

type IPPIndex = u32;

/// First out based graph data structure for time-dependent graphs.
/// All data is owned.
#[derive(Debug, Clone)]
pub struct Graph {
    first_out: Vec<EdgeId>,
    head: Vec<NodeId>,
    first_ipp_of_arc: Vec<IPPIndex>,
    ipps: Vec<TTFPoint>,
}

impl Graph {
    /// Create new Graph from raw data.
    /// Performs a bit of clean up on the input.
    pub fn new(
        first_out: Vec<EdgeId>,
        head: Vec<NodeId>,
        mut first_ipp_of_arc: Vec<IPPIndex>,
        ipp_departure_time: Vec<u32>,
        ipp_travel_time: Vec<u32>,
    ) -> Graph {
        let mut new_ipp_departure_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());
        let mut new_ipp_travel_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());

        let mut added = 0;

        // Make sure all nonconst PLFs have a point at time 0 and one at time `period` and these two have the same value
        // Make sure all const PLFs have exactly one point at time 0.
        for i in 0..head.len() {
            let range = first_ipp_of_arc[i] as usize..first_ipp_of_arc[i + 1] as usize;
            assert_ne!(range.start, range.end);

            first_ipp_of_arc[i] += added;

            if range.end - range.start > 1 {
                if ipp_departure_time[range.start] != 0 {
                    new_ipp_departure_time.push(0);
                    new_ipp_travel_time.push(ipp_travel_time[range.start]);
                    added += 1;
                }
                new_ipp_departure_time.extend(ipp_departure_time[range.clone()].iter().cloned());
                new_ipp_travel_time.extend(ipp_travel_time[range.clone()].iter().cloned());
                if *new_ipp_departure_time.last().unwrap() != int_period() {
                    new_ipp_departure_time.push(int_period());
                    new_ipp_travel_time.push(ipp_travel_time[range.start]);
                    added += 1;
                }
            } else {
                new_ipp_departure_time.push(0);
                new_ipp_travel_time.push(ipp_travel_time[range.start]);
            }
        }
        first_ipp_of_arc[head.len()] += added;

        let ipps = new_ipp_departure_time
            .into_iter()
            .zip(new_ipp_travel_time.into_iter())
            .map(|(dt, tt)| TTFPoint {
                // ms to s
                at: Timestamp::new(f64::from(dt) / 1000.0),
                val: FlWeight::new(f64::from(tt) / 1000.0),
            })
            .collect();

        Graph {
            first_out,
            head,
            first_ipp_of_arc,
            ipps,
        }
    }

    /// Borrow PLF
    pub fn travel_time_function(&self, edge_id: EdgeId) -> PeriodicPiecewiseLinearFunction {
        let edge_id = edge_id as usize;
        PeriodicPiecewiseLinearFunction::new(&self.ipps[self.first_ipp_of_arc[edge_id] as usize..self.first_ipp_of_arc[edge_id + 1] as usize])
    }

    /// Outgoing edge iterator
    pub fn neighbor_and_edge_id_iter(&self, node: NodeId) -> impl Iterator<Item = (NodeId, EdgeId)> + '_ {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().cloned().zip(self.neighbor_edge_indices(node))
    }

    pub fn first_out(&self) -> &[EdgeId] {
        &self.first_out[..]
    }

    pub fn head(&self) -> &[NodeId] {
        &self.head[..]
    }

    /// Assert that a time annotated path is valid and that the times of the path match the edge weights at the time.
    pub fn check_path(&self, path: &[(NodeId, Timestamp)]) {
        let mut iter = path.iter();
        let mut prev = *iter.next().unwrap();
        for &(node, t) in iter {
            let (prev_node, prev_t) = prev;
            let evaled = prev_t
                + self
                    .edge_indices(prev_node, node)
                    .map(|EdgeIdT(edge)| self.travel_time_function(edge).evaluate(prev_t))
                    .min()
                    .expect("path contained nonexisting edge");
            assert!(
                t.fuzzy_eq(evaled),
                "expected {:?} - got {:?} from {} (at {:?}) to {}",
                evaled,
                t,
                prev_node,
                prev_t,
                node
            );
            prev = (node, t);
        }
    }

    pub fn get_path_with_times(&self, mut dt: Timestamp, path: &[EdgeId]) -> Vec<(NodeId, Timestamp)> {
        let mut new_path = Vec::with_capacity(path.len() + 1);
        new_path.push((link_id_to_tail_mapper::link_id_to_tail(&self.first_out, path[0]), dt));
        for &edge in path {
            dt = dt + self.travel_time_function(edge).evaluate(dt);
            new_path.push((self.head[edge as usize], dt));
        }
        new_path
    }

    /// Total number of interpolation points
    pub fn num_ipps(&self) -> usize {
        self.ipps.len()
    }

    /// Number of edges with constant PLF
    pub fn num_constant(&self) -> usize {
        self.first_ipp_of_arc
            .windows(2)
            .map(|firsts| firsts[1] - firsts[0])
            .filter(|&deg| deg == 1)
            .count()
    }

    pub fn report_relative_delays(&self) {
        let mut num_constant = 0;
        let mut sum_lower = FlWeight::ZERO;
        let mut sum_upper = FlWeight::ZERO;
        let mut sum_nonconst_lower = FlWeight::ZERO;
        let mut sum_nonconst_upper = FlWeight::ZERO;
        let mut sum_rel_delays = 0.0;
        let mut sum_nonconst_rel_delays = 0.0;

        for edge_id in 0..self.num_arcs() {
            let ttf = self.travel_time_function(edge_id as EdgeId);

            let lower = ttf.lower_bound();
            let upper = ttf.upper_bound();

            sum_lower += lower;
            sum_upper += upper;
            if FlWeight::ZERO.fuzzy_lt(lower) {
                sum_rel_delays += f64::from((upper - lower) / lower);
            }

            if !ttf.constant() {
                sum_nonconst_lower += lower;
                sum_nonconst_upper += upper;
                if FlWeight::ZERO.fuzzy_lt(lower) {
                    sum_nonconst_rel_delays += f64::from((upper - lower) / lower);
                }
            } else {
                num_constant += 1;
            }
        }

        debug_assert_eq!(num_constant, self.num_constant());

        let nonconst_total_relative_dekay = f64::from((sum_nonconst_upper - sum_nonconst_lower) / sum_nonconst_lower);
        let total_relative_dekay = f64::from((sum_upper - sum_lower) / sum_lower);
        report!("relative_delays", {
            "nonconst_mean": sum_nonconst_rel_delays / (self.num_arcs() - num_constant) as f64,
            "mean": sum_rel_delays / self.num_arcs() as f64,
            "nonconst_total_relative_delay": nonconst_total_relative_dekay,
            "total_relative_delay": total_relative_dekay
        });
    }
}

impl GraphTrait for Graph {
    fn num_nodes(&self) -> usize {
        self.first_out.len() - 1
    }

    fn num_arcs(&self) -> usize {
        self.head.len()
    }

    fn degree(&self, node: NodeId) -> usize {
        let node = node as usize;
        (self.first_out[node + 1] - self.first_out[node]) as usize
    }
}

impl EdgeIdGraph for Graph {
    // https://github.com/rust-lang/rustfmt/issues/4911
    #[rustfmt::skip]
    type IdxIter<'a> where Self: 'a = impl Iterator<Item = EdgeIdT> + 'a;

    fn edge_indices(&self, from: NodeId, to: NodeId) -> Self::IdxIter<'_> {
        self.neighbor_edge_indices(from).filter(move |&e| self.head[e as usize] == to).map(EdgeIdT)
    }

    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        (self.first_out[node as usize] as EdgeId)..(self.first_out[(node + 1) as usize] as EdgeId)
    }
}

impl EdgeRandomAccessGraph<NodeIdT> for Graph {
    fn link(&self, edge_id: EdgeId) -> NodeIdT {
        NodeIdT(self.head[edge_id as usize])
    }
}

impl LinkIterable<(NodeIdT, EdgeIdT)> for Graph {
    type Iter<'a> = impl Iterator<Item = (NodeIdT, EdgeIdT)> + 'a;

    #[inline(always)]
    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().copied().map(NodeIdT).zip(self.neighbor_edge_indices(node).map(EdgeIdT))
    }
}

impl LinkIterable<NodeIdT> for Graph {
    type Iter<'a> = impl Iterator<Item = NodeIdT> + 'a;

    #[inline(always)]
    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        let range = self.neighbor_edge_indices_usize(node);
        self.head[range].iter().copied().map(NodeIdT)
    }
}

#[derive(Debug, Clone)]
pub struct LiveGraph {
    pub graph: Graph,
    first_live_ipp_of_arc: Vec<IPPIndex>,
    live_ipps: Vec<TTFPoint>,
}

impl LiveGraph {
    pub fn new(graph: Graph, t_live: Timestamp, live: &[Option<(u32, u32)>]) -> Self {
        let mut first_live_ipp_of_arc = Vec::with_capacity(graph.num_arcs() + 1);
        first_live_ipp_of_arc.push(0);
        let mut live_ipps = Vec::new();

        for (edge_id, live) in live.iter().enumerate() {
            if let &Some((live, t_soon)) = live {
                let live = FlWeight::new(f64::from(live) / 1000.0);
                let t_soon = Timestamp::new(f64::from(t_soon) / 1000.0);
                live_ipps.push(TTFPoint { at: t_live, val: live });
                let pred_plf = graph.travel_time_function(edge_id as EdgeId);
                let switchpoint = Self::switchpoint(pred_plf, live, t_soon);
                if t_soon.fuzzy_lt(switchpoint.at) {
                    live_ipps.push(TTFPoint { at: t_soon, val: live });
                }
                live_ipps.push(switchpoint);
                debug_assert!(
                    pred_plf.evaluate(switchpoint.at).fuzzy_eq(switchpoint.val),
                    "{:?}",
                    dbg_each!(&live_ipps[*first_live_ipp_of_arc.last().unwrap() as usize..])
                );
            }
            first_live_ipp_of_arc.push(live_ipps.len() as u32);
        }

        Self {
            graph,
            first_live_ipp_of_arc,
            live_ipps,
        }
    }

    fn switchpoint(plf: PeriodicPiecewiseLinearFunction, live: FlWeight, t_soon: Timestamp) -> TTFPoint {
        let evaled = plf.evaluate(t_soon);
        if evaled.fuzzy_eq(live) {
            return TTFPoint { at: t_soon, val: live };
        }
        let mut cursor = Cursor::starting_at_or_after(&plf, t_soon);
        let pred_below = evaled.fuzzy_lt(live);
        loop {
            let live_at_cur = if pred_below {
                t_soon + live - cursor.cur().at
            } else {
                live + cursor.cur().at - t_soon
            };

            if live_at_cur.fuzzy_eq(cursor.cur().val) {
                return cursor.cur();
            } else if (pred_below && live_at_cur.fuzzy_lt(cursor.cur().val)) || (!pred_below && cursor.cur().val.fuzzy_lt(live_at_cur)) {
                return intersection_point(
                    &cursor.prev(),
                    &cursor.cur(),
                    &TTFPoint { at: t_soon, val: live },
                    &TTFPoint {
                        at: cursor.cur().at,
                        val: live_at_cur,
                    },
                );
            } else {
                cursor.advance();
            }
        }
    }

    /// Borrow PLF
    pub fn travel_time_function(&self, edge_id: EdgeId) -> UpdatedPiecewiseLinearFunction {
        let edge_idx = edge_id as usize;
        UpdatedPiecewiseLinearFunction::new(
            self.graph.travel_time_function(edge_id),
            &self.live_ipps[self.first_live_ipp_of_arc[edge_idx] as usize..self.first_live_ipp_of_arc[edge_idx + 1] as usize],
        )
    }

    pub fn t_live(&self) -> Timestamp {
        self.live_ipps[0].at
    }
}

pub trait TDGraphTrait<'a> {
    type TTF: PLF;
    fn travel_time_function(&'a self, edge_id: EdgeId) -> Self::TTF;

    fn evaluate_path(&'a self, path: &[EdgeId], t: Timestamp) -> FlWeight {
        let mut tt = FlWeight::ZERO;
        for &edge in path {
            tt = tt + self.travel_time_function(edge).evaluate(t + tt);
        }
        tt
    }

    fn inverse_evaluate_path(&'a self, path: &[EdgeId], t: Timestamp) -> Timestamp {
        let mut departure = t;
        for &edge in path.iter().rev() {
            departure = self.travel_time_function(edge).inverse_evaluate(departure);
        }
        departure
    }
}

impl<'a> TDGraphTrait<'a> for Graph {
    type TTF = PeriodicPiecewiseLinearFunction<'a>;
    fn travel_time_function(&'a self, edge_id: EdgeId) -> Self::TTF {
        Graph::travel_time_function(self, edge_id)
    }
}

impl<'a> TDGraphTrait<'a> for LiveGraph {
    type TTF = UpdatedPiecewiseLinearFunction<'a>;
    fn travel_time_function(&'a self, edge_id: EdgeId) -> Self::TTF {
        LiveGraph::travel_time_function(self, edge_id)
    }
}

impl Reconstruct for Graph {
    fn reconstruct_with(loader: Loader) -> std::io::Result<Self> {
        let first_out: Vec<_> = loader.load("first_out")?;
        let head: Vec<_> = loader.load("head")?;
        let ipp_departure_time: Vec<_> = loader.load("ipp_departure_time")?;

        report!("unprocessed_graph", { "num_nodes": first_out.len() - 1, "num_arcs": head.len(), "num_ipps": ipp_departure_time.len() });

        let graph = Self::new(
            first_out,
            head,
            loader.load("first_ipp_of_arc")?,
            ipp_departure_time,
            loader.load("ipp_travel_time")?,
        );

        report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs(), "num_ipps": graph.num_ipps(), "num_constant_ttfs": graph.num_constant() });

        Ok(graph)
    }
}
