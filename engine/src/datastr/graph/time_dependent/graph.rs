use super::*;
use crate::datastr::graph::Graph as GraphTrait;
use crate::io::*;
use crate::report::*;
use crate::util::{in_range_option::*, *};

type IPPIndex = u32;

/// Container for basic TD-Graph data.
#[derive(Debug, Clone)]
pub struct Graph {
    first_out: Vec<EdgeId>,
    head: Vec<NodeId>,
    first_ipp_of_arc: Vec<IPPIndex>,
    ipp_departure_time: Vec<Timestamp>,
    ipp_travel_time: Vec<Weight>,
}

impl Graph {
    /// Construct graph from raw data.
    pub fn new(
        first_out: Vec<EdgeId>,
        head: Vec<NodeId>,
        mut first_ipp_of_arc: Vec<IPPIndex>,
        ipp_departure_time: Vec<Timestamp>,
        ipp_travel_time: Vec<Weight>,
    ) -> Self {
        let mut new_ipp_departure_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());
        let mut new_ipp_travel_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());

        let mut added = 0;

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
                if *new_ipp_departure_time.last().unwrap() != period() {
                    new_ipp_departure_time.push(period());
                    new_ipp_travel_time.push(ipp_travel_time[range.start]);
                    added += 1;
                }
            } else {
                new_ipp_departure_time.push(0);
                new_ipp_travel_time.push(ipp_travel_time[range.start]);
            }
        }
        first_ipp_of_arc[head.len()] += added;

        Self {
            first_out,
            head,
            first_ipp_of_arc,
            ipp_departure_time: new_ipp_departure_time,
            ipp_travel_time: new_ipp_travel_time,
        }
    }

    /// Borrow an individual travel time function.
    #[inline(always)]
    pub fn travel_time_function(&self, edge_id: EdgeId) -> PiecewiseLinearFunction {
        let edge_id = edge_id as usize;
        PiecewiseLinearFunction::new(
            &self.ipp_departure_time[self.first_ipp_of_arc[edge_id] as usize..self.first_ipp_of_arc[edge_id + 1] as usize],
            &self.ipp_travel_time[self.first_ipp_of_arc[edge_id] as usize..self.first_ipp_of_arc[edge_id + 1] as usize],
        )
    }

    /// Iterator over neighbors and corresponding edge ids.
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

    /// Total number of interpolation points
    pub fn num_ipps(&self) -> usize {
        self.ipp_departure_time.len()
    }

    /// Number of edges with constant PLF
    pub fn num_constant(&self) -> usize {
        self.first_ipp_of_arc
            .windows(2)
            .map(|firsts| firsts[1] - firsts[0])
            .filter(|&deg| deg == 1)
            .count()
    }

    pub fn fix_zero_travel_times(&mut self) {
        for tt in &mut self.ipp_travel_time {
            *tt = std::cmp::max(*tt, 1);
        }
    }

    pub fn line_graph(&self, mut turn_costs: impl FnMut(EdgeId, EdgeId) -> Option<Weight>) -> Self {
        let mut first_out = Vec::with_capacity(self.num_arcs() + 1);
        first_out.push(0);
        let mut head = Vec::new();
        let mut first_ipp_of_arc = Vec::new();
        first_ipp_of_arc.push(0);
        let mut ipp_departure_time = Vec::new();
        let mut ipp_travel_time = Vec::new();
        let mut num_turns = 0;
        let mut num_ipps = 0;

        for edge_id in 0..self.num_arcs() {
            let link = self.link(edge_id as EdgeId);
            for next_link_id in self.neighbor_edge_indices(link.0) {
                if let Some(turn_cost) = turn_costs(edge_id as EdgeId, next_link_id) {
                    head.push(next_link_id);

                    let ipp_range = self.first_ipp_of_arc[edge_id] as usize..self.first_ipp_of_arc[edge_id + 1] as usize;
                    for (&dt, &tt) in self.ipp_departure_time[ipp_range.clone()].iter().zip(self.ipp_travel_time[ipp_range].iter()) {
                        ipp_departure_time.push(dt);
                        ipp_travel_time.push(tt + turn_cost);
                        num_ipps += 1;
                    }
                    first_ipp_of_arc.push(num_ipps);
                    num_turns += 1;
                }
            }
            first_out.push(num_turns as EdgeId);
        }

        Self {
            first_out,
            head,
            first_ipp_of_arc,
            ipp_departure_time,
            ipp_travel_time,
        }
    }

    pub fn to_constant_lower(&mut self) {
        self.ipp_travel_time = (0..self.num_arcs())
            .map(|edge| self.travel_time_function(edge as EdgeId).lower_bound())
            .collect();
        self.first_ipp_of_arc = (0..(self.num_arcs() + 1) as u32).collect();
        self.ipp_departure_time = std::iter::repeat(0).take(self.num_arcs()).collect();
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

impl LinkIterable<NodeIdT> for Graph {
    type Iter<'a> = impl Iterator<Item = NodeIdT> + 'a;

    #[inline(always)]
    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        self.head[self.neighbor_edge_indices_usize(node)].iter().copied().map(NodeIdT)
    }
}

impl EdgeIdGraph for Graph {
    type IdxIter<'a> = impl Iterator<Item = EdgeIdT> + 'a where Self: 'a;

    fn edge_indices(&self, from: NodeId, to: NodeId) -> Self::IdxIter<'_> {
        self.neighbor_edge_indices(from).filter(move |&e| self.head[e as usize] == to).map(EdgeIdT)
    }

    #[inline(always)]
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

impl BuildPermutated<Graph> for Graph {
    fn permutated_filtered(graph: &Graph, order: &NodeOrder, mut predicate: Box<dyn FnMut(NodeId, NodeId) -> bool>) -> Self {
        let mut first_out: Vec<EdgeId> = Vec::with_capacity(graph.num_nodes() + 1);
        first_out.push(0);
        let mut head = Vec::with_capacity(graph.num_arcs());
        let mut first_ipp_of_arc = Vec::<IPPIndex>::with_capacity(graph.num_arcs());
        first_ipp_of_arc.push(0);
        let mut ipp_departure_time = Vec::<Timestamp>::with_capacity(graph.ipp_departure_time.len());
        let mut ipp_travel_time = Vec::<Weight>::with_capacity(graph.ipp_travel_time.len());

        for (rank, &node) in order.order().iter().enumerate() {
            let mut links = graph
                .neighbor_and_edge_id_iter(node)
                .filter(|&(h, _)| predicate(rank as NodeId, order.rank(h)))
                .collect::<Vec<_>>();
            first_out.push(first_out.last().unwrap() + links.len() as EdgeId);
            links.sort_unstable_by_key(|&(head, _)| order.rank(head));

            for (h, e) in links {
                head.push(order.rank(h));
                let ipp_range = graph.first_ipp_of_arc[e as usize] as usize..graph.first_ipp_of_arc[e as usize + 1] as usize;
                first_ipp_of_arc.push(first_ipp_of_arc.last().unwrap() + (ipp_range.end - ipp_range.start) as IPPIndex);
                ipp_departure_time.extend_from_slice(&graph.ipp_departure_time[ipp_range.clone()]);
                ipp_travel_time.extend_from_slice(&graph.ipp_travel_time[ipp_range]);
            }
        }

        Graph {
            first_out,
            head,
            first_ipp_of_arc,
            ipp_departure_time,
            ipp_travel_time,
        }
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

#[derive(Clone)]
pub struct LiveTDGraph {
    graph: Graph,
    soon: Timestamp,
    live: Vec<InRangeOption<Weight>>,
}

impl LiveTDGraph {
    pub fn new(graph: Graph, soon: Timestamp, live: Vec<InRangeOption<Weight>>) -> Self {
        LiveTDGraph { graph, soon, live }
    }

    pub fn eval(&self, edge_id: EdgeId, t: Timestamp) -> Weight {
        let ttf = self.graph.travel_time_function(edge_id);
        let predicted = ttf.eval(t);
        if let Some(live) = self.live[edge_id as usize].value() {
            if t < self.soon {
                live
            } else {
                if ttf.eval(self.soon) < live {
                    std::cmp::max((live + self.soon).saturating_sub(t), predicted)
                } else {
                    std::cmp::min(live.saturating_add(t) - self.soon, predicted)
                }
            }
        } else {
            predicted
        }
    }

    pub fn line_graph(&self, mut turn_costs: impl FnMut(EdgeId, EdgeId) -> Option<Weight>) -> Self {
        let mut live = Vec::new();
        let graph = self.graph.line_graph(|from_edge, to_edge| {
            turn_costs(from_edge, to_edge).tap(|turn| {
                if let &mut Some(cost) = turn {
                    live.push(InRangeOption::new(self.live[from_edge as usize].value().map(|tt| tt + cost)))
                }
            })
        });
        Self { graph, live, soon: self.soon }
    }

    pub fn graph(&self) -> &Graph {
        &self.graph
    }
}

impl crate::datastr::graph::Graph for LiveTDGraph {
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }
    fn num_arcs(&self) -> usize {
        self.graph.num_arcs()
    }
    fn degree(&self, node: NodeId) -> usize {
        self.graph.degree(node)
    }
}

impl LinkIterable<NodeIdT> for LiveTDGraph {
    type Iter<'a> = <Graph as LinkIterable<NodeIdT>>::Iter<'a>;

    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        LinkIterable::<NodeIdT>::link_iter(&self.graph, node)
    }
}
impl LinkIterable<(NodeIdT, EdgeIdT)> for LiveTDGraph {
    type Iter<'a> = <Graph as LinkIterable<(NodeIdT, EdgeIdT)>>::Iter<'a>;

    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        LinkIterable::<(NodeIdT, EdgeIdT)>::link_iter(&self.graph, node)
    }
}

impl BuildPermutated<LiveTDGraph> for LiveTDGraph {
    fn permutated_filtered(graph: &LiveTDGraph, order: &NodeOrder, mut predicate: Box<dyn FnMut(NodeId, NodeId) -> bool>) -> Self {
        let mut first_out: Vec<EdgeId> = Vec::with_capacity(graph.num_nodes() + 1);
        first_out.push(0);
        let mut head = Vec::with_capacity(graph.num_arcs());
        let mut live = Vec::with_capacity(graph.num_arcs());
        let mut first_ipp_of_arc = Vec::<IPPIndex>::with_capacity(graph.num_arcs());
        first_ipp_of_arc.push(0);
        let mut ipp_departure_time = Vec::<Timestamp>::with_capacity(graph.graph.ipp_departure_time.len());
        let mut ipp_travel_time = Vec::<Weight>::with_capacity(graph.graph.ipp_travel_time.len());

        for (rank, &node) in order.order().iter().enumerate() {
            let mut links = graph
                .graph
                .neighbor_and_edge_id_iter(node)
                .filter(|&(h, _)| predicate(rank as NodeId, order.rank(h)))
                .collect::<Vec<_>>();
            first_out.push(first_out.last().unwrap() + links.len() as EdgeId);
            links.sort_unstable_by_key(|&(head, _)| order.rank(head));

            for (h, e) in links {
                head.push(order.rank(h));
                live.push(graph.live[e as usize]);
                let ipp_range = graph.graph.first_ipp_of_arc[e as usize] as usize..graph.graph.first_ipp_of_arc[e as usize + 1] as usize;
                first_ipp_of_arc.push(first_ipp_of_arc.last().unwrap() + (ipp_range.end - ipp_range.start) as IPPIndex);
                ipp_departure_time.extend_from_slice(&graph.graph.ipp_departure_time[ipp_range.clone()]);
                ipp_travel_time.extend_from_slice(&graph.graph.ipp_travel_time[ipp_range]);
            }
        }

        LiveTDGraph {
            graph: Graph {
                first_out,
                head,
                first_ipp_of_arc,
                ipp_departure_time,
                ipp_travel_time,
            },
            soon: graph.soon,
            live,
        }
    }
}

#[derive(Clone)]
pub struct PessimisticLiveTDGraph {
    graph: Graph,
    live: Vec<InRangeOption<(Weight, Timestamp)>>,
}

impl PessimisticLiveTDGraph {
    pub fn new(graph: Graph, live: Vec<InRangeOption<(Weight, Timestamp)>>) -> Self {
        Self { graph, live }
    }

    pub fn eval(&self, edge_id: EdgeId, t: Timestamp) -> Weight {
        let ttf = self.graph.travel_time_function(edge_id);
        let predicted = ttf.eval(t);
        if let Some((live, soon)) = self.live[edge_id as usize].value() {
            let tt_live = if t >= soon {
                ttf.eval(t)
            } else {
                let tt_soon = ttf.eval(soon);
                std::cmp::min(live, tt_soon.saturating_add(soon - t))
            };
            std::cmp::max(predicted, tt_live)
        } else {
            predicted
        }
    }

    pub fn upper_bound(&self, edge_id: EdgeId, now: Timestamp) -> Weight {
        let ttf = self.graph.travel_time_function(edge_id);
        std::cmp::max(
            ttf.upper_bound(),
            // this line catches the case of an inf live value - the evaluated tt will be smaller than inf
            std::cmp::min(self.live[edge_id as usize].value().map(|(l, _)| l).unwrap_or(0), self.eval(edge_id, now)),
        )
    }

    pub fn live_lower_bound(&self, edge_id: EdgeId, now: Timestamp, until: Timestamp) -> Weight {
        let ttf = self.graph.travel_time_function(edge_id);
        if let Some((live, soon)) = self.live[edge_id as usize].value() {
            if until >= soon {
                ttf.lower_bound_in_range(soon..until)
            } else {
                let tt_soon = ttf.eval(soon);
                std::cmp::min(live, tt_soon.saturating_add(soon - until))
            }
        } else {
            ttf.lower_bound_in_range(now..until)
        }
    }

    pub fn predicted_upper_bound(&self, edge_id: EdgeId) -> Weight {
        self.graph.travel_time_function(edge_id).upper_bound()
    }

    pub fn live_ended_at(&self, edge_id: EdgeId) -> Timestamp {
        self.live[edge_id as usize].value().map(|(_, t)| t).unwrap_or(0)
    }

    pub fn live_starts_switching_at(&self, edge_id: EdgeId) -> Option<Weight> {
        self.live[edge_id as usize].value().map(|(live, soon)| {
            let ttf = self.graph.travel_time_function(edge_id);
            let tt_soon = ttf.eval(soon);
            soon.saturating_sub(live.saturating_sub(tt_soon))
        })
    }

    pub fn is_live_slower(&self, edge_id: EdgeId, now: Timestamp) -> Option<bool> {
        self.live[edge_id as usize]
            .value()
            .filter(|&(l, _t)| self.graph.travel_time_function(edge_id).lower_bound() <= l)
            .map(|(l, t)| self.graph.travel_time_function(edge_id).lower_bound_in_range(now..t) <= l)
    }

    pub fn check_global_lower(&self) {
        for edge_id in 0..self.num_arcs() {
            if let Some((tt, _)) = self.live[edge_id].value() {
                assert!(tt >= self.graph.travel_time_function(edge_id as EdgeId).lower_bound());
            }
        }
    }

    pub fn remove_live_faster_than_global_lower(&mut self) {
        for edge_id in 0..self.num_arcs() {
            if let Some((tt, _)) = self.live[edge_id].value() {
                if tt < self.graph.travel_time_function(edge_id as EdgeId).lower_bound() {
                    self.live[edge_id] = InRangeOption::NONE
                }
            }
        }
    }

    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    pub fn fix_zero_travel_times(&mut self) {
        self.graph.fix_zero_travel_times()
    }

    pub fn line_graph(&self, mut turn_costs: impl FnMut(EdgeId, EdgeId) -> Option<Weight>) -> Self {
        let mut live = Vec::new();

        let graph = self.graph.line_graph(|edge_id, next_link_id| {
            turn_costs(edge_id, next_link_id).map(|turn_cost| {
                live.push(InRangeOption::new(self.live[edge_id as usize].value().map(|(w, t)| (w + turn_cost, t))));
                turn_cost
            })
        });

        Self { graph, live }
    }

    pub fn to_constant_lower(&mut self) {
        self.graph.to_constant_lower()
    }
}

impl crate::datastr::graph::Graph for PessimisticLiveTDGraph {
    fn num_nodes(&self) -> usize {
        self.graph.num_nodes()
    }
    fn num_arcs(&self) -> usize {
        self.graph.num_arcs()
    }
    fn degree(&self, node: NodeId) -> usize {
        self.graph.degree(node)
    }
}

impl LinkIterable<NodeIdT> for PessimisticLiveTDGraph {
    type Iter<'a> = <Graph as LinkIterable<NodeIdT>>::Iter<'a>;

    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        LinkIterable::<NodeIdT>::link_iter(&self.graph, node)
    }
}
impl LinkIterable<(NodeIdT, EdgeIdT)> for PessimisticLiveTDGraph {
    type Iter<'a> = <Graph as LinkIterable<(NodeIdT, EdgeIdT)>>::Iter<'a>;

    fn link_iter(&self, node: NodeId) -> Self::Iter<'_> {
        LinkIterable::<(NodeIdT, EdgeIdT)>::link_iter(&self.graph, node)
    }
}

impl BuildPermutated<PessimisticLiveTDGraph> for PessimisticLiveTDGraph {
    fn permutated_filtered(graph: &PessimisticLiveTDGraph, order: &NodeOrder, mut predicate: Box<dyn FnMut(NodeId, NodeId) -> bool>) -> Self {
        let mut first_out: Vec<EdgeId> = Vec::with_capacity(graph.num_nodes() + 1);
        first_out.push(0);
        let mut head = Vec::with_capacity(graph.num_arcs());
        let mut live = Vec::with_capacity(graph.num_arcs());
        let mut first_ipp_of_arc = Vec::<IPPIndex>::with_capacity(graph.num_arcs());
        first_ipp_of_arc.push(0);
        let mut ipp_departure_time = Vec::<Timestamp>::with_capacity(graph.graph.ipp_departure_time.len());
        let mut ipp_travel_time = Vec::<Weight>::with_capacity(graph.graph.ipp_travel_time.len());

        for (rank, &node) in order.order().iter().enumerate() {
            let mut links = graph
                .graph
                .neighbor_and_edge_id_iter(node)
                .filter(|&(h, _)| predicate(rank as NodeId, order.rank(h)))
                .collect::<Vec<_>>();
            first_out.push(first_out.last().unwrap() + links.len() as EdgeId);
            links.sort_unstable_by_key(|&(head, _)| order.rank(head));

            for (h, e) in links {
                head.push(order.rank(h));
                live.push(graph.live[e as usize]);
                let ipp_range = graph.graph.first_ipp_of_arc[e as usize] as usize..graph.graph.first_ipp_of_arc[e as usize + 1] as usize;
                first_ipp_of_arc.push(first_ipp_of_arc.last().unwrap() + (ipp_range.end - ipp_range.start) as IPPIndex);
                ipp_departure_time.extend_from_slice(&graph.graph.ipp_departure_time[ipp_range.clone()]);
                ipp_travel_time.extend_from_slice(&graph.graph.ipp_travel_time[ipp_range]);
            }
        }

        PessimisticLiveTDGraph {
            graph: Graph {
                first_out,
                head,
                first_ipp_of_arc,
                ipp_departure_time,
                ipp_travel_time,
            },
            live,
        }
    }
}

impl ReconstructPrepared<PessimisticLiveTDGraph> for (String, Timestamp) {
    fn reconstruct_with(self, loader: Loader) -> std::io::Result<PessimisticLiveTDGraph> {
        let graph: Graph = loader.reconstruct("")?;
        let (live_data_file, t_live) = self;

        let mut live = vec![InRangeOption::NONE; graph.num_arcs()];
        let live_data: Vec<(EdgeId, Weight, Weight)> = loader.load(live_data_file)?;
        report!("num_edges_with_live", live_data.len());
        let mut not_really_live: usize = 0;
        let mut blocked: usize = 0;
        let mut blocked_but_also_long_term: usize = 0;
        let max_t_soon = period();
        report!("max_t_soon", max_t_soon);
        let mut live_counter = 0;
        for (edge, weight, duration) in live_data {
            if weight >= INFINITY {
                blocked += 1;
            }
            if duration < max_t_soon {
                live[edge as usize] = InRangeOption::some((weight, duration + t_live));
                live_counter += 1;
            } else {
                if weight >= INFINITY && duration >= max_t_soon {
                    blocked_but_also_long_term += 1;
                }
                not_really_live += 1;
            }
        }
        report!("num_applied_live_dates", live_counter);
        report!("num_long_term_live_reports", not_really_live);
        report!("blocked", blocked);
        report!("num_long_term_blocks", blocked_but_also_long_term);
        Ok(PessimisticLiveTDGraph::new(graph, live))
    }
}

impl EdgeIdGraph for PessimisticLiveTDGraph {
    type IdxIter<'a> = impl Iterator<Item = EdgeIdT> + 'a where Self: 'a;

    fn edge_indices(&self, from: NodeId, to: NodeId) -> Self::IdxIter<'_> {
        self.graph.edge_indices(from, to)
    }

    #[inline(always)]
    fn neighbor_edge_indices(&self, node: NodeId) -> Range<EdgeId> {
        self.graph.neighbor_edge_indices(node)
    }
}
