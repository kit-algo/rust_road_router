use super::*;

use crate::{
    algo::{
        a_star::*,
        ch_potentials::*,
        dijkstra::{generic_dijkstra::*, *},
        minimal_nonshortest_subpaths::*,
    },
    datastr::{graph::first_out_graph::BorrowedGraph, timestamped_vector::*},
    report::*,
};

#[derive(Clone, Debug, PartialEq)]
pub enum ActiveForbittenPaths {
    One(u128),
    More(Box<[u128]>),
}

impl ActiveForbittenPaths {
    fn new(size: usize) -> Self {
        if size <= 128 {
            Self::One(0)
        } else {
            Self::More(std::iter::repeat(0).take((size + 127) / 128).collect())
        }
    }

    fn get(&self, idx: usize) -> bool {
        match self {
            Self::One(active) => active & (1 << idx) != 0,
            Self::More(active) => (active[idx / 128] & (1 << (idx % 128))) != 0,
        }
    }

    fn set(&mut self, idx: usize) {
        match self {
            Self::One(active) => *active |= 1 << idx,
            Self::More(active) => active[idx / 128] |= 1 << (idx % 128),
        }
    }

    fn is_subset(&self, rhs: &Self) -> bool {
        match (self, rhs) {
            (&Self::One(self_active), &Self::One(rhs_active)) => !(!self_active | rhs_active) == 0,
            (&Self::One(self_active), Self::More(rhs_active)) => !(!self_active | rhs_active[0]) == 0,
            (Self::More(self_active), &Self::One(rhs_active)) => !(!self_active[0] | rhs_active) == 0 && self_active[1..].iter().all(|&a| a == 0),
            (Self::More(self_active), Self::More(rhs_active)) => self_active
                .iter()
                .zip(rhs_active.iter().chain(std::iter::repeat(&0)))
                .all(|(&s_a, &rhs_a)| !(!s_a | rhs_a) == 0),
        }
    }
}

impl Reset for ForbiddenPathLabel {
    const DEFAULT: Self = (INFINITY, ActiveForbittenPaths::One(0), (NodeIdT(0), ActiveForbittenPaths::One(0)));
}

impl Label for ForbiddenPathLabel {
    type Key = Weight;
    fn neutral() -> Self {
        Self::DEFAULT
    }
    fn key(&self) -> Self::Key {
        self.0
    }
}

impl<T: Clone> Reset for MultiCritNodeData<T>
where
    NodeQueueLabelOrder<T>: Ord,
{
    const DEFAULT: Self = Self::new();

    fn reset(&mut self) {
        self.clear();
    }
}

pub struct BlockedPathsDijkstra {
    // Next Node, Next Node Local Offset
    forbidden_paths: Vec<Vec<(NodeIdT, u32)>>,
    // Global forbidden path id, offset on path, Tail node Local offset
    edge_forbidden_paths: Vec<Vec<(usize, usize, u32)>>,
    node_forbidden_path_counter: Vec<usize>,
    num_labels_pushed: usize,
}

impl BlockedPathsDijkstra {
    pub fn new(n: usize, m: usize) -> Self {
        Self {
            forbidden_paths: Vec::new(),
            edge_forbidden_paths: vec![Vec::new(); m],
            node_forbidden_path_counter: vec![0; n],
            num_labels_pushed: 0,
        }
    }

    pub fn add_forbidden_path(&mut self, path: &[NodeId], graph: &impl EdgeIdGraph) {
        if cfg!(debug_assertions) {
            let mut active_forbidden_paths = ActiveForbittenPaths::new(self.node_forbidden_path_counter[path[0] as usize]);

            for &[tail, head] in path.array_windows::<2>() {
                let mut head_active_forbidden_paths = ActiveForbittenPaths::new(self.node_forbidden_path_counter[head as usize]);
                if let Some(EdgeIdT(link_id)) = graph.edge_indices(tail, head).next() {
                    for &(global_id, current_edge_path_offset, tail_local_idx) in &self.edge_forbidden_paths[link_id as usize] {
                        if current_edge_path_offset == 1 || active_forbidden_paths.get(tail_local_idx as usize) {
                            if self.forbidden_paths[global_id].len() == current_edge_path_offset + 1 {
                                if head == *path.last().unwrap() {
                                    panic!("path already forbidden: {:#?}", path);
                                } else {
                                    dbg!("subpath already forbidden");
                                    return;
                                }
                            } else {
                                head_active_forbidden_paths.set(self.forbidden_paths[global_id][current_edge_path_offset].1 as usize);
                            }
                        }
                    }
                }
                active_forbidden_paths = head_active_forbidden_paths;
            }
        }

        let global_id = self.forbidden_paths.len();
        let mut forbidden_path = Vec::with_capacity(path.len());
        forbidden_path.push((NodeIdT(path[0]), 0));

        for (offset, &[tail, head]) in path.array_windows::<2>().enumerate() {
            for EdgeIdT(link_id) in graph.edge_indices(tail, head) {
                self.edge_forbidden_paths[link_id as usize].push((global_id, offset + 1, self.node_forbidden_path_counter[tail as usize] as u32));
            }
            forbidden_path.push((NodeIdT(head), self.node_forbidden_path_counter[head as usize] as u32));
        }
        for &node in &path[1..path.len() - 1] {
            self.node_forbidden_path_counter[node as usize] += 1;
        }

        self.forbidden_paths.push(forbidden_path);
    }

    pub fn reset(&mut self, graph: &impl EdgeIdGraph) {
        self.num_labels_pushed = 0;

        for path in self.forbidden_paths.drain(..) {
            for &[(NodeIdT(tail), _), (NodeIdT(head), _)] in path.array_windows::<2>() {
                for EdgeIdT(link_id) in graph.edge_indices(tail, head) {
                    self.edge_forbidden_paths[link_id as usize].clear();
                }
            }
            for (NodeIdT(node), _) in path {
                self.node_forbidden_path_counter[node as usize] = 0;
            }
        }
    }

    pub fn num_labels_pushed(&self) -> usize {
        self.num_labels_pushed
    }

    pub fn num_forbidden_paths(&self) -> usize {
        self.forbidden_paths.len()
    }
}

impl<G: EdgeIdGraph + EdgeRandomAccessGraph<Link>> MultiCritDijkstraOps<G> for BlockedPathsDijkstra {
    type Label = ForbiddenPathLabel;
    type Arc = (NodeIdT, EdgeIdT);
    type LinkResult = Option<ForbiddenPathLabel>;
    type PredecessorLink = ();

    fn link(
        &mut self,
        #[allow(unused)] graph: &G,
        #[allow(unused)] labels: &TimestampedVector<MultiCritNodeData<Self::Label>>,
        _parents: &[(NodeId, Self::PredecessorLink)],
        NodeIdT(tail): NodeIdT,
        _key: Weight,
        label: &Self::Label,
        &(NodeIdT(head), EdgeIdT(link_id)): &Self::Arc,
    ) -> Self::LinkResult {
        self.num_labels_pushed += 1;
        let weight = graph.link(link_id).weight;

        let mut illegal = false;
        let mut head_active_forbidden_paths = ActiveForbittenPaths::new(self.node_forbidden_path_counter[head as usize]);
        let active_forbidden_paths = &label.1;

        for &(global_id, current_edge_path_offset, tail_local_idx) in &self.edge_forbidden_paths[link_id as usize] {
            if current_edge_path_offset == 1 || active_forbidden_paths.get(tail_local_idx as usize) {
                if self.forbidden_paths[global_id].len() == current_edge_path_offset + 1 {
                    illegal = true;
                    break;
                } else {
                    head_active_forbidden_paths.set(self.forbidden_paths[global_id][current_edge_path_offset].1 as usize);
                }
            }
        }

        if !illegal {
            Some((label.0 + weight, head_active_forbidden_paths, (NodeIdT(tail), label.1.clone())))
        } else {
            None
        }
    }
    fn merge(&mut self, label: &mut MultiCritNodeData<Self::Label>, linked: Self::LinkResult) -> Option<Weight> {
        let linked = linked?;
        let mut dominated = false;

        label.retain(|NodeQueueLabelOrder(old_l)| {
            if old_l.0 <= linked.0 && old_l.1.is_subset(&linked.1) {
                dominated = true;
            }
            dominated || old_l.0 < linked.0 || !linked.1.is_subset(&old_l.1)
        });

        if dominated {
            None
        } else {
            let update_dist = linked.0;
            label.push(NodeQueueLabelOrder(linked));
            Some(update_dist)
        }
    }
    fn predecessor_link(&self, _link: &Self::Arc) -> Self::PredecessorLink {}
}

impl PartialEq for NodeQueueLabelOrder<ForbiddenPathLabel> {
    fn eq(&self, other: &Self) -> bool {
        self.0 .0.eq(&other.0 .0)
    }
}
impl PartialOrd for NodeQueueLabelOrder<ForbiddenPathLabel> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // switched for reversing
        other.0 .0.partial_cmp(&self.0 .0)
    }
}
impl Eq for NodeQueueLabelOrder<ForbiddenPathLabel> {}
impl Ord for NodeQueueLabelOrder<ForbiddenPathLabel> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        self.partial_cmp(other).unwrap()
    }
}

pub struct BlockedDetoursDijkstra {
    node_forbidden_paths: Vec<Vec<Box<[NodeIdT]>>>,
    forbidden_paths_end_nodes: Vec<NodeIdT>,
}

impl BlockedDetoursDijkstra {
    pub fn new(n: usize) -> Self {
        Self {
            node_forbidden_paths: vec![Vec::new(); n],
            forbidden_paths_end_nodes: Vec::new(),
        }
    }

    pub fn add_forbidden_path(&mut self, path: &[NodeId]) {
        let (&last, rest) = path.split_last().unwrap();
        self.forbidden_paths_end_nodes.push(NodeIdT(last));
        self.node_forbidden_paths[last as usize].push(rest.iter().copied().map(NodeIdT).collect());
    }

    pub fn reset(&mut self) {
        for NodeIdT(node) in self.forbidden_paths_end_nodes.drain(..) {
            self.node_forbidden_paths[node as usize].clear();
        }
    }

    pub fn num_forbidden_paths(&self) -> usize {
        self.forbidden_paths_end_nodes.len()
    }
}

impl<G> DijkstraOps<G> for BlockedDetoursDijkstra {
    type Label = Weight;
    type Arc = Link;
    type LinkResult = Weight;
    type PredecessorLink = ();

    fn link(
        &mut self,
        _graph: &G,
        parents: &[(NodeId, Self::PredecessorLink)],
        NodeIdT(tail): NodeIdT,
        label: &Self::Label,
        link: &Self::Arc,
    ) -> Self::LinkResult {
        let illegal = self.node_forbidden_paths[link.node as usize].iter().any(|forbidden_path| {
            let mut deviated = false;
            let mut cur = tail;
            for &NodeIdT(forbidden_parent) in forbidden_path.iter().rev() {
                if forbidden_parent == cur {
                    cur = parents[cur as usize].0;
                } else {
                    deviated = true;
                    break;
                }
            }
            !deviated
        });

        if illegal {
            INFINITY
        } else {
            label + link.weight
        }
    }
    fn merge(&mut self, label: &mut Self::Label, linked: Self::LinkResult) -> bool {
        if linked < *label {
            *label = linked;
            return true;
        }
        false
    }
    fn predecessor_link(&self, _link: &Self::Arc) -> Self::PredecessorLink {}
}

type ForbiddenPathLabel = (Weight, ActiveForbittenPaths, (NodeIdT, ActiveForbittenPaths));

pub struct TrafficAwareServer<'a> {
    ubs_checker: MinimalNonShortestSubPaths<'a>,
    dijkstra_data: DijkstraData<ForbiddenPathLabel, (), MultiCritNodeData<ForbiddenPathLabel>>,
    live_pot: BorrowedCCHPot<'a>,
    live_graph: BorrowedGraph<'a>,
    smooth_graph: BorrowedGraph<'a>,
    dijkstra_ops: BlockedPathsDijkstra,
}

impl<'a> TrafficAwareServer<'a> {
    pub fn new(smooth_graph: BorrowedGraph<'a>, live_graph: BorrowedGraph<'a>, smooth_cch_pot: &'a CCHPotData, live_cch_pot: &'a CCHPotData) -> Self {
        let n = smooth_graph.num_nodes();
        let m = smooth_graph.num_arcs();
        Self {
            ubs_checker: MinimalNonShortestSubPaths::new(smooth_cch_pot),
            dijkstra_data: DijkstraData::new(n),
            live_pot: live_cch_pot.forward_potential(),
            live_graph,
            smooth_graph,
            dijkstra_ops: BlockedPathsDijkstra::new(n, m),
        }
    }

    pub fn query(&mut self, query: Query, epsilon: f64) -> Option<()> {
        self.dijkstra_ops.reset(&self.smooth_graph);

        self.live_pot.init(query.to);
        let base_live_dist = self.live_pot.potential(query.from)?;
        let mut final_live_dist = base_live_dist;

        let mut explore_time = time::Duration::zero();
        let mut ubs_time = time::Duration::zero();

        let mut i = 0;
        let mut total_queue_pops = 0usize;
        let mut iterations_ctxt = push_collection_context("iterations".to_string());
        let result = loop {
            if i > 200 {
                break None;
            }

            let _it_ctxt = iterations_ctxt.push_collection_item();
            i += 1;
            report!("iteration", i);
            let live_pot = &mut self.live_pot;
            let mut dijk_run = MultiCritDijkstraRun::query(
                &self.live_graph,
                &mut self.dijkstra_data,
                &mut self.dijkstra_ops,
                DijkstraInit {
                    source: NodeIdT(query.from),
                    initial_state: (0, ActiveForbittenPaths::new(0), (NodeIdT(query.from), ActiveForbittenPaths::new(0))),
                },
                |node| live_pot.potential(node),
            );

            let mut num_queue_pops = 0;
            let (_, time) = measure(|| {
                while let Some(node) = dijk_run.next_step_with_potential(|node| live_pot.potential(node)) {
                    num_queue_pops += 1;
                    total_queue_pops += 1;
                    if node == query.to {
                        break;
                    }
                }
            });

            report!("num_queue_pops", num_queue_pops);
            report!("exploration_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            explore_time = explore_time + time;

            if self.dijkstra_data.distances[query.to as usize].popped().is_empty() {
                break None;
            }

            debug_assert_eq!(self.dijkstra_data.distances[query.to as usize].popped().len(), 1);
            final_live_dist = self.dijkstra_data.distances[query.to as usize].popped()[0].0 .0;

            let mut path = vec![query.to];

            let mut label_idx = 0;
            while *path.last().unwrap() != query.from {
                let (dist, _, (NodeIdT(parent), parent_active_forb_paths)) =
                    &self.dijkstra_data.distances[*path.last().unwrap() as usize].popped()[label_idx].0;

                let min_weight_edge = self
                    .live_graph
                    .edge_indices(*parent, *path.last().unwrap())
                    .min_by_key(|&EdgeIdT(edge_id)| self.live_graph.link(edge_id).weight)
                    .unwrap();

                label_idx = self.dijkstra_data.distances[*parent as usize]
                    .popped()
                    .iter()
                    // why is_subset instead of ==
                    // because at node u, a better label (without some forbidden path [..., u, w])
                    // with same dist might get merged but when linking this new label along uv this will not cause a merge at v
                    // because its not on that forbidden path and thus both labels have the same bitsets.
                    .position(|NodeQueueLabelOrder(l)| l.0 == dist - self.live_graph.link(min_weight_edge.0).weight && l.1.is_subset(parent_active_forb_paths))
                    .unwrap();

                path.push(*parent);
            }

            path.reverse();
            report!("num_nodes_on_path", path.len());

            let (violating, time) = measure(|| self.ubs_checker.find_ubs_violating_subpaths(&path, &self.smooth_graph, epsilon));
            report!("ubs_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            ubs_time = ubs_time + time;

            if violating.is_empty() {
                break Some(());
            }

            for violating_range in violating {
                self.dijkstra_ops
                    .add_forbidden_path(&path[violating_range.start..=violating_range.end], &self.smooth_graph);
            }
        };
        drop(iterations_ctxt);
        report!("failed", result.is_none());
        report!("num_iterations", i);
        report!("num_labels_pushed", self.dijkstra_ops.num_labels_pushed());
        report!("total_queue_pops", total_queue_pops);
        report!("num_forbidden_paths", self.dijkstra_ops.num_forbidden_paths());
        report!(
            "length_increase_percent",
            (final_live_dist - base_live_dist) as f64 / base_live_dist as f64 * 100.0
        );
        report!("total_exploration_time_ms", explore_time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        report!("total_ubs_time_ms", ubs_time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        result
    }
}

use crate::algo::ch_potentials::query::Server as TopoDijkServer;

pub struct HeuristicTrafficAwareServer<'a> {
    ubs_checker: MinimalNonShortestSubPaths<'a>,
    smooth_graph: BorrowedGraph<'a>,
    shortest_path: TopoDijkServer<OwnedGraph, BlockedDetoursDijkstra, RecyclingPotential<BorrowedCCHPot<'a>>, true, true, true>,
}

impl<'a> HeuristicTrafficAwareServer<'a> {
    pub fn new(smooth_graph: BorrowedGraph<'a>, live_graph: BorrowedGraph<'a>, smooth_cch_pot: &'a CCHPotData, live_cch_pot: &'a CCHPotData) -> Self {
        let n = smooth_graph.num_nodes();
        let _blocked = block_reporting();
        Self {
            ubs_checker: MinimalNonShortestSubPaths::new(smooth_cch_pot),
            smooth_graph,
            shortest_path: TopoDijkServer::new(
                &live_graph,
                RecyclingPotential::new(live_cch_pot.forward_potential()),
                BlockedDetoursDijkstra::new(n),
            ),
        }
    }

    pub fn query(&mut self, query: Query, epsilon: f64) -> Option<()> {
        self.shortest_path.ops().reset();

        let mut base_live_dist = None;
        let mut final_live_dist = INFINITY;

        let mut explore_time = time::Duration::zero();
        let mut ubs_time = time::Duration::zero();

        let mut i = 0;
        let mut iterations_ctxt = push_collection_context("iterations".to_string());
        let result = loop {
            if i > 200 {
                break None;
            }

            let _it_ctxt = iterations_ctxt.push_collection_item();
            i += 1;
            report!("iteration", i);

            let (res, time) = measure(|| self.shortest_path.query(query));

            report!("exploration_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            explore_time = explore_time + time;

            let mut res = if let Some(res) = res.found() {
                res
            } else {
                break None;
            };

            final_live_dist = res.distance();
            if base_live_dist.is_none() {
                base_live_dist = Some(res.distance());
            }
            let mut path = res.node_path();
            report!("num_nodes_on_path", path.len());

            let (violating, time) = measure(|| self.ubs_checker.find_ubs_violating_subpaths(&path, &self.smooth_graph, epsilon));
            report!("ubs_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            ubs_time = ubs_time + time;

            if violating.is_empty() {
                break Some(());
            }

            for node in &mut path {
                *node = self.shortest_path.order().rank(*node);
            }
            for violating_range in violating {
                self.shortest_path.ops().add_forbidden_path(&path[violating_range.start..=violating_range.end]);
            }
        };
        drop(iterations_ctxt);
        report!("failed", result.is_none());
        report!("num_iterations", i);
        report!("num_forbidden_paths", self.shortest_path.ops().num_forbidden_paths());
        if let Some(base_live_dist) = base_live_dist {
            report!(
                "length_increase_percent",
                (final_live_dist - base_live_dist) as f64 / base_live_dist as f64 * 100.0
            );
        }
        report!("total_exploration_time_ms", explore_time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        report!("total_ubs_time_ms", ubs_time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        result
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_linking_forbidden_paths() {
        let graph = FirstOutGraph::new(&[0, 1, 2], &[1, 0], &[1, 1]);
        let mut ops = BlockedPathsDijkstra::new(2, 2);
        let dd = DijkstraData::<ForbiddenPathLabel, (), MultiCritNodeData<ForbiddenPathLabel>>::new(2);
        ops.add_forbidden_path(&[0, 1], &graph);

        assert_eq!(
            MultiCritDijkstraOps::link(
                &mut ops,
                &graph,
                &dd.distances,
                &dd.predecessors,
                NodeIdT(0),
                0,
                &(0, ActiveForbittenPaths::One(0), (NodeIdT(0), ActiveForbittenPaths::One(0))),
                &(NodeIdT(1), EdgeIdT(0)),
            ),
            None,
        );

        let mut ops = BlockedPathsDijkstra::new(2, 2);
        ops.add_forbidden_path(&[0, 1, 0], &graph);
        assert_eq!(
            MultiCritDijkstraOps::link(
                &mut ops,
                &graph,
                &dd.distances,
                &dd.predecessors,
                NodeIdT(0),
                0,
                &(0, ActiveForbittenPaths::One(0), (NodeIdT(0), ActiveForbittenPaths::One(0))),
                &(NodeIdT(1), EdgeIdT(0)),
            ),
            Some((1, ActiveForbittenPaths::One(1), (NodeIdT(0), ActiveForbittenPaths::One(0))))
        );
        assert_eq!(
            MultiCritDijkstraOps::link(
                &mut ops,
                &graph,
                &dd.distances,
                &dd.predecessors,
                NodeIdT(1),
                0,
                &(1, ActiveForbittenPaths::One(1), (NodeIdT(0), ActiveForbittenPaths::One(0))),
                &(NodeIdT(0), EdgeIdT(1))
            ),
            None
        );
    }

    #[test]
    fn test_merging() {
        let mut ops = BlockedPathsDijkstra::new(0, 0);
        let mut current = MultiCritNodeData::<ForbiddenPathLabel>::new();
        current.push(NodeQueueLabelOrder((
            10,
            ActiveForbittenPaths::One(1),
            (NodeIdT(1), ActiveForbittenPaths::One(0)),
        )));
        let improved = MultiCritDijkstraOps::<OwnedGraph>::merge(
            &mut ops,
            &mut current,
            Some((10, ActiveForbittenPaths::One(0), (NodeIdT(2), ActiveForbittenPaths::One(0)))),
        );
        assert_eq!(improved, Some(10));
        assert!(matches!(
            current.pop(),
            Some(&NodeQueueLabelOrder((
                10,
                ActiveForbittenPaths::One(0),
                (NodeIdT(2), ActiveForbittenPaths::One(0))
            )))
        ));
        assert!(current.peek().is_none());
    }
}
