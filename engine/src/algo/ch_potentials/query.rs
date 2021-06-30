use super::*;

use crate::datastr::rank_select_map::FastClearBitVec;
use crate::{
    algo::{a_star::*, dijkstra::gen_topo_dijkstra::*, topocore::*},
    datastr::graph::time_dependent::*,
};

pub struct Server<Graph, Ops, P, const BCC_CORE: bool, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool>
where
    Ops: DijkstraOps<Graph>,
{
    core_search: SkipLowDegServer<VirtualTopocoreGraph<Graph>, VirtualTopocoreOps<Ops>, PotentialForPermutated<P>, SKIP_DEG_2, SKIP_DEG_3>,
    comp_graph: VirtualTopocoreGraph<Graph>,

    virtual_topocore: VirtualTopocore,
}

impl<Graph, Ops: DijkstraOps<Graph, Label = Timestamp>, P: Potential> Server<Graph, Ops, P, true, true, true>
where
    Graph: LinkIterable<NodeId> + LinkIterable<Ops::Arc>,
{
    pub fn new<G>(graph: &G, potential: P, ops: Ops) -> Self
    where
        G: LinkIterable<NodeId>,
        Graph: BuildPermutated<G>,
    {
        Self::new_custom(graph, potential, ops)
    }
}

impl<Graph, Ops: DijkstraOps<Graph, Label = Timestamp>, P: Potential, const BCC_CORE: bool, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool>
    Server<Graph, Ops, P, BCC_CORE, SKIP_DEG_2, SKIP_DEG_3>
where
    Graph: LinkIterable<NodeId> + LinkIterable<Ops::Arc>,
{
    pub fn new_custom<G>(graph: &G, potential: P, ops: Ops) -> Self
    where
        G: LinkIterable<NodeId>,
        Graph: BuildPermutated<G>,
    {
        report_time_with_key("TopoDijkstra preprocessing", "topo_dijk_prepro", move || {
            let (main_graph, comp_graph, virtual_topocore) = if BCC_CORE {
                VirtualTopocoreGraph::new_topo_dijkstra_graphs(graph)
            } else {
                VirtualTopocoreGraph::new(graph)
            };

            Self {
                core_search: SkipLowDegServer::new(
                    main_graph,
                    PotentialForPermutated {
                        order: virtual_topocore.order.clone(),
                        potential,
                    },
                    VirtualTopocoreOps(ops),
                ),
                comp_graph,

                virtual_topocore,
            }
        })
    }

    fn distance<Q: GenQuery<Timestamp> + Copy>(
        &mut self,
        mut query: Q,
        mut inspect: impl FnMut(
            NodeId,
            &NodeOrder,
            &TopoDijkstraRun<VirtualTopocoreGraph<Graph>, VirtualTopocoreOps<Ops>, SKIP_DEG_2, SKIP_DEG_3>,
            &mut PotentialForPermutated<P>,
        ),
    ) -> Option<Weight> {
        query.permutate(&self.virtual_topocore.order);

        if !BCC_CORE {
            let order = &self.virtual_topocore.order;
            return self.core_search.distance(query, |n, d, p| inspect(n, order, d, p), INFINITY);
        }

        report!("algo", "Virtual Topocore Component Query");

        let virtual_topocore = &self.virtual_topocore;
        let potential = &mut self.core_search.potential;

        let departure = query.initial_state();
        let mut num_queue_pops = 0;
        potential.init(query.to());

        let into_core = virtual_topocore.bridge_node(query.from()).unwrap_or(query.from());
        let out_of_core = virtual_topocore.bridge_node(query.to()).unwrap_or(query.to());
        let into_core_pot = potential.potential(into_core)?;

        let mut comp_search = TopoDijkstraRun::query(&self.comp_graph, &mut self.core_search.dijkstra_data, &mut self.core_search.ops, query);

        while let Some(node) = comp_search.next_step_with_potential(|node| potential.potential(node)) {
            num_queue_pops += 1;
            inspect(node, &virtual_topocore.order, &comp_search, potential);

            if comp_search
                .queue()
                .peek()
                .map(|e| e.key >= *comp_search.tentative_distance(into_core) + into_core_pot)
                .unwrap_or(true)
            {
                break;
            }
        }

        if *comp_search.tentative_distance(into_core) >= INFINITY {
            return None;
        }

        let to_core_pushs = comp_search.num_queue_pushs();
        let to_core_relaxed = comp_search.num_relaxed_arcs();

        let core_dist = {
            let _ctxt = push_context("core_search".to_string());
            let core_query = Q::new(into_core, out_of_core, *comp_search.tentative_distance(into_core));
            let mut core_search = TopoDijkstraRun::continue_query(
                &self.core_search.graph,
                &mut self.core_search.dijkstra_data,
                &mut self.core_search.ops,
                into_core,
            );
            SkipLowDegServer::distance_manually_initialized(
                &mut core_search,
                core_query,
                potential,
                &mut self.core_search.visited,
                &self.core_search.reversed,
                |n, d, p| inspect(n, &virtual_topocore.order, d, p),
                INFINITY,
            )
        };

        let mut comp_search = TopoDijkstraRun::continue_query(
            &self.comp_graph,
            &mut self.core_search.dijkstra_data,
            &mut self.core_search.ops,
            if core_dist.is_some() { out_of_core } else { query.from() },
        );

        while let Some(node) = comp_search.next_step_with_potential(|node| potential.potential(node)) {
            num_queue_pops += 1;
            inspect(node, &virtual_topocore.order, &comp_search, potential);

            if comp_search
                .queue()
                .peek()
                .map(|e| e.key >= *comp_search.tentative_distance(query.to()))
                .unwrap_or(true)
            {
                break;
            }
        }

        report!("num_queue_pops", num_queue_pops);
        report!("num_queue_pushs", to_core_pushs + comp_search.num_queue_pushs());
        report!("num_relaxed_arcs", to_core_relaxed + comp_search.num_relaxed_arcs());

        let dist = *comp_search.tentative_distance(query.to());
        if dist < INFINITY {
            Some(dist - departure)
        } else {
            None
        }
    }

    pub fn visualize_query(&mut self, query: impl GenQuery<Timestamp> + Copy, lat: &[f32], lng: &[f32]) -> Option<Weight> {
        let mut num_settled_nodes = 0;
        let res = self.distance(query, |node, order, dijk, pot| {
            let node_id = order.node(node) as usize;
            println!(
                "var marker = L.marker([{}, {}], {{ icon: L.dataIcon({{ data: {{ popped: {} }}, ...blueIconOptions }}) }}).addTo(map);",
                lat[node_id], lng[node_id], num_settled_nodes
            );
            println!(
                "marker.bindPopup(\"id: {}<br>distance: {}<br>potential: {}\");",
                node_id,
                dijk.tentative_distance(node),
                pot.potential(node_id as NodeId).unwrap()
            );
            num_settled_nodes += 1;
        });
        println!(
            "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
            lat[query.from() as usize],
            lng[query.from() as usize]
        );
        println!(
            "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
            lat[query.to() as usize],
            lng[query.to() as usize]
        );
        res
    }

    fn path(&self, mut query: impl GenQuery<Timestamp>) -> Vec<NodeId> {
        query.permutate(&self.virtual_topocore.order);
        let mut path = Vec::new();
        path.push(query.to());

        while *path.last().unwrap() != query.from() {
            let next = self.core_search.dijkstra_data.predecessors[*path.last().unwrap() as usize];
            path.push(next);
        }

        path.reverse();
        for node in &mut path {
            *node = self.virtual_topocore.order.node(*node);
        }

        path
    }

    fn tentative_distance(&self, node: NodeId) -> Weight {
        let rank = self.virtual_topocore.order.rank(node);
        self.core_search.dijkstra_data.distances[rank as usize]
    }

    fn predecessor(&self, node: NodeId) -> NodeId {
        let rank = self.virtual_topocore.order.rank(node);
        self.core_search.dijkstra_data.predecessors[rank as usize]
    }
}

pub struct PathServerWrapper<'s, G, O: DijkstraOps<G>, P, Q, const BCC_CORE: bool, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool>(
    &'s mut Server<G, O, P, BCC_CORE, SKIP_DEG_2, SKIP_DEG_3>,
    Q,
);

impl<'s, G, O, P, Q, const BCC_CORE: bool, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool> PathServer
    for PathServerWrapper<'s, G, O, P, Q, BCC_CORE, SKIP_DEG_2, SKIP_DEG_3>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: LinkIterable<NodeId> + LinkIterable<O::Arc>,
    Q: GenQuery<Timestamp> + Copy,
{
    type NodeInfo = NodeId;

    fn reconstruct_path(&mut self) -> Vec<Self::NodeInfo> {
        Server::path(self.0, self.1)
    }
}

impl<'s, G, O, P, Q, const BCC_CORE: bool, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool> PathServerWrapper<'s, G, O, P, Q, BCC_CORE, SKIP_DEG_2, SKIP_DEG_3>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: LinkIterable<NodeId> + LinkIterable<O::Arc>,
    Q: GenQuery<Timestamp> + Copy,
{
    /// Print path with debug info as js to stdout.
    pub fn debug_path(&mut self, lat: &[f32], lng: &[f32]) {
        for node in self.reconstruct_path() {
            println!(
                "var marker = L.marker([{}, {}], {{ icon: blackIcon }}).addTo(map);",
                lat[node as usize], lng[node as usize]
            );
            let dist = self.distance(node);
            let pot = self.lower_bound(node).unwrap_or(INFINITY);
            println!(
                "marker.bindPopup(\"id: {}<br>distance: {}<br>lower_bound: {}<br>sum: {}\");",
                node,
                dist / 1000,
                pot / 1000,
                (pot + dist) / 1000
            );
        }
    }

    pub fn distance(&self, node: NodeId) -> Weight {
        self.0.tentative_distance(node)
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.0.predecessor(node)
    }

    pub fn potential(&self) -> &P {
        &self.0.core_search.potential.potential
    }

    pub fn lower_bound(&mut self, node: NodeId) -> Option<Weight> {
        self.0.core_search.potential.potential(node)
    }

    pub fn query(&self) -> &Q {
        &self.1
    }
}

impl<G, O, P, const BCC_CORE: bool, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool> TDQueryServer<Timestamp, Weight>
    for Server<G, O, P, BCC_CORE, SKIP_DEG_2, SKIP_DEG_3>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: LinkIterable<NodeId> + LinkIterable<O::Arc>,
{
    type P<'s>
    where
        Self: 's,
    = PathServerWrapper<'s, G, O, P, TDQuery<Timestamp>, BCC_CORE, SKIP_DEG_2, SKIP_DEG_3>;

    fn td_query(&mut self, query: TDQuery<Timestamp>) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query, |_, _, _, _| ()), PathServerWrapper(self, query))
    }
}

impl<G, O, P, const BCC_CORE: bool, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool> QueryServer for Server<G, O, P, BCC_CORE, SKIP_DEG_2, SKIP_DEG_3>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: LinkIterable<NodeId> + LinkIterable<O::Arc>,
{
    type P<'s>
    where
        Self: 's,
    = PathServerWrapper<'s, G, O, P, Query, BCC_CORE, SKIP_DEG_2, SKIP_DEG_3>;

    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query, |_, _, _, _| ()), PathServerWrapper(self, query))
    }
}

struct VirtualTopocoreOps<O>(O);

impl<G, O> DijkstraOps<VirtualTopocoreGraph<G>> for VirtualTopocoreOps<O>
where
    O: DijkstraOps<G>,
{
    type Label = O::Label;
    type Arc = O::Arc;
    type LinkResult = O::LinkResult;

    #[inline(always)]
    fn link(&mut self, graph: &VirtualTopocoreGraph<G>, label: &Self::Label, link: &Self::Arc) -> Self::LinkResult {
        self.0.link(&graph.graph, label, link)
    }

    #[inline(always)]
    fn merge(&mut self, label: &mut Self::Label, linked: Self::LinkResult) -> bool {
        self.0.merge(label, linked)
    }
}

pub struct SkipLowDegServer<Graph, Ops, P, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool>
where
    Ops: DijkstraOps<Graph>,
{
    graph: Graph,
    dijkstra_data: DijkstraData<Ops::Label>,
    ops: Ops,
    potential: P,

    reversed: UnweightedOwnedGraph,
    visited: FastClearBitVec,
}

impl<Graph, Ops: DijkstraOps<Graph, Label = Timestamp>, P: Potential, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool>
    SkipLowDegServer<Graph, Ops, P, SKIP_DEG_2, SKIP_DEG_3>
where
    Graph: LinkIterable<NodeId> + LinkIterable<Ops::Arc> + SymmetricDegreeGraph,
{
    pub fn new(graph: Graph, potential: P, ops: Ops) -> Self {
        let n = graph.num_nodes();
        let reversed = UnweightedOwnedGraph::reversed(&graph);
        Self {
            graph,
            dijkstra_data: DijkstraData::new(n),
            ops,
            potential,

            reversed,
            visited: FastClearBitVec::new(n),
        }
    }

    fn dfs(graph: &UnweightedOwnedGraph, node: NodeId, visited: &mut FastClearBitVec, in_core: &mut impl FnMut(NodeId) -> bool) {
        if visited.get(node as usize) {
            return;
        }
        visited.set(node as usize);
        if in_core(node) {
            return;
        }
        for head in graph.link_iter(node) {
            Self::dfs(graph, head, visited, in_core);
        }
    }

    pub fn distance_with_cap<Q: GenQuery<Timestamp> + Copy>(
        &mut self,
        query: Q,
        cap: Weight,
    ) -> QueryResult<BiconnectedPathServerWrapper<Graph, Ops, P, Q, SKIP_DEG_2, SKIP_DEG_3>, Weight> {
        QueryResult::new(self.distance(query, |_, _, _| (), cap), BiconnectedPathServerWrapper(self, query))
    }

    fn distance(
        &mut self,
        query: impl GenQuery<Timestamp> + Copy,
        inspect: impl FnMut(NodeId, &TopoDijkstraRun<Graph, Ops, SKIP_DEG_2, SKIP_DEG_3>, &mut P),
        cap: Weight,
    ) -> Option<Weight> {
        let mut dijkstra = TopoDijkstraRun::query(&self.graph, &mut self.dijkstra_data, &mut self.ops, query);
        self.potential.init(query.to());
        Self::distance_manually_initialized(&mut dijkstra, query, &mut self.potential, &mut self.visited, &self.reversed, inspect, cap)
    }

    fn distance_manually_initialized(
        dijkstra: &mut TopoDijkstraRun<Graph, Ops, SKIP_DEG_2, SKIP_DEG_3>,
        query: impl GenQuery<Timestamp> + Copy,
        potential: &mut P,
        visited: &mut FastClearBitVec,
        reversed: &UnweightedOwnedGraph,
        mut inspect: impl FnMut(NodeId, &TopoDijkstraRun<Graph, Ops, SKIP_DEG_2, SKIP_DEG_3>, &mut P),
        cap: Weight,
    ) -> Option<Weight> {
        report!("algo", "Virtual Topocore Core Query");
        let mut num_queue_pops = 0;

        let departure = *dijkstra.tentative_distance(query.from());
        let target_pot = potential.potential(query.to())?;

        let mut counter = 0;
        visited.clear();
        Self::dfs(reversed, query.to(), visited, &mut |_| {
            if counter < 100 {
                counter += 1;
                false
            } else {
                true
            }
        });
        if counter < 100 {
            return None;
        }

        while let Some(node) = dijkstra.next_step_with_potential(|node| potential.potential(node)) {
            num_queue_pops += 1;
            inspect(node, dijkstra, potential);

            if dijkstra
                .queue()
                .peek()
                .map(|e| e.key >= *dijkstra.tentative_distance(query.to()) + target_pot || e.key > cap)
                // .map(|e| e.key >= *dijkstra.tentative_distance(query.to()) + target_pot)
                .unwrap_or(false)
            {
                break;
            }
        }

        report!("num_queue_pops", num_queue_pops);
        report!("num_queue_pushs", dijkstra.num_queue_pushs());
        report!("num_relaxed_arcs", dijkstra.num_relaxed_arcs());

        let dist = *dijkstra.tentative_distance(query.to());
        if dist < INFINITY {
            Some(dist - departure)
        } else {
            None
        }
    }

    pub fn visualize_query(&mut self, query: impl GenQuery<Timestamp> + Copy, lat: &[f32], lng: &[f32], order: &NodeOrder) -> Option<Weight> {
        let mut num_settled_nodes = 0;
        let res = self.distance(
            query,
            |node, dijk, pot| {
                let node_id = order.node(node) as usize;
                println!(
                    "var marker = L.marker([{}, {}], {{ icon: L.dataIcon({{ data: {{ popped: {} }}, ...blueIconOptions }}) }}).addTo(map);",
                    lat[node_id], lng[node_id], num_settled_nodes
                );
                println!(
                    "marker.bindPopup(\"id: {}<br>distance: {}<br>potential: {}\");",
                    node_id,
                    dijk.tentative_distance(node),
                    pot.potential(node).unwrap()
                );
                num_settled_nodes += 1;
            },
            INFINITY,
        );
        println!(
            "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
            lat[query.from() as usize],
            lng[query.from() as usize]
        );
        println!(
            "L.marker([{}, {}], {{ title: \"from\", icon: blackIcon }}).addTo(map);",
            lat[query.to() as usize],
            lng[query.to() as usize]
        );
        res
    }

    fn path(&self, query: impl GenQuery<Timestamp>) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(query.to());

        while *path.last().unwrap() != query.from() {
            let next = self.dijkstra_data.predecessors[*path.last().unwrap() as usize];
            path.push(next);
        }

        path.reverse();

        path
    }

    pub(super) fn graph(&self) -> &Graph {
        &self.graph
    }

    pub(super) fn graph_mut(&mut self) -> &mut Graph {
        &mut self.graph
    }

    pub(super) fn potential(&self) -> &P {
        &self.potential
    }
}

pub struct BiconnectedPathServerWrapper<'s, G, O: DijkstraOps<G>, P, Q, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool>(
    &'s mut SkipLowDegServer<G, O, P, SKIP_DEG_2, SKIP_DEG_3>,
    Q,
);

impl<'s, G, O, P, Q, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool> PathServer for BiconnectedPathServerWrapper<'s, G, O, P, Q, SKIP_DEG_2, SKIP_DEG_3>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: LinkIterable<NodeId> + LinkIterable<O::Arc> + SymmetricDegreeGraph,
    Q: GenQuery<Timestamp> + Copy,
{
    type NodeInfo = NodeId;

    fn reconstruct_path(&mut self) -> Vec<Self::NodeInfo> {
        SkipLowDegServer::path(self.0, self.1)
    }
}

impl<'s, G, O, P, Q, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool> BiconnectedPathServerWrapper<'s, G, O, P, Q, SKIP_DEG_2, SKIP_DEG_3>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: LinkIterable<NodeId> + LinkIterable<O::Arc> + SymmetricDegreeGraph,
    Q: GenQuery<Timestamp> + Copy,
{
    /// Print path with debug info as js to stdout.
    pub fn debug_path(&mut self, lat: &[f32], lng: &[f32]) {
        for node in self.reconstruct_path() {
            println!(
                "var marker = L.marker([{}, {}], {{ icon: blackIcon }}).addTo(map);",
                lat[node as usize], lng[node as usize]
            );
            let dist = self.0.dijkstra_data.distances[node as usize];
            let pot = self.lower_bound(node).unwrap_or(INFINITY);
            println!(
                "marker.bindPopup(\"id: {}<br>distance: {}<br>lower_bound: {}<br>sum: {}\");",
                node,
                dist / 1000,
                pot / 1000,
                (pot + dist) / 1000
            );
        }
    }

    pub fn distance(&self, node: NodeId) -> Weight {
        self.0.dijkstra_data.distances[node as usize]
    }

    pub fn predecessor(&self, node: NodeId) -> NodeId {
        self.0.dijkstra_data.predecessors[node as usize]
    }

    pub fn potential(&self) -> &P {
        &self.0.potential
    }

    pub fn lower_bound(&mut self, node: NodeId) -> Option<Weight> {
        self.0.potential.potential(node)
    }
}

impl<G, O, P, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool> TDQueryServer<Timestamp, Weight> for SkipLowDegServer<G, O, P, SKIP_DEG_2, SKIP_DEG_3>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: LinkIterable<NodeId> + LinkIterable<O::Arc> + SymmetricDegreeGraph,
{
    type P<'s>
    where
        Self: 's,
    = BiconnectedPathServerWrapper<'s, G, O, P, TDQuery<Timestamp>, SKIP_DEG_2, SKIP_DEG_3>;

    fn td_query(&mut self, query: TDQuery<Timestamp>) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query, |_, _, _| (), INFINITY), BiconnectedPathServerWrapper(self, query))
    }
}

impl<G, O, P, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool> QueryServer for SkipLowDegServer<G, O, P, SKIP_DEG_2, SKIP_DEG_3>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: LinkIterable<NodeId> + LinkIterable<O::Arc> + SymmetricDegreeGraph,
{
    type P<'s>
    where
        Self: 's,
    = BiconnectedPathServerWrapper<'s, G, O, P, Query, SKIP_DEG_2, SKIP_DEG_3>;

    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query, |_, _, _| (), INFINITY), BiconnectedPathServerWrapper(self, query))
    }
}

use std::cell::*;

pub struct BiDirSkipLowDegServer<P = ZeroPotential> {
    forward_graph: VirtualTopocoreGraph<OwnedGraph>,
    forward_dijkstra_data: DijkstraData<Weight>,
    backward_graph: VirtualTopocoreGraph<OwnedGraph>,
    backward_dijkstra_data: DijkstraData<Weight>,
    tentative_distance: Weight,
    meeting_node: NodeId,
    forward_potential: RefCell<P>,
    backward_potential: RefCell<P>,
    forward_to_backward_edge_ids: Vec<EdgeId>,
}

impl<P: Potential> BiDirSkipLowDegServer<P> {
    pub fn new(graph: VirtualTopocoreGraph<OwnedGraph>, forward_potential: P, backward_potential: P) -> Self {
        let n = graph.num_nodes();
        let reversed = VirtualTopocoreGraph::<OwnedGraph>::reversed(&graph);

        let mut reversed_edge_ids = vec![Vec::new(); n];
        let mut edge_id: EdgeId = 0;
        for tail in 0..n {
            for head in LinkIterable::<NodeId>::link_iter(&graph, tail as NodeId) {
                reversed_edge_ids[head as usize].push(edge_id);
                edge_id += 1;
            }
        }
        let mut forward_to_backward = vec![edge_id; edge_id as usize];
        let mut backward_id = 0;
        for forward_ids in reversed_edge_ids {
            for forward_id in forward_ids {
                forward_to_backward[forward_id as usize] = backward_id;
                backward_id += 1;
            }
        }

        Self {
            forward_graph: graph,
            backward_graph: reversed,
            forward_dijkstra_data: DijkstraData::new(n),
            backward_dijkstra_data: DijkstraData::new(n),
            forward_potential: RefCell::new(forward_potential),
            backward_potential: RefCell::new(backward_potential),
            tentative_distance: INFINITY,
            meeting_node: n as NodeId,
            forward_to_backward_edge_ids: forward_to_backward,
        }
    }

    pub fn distance_with_cap<Q: GenQuery<Timestamp> + Copy>(
        &mut self,
        query: Q,
        cap: Weight,
        pot_cap: Weight,
    ) -> Option<QueryResult<BiDirCorePathServerWrapper<P, Q>, Weight>> {
        self.distance(query, cap, pot_cap)
            .map(move |distance| QueryResult::new(distance, BiDirCorePathServerWrapper(self, query)))
    }

    pub fn distance(&mut self, query: impl GenQuery<Timestamp> + Copy, cap: Weight, pot_cap: Weight) -> Option<Weight> {
        report!("algo", "Virtual Topocore Bidirectional Core Query");
        self.tentative_distance = INFINITY;

        let mut ops = DefaultOps::default();
        let mut forward_dijkstra = TopoDijkstraRun::query(&self.forward_graph, &mut self.forward_dijkstra_data, &mut ops, query);
        let mut ops = DefaultOps::default();
        let mut backward_dijkstra = TopoDijkstraRun::query(
            &self.backward_graph,
            &mut self.backward_dijkstra_data,
            &mut ops,
            Query {
                from: query.to(),
                to: query.from(),
            },
        );

        self.forward_potential.borrow_mut().init(query.to());
        self.backward_potential.borrow_mut().init(query.from());

        let mut num_queue_pops = 0;

        let meeting_node = &mut self.meeting_node;
        let tentative_distance = &mut self.tentative_distance;
        let forward_potential = &self.forward_potential;
        let backward_potential = &self.backward_potential;

        let mut dir_toggle = false;

        let result = (|| {
            while forward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY) < std::cmp::min(*tentative_distance, cap)
                || backward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY) < std::cmp::min(*tentative_distance, cap)
            {
                let stop_dist = std::cmp::min(*tentative_distance, cap);

                dir_toggle = !dir_toggle;

                if dir_toggle {
                    if let Some(node) = forward_dijkstra.next_with_improve_callback_and_potential(
                        |head, &dist| {
                            // if dist + forward_potential.borrow_mut().potential(head).unwrap_or(INFINITY) > cap {
                            //     return false;
                            // }
                            if forward_potential.borrow_mut().potential(head).unwrap_or(INFINITY)
                                + backward_potential.borrow_mut().potential(head).unwrap_or(INFINITY)
                                > pot_cap
                            {
                                return false;
                            }
                            if stop_dist < INFINITY {
                                if dist + forward_potential.borrow_mut().potential(head).unwrap_or(INFINITY) >= stop_dist {
                                    return false;
                                }
                                let remaining_by_queue = backward_dijkstra
                                    .queue()
                                    .peek()
                                    .map(|q| q.key)
                                    .unwrap_or(INFINITY)
                                    .saturating_sub(backward_potential.borrow_mut().potential(head).unwrap_or(INFINITY));
                                if dist + remaining_by_queue >= stop_dist {
                                    return false;
                                }
                            }
                            if dist + backward_dijkstra.tentative_distance(head) < *tentative_distance {
                                *tentative_distance = dist + backward_dijkstra.tentative_distance(head);
                                *meeting_node = head;
                            }
                            if *backward_dijkstra.tentative_distance(head) < INFINITY && !backward_dijkstra.queue().contains_index(head as usize) {
                                return false;
                            }
                            true
                        },
                        |node| forward_potential.borrow_mut().potential(node),
                    ) {
                        num_queue_pops += 1;
                        if node == query.to() {
                            *meeting_node = query.to();
                            return Some(*tentative_distance);
                        }
                    }
                } else {
                    if let Some(node) = backward_dijkstra.next_with_improve_callback_and_potential(
                        |head, &dist| {
                            // if dist + backward_potential.borrow_mut().potential(head).unwrap_or(INFINITY) > cap {
                            //     return false;
                            // }
                            if forward_potential.borrow_mut().potential(head).unwrap_or(INFINITY)
                                + backward_potential.borrow_mut().potential(head).unwrap_or(INFINITY)
                                > pot_cap
                            {
                                return false;
                            }
                            if stop_dist < INFINITY {
                                if dist + backward_potential.borrow_mut().potential(head).unwrap_or(INFINITY) >= stop_dist {
                                    return false;
                                }
                                let remaining_by_queue = forward_dijkstra
                                    .queue()
                                    .peek()
                                    .map(|q| q.key)
                                    .unwrap_or(INFINITY)
                                    .saturating_sub(forward_potential.borrow_mut().potential(head).unwrap_or(INFINITY));
                                if dist + remaining_by_queue >= stop_dist {
                                    return false;
                                }
                            }
                            if dist + forward_dijkstra.tentative_distance(head) < *tentative_distance {
                                *tentative_distance = dist + forward_dijkstra.tentative_distance(head);
                                *meeting_node = head;
                            }
                            if *forward_dijkstra.tentative_distance(head) < INFINITY && !forward_dijkstra.queue().contains_index(head as usize) {
                                return false;
                            }
                            true
                        },
                        |node| backward_potential.borrow_mut().potential(node),
                    ) {
                        num_queue_pops += 1;
                        if node == query.from() {
                            *meeting_node = query.from();
                            return Some(*tentative_distance);
                        }
                    }
                }
            }

            match *tentative_distance {
                INFINITY => None,
                dist => Some(dist),
            }
        })();

        report!("num_queue_pops", num_queue_pops);
        report!("num_queue_pushs", forward_dijkstra.num_queue_pushs() + backward_dijkstra.num_queue_pushs());
        report!("num_relaxed_arcs", forward_dijkstra.num_relaxed_arcs() + backward_dijkstra.num_queue_pushs());

        result
    }

    fn path(&self, query: impl GenQuery<Timestamp>) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(self.meeting_node);

        while *path.last().unwrap() != query.from() {
            let next = self.forward_dijkstra_data.predecessors[*path.last().unwrap() as usize];
            path.push(next);
        }

        path.reverse();

        while *path.last().unwrap() != query.to() {
            let next = self.backward_dijkstra_data.predecessors[*path.last().unwrap() as usize];
            path.push(next);
        }

        path
    }

    pub(super) fn graph(&self) -> &OwnedGraph {
        &self.forward_graph.graph
    }

    pub(super) fn tail(&self, edge: EdgeId) -> NodeId {
        self.backward_graph.graph.head()[self.forward_to_backward_edge_ids[edge as usize] as usize]
    }

    pub(super) fn set_edge_weight(&mut self, edge: EdgeId, weight: Weight) {
        self.forward_graph.graph.weights_mut()[edge as usize] = weight;
        self.backward_graph.graph.weights_mut()[self.forward_to_backward_edge_ids[edge as usize] as usize] = weight;
    }

    pub(super) fn forward_potential(&self) -> Ref<P> {
        self.forward_potential.borrow()
    }

    pub(super) fn backward_potential(&self) -> Ref<P> {
        self.backward_potential.borrow()
    }
}

pub struct BiDirCorePathServerWrapper<'s, P, Q>(&'s mut BiDirSkipLowDegServer<P>, Q);

impl<'s, P, Q> PathServer for BiDirCorePathServerWrapper<'s, P, Q>
where
    P: Potential,
    Q: GenQuery<Timestamp> + Copy,
{
    type NodeInfo = NodeId;

    fn path(&mut self) -> Vec<Self::NodeInfo> {
        BiDirSkipLowDegServer::path(self.0, self.1)
    }
}

impl<'s, P: 's> QueryServer<'s> for BiDirSkipLowDegServer<P>
where
    P: Potential,
{
    type P = BiDirCorePathServerWrapper<'s, P, Query>;

    fn query(&'s mut self, query: Query) -> Option<QueryResult<Self::P, Weight>> {
        self.distance(query, INFINITY, INFINITY)
            .map(move |distance| QueryResult::new(distance, BiDirCorePathServerWrapper(self, query)))
    }
}
