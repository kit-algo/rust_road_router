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
    Graph: LinkIterable<NodeIdT> + LinkIterable<Ops::Arc>,
{
    pub fn new<G>(graph: &G, potential: P, ops: Ops) -> Self
    where
        G: LinkIterable<NodeIdT>,
        Graph: BuildPermutated<G>,
    {
        Self::new_custom(graph, potential, ops)
    }
}

impl<Graph, Ops: DijkstraOps<Graph, Label = Timestamp>, P: Potential, const BCC_CORE: bool, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool>
    Server<Graph, Ops, P, BCC_CORE, SKIP_DEG_2, SKIP_DEG_3>
where
    Graph: LinkIterable<NodeIdT> + LinkIterable<Ops::Arc>,
{
    pub fn new_custom<G>(graph: &G, potential: P, ops: Ops) -> Self
    where
        G: LinkIterable<NodeIdT>,
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

    fn node_path(&self, mut query: impl GenQuery<Timestamp>) -> Vec<NodeId> {
        query.permutate(&self.virtual_topocore.order);
        let mut path = self.core_search.dijkstra_data.node_path(query.from(), query.to());

        for node in &mut path {
            *node = self.virtual_topocore.order.node(*node);
        }

        path
    }

    fn edge_path(&self, mut query: impl GenQuery<Weight>) -> Vec<Ops::PredecessorLink> {
        query.permutate(&self.virtual_topocore.order);
        self.core_search.dijkstra_data.edge_path(query.from(), query.to())
    }

    fn tentative_distance(&self, node: NodeId) -> Weight {
        let rank = self.virtual_topocore.order.rank(node);
        self.core_search.dijkstra_data.distances[rank as usize]
    }

    fn predecessor(&self, node: NodeId) -> NodeId {
        let rank = self.virtual_topocore.order.rank(node);
        self.core_search.dijkstra_data.predecessors[rank as usize].0
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
    G: LinkIterable<NodeIdT> + LinkIterable<O::Arc>,
    Q: GenQuery<Timestamp> + Copy,
{
    type NodeInfo = NodeId;
    type EdgeInfo = O::PredecessorLink;

    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
        Server::node_path(self.0, self.1)
    }
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
        Server::edge_path(self.0, self.1)
    }
}

impl<'s, G, O, P, Q, const BCC_CORE: bool, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool> PathServerWrapper<'s, G, O, P, Q, BCC_CORE, SKIP_DEG_2, SKIP_DEG_3>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: LinkIterable<NodeIdT> + LinkIterable<O::Arc>,
    Q: GenQuery<Timestamp> + Copy,
{
    /// Print path with debug info as js to stdout.
    pub fn debug_path(&mut self, lat: &[f32], lng: &[f32]) {
        for node in self.reconstruct_node_path() {
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
    G: LinkIterable<NodeIdT> + LinkIterable<O::Arc>,
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
    G: LinkIterable<NodeIdT> + LinkIterable<O::Arc>,
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
    type PredecessorLink = O::PredecessorLink;

    #[inline(always)]
    fn link(&mut self, graph: &VirtualTopocoreGraph<G>, label: &Self::Label, link: &Self::Arc) -> Self::LinkResult {
        self.0.link(&graph.graph, label, link)
    }

    #[inline(always)]
    fn merge(&mut self, label: &mut Self::Label, linked: Self::LinkResult) -> bool {
        self.0.merge(label, linked)
    }

    #[inline(always)]
    fn predecessor_link(&self, link: &Self::Arc) -> Self::PredecessorLink {
        self.0.predecessor_link(link)
    }
}

pub struct SkipLowDegServer<Graph, Ops, P, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool>
where
    Ops: DijkstraOps<Graph>,
{
    graph: Graph,
    dijkstra_data: DijkstraData<Ops::Label, Ops::PredecessorLink>,
    ops: Ops,
    potential: P,

    reversed: UnweightedOwnedGraph,
    visited: FastClearBitVec,
}

impl<Graph, Ops: DijkstraOps<Graph, Label = Timestamp>, P: Potential, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool>
    SkipLowDegServer<Graph, Ops, P, SKIP_DEG_2, SKIP_DEG_3>
where
    Graph: LinkIterable<NodeIdT> + LinkIterable<Ops::Arc> + SymmetricDegreeGraph,
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
        for NodeIdT(head) in LinkIterable::<NodeIdT>::link_iter(graph, node) {
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

    fn node_path(&self, query: impl GenQuery<Timestamp>) -> Vec<NodeId> {
        self.dijkstra_data.node_path(query.from(), query.to())
    }

    fn edge_path(&self, query: impl GenQuery<Timestamp>) -> Vec<Ops::PredecessorLink> {
        self.dijkstra_data.edge_path(query.from(), query.to())
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
    G: LinkIterable<NodeIdT> + LinkIterable<O::Arc> + SymmetricDegreeGraph,
    Q: GenQuery<Timestamp> + Copy,
{
    type NodeInfo = NodeId;
    type EdgeInfo = O::PredecessorLink;

    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
        SkipLowDegServer::node_path(self.0, self.1)
    }
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
        SkipLowDegServer::edge_path(self.0, self.1)
    }
}

impl<'s, G, O, P, Q, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool> BiconnectedPathServerWrapper<'s, G, O, P, Q, SKIP_DEG_2, SKIP_DEG_3>
where
    P: Potential,
    O: DijkstraOps<G, Label = Timestamp>,
    G: LinkIterable<NodeIdT> + LinkIterable<O::Arc> + SymmetricDegreeGraph,
    Q: GenQuery<Timestamp> + Copy,
{
    /// Print path with debug info as js to stdout.
    pub fn debug_path(&mut self, lat: &[f32], lng: &[f32]) {
        for node in self.reconstruct_node_path() {
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
        self.0.dijkstra_data.predecessors[node as usize].0
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
    G: LinkIterable<NodeIdT> + LinkIterable<O::Arc> + SymmetricDegreeGraph,
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
    G: LinkIterable<NodeIdT> + LinkIterable<O::Arc> + SymmetricDegreeGraph,
{
    type P<'s>
    where
        Self: 's,
    = BiconnectedPathServerWrapper<'s, G, O, P, Query, SKIP_DEG_2, SKIP_DEG_3>;

    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query, |_, _, _| (), INFINITY), BiconnectedPathServerWrapper(self, query))
    }
}
