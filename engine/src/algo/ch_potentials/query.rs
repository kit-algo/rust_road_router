use super::*;

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
        report_time_with_key("TopoDijkstra preprocessing", "topo_dijk_prepro_running_time_ms", move || {
            let (main_graph, comp_graph, virtual_topocore) = if BCC_CORE {
                VirtualTopocoreGraph::new_into_core_and_component(graph)
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
        let mut num_queue_pops: usize = 0;
        potential.init(query.to());
        report!("lower_bound", potential.potential(query.from()).unwrap_or(INFINITY));

        let out_of_core = virtual_topocore.bridge_node(query.to()).unwrap_or(query.to());

        let core_dist = {
            let _ctxt = push_context("core_search");
            let core_query = Q::new(query.from(), out_of_core, query.initial_state());
            let mut core_search = TopoDijkstraRun::query(
                &self.core_search.graph,
                &mut self.core_search.dijkstra_data,
                &mut self.core_search.ops,
                DijkstraInit::from_query(&core_query),
            );
            SkipLowDegServer::distance_manually_initialized(
                &mut core_search,
                core_query,
                potential,
                |n, d, p| inspect(n, &virtual_topocore.order, d, p),
                INFINITY,
            )
        };

        core_dist?;
        // if core_dist.is_none() {
        //     return None;
        // }

        let mut comp_search = TopoDijkstraRun::continue_query(&self.comp_graph, &mut self.core_search.dijkstra_data, &mut self.core_search.ops, out_of_core);

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

        // report!("num_queue_pops", num_queue_pops);
        // report!("num_queue_pushs", to_core_pushs + comp_search.num_queue_pushs());
        // report!("num_relaxed_arcs", to_core_relaxed + comp_search.num_relaxed_arcs());

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

    pub fn ops(&mut self) -> &mut Ops {
        &mut self.core_search.ops.0
    }

    pub fn order(&self) -> &NodeOrder {
        &self.virtual_topocore.order
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
        self.0.core_search.potential.potential.potential(node)
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
    fn link(
        &mut self,
        graph: &VirtualTopocoreGraph<G>,
        parents: &[(NodeId, Self::PredecessorLink)],
        tail: NodeIdT,
        label: &Self::Label,
        link: &Self::Arc,
    ) -> Self::LinkResult {
        self.0.link(&graph.graph, parents, tail, label, link)
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
}

impl<Graph, Ops: DijkstraOps<Graph, Label = Timestamp>, P: Potential, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool>
    SkipLowDegServer<Graph, Ops, P, SKIP_DEG_2, SKIP_DEG_3>
where
    Graph: LinkIterable<NodeIdT> + LinkIterable<Ops::Arc> + SymmetricDegreeGraph,
{
    pub fn new(graph: Graph, potential: P, ops: Ops) -> Self {
        let n = graph.num_nodes();
        Self {
            graph,
            dijkstra_data: DijkstraData::new(n),
            ops,
            potential,
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
        let mut dijkstra = TopoDijkstraRun::query(&self.graph, &mut self.dijkstra_data, &mut self.ops, DijkstraInit::from_query(&query));
        self.potential.init(query.to());
        Self::distance_manually_initialized(&mut dijkstra, query, &mut self.potential, inspect, cap)
    }

    fn distance_manually_initialized(
        dijkstra: &mut TopoDijkstraRun<Graph, Ops, SKIP_DEG_2, SKIP_DEG_3>,
        query: impl GenQuery<Timestamp> + Copy,
        potential: &mut P,
        mut inspect: impl FnMut(NodeId, &TopoDijkstraRun<Graph, Ops, SKIP_DEG_2, SKIP_DEG_3>, &mut P),
        cap: Weight,
    ) -> Option<Weight> {
        report!("algo", "Virtual Topocore Core Query");
        let mut num_queue_pops = 0;

        let departure = *dijkstra.tentative_distance(query.from());
        let target_pot = potential.potential(query.to())?;

        while let Some(node) = dijkstra.next_step_with_potential(|node| potential.potential(node)) {
            num_queue_pops += 1;
            inspect(node, dijkstra, potential);

            if dijkstra
                .queue()
                .peek()
                .map(|e| e.key >= *dijkstra.tentative_distance(query.to()) + target_pot || e.key > cap)
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

    pub fn one_to_all(&mut self, from: NodeId) -> BiconnectedPathServerWrapper<Graph, Ops, P, Query, SKIP_DEG_2, SKIP_DEG_3> {
        let mut dijkstra =
            TopoDijkstraRun::<Graph, Ops, SKIP_DEG_2, SKIP_DEG_3>::query(&self.graph, &mut self.dijkstra_data, &mut self.ops, DijkstraInit::from(from));
        while let Some(_) = dijkstra.next_step() {}
        BiconnectedPathServerWrapper(
            self,
            Query {
                from,
                to: self.graph.num_nodes() as NodeId,
            },
        )
    }

    fn node_path(&self, query: impl GenQuery<Timestamp>) -> Vec<NodeId> {
        self.dijkstra_data.node_path(query.from(), query.to())
    }

    fn edge_path(&self, query: impl GenQuery<Timestamp>) -> Vec<Ops::PredecessorLink> {
        self.dijkstra_data.edge_path(query.from(), query.to())
    }

    pub fn graph(&self) -> &Graph {
        &self.graph
    }

    pub(super) fn graph_mut(&mut self) -> &mut Graph {
        &mut self.graph
    }

    pub fn potential(&self) -> &P {
        &self.potential
    }
}

impl<Ops: DijkstraOps<VirtualTopocoreGraph<OwnedGraph>, Label = Timestamp>, P, const SKIP_DEG_2: bool, const SKIP_DEG_3: bool>
    SkipLowDegServer<VirtualTopocoreGraph<OwnedGraph>, Ops, P, SKIP_DEG_2, SKIP_DEG_3>
{
    #[allow(unused)]
    pub(super) fn set_edge_weight(&mut self, edge: EdgeId, weight: Weight) {
        self.graph.graph.weights_mut()[edge as usize] = weight;
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

    pub fn graph(&self) -> &G {
        self.0.graph()
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

use std::cell::*;
use std::marker::PhantomData;
use std::sync::atomic::AtomicU32;

pub struct BiDirServer<P = ZeroPotential, D = ChooseMinKeyDir> {
    runner: BiDirSkipLowDegRunner<PotentialForPermutated<P>, D>,
    virtual_topocore: VirtualTopocore,
}

impl<P: BiDirPotential, D: BidirChooseDir> BiDirServer<P, D> {
    pub fn new<G>(graph: &G, potential: P) -> Self
    where
        G: LinkIterable<NodeIdT>,
        OwnedGraph: BuildReversed<G>,
        OwnedGraph: BuildPermutated<G>,
    {
        let n = graph.num_nodes();
        let (fw, bw, virtual_topocore) = VirtualTopocoreGraph::new_bidir_graphs(graph);
        Self {
            runner: BiDirSkipLowDegRunner {
                forward_graph: fw,
                backward_graph: bw,
                forward_dijkstra_data: DijkstraData::new(n),
                backward_dijkstra_data: DijkstraData::new(n),
                potential: PotentialForPermutated {
                    order: virtual_topocore.order.clone(),
                    potential,
                },
                meeting_node: n as NodeId,
                dir_chooser: Default::default(),
            },
            virtual_topocore,
        }
    }

    pub fn potentials(&self) -> &P {
        self.runner.potential.inner()
    }
}

pub struct BiDirPathServerWrapper<'s, P, D, Q>(&'s mut BiDirServer<P, D>, Q);

impl<'s, P, D, Q> PathServer for BiDirPathServerWrapper<'s, P, D, Q>
where
    P: BiDirPotential,
    D: BidirChooseDir,
    Q: GenQuery<Timestamp> + Copy,
{
    type NodeInfo = NodeId;
    type EdgeInfo = ();

    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
        let mut p = BiDirSkipLowDegRunner::path(&self.0.runner, self.1);
        for n in &mut p {
            *n = self.0.virtual_topocore.order.node(*n);
        }
        p
    }
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
        vec![(); self.reconstruct_node_path().len() - 1]
    }
}

impl<'s, P, D, Q> BiDirPathServerWrapper<'s, P, D, Q>
where
    P: BiDirPotential,
    D: BidirChooseDir,
    Q: GenQuery<Timestamp> + Copy,
{
    pub fn potential(&self) -> &P {
        &self.0.runner.potential.potential
    }

    pub fn lower_bound(&mut self, node: NodeId) -> Option<Weight> {
        self.0.runner.potential.potential.forward_potential_raw(node)
    }
}

impl<P, D> QueryServer for BiDirServer<P, D>
where
    P: BiDirPotential,
    D: BidirChooseDir,
{
    type P<'s>
    where
        Self: 's,
    = BiDirPathServerWrapper<'s, P, D, Query>;

    fn query(&mut self, mut query: Query) -> QueryResult<Self::P<'_>, Weight> {
        query.permutate(&self.virtual_topocore.order);
        QueryResult::new(self.runner.distance(query, INFINITY, None), BiDirPathServerWrapper(self, query))
    }
}

pub struct BiDirCoreServer<P = ZeroPotential, D = ChooseMinKeyDir> {
    runner: BiDirSkipLowDegRunner<P, D>,
    forward_to_backward_edge_ids: Vec<EdgeId>,
    backward_to_forward_edge_ids: Vec<EdgeId>,
}

impl<P: BiDirPotential, D: BidirChooseDir> BiDirCoreServer<P, D> {
    pub fn new(graph: VirtualTopocoreGraph<OwnedGraph>, potential: P) -> Self {
        let n = graph.num_nodes();
        let reversed = VirtualTopocoreGraph::<OwnedGraph>::reversed(&graph);

        let mut reversed_edge_ids = vec![Vec::new(); n];
        let mut edge_id: EdgeId = 0;
        for tail in 0..n {
            for NodeIdT(head) in LinkIterable::<NodeIdT>::link_iter(&graph, tail as NodeId) {
                reversed_edge_ids[head as usize].push(edge_id);
                edge_id += 1;
            }
        }
        let mut forward_to_backward = vec![edge_id; edge_id as usize];
        let mut backward_to_forward = vec![edge_id; edge_id as usize];
        let mut backward_id = 0;
        for forward_ids in reversed_edge_ids {
            for forward_id in forward_ids {
                forward_to_backward[forward_id as usize] = backward_id;
                backward_to_forward[backward_id as usize] = forward_id;
                backward_id += 1;
            }
        }

        Self {
            runner: BiDirSkipLowDegRunner {
                forward_graph: graph,
                backward_graph: reversed,
                forward_dijkstra_data: DijkstraData::new(n),
                backward_dijkstra_data: DijkstraData::new(n),
                potential,
                meeting_node: n as NodeId,
                dir_chooser: Default::default(),
            },
            forward_to_backward_edge_ids: forward_to_backward,
            backward_to_forward_edge_ids: backward_to_forward,
        }
    }

    pub fn distance_with_cap<Q: GenQuery<Timestamp> + Copy>(
        &mut self,
        query: Q,
        cap: Weight,
        pot_cap: Option<Weight>,
    ) -> QueryResult<BiDirCorePathServerWrapper<P, D, Q>, Weight> {
        QueryResult::new(self.runner.distance(query, cap, pot_cap), BiDirCorePathServerWrapper(self, query))
    }

    fn edge_path(&self, query: impl GenQuery<Timestamp>) -> Vec<EdgeIdT> {
        let mut path = Vec::new();
        let mut cur = self.runner.meeting_node;

        while cur != query.from() {
            path.push(self.runner.forward_dijkstra_data.predecessors[cur as usize].1);
            cur = self.runner.forward_dijkstra_data.predecessors[cur as usize].0;
        }

        path.reverse();
        cur = self.runner.meeting_node;

        while cur != query.to() {
            path.push(EdgeIdT(
                self.backward_to_forward_edge_ids[self.runner.backward_dijkstra_data.predecessors[cur as usize].1 .0 as usize],
            ));
            cur = self.runner.backward_dijkstra_data.predecessors[cur as usize].0;
        }

        path
    }

    pub fn graph(&self) -> &VirtualTopocoreGraph<OwnedGraph> {
        &self.runner.forward_graph
    }

    #[allow(unused)]
    pub(super) fn set_edge_weight(&mut self, edge: EdgeId, weight: Weight) {
        self.runner.forward_graph.graph.weights_mut()[edge as usize] = weight;
        self.runner.backward_graph.graph.weights_mut()[self.forward_to_backward_edge_ids[edge as usize] as usize] = weight;
    }

    pub fn potential(&self) -> &P {
        &self.runner.potential
    }
}

pub struct BiDirCorePathServerWrapper<'s, P, D, Q>(&'s mut BiDirCoreServer<P, D>, Q);

impl<'s, P, D, Q> PathServer for BiDirCorePathServerWrapper<'s, P, D, Q>
where
    P: BiDirPotential,
    D: BidirChooseDir,
    Q: GenQuery<Timestamp> + Copy,
{
    type NodeInfo = NodeId;
    type EdgeInfo = EdgeIdT;

    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
        BiDirSkipLowDegRunner::path(&self.0.runner, self.1)
    }
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
        BiDirCoreServer::edge_path(self.0, self.1)
    }
}

impl<P, D> QueryServer for BiDirCoreServer<P, D>
where
    P: BiDirPotential,
    D: BidirChooseDir,
{
    type P<'s>
    where
        Self: 's,
    = BiDirCorePathServerWrapper<'s, P, D, Query>;

    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.runner.distance(query, INFINITY, None), BiDirCorePathServerWrapper(self, query))
    }
}

struct BiDirSkipLowDegRunner<P = BiDirZeroPot, D = ChooseMinKeyDir> {
    forward_graph: VirtualTopocoreGraph<OwnedGraph>,
    forward_dijkstra_data: DijkstraData<Weight, EdgeIdT>,
    backward_graph: VirtualTopocoreGraph<OwnedGraph>,
    backward_dijkstra_data: DijkstraData<Weight, EdgeIdT>,
    meeting_node: NodeId,
    potential: P,
    dir_chooser: PhantomData<D>,
}

impl<P: BiDirPotential, D: BidirChooseDir> BiDirSkipLowDegRunner<P, D> {
    fn distance(&mut self, query: impl GenQuery<Timestamp> + Copy, cap: Weight, pot_cap: Option<Weight>) -> Option<Weight> {
        use std::cmp::min;

        report!("algo", "Virtual Topocore Bidirectional Core Query");
        D::report();
        P::report();

        let mut ops = DefaultOpsWithLinkPath::default();
        let mut forward_dijkstra =
            TopoDijkstraRun::<_, _, true, true>::query(&self.forward_graph, &mut self.forward_dijkstra_data, &mut ops, DijkstraInit::from(query.from()));
        let mut ops = DefaultOpsWithLinkPath::default();
        let mut backward_dijkstra =
            TopoDijkstraRun::<_, _, true, true>::query(&self.backward_graph, &mut self.backward_dijkstra_data, &mut ops, DijkstraInit::from(query.to()));

        self.potential.init(query.from(), query.to());
        report!("lower_bound", self.potential.forward_potential_raw(query.from()).unwrap_or(INFINITY));

        let mut num_queue_pops = 0;

        let meeting_node = &mut self.meeting_node;
        let mut tentative_distance = INFINITY;
        let mut potential = RefCell::new(&mut self.potential);
        let mut dir_chooser: D = Default::default();

        let result = (|| {
            while !(dir_chooser.may_stop()
                && potential.get_mut().stop(
                    forward_dijkstra.queue().peek().map(|q| q.key),
                    backward_dijkstra.queue().peek().map(|q| q.key),
                    min(tentative_distance, cap),
                ))
            {
                let stop_dist = min(tentative_distance, cap);

                if dir_chooser.choose(forward_dijkstra.queue().peek().map(|q| q.key), backward_dijkstra.queue().peek().map(|q| q.key)) {
                    if let Some(node) = forward_dijkstra.next_with_improve_callback_and_potential(
                        |head, &dist| {
                            let mut pot = potential.borrow_mut();
                            if let Some(pot_cap) = pot_cap {
                                if pot.forward_potential_raw(head).unwrap_or(INFINITY) + pot.backward_potential_raw(head).unwrap_or(INFINITY) > pot_cap {
                                    return false;
                                }
                            }
                            if pot.prune_forward(
                                NodeIdT(head),
                                dist,
                                backward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY),
                                stop_dist,
                            ) {
                                return false;
                            }
                            if dist + backward_dijkstra.tentative_distance(head) < tentative_distance {
                                tentative_distance = dist + backward_dijkstra.tentative_distance(head);
                                *meeting_node = head;
                            }
                            true
                        },
                        |node| potential.borrow_mut().forward_potential(node),
                    ) {
                        num_queue_pops += 1;
                        if node == query.to() {
                            *meeting_node = query.to();
                            return Some(*forward_dijkstra.tentative_distance(query.to()));
                        }
                    }
                } else {
                    if let Some(node) = backward_dijkstra.next_with_improve_callback_and_potential(
                        |head, &dist| {
                            let mut pot = potential.borrow_mut();
                            if let Some(pot_cap) = pot_cap {
                                if pot.forward_potential_raw(head).unwrap_or(INFINITY) + pot.backward_potential_raw(head).unwrap_or(INFINITY) > pot_cap {
                                    return false;
                                }
                            }
                            if pot.prune_backward(
                                NodeIdT(head),
                                dist,
                                forward_dijkstra.queue().peek().map(|q| q.key).unwrap_or(INFINITY),
                                stop_dist,
                            ) {
                                return false;
                            }
                            if dist + forward_dijkstra.tentative_distance(head) < tentative_distance {
                                tentative_distance = dist + forward_dijkstra.tentative_distance(head);
                                *meeting_node = head;
                            }
                            true
                        },
                        |node| potential.borrow_mut().backward_potential(node),
                    ) {
                        num_queue_pops += 1;
                        if node == query.from() {
                            *meeting_node = query.from();
                            return Some(*backward_dijkstra.tentative_distance(query.from()));
                        }
                    }
                }
            }

            if *forward_dijkstra.tentative_distance(query.to()) < tentative_distance {
                *meeting_node = query.to();
                tentative_distance = *forward_dijkstra.tentative_distance(query.to());
            }
            if *backward_dijkstra.tentative_distance(query.from()) < tentative_distance {
                *meeting_node = query.from();
                tentative_distance = *backward_dijkstra.tentative_distance(query.from());
            }

            match tentative_distance {
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
            let next = self.forward_dijkstra_data.predecessors[*path.last().unwrap() as usize].0;
            path.push(next);
        }

        path.reverse();

        while *path.last().unwrap() != query.to() {
            let next = self.backward_dijkstra_data.predecessors[*path.last().unwrap() as usize].0;
            path.push(next);
        }

        path
    }
}

pub struct MultiThreadedBiDirSkipLowDegServer<P = BiDirZeroPot> {
    forward_graph: VirtualTopocoreGraph<OwnedGraph>,
    forward_dijkstra_data: SyncDijkstraData,
    backward_graph: VirtualTopocoreGraph<OwnedGraph>,
    backward_dijkstra_data: SyncDijkstraData,
    meeting_node: NodeId,
    fw_potential: P,
    bw_potential: P,
    forward_to_backward_edge_ids: Vec<EdgeId>,
    backward_to_forward_edge_ids: Vec<EdgeId>,
    thread_pool: rayon::ThreadPool,
}

impl<P: BiDirPotential + Clone + Send> MultiThreadedBiDirSkipLowDegServer<P> {
    pub fn new(graph: VirtualTopocoreGraph<OwnedGraph>, potential: P) -> Self {
        let n = graph.num_nodes();
        let reversed = VirtualTopocoreGraph::<OwnedGraph>::reversed(&graph);

        let mut reversed_edge_ids = vec![Vec::new(); n];
        let mut edge_id: EdgeId = 0;
        for tail in 0..n {
            for NodeIdT(head) in LinkIterable::<NodeIdT>::link_iter(&graph, tail as NodeId) {
                reversed_edge_ids[head as usize].push(edge_id);
                edge_id += 1;
            }
        }
        let mut forward_to_backward = vec![edge_id; edge_id as usize];
        let mut backward_to_forward = vec![edge_id; edge_id as usize];
        let mut backward_id = 0;
        for forward_ids in reversed_edge_ids {
            for forward_id in forward_ids {
                forward_to_backward[forward_id as usize] = backward_id;
                backward_to_forward[backward_id as usize] = forward_id;
                backward_id += 1;
            }
        }

        Self {
            forward_graph: graph,
            backward_graph: reversed,
            forward_dijkstra_data: SyncDijkstraData::new(n),
            backward_dijkstra_data: SyncDijkstraData::new(n),
            fw_potential: potential.clone(),
            bw_potential: potential,
            meeting_node: n as NodeId,
            forward_to_backward_edge_ids: forward_to_backward,
            backward_to_forward_edge_ids: backward_to_forward,
            thread_pool: rayon::ThreadPoolBuilder::new().num_threads(2).build().unwrap(),
        }
    }

    pub fn distance_with_cap<Q: GenQuery<Timestamp> + Copy + Sync>(
        &mut self,
        query: Q,
        cap: Weight,
        pot_cap: Option<Weight>,
    ) -> QueryResult<MultiThreadedBiDirCorePathServerWrapper<P, Q>, Weight> {
        QueryResult::new(self.distance(query, cap, pot_cap), MultiThreadedBiDirCorePathServerWrapper(self, query))
    }

    pub fn distance(&mut self, query: impl GenQuery<Timestamp> + Copy + Sync, cap: Weight, pot_cap: Option<Weight>) -> Option<Weight> {
        use std::cmp::min;

        report!("algo", "Virtual Topocore Parallel Bidirectional Core Query");
        P::report();

        let mut ops = DefaultOpsWithLinkPath::default();
        self.forward_dijkstra_data.distances.reset();
        let mut forward_dijkstra = SendTopoDijkstraRun::<_, _, true, true>::query(
            &self.forward_graph,
            &self.forward_dijkstra_data.distances,
            &mut self.forward_dijkstra_data.predecessors,
            &mut self.forward_dijkstra_data.queue,
            &mut ops,
            DijkstraInit::from(query.from()),
        );
        let mut ops = DefaultOpsWithLinkPath::default();
        self.backward_dijkstra_data.distances.reset();
        let mut backward_dijkstra = SendTopoDijkstraRun::<_, _, true, true>::query(
            &self.backward_graph,
            &self.backward_dijkstra_data.distances,
            &mut self.backward_dijkstra_data.predecessors,
            &mut self.backward_dijkstra_data.queue,
            &mut ops,
            DijkstraInit::from(query.to()),
        );

        let fw_reverse_dist = &self.backward_dijkstra_data.distances;
        let bw_reverse_dist = &self.forward_dijkstra_data.distances;

        self.fw_potential.init(query.from(), query.to());
        self.bw_potential.init(query.from(), query.to());

        let tentative_distance = AtomicU32::new(INFINITY);
        let fw_progress = AtomicU32::new(0);
        let bw_progress = AtomicU32::new(0);
        let fw_potential = &mut self.fw_potential;
        let bw_potential = &mut self.bw_potential;

        let ((fw_meeting, fw_num_queue_pops), (bw_meeting, bw_num_queue_pops)) = self.thread_pool.join(
            || {
                let mut fw_potential = RefCell::new(fw_potential);
                let mut num_queue_pops = 0;
                let mut meeting_node = None;
                let mut fw_tentative_distance = INFINITY;
                let mut stop_dist = cap;

                while !fw_potential.get_mut().stop_forward(
                    forward_dijkstra.queue().peek().map(|q| q.key),
                    Some(bw_progress.load(std::sync::atomic::Ordering::Relaxed)),
                    stop_dist,
                ) {
                    if let Some(node) = forward_dijkstra.next_with_improve_callback_and_potential(
                        |head, &dist| {
                            let mut pot = fw_potential.borrow_mut();
                            if let Some(pot_cap) = pot_cap {
                                if pot.forward_potential_raw(head).unwrap_or(INFINITY) + pot.backward_potential_raw(head).unwrap_or(INFINITY) > pot_cap {
                                    return false;
                                }
                            }
                            if pot.prune_forward(NodeIdT(head), dist, bw_progress.load(std::sync::atomic::Ordering::Relaxed), stop_dist) {
                                return false;
                            }
                            fw_tentative_distance = min(fw_tentative_distance, dist + fw_reverse_dist.get(head as usize));
                            stop_dist = min(fw_tentative_distance, cap);
                            if tentative_distance.fetch_min(fw_tentative_distance, std::sync::atomic::Ordering::Relaxed) > fw_tentative_distance {
                                meeting_node = Some(head);
                            }
                            true
                        },
                        |node| fw_potential.borrow_mut().forward_potential(node),
                    ) {
                        num_queue_pops += 1;
                        let prog = forward_dijkstra.queue().peek().map_or(INFINITY, |p| p.key);
                        fw_progress.store(prog, std::sync::atomic::Ordering::Relaxed);
                        if node == query.to() {
                            fw_progress.store(INFINITY, std::sync::atomic::Ordering::Relaxed);
                            return (Some(query.to()), num_queue_pops);
                        }
                    }
                }

                if meeting_node.is_some() {
                    (meeting_node, num_queue_pops)
                } else {
                    if forward_dijkstra.tentative_distance(query.to()) < INFINITY {
                        (Some(query.to()), num_queue_pops)
                    } else {
                        (None, num_queue_pops)
                    }
                }
            },
            || {
                let mut bw_potential = RefCell::new(bw_potential);
                let mut num_queue_pops = 0;
                let mut meeting_node = None;
                let mut bw_tentative_distance = INFINITY;
                let mut stop_dist = cap;

                while !bw_potential.get_mut().stop_backward(
                    Some(fw_progress.load(std::sync::atomic::Ordering::Relaxed)),
                    backward_dijkstra.queue().peek().map(|q| q.key),
                    stop_dist,
                ) {
                    if let Some(node) = backward_dijkstra.next_with_improve_callback_and_potential(
                        |head, &dist| {
                            let mut pot = bw_potential.borrow_mut();
                            if let Some(pot_cap) = pot_cap {
                                if pot.forward_potential_raw(head).unwrap_or(INFINITY) + pot.backward_potential_raw(head).unwrap_or(INFINITY) > pot_cap {
                                    return false;
                                }
                            }
                            if pot.prune_backward(NodeIdT(head), dist, fw_progress.load(std::sync::atomic::Ordering::Relaxed), stop_dist) {
                                return false;
                            }
                            bw_tentative_distance = min(bw_tentative_distance, dist + bw_reverse_dist.get(head as usize));
                            stop_dist = min(bw_tentative_distance, cap);
                            if tentative_distance.fetch_min(bw_tentative_distance, std::sync::atomic::Ordering::Relaxed) > bw_tentative_distance {
                                meeting_node = Some(head);
                            }
                            true
                        },
                        |node| bw_potential.borrow_mut().backward_potential(node),
                    ) {
                        num_queue_pops += 1;
                        let prog = backward_dijkstra.queue().peek().map_or(INFINITY, |p| p.key);
                        bw_progress.store(prog, std::sync::atomic::Ordering::Relaxed);
                        if node == query.from() {
                            bw_progress.store(INFINITY, std::sync::atomic::Ordering::Relaxed);
                            return (Some(query.from()), num_queue_pops);
                        }
                    }
                }

                if meeting_node.is_some() {
                    (meeting_node, num_queue_pops)
                } else {
                    if backward_dijkstra.tentative_distance(query.from()) < INFINITY {
                        (Some(query.from()), num_queue_pops)
                    } else {
                        (None, num_queue_pops)
                    }
                }
            },
        );

        report!("num_queue_pops", fw_num_queue_pops + bw_num_queue_pops);
        report!("num_queue_pushs", forward_dijkstra.num_queue_pushs() + backward_dijkstra.num_queue_pushs());
        report!("num_relaxed_arcs", forward_dijkstra.num_relaxed_arcs() + backward_dijkstra.num_queue_pushs());

        let fw_meeting = fw_meeting.map(|fw_meeting| {
            (
                fw_meeting,
                self.forward_dijkstra_data.distances.get(fw_meeting as usize) + self.backward_dijkstra_data.distances.get(fw_meeting as usize),
            )
        });
        let bw_meeting = bw_meeting.map(|bw_meeting| {
            (
                bw_meeting,
                self.forward_dijkstra_data.distances.get(bw_meeting as usize) + self.backward_dijkstra_data.distances.get(bw_meeting as usize),
            )
        });

        match (fw_meeting, bw_meeting) {
            (Some((fw_meet, fw_dist)), Some((bw_meet, bw_dist))) => {
                if fw_dist < bw_dist {
                    self.meeting_node = fw_meet;
                    Some(fw_dist)
                } else {
                    self.meeting_node = bw_meet;
                    Some(bw_dist)
                }
            }
            (Some((fw_meet, fw_dist)), None) => {
                self.meeting_node = fw_meet;
                Some(fw_dist)
            }
            (None, Some((bw_meet, bw_dist))) => {
                self.meeting_node = bw_meet;
                Some(bw_dist)
            }
            _ => None,
        }
    }

    fn path(&self, query: impl GenQuery<Timestamp>) -> Vec<NodeId> {
        let mut path = Vec::new();
        path.push(self.meeting_node);

        while *path.last().unwrap() != query.from() {
            let next = self.forward_dijkstra_data.predecessors[*path.last().unwrap() as usize].0;
            path.push(next);
        }

        path.reverse();

        while *path.last().unwrap() != query.to() {
            let next = self.backward_dijkstra_data.predecessors[*path.last().unwrap() as usize].0;
            path.push(next);
        }

        path
    }

    fn edge_path(&self, query: impl GenQuery<Timestamp>) -> Vec<EdgeIdT> {
        let mut path = Vec::new();
        let mut cur = self.meeting_node;

        while cur != query.from() {
            path.push(self.forward_dijkstra_data.predecessors[cur as usize].1);
            cur = self.forward_dijkstra_data.predecessors[cur as usize].0;
        }

        path.reverse();
        cur = self.meeting_node;

        while cur != query.to() {
            path.push(EdgeIdT(
                self.backward_to_forward_edge_ids[self.backward_dijkstra_data.predecessors[cur as usize].1 .0 as usize],
            ));
            cur = self.backward_dijkstra_data.predecessors[cur as usize].0;
        }

        path
    }

    pub fn graph(&self) -> &VirtualTopocoreGraph<OwnedGraph> {
        &self.forward_graph
    }

    pub fn tail(&self, edge: EdgeId) -> NodeId {
        self.backward_graph.graph.head()[self.forward_to_backward_edge_ids[edge as usize] as usize]
    }

    #[allow(unused)]
    pub(super) fn set_edge_weight(&mut self, edge: EdgeId, weight: Weight) {
        self.forward_graph.graph.weights_mut()[edge as usize] = weight;
        self.backward_graph.graph.weights_mut()[self.forward_to_backward_edge_ids[edge as usize] as usize] = weight;
    }

    pub fn potentials(&self) -> (&P, &P) {
        (&self.fw_potential, &self.bw_potential)
    }
}

pub struct MultiThreadedBiDirCorePathServerWrapper<'s, P, Q>(&'s mut MultiThreadedBiDirSkipLowDegServer<P>, Q);

impl<'s, P, Q> PathServer for MultiThreadedBiDirCorePathServerWrapper<'s, P, Q>
where
    P: BiDirPotential + Clone + Send,
    Q: GenQuery<Timestamp> + Copy,
{
    type NodeInfo = NodeId;
    type EdgeInfo = EdgeIdT;

    fn reconstruct_node_path(&mut self) -> Vec<Self::NodeInfo> {
        MultiThreadedBiDirSkipLowDegServer::path(self.0, self.1)
    }
    fn reconstruct_edge_path(&mut self) -> Vec<Self::EdgeInfo> {
        MultiThreadedBiDirSkipLowDegServer::edge_path(self.0, self.1)
    }
}

impl<P> QueryServer for MultiThreadedBiDirSkipLowDegServer<P>
where
    P: BiDirPotential + Clone + Send,
{
    type P<'s>
    where
        Self: 's,
    = MultiThreadedBiDirCorePathServerWrapper<'s, P, Query>;

    fn query(&mut self, query: Query) -> QueryResult<Self::P<'_>, Weight> {
        QueryResult::new(self.distance(query, INFINITY, None), MultiThreadedBiDirCorePathServerWrapper(self, query))
    }
}
