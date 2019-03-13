use std::{
    cmp::Ordering,
    path::Path,
    env,
};

#[macro_use] extern crate bmw_routing_engine;
use bmw_routing_engine::{
    graph::{
        *,
        floating_time_dependent::*,
    },
    shortest_path::{
        customizable_contraction_hierarchy::{self, cch_graph::*},
        node_order::NodeOrder,
    },
    io::*,
    report::*,
};

#[derive(PartialEq,PartialOrd)]
struct NonNan(f32);

impl NonNan {
    fn new(val: f32) -> Option<NonNan> {
        if val.is_nan() {
            None
        } else {
            Some(NonNan(val))
        }
    }
}

impl Eq for NonNan {}

impl Ord for NonNan {
    fn cmp(&self, other: &NonNan) -> Ordering {
        self.partial_cmp(other).unwrap()
    }
}

fn main() {
    let _reporter = enable_reporting();

    report!("program", "tdcch");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());

    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = Vec::load_from(path.join("head").to_str().unwrap()).expect("could not read head");
    let first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc").to_str().unwrap()).expect("could not read first_ipp_of_arc");
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time").to_str().unwrap()).expect("could not read ipp_departure_time");
    let ipp_travel_time = Vec::<u32>::load_from(path.join("ipp_travel_time").to_str().unwrap()).expect("could not read ipp_travel_time");

    report!("unprocessed_graph", { "num_nodes": first_out.len() - 1, "num_arcs": head.len(), "num_ipps": ipp_departure_time.len() });

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time);

    report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs(), "num_ipps": graph.num_ipps(), "num_constant_ttfs": graph.num_constant() });

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());


    let cch_folder = path.join("cch");

    let cch_order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm").to_str().unwrap()).expect("could not read cch_perm"));
    let cch_build_ctxt = algo_runs_ctxt.push_collection_item();
    let cch = customizable_contraction_hierarchy::contract(&graph, cch_order.clone());
    drop(cch_build_ctxt);

    let latitude = Vec::<f32>::load_from(path.join("latitude").to_str().unwrap()).expect("could not read latitude");
    let longitude = Vec::<f32>::load_from(path.join("longitude").to_str().unwrap()).expect("could not read longitude");

    let cch_order = CCHReordering { node_order: cch_order, latitude: &latitude, longitude: &longitude }.reorder(cch.separators());

    let cch_build_ctxt = algo_runs_ctxt.push_collection_item();
    let cch = customizable_contraction_hierarchy::contract(&graph, cch_order.clone());
    drop(cch_build_ctxt);

    let cch_order = CCHReordering { node_order: cch_order, latitude: &latitude, longitude: &longitude }.reorder_for_seperator_based_customization(cch.separators());
    if !cch_folder.exists() { std::fs::create_dir(&cch_folder).expect("could not create cch folder"); }
    cch_order.deconstruct_to(cch_folder.to_str().unwrap()).expect("could not save cch order");

    let cch_build_ctxt = algo_runs_ctxt.push_collection_item();
    let cch = customizable_contraction_hierarchy::contract(&graph, cch_order.clone());
    drop(cch_build_ctxt);

    cch.deconstruct_to(cch_folder.to_str().unwrap()).expect("could not save cch");
}

#[derive(Debug)]
struct CCHReordering<'a> {
    node_order: NodeOrder,
    latitude: &'a [f32],
    longitude: &'a [f32],
}

impl<'a> CCHReordering<'a> {
    fn distance (&self, n1: NodeId, n2: NodeId) -> NonNan {
        use nav_types::WGS84;
        NonNan::new(WGS84::new(self.latitude[self.node_order.node(n1) as usize], self.longitude[self.node_order.node(n1) as usize], 0.0)
            .distance(&WGS84::new(self.latitude[self.node_order.node(n2) as usize], self.longitude[self.node_order.node(n2) as usize], 0.0))).unwrap()
    }

    fn reorder_sep(&self, nodes: &mut [NodeId]) {
        let furthest = nodes.first().map(|&first| {
            nodes.iter().max_by_key(|&&node| self.distance(first, node)).unwrap()
        });

        if let Some(&furthest) = furthest {
            nodes.sort_by_key(|&node| self.distance(node, furthest))
        }
    }

    fn reorder_tree(&self, separators: &mut SeparatorTree, level: usize) {
        if level > 2 { return }

        self.reorder_sep(&mut separators.nodes);
        for child in &mut separators.children {
            self.reorder_tree(child, level + 1);
            // if let Some(&first) = child.nodes.first() {
            //     if let Some(&last) = child.nodes.last() {
            //         if let Some(&node) = separators.nodes.first() {
            //             if self.distance(first, node) < self.distance(last, node) {
            //                 child.nodes.reverse()
            //             }
            //         }
            //     }
            // }
        }
    }

    fn to_ordering(&self, seperators: SeparatorTree, order: &mut Vec<NodeId>) {
        order.extend(seperators.nodes);
        for child in seperators.children {
            self.to_ordering(child, order);
        }
    }

    pub fn reorder(self, mut separators: SeparatorTree) -> NodeOrder {
        self.reorder_tree(&mut separators, 0);
        let mut order = Vec::new();
        self.to_ordering(separators, &mut order);

        for rank in &mut order {
            *rank = self.node_order.node(*rank);
        }
        order.reverse();

        NodeOrder::from_node_order(order)
    }

    pub fn reorder_for_seperator_based_customization(self, separators: SeparatorTree) -> NodeOrder {
        let mut order = Vec::new();
        self.to_ordering(separators, &mut order);

        for rank in &mut order {
            *rank = self.node_order.node(*rank);
        }
        order.reverse();

        NodeOrder::from_node_order(order)
    }
}
