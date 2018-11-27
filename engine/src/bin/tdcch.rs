use std::{
    cmp::Ordering,
    path::Path,
    env,
};

use bmw_routing_engine::{
    graph::{
        *,
        floating_time_dependent::*,
        time_dependent::period as int_period,
    },
    shortest_path::{
        customizable_contraction_hierarchy::{self, cch_graph::SeparatorTree},
        node_order::NodeOrder,
        query::{
            floating_td_customizable_contraction_hierarchy::Server,
            floating_td_dijkstra::Server as DijkServer
        },
    },
    io::Load,
    benchmark::*,
};

use time::Duration;
use rand::prelude::*;

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
    let mut args = env::args();
    args.next();

    let arg = &args.next().expect("No directory arg given");
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out").to_str().unwrap()).expect("could not read first_out");
    let head = Vec::load_from(path.join("head").to_str().unwrap()).expect("could not read head");
    let mut first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc").to_str().unwrap()).expect("could not read first_ipp_of_arc");
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time").to_str().unwrap()).expect("could not read ipp_departure_time");
    let ipp_travel_time = Vec::<u32>::load_from(path.join("ipp_travel_time").to_str().unwrap()).expect("could not read ipp_travel_time");

    println!("nodes: {}, arcs: {}, ipps: {}", first_out.len() - 1, head.len(), ipp_departure_time.len());

    let mut new_ipp_departure_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());
    let mut new_ipp_travel_time = Vec::with_capacity(ipp_departure_time.len() + 2 * head.len());

    let mut added = 0;

    for i in 0..head.len() {
        let range = first_ipp_of_arc[i] as usize .. first_ipp_of_arc[i+1] as usize;
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
            new_ipp_departure_time.push(int_period());
            new_ipp_travel_time.push(ipp_travel_time[range.start]);
            added += 1;
        } else {
            new_ipp_departure_time.push(0);
            new_ipp_travel_time.push(ipp_travel_time[range.start]);
        }
    }
    first_ipp_of_arc[head.len()] += added;

    println!("nodes: {}, arcs: {}, ipps: {}", first_out.len() - 1, head.len(), new_ipp_departure_time.len());

    let points = new_ipp_departure_time.into_iter().zip(new_ipp_travel_time.into_iter()).map(|(dt, tt)| {
        TTFPoint { at: Timestamp::new(f64::from(dt) / 1000.0), val: FlWeight::new(f64::from(tt) / 1000.0) }
    }).collect();

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, points);

    let cch_order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm").to_str().unwrap()).expect("could not read cch_perm"));
    let cch = customizable_contraction_hierarchy::contract(&graph, cch_order);

    let cch_order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm").to_str().unwrap()).expect("could not read cch_perm"));
    let latitude = Vec::<f32>::load_from(path.join("latitude").to_str().unwrap()).expect("could not read latitude");
    let longitude = Vec::<f32>::load_from(path.join("longitude").to_str().unwrap()).expect("could not read longitude");
    let cch_order = CCHReordering { node_order: cch_order, latitude, longitude }.reorder(cch.separators());
    let cch = customizable_contraction_hierarchy::contract(&graph, cch_order);

    let td_cch_graph = cch.customize_floating_td(&graph);

    let mut td_dijk_server = DijkServer::new(graph.clone());
    let mut server = Server::new(&cch, &td_cch_graph);

    let mut rng = StdRng::from_seed(Default::default());

    let mut rank_times = vec![Vec::new(); 64];

    for _ in 0..50 {
        let from: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);
        let at = Timestamp::new(rng.gen_range(0.0, f64::from(period())));
        td_dijk_server.ranks(from, at, |to, ea_ground_truth, rank| {
            let (ea, duration) = measure(|| server.distance(from, to, at).map(|dist| dist + at));
            rank_times[rank].push(duration);
            if ea.unwrap_or_else(|| Timestamp::new(f64::from(INFINITY))).fuzzy_eq(ea_ground_truth) {
                println!("TDCCH ✅ {:?} {:?}", ea, ea_ground_truth);
            } else {
                println!("TDCCH ❌ {:?} {:?}", ea, ea_ground_truth);
            }
            if cfg!(feature = "tdcch-approx") {
                assert!(!ea.unwrap_or_else(|| Timestamp::new(f64::from(INFINITY))).fuzzy_lt(ea_ground_truth), "{} {} {:?}", from, to, at);
            } else {
                assert!(ea_ground_truth.fuzzy_eq(ea.unwrap_or_else(|| Timestamp::new(f64::from(INFINITY)))), "{} {} {:?}", from, to, at);
            }
        });
    }

    for (rank, rank_times) in rank_times.into_iter().enumerate() {
        let count = rank_times.len();
        if count > 0 {
            let sum = rank_times.into_iter().fold(Duration::zero(), std::ops::Add::add);
            let avg = sum / count as i32;
            println!("rank: {} - avg running time: {}", rank, avg);
        }
    }

    let mut query_dir = None;
    let mut base_dir = Some(path);

    while let Some(base) = base_dir {
        if base.join("uniform_queries").exists() {
            query_dir = Some(base.join("uniform_queries"));
            break;
        } else {
            base_dir = base.parent();
        }
    }

    if let Some(path) = query_dir {
        let from = Vec::load_from(path.join("source_node").to_str().unwrap()).expect("could not read source node");
        let at = Vec::<u32>::load_from(path.join("source_time").to_str().unwrap()).expect("could not read source time");
        let to = Vec::load_from(path.join("target_node").to_str().unwrap()).expect("could not read target node");

        let num_queries = 50;

        let mut dijkstra_time = Duration::zero();
        let mut tdcch_time = Duration::zero();

        for ((from, to), at) in from.into_iter().zip(to.into_iter()).zip(at.into_iter()).take(num_queries) {
            let at = Timestamp::new(f64::from(at) / 1000.0);
            let (ground_truth, time) = measure(|| {
                td_dijk_server.distance(from, to, at).map(|dist| dist + at)
            });
            println!("from {} to {} at {:?} - EA {:?}", from, to, at, ground_truth);
            dijkstra_time =  dijkstra_time + time;

            tdcch_time = tdcch_time + measure(|| {
                let dist = server.distance(from, to, at).map(|dist| dist + at);
                if dist.unwrap_or_else(|| Timestamp::new(f64::from(INFINITY))).fuzzy_eq(ground_truth.unwrap_or_else(|| Timestamp::new(f64::from(INFINITY)))) {
                    println!("TDCCH ✅ {:?} {:?}", dist, ground_truth);
                } else {
                    println!("TDCCH ❌ {:?} {:?}", dist, ground_truth);
                }
                if cfg!(feature = "tdcch-approx") {
                    assert!(!dist.unwrap_or_else(|| Timestamp::new(f64::from(INFINITY))).fuzzy_lt(ground_truth.unwrap_or_else(|| Timestamp::new(f64::from(INFINITY)))), "{} {} {:?}", from, to, at);
                } else {
                    assert!(dist.unwrap_or_else(|| Timestamp::new(f64::from(INFINITY))).fuzzy_eq(ground_truth.unwrap_or_else(|| Timestamp::new(f64::from(INFINITY)))));
                }
            }).1;
        }
        println!("Dijkstra {}", dijkstra_time / (num_queries as i32));
        println!("TDCCH {}", tdcch_time / (num_queries as i32));
    }
}

#[derive(Debug)]
struct CCHReordering {
    node_order: NodeOrder,
    latitude: Vec<f32>,
    longitude: Vec<f32>,
}

impl CCHReordering {
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
            self.to_ordering(*child, order);
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
}
