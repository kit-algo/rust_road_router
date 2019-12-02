use bmw_routing_engine::{
    algo::{
        ch_potentials::query::Server as TopoServer,
        customizable_contraction_hierarchy::{query::Server, *},
        topocore::*,
        *,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder, rank_select_map::*},
    io::*,
    report::benchmark::*,
};
use std::{env, error::Error, fs::File, path::Path};

use csv::ReaderBuilder;
use glob::glob;
use rand::prelude::*;
use time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::<NodeId>::load_from(path.join("first_out"))?;
    let head = Vec::<EdgeId>::load_from(path.join("head"))?;
    let mut travel_time = Vec::<EdgeId>::load_from(path.join("travel_time"))?;
    #[cfg(feature = "chpot_visualize")]
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    #[cfg(feature = "chpot_visualize")]
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;
    let mut live_travel_time = travel_time.clone();
    let geo_distance = Vec::<EdgeId>::load_from(path.join("geo_distance"))?;
    let osm_node_ids = Vec::<u64>::load_from(path.join("osm_node_ids"))?;

    for ids in osm_node_ids.windows(2) {
        assert!(ids[0] < ids[1]);
    }

    let mut osm_ids_present = BitVec::new(*osm_node_ids.last().unwrap() as usize + 1);
    for osm_id in osm_node_ids {
        osm_ids_present.set(osm_id as usize);
    }
    let id_map = RankSelectMap::new(osm_ids_present);
    let mut graph = FirstOutGraph::new(&first_out[..], &head[..], &mut travel_time[..]);
    unify_parallel_edges(&mut graph);
    drop(graph);
    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);

    let arg = &args.next().ok_or(CliErr("No live data directory arg given"))?;
    let live_dir = Path::new(arg);

    let mut total = 0;
    let mut found = 0;
    let mut too_fast = 0;

    for live in glob(live_dir.join("*").to_str().unwrap()).unwrap() {
        let file = File::open(live?.clone()).unwrap();
        let mut reader = ReaderBuilder::new()
            .has_headers(false)
            .delimiter(b',')
            .quoting(false)
            .double_quote(false)
            .escape(None)
            .from_reader(file);

        for line in reader.records() {
            total += 1;

            let record = line?;
            let from = record[0].parse()?;
            let to = record[1].parse()?;
            let speed = record[2].parse::<u32>()?;

            if let (Some(from), Some(to)) = (id_map.get(from), id_map.get(to)) {
                if let Some(edge_idx) = graph.edge_index(from as NodeId, to as NodeId) {
                    found += 1;
                    let edge_idx = edge_idx as usize;

                    let new_tt = 100 * 36 * geo_distance[edge_idx] / speed;
                    if travel_time[edge_idx] <= new_tt {
                        live_travel_time[edge_idx] = new_tt;
                    } else {
                        too_fast += 1;
                    }
                }
            }
        }
    }

    dbg!(total, found, too_fast);

    let cch_order = Vec::load_from(path.join("cch_perm"))?;
    let cch_order = NodeOrder::from_node_order(cch_order);

    let cch = contract(&graph, cch_order.clone());
    let cch_order = CCHReordering {
        node_order: cch_order,
        latitude: &[],
        longitude: &[],
    }
    .reorder_for_seperator_based_customization(cch.separators());
    let cch = contract(&graph, cch_order);

    let mut live_graph = FirstOutGraph::new(&first_out[..], &head[..], &mut live_travel_time[..]);
    unify_parallel_edges(&mut live_graph);
    drop(live_graph);
    let live_graph = FirstOutGraph::new(&first_out[..], &head[..], &live_travel_time[..]);

    let mut cch_static_server = Server::new(&cch, &graph);
    let mut cch_live_server = Server::new(&cch, &live_graph);

    let topocore = report_time("topocore preprocessing", || preprocess(&live_graph));
    let mut topocore = {
        #[cfg(feature = "chpot_visualize")]
        {
            TopoServer::new(topocore, &cch, &graph, &lat, &lng)
        }
        #[cfg(not(feature = "chpot_visualize"))]
        {
            TopoServer::new(topocore, &cch, &graph)
        }
    };

    let mut query_count = 0;
    let mut live_count = 0;
    let seed = Default::default();
    let mut rng = StdRng::from_seed(seed);
    let mut total_query_time = Duration::zero();
    let mut live_query_time = Duration::zero();

    for i in 0..100 {
        dbg!(i);
        let from: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);
        let to: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);
        let ground_truth = cch_live_server.query(Query { from, to }).map(|res| res.distance());
        // let ground_truth = cch_static_server.distance(from, to);

        if ground_truth.is_none() {
            continue;
        }

        query_count += 1;

        let lower_bound = cch_static_server.query(Query { from, to }).map(|res| res.distance());
        let (mut res, time) = measure(|| topocore.query(Query { from, to }));
        let dist = res.as_ref().map(|res| res.distance());
        res.as_mut().map(|res| res.path());
        let live = lower_bound != ground_truth;
        eprintln!("live: {:?}", live);
        if live {
            eprintln!("{}% length of static", ground_truth.unwrap() * 100 / lower_bound.unwrap());
            live_query_time = live_query_time + time;
            live_count += 1;
        }
        if dist != ground_truth {
            eprintln!("topo {:?} ground_truth {:?} ({} - {})", dist, ground_truth, from, to);
            assert!(ground_truth < dist);
        }

        total_query_time = total_query_time + time;
    }

    if query_count > 0 {
        eprintln!("Avg. query time {}", total_query_time / (query_count as i32))
    };
    if live_count > 0 {
        eprintln!("Avg. live query time {}", live_query_time / (live_count as i32))
    };

    Ok(())
}
