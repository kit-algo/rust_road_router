// WIP: CH potentials for TD Routing.

use std::{env, error::Error, fs::File, path::Path};

#[macro_use]
extern crate rust_road_router;
#[cfg(feature = "chpot-cch")]
use rust_road_router::algo::customizable_contraction_hierarchy::*;
use rust_road_router::{
    algo::{
        ch_potentials::{query::Server, *},
        dijkstra::query::{dijkstra::Server as DijkServer, td_dijkstra::LiveTDDijkstraOps},
        *,
    },
    cli::CliErr,
    datastr::{
        graph::{time_dependent::*, *},
        node_order::NodeOrder,
        rank_select_map::*,
    },
    io::*,
    report::*,
    util::in_range_option::*,
};

use csv::ReaderBuilder;
use glob::glob;
use rand::prelude::*;
use time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_td_live");

    let seed = Default::default();
    report!("seed", seed);
    let mut rng = StdRng::from_seed(seed);

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc"))?;
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time"))?;
    let ipp_travel_time = Vec::load_from(path.join("ipp_travel_time"))?;

    let geo_distance = Vec::<EdgeId>::load_from(path.join("geo_distance"))?;
    let osm_node_ids = Vec::<u64>::load_from(path.join("osm_node_ids"))?;

    let mut osm_ids_present = BitVec::new(*osm_node_ids.last().unwrap() as usize + 1);
    for osm_id in osm_node_ids {
        osm_ids_present.set(osm_id as usize);
    }
    let id_map = RankSelectMap::new(osm_ids_present);

    report!("unprocessed_graph", { "num_nodes": first_out.len() - 1, "num_arcs": head.len(), "num_ipps": ipp_departure_time.len() });

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time);

    let n = graph.num_nodes();

    report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs(), "num_ipps": graph.num_ipps(), "num_constant_ttfs": graph.num_constant() });

    let mut lower_bound = (0..graph.num_arcs() as EdgeId)
        .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
        .collect::<Vec<Weight>>();
    unify_parallel_edges(&mut FirstOutGraph::new(graph.first_out(), graph.head(), &mut lower_bound[..]));

    let arg = &args.next().ok_or(CliErr("No live data directory arg given"))?;
    let live_dir = Path::new(arg);

    let mut live = vec![InRangeOption::new(None); graph.num_arcs()];

    let mut total = 0;
    let mut found = 0;
    let mut too_fast = 0;

    for live_file in glob(live_dir.join("*").to_str().unwrap()).unwrap() {
        let file = File::open(live_file?.clone()).unwrap();
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
                    if lower_bound[edge_idx] <= new_tt {
                        live[edge_idx] = InRangeOption::new(Some(new_tt));
                    } else {
                        too_fast += 1;
                    }
                }
            }
        }
    }

    report!("live_traffic", { "num_modifications": total, "num_matched": found, "num_too_fast": too_fast });

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    // let now = 15 * 3600 * 1000 + 41 * 60 * 1000;
    let now = 12 * 3600 * 1000;
    let soon = now + 3600 * 1000;

    #[cfg(feature = "chpot-cch")]
    let cch = {
        let cch_order = Vec::load_from(path.join("cch_perm"))?;
        let cch_order = NodeOrder::from_node_order(cch_order);

        let cch_build_ctxt = algo_runs_ctxt.push_collection_item();
        let cch = contract(&graph, cch_order);
        drop(cch_build_ctxt);
        let cch_order = CCHReordering {
            cch: &cch,
            latitude: &[],
            longitude: &[],
        }
        .reorder_for_seperator_based_customization();
        let _cch_build_ctxt = algo_runs_ctxt.push_collection_item();
        contract(&graph, cch_order)
    };

    let potential = {
        #[cfg(feature = "chpot-only-topo")]
        {
            ZeroPotential()
        }
        #[cfg(not(feature = "chpot-only-topo"))]
        {
            #[cfg(feature = "chpot-cch")]
            {
                let _potential_ctxt = algo_runs_ctxt.push_collection_item();
                CCHPotential::new(&cch, &FirstOutGraph::new(graph.first_out(), graph.head(), lower_bound))
            }
            #[cfg(not(feature = "chpot-cch"))]
            {
                let forward_first_out = Vec::<EdgeId>::load_from(path.join("lower_bound_ch/forward_first_out"))?;
                let forward_head = Vec::<NodeId>::load_from(path.join("lower_bound_ch/forward_head"))?;
                let forward_weight = Vec::<Weight>::load_from(path.join("lower_bound_ch/forward_weight"))?;
                let backward_first_out = Vec::<EdgeId>::load_from(path.join("lower_bound_ch/backward_first_out"))?;
                let backward_head = Vec::<NodeId>::load_from(path.join("lower_bound_ch/backward_head"))?;
                let backward_weight = Vec::<Weight>::load_from(path.join("lower_bound_ch/backward_weight"))?;
                let order = NodeOrder::from_node_order(Vec::<NodeId>::load_from(path.join("lower_bound_ch/order"))?);
                CHPotential::new(
                    OwnedGraph::new(forward_first_out, forward_head, forward_weight),
                    OwnedGraph::new(backward_first_out, backward_head, backward_weight),
                    order,
                )
            }
        }
    };

    let graph = LiveTDGraph::new(graph, soon, live);

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut server = Server::new(&graph, potential, LiveTDDijkstraOps::default());
    drop(virtual_topocore_ctxt);

    let num_queries = rust_road_router::experiments::chpot::num_queries();

    let mut astar_time = Duration::zero();

    for _i in 0..num_queries {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0, n as NodeId);
        let to: NodeId = rng.gen_range(0, n as NodeId);
        // let at: NodeId = rng.gen_range(0, period() as Timestamp);

        report!("from", from);
        report!("to", to);
        report!("at", now);
        let (mut res, time) = measure(|| server.td_query(TDQuery { from, to, departure: now }));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        report!("result", res.as_ref().map(|res| res.distance()));
        #[cfg(all(not(feature = "chpot-only-topo"), not(feature = "chpot-alt")))]
        report!(
            "num_pot_computations",
            res.as_mut().map(|res| res.data().potential().num_pot_computations()).unwrap_or(0)
        );
        report!("lower_bound", res.as_mut().map(|res| res.data().lower_bound(from)).flatten());
        astar_time = astar_time + time;
    }
    eprintln!("A* {}", astar_time / (num_queries as i32));

    let num_queries = rust_road_router::experiments::num_dijkstra_queries();

    let mut server = DijkServer::<_, LiveTDDijkstraOps>::new(graph);

    let mut dijkstra_time = Duration::zero();

    for _i in 0..num_queries {
        let _query_ctxt = algo_runs_ctxt.push_collection_item();
        let from: NodeId = rng.gen_range(0, n as NodeId);
        let to: NodeId = rng.gen_range(0, n as NodeId);
        let at: NodeId = rng.gen_range(0, period() as Timestamp);

        report!("from", from);
        report!("to", to);
        report!("at", at);
        let (ea, time) = measure(|| server.td_query(TDQuery { from, to, departure: at }).map(|res| res.distance()));
        report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
        report!("result", ea);
        dijkstra_time = dijkstra_time + time;
    }
    eprintln!("Dijk* {}", dijkstra_time / (num_queries as i32));

    Ok(())
}
