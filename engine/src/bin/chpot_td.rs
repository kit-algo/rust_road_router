// WIP: CH potentials for TD Routing.

use std::{env, error::Error, path::Path};
#[macro_use]
extern crate bmw_routing_engine;
use bmw_routing_engine::{
    algo::{ch_potentials::td_query::Server, customizable_contraction_hierarchy::*, *},
    cli::CliErr,
    datastr::graph::*,
    datastr::{graph::time_dependent::*, node_order::NodeOrder},
    io::*,
    report::*,
};

use time::Duration;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting();

    report!("program", "chpot_td");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());

    let mut args = env::args();
    args.next();

    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let mut first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc"))?;
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time"))?;
    let ipp_travel_time = Vec::load_from(path.join("ipp_travel_time"))?;

    report!("unprocessed_graph", { "num_nodes": first_out.len() - 1, "num_arcs": head.len(), "num_ipps": ipp_departure_time.len() });

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
            new_ipp_departure_time.push(period());
            new_ipp_travel_time.push(ipp_travel_time[range.start]);
            added += 1;
        } else {
            new_ipp_departure_time.push(0);
            new_ipp_travel_time.push(ipp_travel_time[range.start]);
        }
    }
    first_ipp_of_arc[head.len()] += added;

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, new_ipp_departure_time, new_ipp_travel_time);

    report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs(), "num_ipps": graph.num_ipps(), "num_constant_ttfs": graph.num_constant() });

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

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
    let cch_build_ctxt = algo_runs_ctxt.push_collection_item();
    let cch = contract(&graph, cch_order);
    drop(cch_build_ctxt);

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut server = Server::new(graph, &cch);
    drop(virtual_topocore_ctxt);

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

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    if let Some(path) = query_dir {
        let from = Vec::load_from(path.join("source_node"))?;
        let at = Vec::<u32>::load_from(path.join("source_time"))?;
        let to = Vec::load_from(path.join("target_node"))?;

        let num_queries = 100;

        let mut astar_time = Duration::zero();

        for ((from, to), at) in from.into_iter().zip(to.into_iter()).zip(at.into_iter()).take(num_queries) {
            let _query_ctxt = algo_runs_ctxt.push_collection_item();
            report!("from", from);
            report!("to", to);
            let (ea, time) = measure(|| server.query(TDQuery { from, to, departure: at }).map(|res| res.distance() + at));
            report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            report!("result", ea.unwrap_or(INFINITY));
            astar_time = astar_time + time;
        }
        eprintln!("A* {}", astar_time / (num_queries as i32));
    }

    Ok(())
}
