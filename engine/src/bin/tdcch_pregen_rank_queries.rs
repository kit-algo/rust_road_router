use std::{env, error::Error, path::Path};

#[macro_use]
extern crate bmw_routing_engine;
use bmw_routing_engine::{
    algo::{catchup::Server, customizable_contraction_hierarchy::*, *},
    cli::CliErr,
    datastr::{
        graph::{
            floating_time_dependent::{shortcut_graph::CustomizedGraphReconstrctor, *},
            *,
        },
        node_order::NodeOrder,
    },
    io::*,
    report::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting();

    report!("program", "tdcch");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());
    report!("num_threads", rayon::current_num_threads());

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let mut args = env::args();
    args.next();

    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc"))?;
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time"))?;
    let ipp_travel_time = Vec::<u32>::load_from(path.join("ipp_travel_time"))?;

    report!("unprocessed_graph", { "num_nodes": first_out.len() - 1, "num_arcs": head.len(), "num_ipps": ipp_departure_time.len() });

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time);

    report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs(), "num_ipps": graph.num_ipps(), "num_constant_ttfs": graph.num_constant() });

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let cch_folder = path.join("cch");
    let node_order = NodeOrder::reconstruct_from(&cch_folder)?;
    let cch = CCHReconstrctor {
        original_graph: &graph,
        node_order,
    }
    .reconstruct_from(&cch_folder)?;

    let customized_folder = path.join("customized");

    let td_cch_graph = CustomizedGraphReconstrctor {
        original_graph: &graph,
        first_out: cch.first_out(),
        head: cch.head(),
    }
    .reconstruct_from(&customized_folder)?;

    let mut server = Server::new(&cch, &td_cch_graph);

    let mut query_dir = None;
    let mut base_dir = Some(path);

    while let Some(base) = base_dir {
        if base.join("rank_queries").exists() {
            query_dir = Some(base.join("rank_queries"));
            break;
        } else {
            base_dir = base.parent();
        }
    }

    if let Some(path) = query_dir {
        let from = Vec::load_from(path.join("source_node"))?;
        let at = Vec::<u32>::load_from(path.join("source_time"))?;
        let to = Vec::load_from(path.join("target_node"))?;
        let rank = Vec::<u32>::load_from(path.join("dij_rank"))?;

        for (((from, to), at), rank) in from.into_iter().zip(to.into_iter()).zip(at.into_iter()).zip(rank.into_iter()) {
            let at = Timestamp::new(f64::from(at) / 1000.0);

            let _tdcch_query_ctxt = algo_runs_ctxt.push_collection_item();
            let (result, time) = measure(|| server.query(TDQuery { from, to, departure: at }));

            report!("from", from);
            report!("to", to);
            report!("rank", rank);
            report!("departure_time", f64::from(at));
            report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            if let Some(mut result) = result {
                report!("earliest_arrival", f64::from(result.distance() + at));

                let (path, unpacking_duration) = measure(|| result.path());
                report!("num_nodes_on_shortest_path", path.len());
                report!(
                    "unpacking_running_time_ms",
                    unpacking_duration.to_std().unwrap().as_nanos() as f64 / 1_000_000.0
                );
            }
        }
    }

    Ok(())
}
