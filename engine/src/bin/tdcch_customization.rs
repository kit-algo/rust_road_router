use std::{
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
        customizable_contraction_hierarchy::{cch_graph::*},
        node_order::NodeOrder,
    },
    io::*,
    report::*,
};

fn main() {
    let _reporter = enable_reporting();

    let core_ids = core_affinity::get_core_ids().unwrap();
    rayon::ThreadPoolBuilder::new().start_handler(move |thread_idx| core_affinity::set_for_current(core_ids[thread_idx])).build_global().unwrap();

    report!("program", "tdcch");
    report!("start_time", format!("{}", time::now_utc().rfc822()));
    report!("args", env::args().collect::<Vec<String>>());
    report!("num_threads", rayon::current_num_threads());

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
    let node_order = NodeOrder::reconstruct_from(cch_folder.to_str().unwrap()).expect("could not read node order");
    let cch = CCHGraphReconstrctor { original_graph: &graph, node_order }.reconstruct_from(cch_folder.to_str().unwrap()).expect("could not read cch");

    let customized_folder = path.join("customized");

    let _cch_customization_ctxt = algo_runs_ctxt.push_collection_item();
    let td_cch_graph: CustomizedGraph = cch.customize_floating_td(&graph).into();
    if !customized_folder.exists() { std::fs::create_dir(&customized_folder).expect("could not create cch folder"); }
    td_cch_graph.deconstruct_to(customized_folder.to_str().unwrap()).expect("could not save customized");
}
