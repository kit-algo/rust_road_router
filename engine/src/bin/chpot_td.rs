// WIP: CH potentials for TD Routing.

use std::{env, error::Error, path::Path};
#[macro_use]
extern crate bmw_routing_engine;
use bmw_routing_engine::{
    algo::{
        ch_potentials::{td_query::Server, *},
        customizable_contraction_hierarchy::*,
        dijkstra::query::td_dijkstra::Server as DijkServer,
        *,
    },
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
    let first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc"))?;
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time"))?;
    let ipp_travel_time = Vec::load_from(path.join("ipp_travel_time"))?;

    report!("unprocessed_graph", { "num_nodes": first_out.len() - 1, "num_arcs": head.len(), "num_ipps": ipp_departure_time.len() });

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time);

    report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs(), "num_ipps": graph.num_ipps(), "num_constant_ttfs": graph.num_constant() });

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

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
        #[cfg(feature = "chpot-cch")]
        {
            let mut lower_bound = (0..graph.num_arcs() as EdgeId)
                .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
                .collect::<Vec<Weight>>();
            unify_parallel_edges(&mut FirstOutGraph::new(graph.first_out(), graph.head(), &mut lower_bound[..]));

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
    };

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut server = Server::new(graph.clone(), potential);
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

    if let Some(path) = query_dir {
        let from = Vec::load_from(path.join("source_node"))?;
        let at = Vec::<u32>::load_from(path.join("source_time"))?;
        let to = Vec::load_from(path.join("target_node"))?;

        let num_queries = bmw_routing_engine::experiments::chpot::NUM_QUERIES;

        let mut astar_time = Duration::zero();

        for ((&from, &to), &at) in from.iter().zip(to.iter()).zip(at.iter()).take(num_queries) {
            let _query_ctxt = algo_runs_ctxt.push_collection_item();
            report!("from", from);
            report!("to", to);
            let (ea, time) = measure(|| server.query(TDQuery { from, to, departure: at }).map(|res| res.distance()));
            report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            report!("result", ea);
            report!("lower_bound", server.lower_bound(from));
            astar_time = astar_time + time;
        }
        eprintln!("A* {}", astar_time / (num_queries as i32));

        let num_queries = bmw_routing_engine::experiments::NUM_DIJKSTRA_QUERIES;

        let mut server = DijkServer::new(graph);

        for ((&from, &to), &at) in from.iter().zip(to.iter()).zip(at.iter()).take(num_queries) {
            let _query_ctxt = algo_runs_ctxt.push_collection_item();
            report!("from", from);
            report!("to", to);
            let (ea, time) = measure(|| server.query(TDQuery { from, to, departure: at }).map(|res| res.distance()));
            report!("running_time_ms", time.to_std().unwrap().as_nanos() as f64 / 1_000_000.0);
            report!("result", ea);
        }
    }

    Ok(())
}
