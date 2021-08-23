use rust_road_router::{
    algo::{
        ch_potentials::{query::Server as TopoServer, *},
        customizable_contraction_hierarchy::*,
        dijkstra::{query::dijkstra::Server as DijkServer, *},
    },
    cli::CliErr,
    datastr::graph::*,
    datastr::node_order::*,
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("cchpot_live");

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("live_travel_time").reconstruct_from(&path)?;
    let n = graph.num_nodes();

    let mut rng = experiments::rng(Default::default());

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let cch = {
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
    };
    let cch_pot_data = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &graph)
    };

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut topocore: TopoServer<OwnedGraph, _, _, true, true, true> = TopoServer::new(&graph, cch_pot_data.forward_potential(), DefaultOps::default());
    drop(virtual_topocore_ctxt);

    experiments::run_random_queries_with_callbacks(
        n,
        &mut topocore,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
        |_, _, _| (),
        // |mut res| {
        //     report!("num_pot_computations", res.data().potential().num_pot_computations());
        //     let from = res.data().query().from();
        //     report!("lower_bound", res.data().lower_bound(from));
        // },
        |_, _| None,
    );

    let mut server = DijkServer::<_, DefaultOps>::new(graph);
    experiments::run_random_queries(n, &mut server, &mut rng, &mut algo_runs_ctxt, experiments::num_dijkstra_queries());
    Ok(())
}
