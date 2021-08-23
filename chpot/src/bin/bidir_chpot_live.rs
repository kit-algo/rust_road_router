#[macro_use]
extern crate rust_road_router;
use rand::prelude::*;
use rust_road_router::{
    algo::{
        a_star::*,
        ch_potentials::{query::BiDirServer as BiDirTopo, *},
        dijkstra::{query::bidirectional_dijkstra::Server as DijkServer, *},
    },
    cli::CliErr,
    datastr::graph::*,
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_live");

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let live_travel_time = Vec::<Weight>::load_from(path.join("live_travel_time"))?;

    run(path, |_graph, _rng, query_weights: &mut [Weight]| {
        let mut live_count = 0;

        for (query, input) in query_weights.iter_mut().zip(live_travel_time.iter()) {
            if input > query {
                *query = *input;
                live_count += 1;
            }
        }

        report!("live_traffic", { "num_applied": live_count });

        Ok(())
    })
}

pub fn run(
    path: &Path,
    modify_travel_time: impl FnOnce(&FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>, &mut StdRng, &mut [Weight]) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let mut rng = experiments::rng(Default::default());

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let mut modified_travel_time = graph.weight().to_vec();

    modify_travel_time(&graph.borrowed(), &mut rng, &mut modified_travel_time)?;
    let modified_graph = FirstOutGraph::new(graph.first_out(), graph.head(), &modified_travel_time[..]);

    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let chpot_data = CHPotLoader::reconstruct_from(&path.join("lower_bound_ch"))?;
    let pots = chpot_data.potentials();

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let infinity_filtered = InfinityFilteringGraph(modified_graph);
    let mut server = BiDirTopo::<_, AlternatingDirs>::new(&infinity_filtered, SymmetricBiDirPotential::<_, _>::new(pots.0, pots.1));
    let InfinityFilteringGraph(modified_graph) = infinity_filtered;
    drop(virtual_topocore_ctxt);

    experiments::run_random_queries_with_callbacks(
        graph.num_nodes(),
        &mut server,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
        |_, _, _| (),
        // |mut res| {
        //     report!(
        //         "num_pot_computations",
        //         res.as_mut().map(|res| res.data().potential().num_pot_computations()).unwrap_or(0)
        //     );
        //     report!(
        //         "lower_bound",
        //         res.as_mut()
        //             .map(|res| {
        //                 let from = res.data().query().from();
        //                 res.data().lower_bound(from)
        //             })
        //             .flatten()
        //     );
        // },
        |_, _| None,
    );

    let mut server = DijkServer::<_, _>::new(modified_graph);
    experiments::run_random_queries(
        graph.num_nodes(),
        &mut server,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::num_dijkstra_queries(),
    );

    Ok(())
}
