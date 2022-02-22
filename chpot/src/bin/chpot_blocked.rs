#[macro_use]
extern crate rust_road_router;
use rand::prelude::*;
use rust_road_router::{
    algo::{
        ch_potentials::{query::Server as TopoServer, *},
        dijkstra::{query::dijkstra::Server as DijkServer, DefaultOps},
    },
    cli::CliErr,
    datastr::graph::*,
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

const TUNNEL_BIT: u8 = 1;
const FREEWAY_BIT: u8 = 2;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_blocked");
    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let arc_category = Vec::<u8>::load_from(path.join("arc_category"))?;

    let mut exps_ctxt = push_collection_context("experiments");

    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "no_tunnels");

        run(path, |_graph, _rng, travel_time| {
            for (weight, &category) in travel_time.iter_mut().zip(arc_category.iter()) {
                if (category & TUNNEL_BIT) != 0 {
                    *weight = INFINITY;
                }
            }

            Ok(())
        })?;
    }

    {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "no_highways");

        run(path, |_graph, _rng, travel_time| {
            for (weight, &category) in travel_time.iter_mut().zip(arc_category.iter()) {
                if (category & FREEWAY_BIT) != 0 {
                    *weight = INFINITY;
                }
            }

            Ok(())
        })?;
    }

    Ok(())
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

    let mut algo_runs_ctxt = push_collection_context("algo_runs");

    affinity::set_thread_affinity(&[0]).unwrap();

    let potential = CHPotential::reconstruct_from(&path.join("lower_bound_ch"))?;

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let infinity_filtered = InfinityFilteringGraph(modified_graph);
    let mut topocore: TopoServer<OwnedGraph, _, _, true, true, true> = TopoServer::new(&infinity_filtered, potential, DefaultOps::default());
    let InfinityFilteringGraph(modified_graph) = infinity_filtered;
    drop(virtual_topocore_ctxt);

    experiments::run_random_queries_with_callbacks(
        graph.num_nodes(),
        &mut topocore,
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

    let mut server = DijkServer::<_, DefaultOps>::new(modified_graph);
    experiments::run_random_queries(
        graph.num_nodes(),
        &mut server,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::num_dijkstra_queries(),
    );

    Ok(())
}
