#[macro_use]
extern crate rust_road_router;
#[allow(unused_imports)]
use rust_road_router::{
    algo::{
        a_star::*,
        alt::*,
        ch_potentials::{
            query::{BiDirServer as BiDirTopo, Server as UniDirTopo},
            *,
        },
        customizable_contraction_hierarchy::*,
        dijkstra::{
            query::{bidirectional_dijkstra::Server as BiDir, dijkstra::Server as UniDir},
            AlternatingDirs, ChooseMinKeyDir, DefaultOps,
        },
        *,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    experiments::{chpot::ProbabilisticSpeedWeightedScaler, *},
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

macro_rules! bidir_pre_callback {
    () => {
        |from, to, server| {
            let mut res = {
                let _blocked = block_reporting();
                server.query(Query { from, to })
            };
            #[cfg(all(not(feature = "chpot-alt"), not(feature = "chpot-only-topo"), not(feature = "chpot-oracle")))]
            report!(
                "num_pot_computations",
                res.data().potential().forward().num_pot_computations() + res.data().potential().backward().num_pot_computations()
            );
            #[cfg(feature = "chpot-oracle")]
            report!(
                "num_pot_computations",
                res.data().potential().forward().inner().num_pot_computations() + res.data().potential().backward().inner().num_pot_computations()
            );
            report!("lower_bound", res.data().lower_bound(from));
        }
    };
}

macro_rules! unidir_pre_callback {
    () => {
        |from, to, server| {
            let mut res = {
                let _blocked = block_reporting();
                server.query(Query { from, to })
            };
            #[cfg(all(not(feature = "chpot-alt"), not(feature = "chpot-only-topo"), not(feature = "chpot-oracle")))]
            report!("num_pot_computations", res.data().potential().num_pot_computations());
            #[cfg(feature = "chpot-oracle")]
            report!("num_pot_computations", res.data().potential().inner().num_pot_computations());
            report!("lower_bound", res.data().lower_bound(from));
        }
    };
}

#[allow(unused_braces)]
fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("bidir_chpot_scaling");
    let rng = rng(Default::default());
    let mut modify_rng = rng.clone();
    let q = chpot::num_queries();

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let n = graph.num_nodes();
    let geo_distance = Vec::<Weight>::load_from(path.join("geo_distance"))?;
    let tt_units_per_s = Vec::<u32>::load_from(path.join("tt_units_per_s"))?[0];
    let dist_units_per_m = Vec::<u32>::load_from(path.join("dist_units_per_m"))?[0];

    let mut pot_name = "CH";

    #[cfg(feature = "chpot-cch")]
    let cch = {
        // if cfg!(feature = "chpot-cch") {
        pot_name = "CCH";
        // }
        let _blocked = block_reporting();
        let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
        CCH::fix_order_and_build(&graph, order)
    };
    #[cfg(feature = "chpot-cch")]
    let cch_pot_data = {
        let _blocked = block_reporting();
        CCHPotData::new(&cch, &graph)
    };
    #[cfg(feature = "chpot-alt")]
    let alt_pot_data = {
        pot_name = "ALT";
        let _blocked = block_reporting();
        ALTPotData::new_with_avoid(&graph, 16, &mut rng.clone())
    };
    #[cfg(all(not(feature = "chpot-cch"), not(feature = "chpot-alt")))]
    let chpot_data = CHPotLoader::reconstruct_from(&path.join("lower_bound_ch"))?;

    if cfg!(feature = "chpot-only-topo") {
        pot_name = "Zero";
    }

    if cfg!(feature = "chpot-oracle") {
        pot_name = "Oracle";
    }

    report!("potential", pot_name);

    let base_potentials = || {
        #[cfg(feature = "chpot-only-topo")]
        {
            (ZeroPotential(), ZeroPotential())
        }
        #[cfg(not(feature = "chpot-only-topo"))]
        {
            #[cfg(feature = "chpot-cch")]
            {
                (cch_pot_data.forward_potential(), cch_pot_data.backward_potential())
            }
            #[cfg(feature = "chpot-alt")]
            {
                (alt_pot_data.forward_potential(), alt_pot_data.backward_potential())
            }
            #[cfg(all(not(feature = "chpot-cch"), not(feature = "chpot-alt")))]
            {
                chpot_data.potentials()
            }
        }
    };

    let potentials = || {
        let pots = base_potentials();
        #[cfg(feature = "chpot-oracle")]
        {
            (RecyclingPotential::new(pots.0), RecyclingPotential::new(pots.1))
        }
        #[cfg(not(feature = "chpot-oracle"))]
        pots
    };

    let mut exps_ctxt = push_collection_context("experiments".to_string());

    let run = |modified_travel_time: &[Weight]| {
        let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());
        let modified_graph = FirstOutGraph::new(graph.first_out(), graph.head(), modified_travel_time);

        // let mut gt = {
        //     let _blocked = block_reporting();
        //     customizable_contraction_hierarchy::query::Server::new(customize(&cch, &modified_graph))
        // };
        // let gt_cb = |from, to| Some(gt.query(Query { from, to }).distance());
        let gt_cb = |_, _| None;

        let mut server = {
            let _blocked = block_reporting();
            UniDirTopo::<OwnedGraph, _, _, true, true, true>::new(&modified_graph, potentials().0, DefaultOps::default())
        };
        run_random_queries_with_callbacks(n, &mut server, &mut rng.clone(), &mut algo_runs_ctxt, q, unidir_pre_callback!(), gt_cb);

        let (forward_pot, backward_pot) = potentials();
        let mut server = {
            let _blocked = block_reporting();
            BiDirTopo::<_, ChooseMinKeyDir>::new(
                &modified_graph,
                SymmetricBiDirPotential::<_, _, { cfg!(feature = "chpot-improved-pruning") }>::new(forward_pot, backward_pot),
            )
        };
        run_random_queries_with_callbacks(n, &mut server, &mut rng.clone(), &mut algo_runs_ctxt, q, bidir_pre_callback!(), gt_cb);

        let (forward_pot, backward_pot) = potentials();
        let mut server = {
            let _blocked = block_reporting();
            BiDirTopo::<_, AlternatingDirs>::new(
                &modified_graph,
                SymmetricBiDirPotential::<_, _, { cfg!(feature = "chpot-improved-pruning") }>::new(forward_pot, backward_pot),
            )
        };
        run_random_queries_with_callbacks(n, &mut server, &mut rng.clone(), &mut algo_runs_ctxt, q, bidir_pre_callback!(), gt_cb);

        let (forward_pot, backward_pot) = potentials();
        let mut server = {
            let _blocked = block_reporting();
            BiDirTopo::<_, ChooseMinKeyDir>::new(
                &modified_graph,
                AveragePotential::<_, _, { cfg!(feature = "chpot-improved-pruning") }>::new(forward_pot, backward_pot),
            )
        };
        run_random_queries_with_callbacks(n, &mut server, &mut rng.clone(), &mut algo_runs_ctxt, q, bidir_pre_callback!(), gt_cb);

        let (forward_pot, backward_pot) = potentials();
        let mut server = {
            let _blocked = block_reporting();
            BiDirTopo::<_, AlternatingDirs>::new(
                &modified_graph,
                AveragePotential::<_, _, { cfg!(feature = "chpot-improved-pruning") }>::new(forward_pot, backward_pot),
            )
        };
        run_random_queries_with_callbacks(n, &mut server, &mut rng.clone(), &mut algo_runs_ctxt, q, bidir_pre_callback!(), gt_cb);

        let mut server = {
            let _blocked = block_reporting();
            UniDir::<_, DefaultOps, _>::with_potential(modified_graph.clone(), potentials().0)
        };
        run_random_queries_with_callbacks(n, &mut server, &mut rng.clone(), &mut algo_runs_ctxt, q, unidir_pre_callback!(), gt_cb);

        let (forward_pot, backward_pot) = potentials();
        let mut server = {
            let _blocked = block_reporting();
            BiDir::<_, _, _, ChooseMinKeyDir>::new_with_potentials(
                modified_graph.clone(),
                SymmetricBiDirPotential::<_, _, { cfg!(feature = "chpot-improved-pruning") }>::new(forward_pot, backward_pot),
            )
        };
        run_random_queries_with_callbacks(n, &mut server, &mut rng.clone(), &mut algo_runs_ctxt, q, bidir_pre_callback!(), gt_cb);

        let (forward_pot, backward_pot) = potentials();
        let mut server = {
            let _blocked = block_reporting();
            BiDir::<_, _, _, AlternatingDirs>::new_with_potentials(
                modified_graph.clone(),
                SymmetricBiDirPotential::<_, _, { cfg!(feature = "chpot-improved-pruning") }>::new(forward_pot, backward_pot),
            )
        };
        run_random_queries_with_callbacks(n, &mut server, &mut rng.clone(), &mut algo_runs_ctxt, q, bidir_pre_callback!(), gt_cb);

        let (forward_pot, backward_pot) = potentials();
        let mut server = {
            let _blocked = block_reporting();
            BiDir::<_, _, _, ChooseMinKeyDir>::new_with_potentials(
                modified_graph.clone(),
                AveragePotential::<_, _, { cfg!(feature = "chpot-improved-pruning") }>::new(forward_pot, backward_pot),
            )
        };
        run_random_queries_with_callbacks(n, &mut server, &mut rng.clone(), &mut algo_runs_ctxt, q, bidir_pre_callback!(), gt_cb);

        let (forward_pot, backward_pot) = potentials();
        let mut server = {
            let _blocked = block_reporting();
            BiDir::<_, _, _, AlternatingDirs>::new_with_potentials(
                modified_graph.clone(),
                AveragePotential::<_, _, { cfg!(feature = "chpot-improved-pruning") }>::new(forward_pot, backward_pot),
            )
        };
        run_random_queries_with_callbacks(n, &mut server, &mut rng.clone(), &mut algo_runs_ctxt, q, bidir_pre_callback!(), gt_cb);
    };

    for factor in [1., 1.05, 1.1] {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "weight_scale");
        report!("factor", factor);

        let mut modified_travel_time = graph.weight().to_vec();
        ProbabilisticSpeedWeightedScaler::scale_all(tt_units_per_s, dist_units_per_m, factor).scale_with_speed_weighted_prob(
            &mut modify_rng,
            &mut modified_travel_time,
            &geo_distance,
        );
        run(&modified_travel_time);
    }

    for factor in [1.25, 1.5] {
        for p in [0.0, 1.0] {
            for speed in [80.0] {
                let _exp_ctx = exps_ctxt.push_collection_item();
                report!("experiment", "probabilistic_scale_by_speed");
                report!("factor", factor);
                report!("probability", p);
                report!("speed", speed);

                let mut modified_travel_time = graph.weight().to_vec();
                ProbabilisticSpeedWeightedScaler::speed_cutoff(tt_units_per_s, dist_units_per_m, factor, speed, p).scale_with_speed_weighted_prob(
                    &mut modify_rng,
                    &mut modified_travel_time,
                    &geo_distance,
                );
                run(&modified_travel_time);
            }
        }
    }

    Ok(())
}
