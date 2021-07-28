#[macro_use]
extern crate rust_road_router;
#[allow(unused_imports)]
use rust_road_router::{
    algo::{
        a_star::*,
        alt::*,
        ch_potentials::{
            query::{BiDirServer, Server},
            *,
        },
        customizable_contraction_hierarchy::*,
        dijkstra::{AlternatingDirs, ChooseMinKeyDir, DefaultOps},
    },
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    experiments::{chpot::ProbabilisticSpeedWeightedScaler, *},
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("bidir_chpot_scaling");
    let rng = rng(Default::default());
    let mut modify_rng = rng.clone();

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let geo_distance = Vec::<Weight>::load_from(path.join("geo_distance"))?;

    let mut pot_name = "CH";

    #[cfg(feature = "chpot-cch")]
    let cch = {
        pot_name = "CCH";
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
        let _ = block_reporting();
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

        let mut server = Server::<OwnedGraph, _, _, true, true, true>::new(&modified_graph, potentials().0, DefaultOps::default());
        run_random_queries(graph.num_nodes(), &mut server, &mut rng.clone(), &mut algo_runs_ctxt, chpot::num_queries());

        let (forward_pot, backward_pot) = potentials();
        let mut server = BiDirServer::<_, ChooseMinKeyDir>::new(&modified_graph, forward_pot, backward_pot);
        run_random_queries(graph.num_nodes(), &mut server, &mut rng.clone(), &mut algo_runs_ctxt, chpot::num_queries());

        let (forward_pot, backward_pot) = potentials();
        let mut server = BiDirServer::<_, AlternatingDirs>::new(&modified_graph, forward_pot, backward_pot);
        run_random_queries(graph.num_nodes(), &mut server, &mut rng.clone(), &mut algo_runs_ctxt, chpot::num_queries());
    };

    for factor in [1., 1.05, 1.1] {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "weight_scale");
        report!("factor", factor);

        let mut modified_travel_time = graph.weight().to_vec();
        ProbabilisticSpeedWeightedScaler::scale_all(factor).scale_with_speed_weighted_prob(&mut modify_rng, &mut modified_travel_time, &geo_distance);
        run(&modified_travel_time);
    }

    for factor in [1., 1.25, 1.5] {
        for p in [0.0, 1.0] {
            for speed in [80.0] {
                let _exp_ctx = exps_ctxt.push_collection_item();
                report!("experiment", "probabilistic_scale_by_speed");
                report!("factor", factor);
                report!("probability", p);
                report!("speed", speed);

                let mut modified_travel_time = graph.weight().to_vec();
                ProbabilisticSpeedWeightedScaler::speed_cutoff(factor, speed, p).scale_with_speed_weighted_prob(
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
