#[macro_use]
extern crate rust_road_router;
#[allow(unused_imports)]
use rust_road_router::{
    algo::{
        a_star::*,
        alt::*,
        ch_potentials::{query::Server as TopoServer, *},
        customizable_contraction_hierarchy::*,
        dijkstra::{query::dijkstra::Server as DijkServer, DefaultOps},
        *,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("chpot_features");

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let mut modified_travel_time = graph.weight().to_vec();
    for weight in &mut modified_travel_time {
        *weight = (*weight as f64 * 1.05) as Weight;
    }

    let mut rng = experiments::rng(Default::default());
    report!("experiment", "weight_scale");
    report!("factor", "1.05");
    let modified_graph = FirstOutGraph::new(graph.first_out(), graph.head(), &modified_travel_time[..]);

    #[cfg(feature = "chpot-cch")]
    let cch = {
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
        let _ = block_reporting();
        ALTPotData::new_with_avoid(&graph, 16, &mut rng)
    };

    #[allow(unused_mut)]
    let mut pot_name;

    let potential = {
        #[cfg(feature = "chpot-only-topo")]
        {
            pot_name = "Zero";
            ZeroPotential()
        }
        #[cfg(not(feature = "chpot-only-topo"))]
        {
            #[cfg(feature = "chpot-cch")]
            {
                pot_name = "CCH";
                cch_pot_data.forward_potential()
            }
            #[cfg(feature = "chpot-alt")]
            {
                pot_name = "ALT";
                alt_pot_data.forward_potential()
            }
            #[cfg(all(not(feature = "chpot-cch"), not(feature = "chpot-alt")))]
            {
                pot_name = "CH";
                CHPotential::reconstruct_from(&path.join("lower_bound_ch"))?
            }
        }
    };

    #[cfg(feature = "chpot-oracle")]
    let potential = RecyclingPotential::new(potential);
    #[cfg(feature = "chpot-oracle")]
    {
        pot_name = "Oracle";
    }

    report!("bcc_core", !cfg!(feature = "chpot-no-bcc"));
    report!("skip_deg2", !cfg!(feature = "chpot-no-deg2"));
    report!("skip_deg3", !cfg!(feature = "chpot-no-deg3"));
    report!("potential", pot_name);
    let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());

    #[cfg(not(feature = "chpot-no-reorder"))]
    let mut server = {
        let _ = algo_runs_ctxt.push_collection_item();
        TopoServer::<OwnedGraph, _, _, { !cfg!(feature = "chpot-no-bcc") }, { !cfg!(feature = "chpot-no-deg2") }, { !cfg!(feature = "chpot-no-deg3") }>::new_custom(
            &modified_graph,
            potential,
            DefaultOps::default(),
        )
    };
    #[cfg(feature = "chpot-no-reorder")]
    let mut server: DijkServer<_, DefaultOps, _> = {
        let _ = algo_runs_ctxt.push_collection_item();
        DijkServer::with_potential(modified_graph, potential)
    };

    experiments::run_random_queries_with_callbacks(
        graph.num_nodes(),
        &mut server,
        &mut rng,
        &mut algo_runs_ctxt,
        experiments::chpot::num_queries(),
        |_from, _to, _server| {
            #[cfg(feature = "chpot-oracle")]
            _server.query(Query { from: _from, to: _to });
        },
        // |mut res| {
        //     #[cfg(all(not(feature = "chpot-only-topo"), not(feature = "chpot-alt")))]
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

    Ok(())
}
