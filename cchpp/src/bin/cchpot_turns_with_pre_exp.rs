use rust_road_router::{
    algo::{
        a_star::*,
        ch_potentials::{query::BiDirServer as BiDirTopo, *},
        customizable_contraction_hierarchy::*,
        dijkstra::*,
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
    let _reporter = enable_reporting("bidir_cchpot_turns");

    let mut rng = experiments::rng(Default::default());

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;

    let order = NodeOrder::from_node_order(Vec::load_from(path.join(args.next().unwrap_or("cch_perm".to_string())))?);
    let cch = without_reporting(|| CCH::fix_order_and_build(&graph, order));
    let cch_pot_data = without_reporting(|| CCHPotData::new(&cch, &graph));

    let pots = (
        TurnExpandedPotential::new(&graph, cch_pot_data.forward_potential()),
        TurnExpandedPotential::new(&graph, cch_pot_data.backward_potential()),
    );

    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);
    let exp_graph = without_reporting(|| WeightedGraphReconstructor("travel_time").reconstruct_from(&path))?;

    let mut topocore = without_reporting(|| BiDirTopo::<_, AlternatingDirs>::new(&exp_graph, SymmetricBiDirPotential::<_, _>::new(pots.0, pots.1)));

    let mut algo_runs_ctxt = push_collection_context("algo_runs");
    let n = exp_graph.num_nodes();
    experiments::run_random_queries(n, &mut topocore, &mut rng, &mut algo_runs_ctxt, 100000);

    Ok(())
}
