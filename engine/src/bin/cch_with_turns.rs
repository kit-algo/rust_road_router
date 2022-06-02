// Example of complete CCH toolchain with turn costs.
// Takes a directory as argument, which has to contain the graph (in RoutingKit format),
// a nested disection order for the turn expanded graph and queries.

use std::{env, error::Error, path::Path};

use rust_road_router::{
    algo::customizable_contraction_hierarchy::{query::Server, *},
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    experiments,
    io::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let graph = rust_road_router::datastr::graph::line_graph(&graph, |_edge1_idx, _edge2_idx| Some(0));

    // use InertialFlowCutter with edge order (cut based) and separator reordering to obtain
    let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_exp_perm"))?);
    let cch = CCH::fix_order_and_build(&graph, order);
    let cch = (move || cch.to_directed_cch())();

    let mut server = Server::new(customize_directed(&cch, &graph));

    let from = Vec::load_from(path.join("test/exp_source"))?;
    let to = Vec::load_from(path.join("test/exp_target"))?;
    let ground_truth = Vec::load_from(path.join("test/exp_travel_time_length"))?;

    let mut gt_iter = ground_truth.iter().map(|&gt| match gt {
        INFINITY => None,
        val => Some(val),
    });

    experiments::run_queries(
        from.iter().copied().zip(to.iter().copied()).take(10000),
        &mut server,
        None,
        |_, _, _| (),
        // |_| (),
        |_, _| gt_iter.next(),
    );

    Ok(())
}
