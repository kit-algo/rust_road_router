// Example of complete CCH toolchain.
// Takes a directory as argument, which has to contain the graph (in RoutingKit format), a nested disection order and queries.

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
    let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
    let cch = CCH::fix_order_and_build(&graph, order);

    let mut server = Server::new(customize_perfect(customize(&cch, &graph)));

    let from = Vec::load_from(path.join("test/source"))?;
    let to = Vec::load_from(path.join("test/target"))?;
    let ground_truth = Vec::load_from(path.join("test/travel_time_length"))?;

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
