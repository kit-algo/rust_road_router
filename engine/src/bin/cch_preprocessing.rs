// Example of complete CCH toolchain.
// Takes a directory as argument, which has to contain the graph (in RoutingKit format), a nested disection order and queries.

use std::{env, error::Error, path::Path};

use rust_road_router::{
    algo::customizable_contraction_hierarchy::*,
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    io::*,
    report::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("cch_preprocessing");
    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("lower_bound").reconstruct_from(&path)?;
    let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
    let cch = CCH::fix_order_and_build(&graph, order);

    customize_perfect(customize(&cch, &graph));

    Ok(())
}
