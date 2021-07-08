// Example of complete Time-Dependent Sampling toolchain.
// Takes a directory as argument, which has to contain the graph (in RoutingKit format) and a nested disection order.

use std::{env, error::Error, path::Path};

use rust_road_router::{
    algo::{customizable_contraction_hierarchy, time_dependent_sampling::Server, *},
    cli::CliErr,
    datastr::{graph::time_dependent::*, node_order::NodeOrder},
    io::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;
    let cch_order = Vec::load_from(path.join("cch_perm"))?;

    let cch = customizable_contraction_hierarchy::contract(&graph, NodeOrder::from_node_order(cch_order));
    let mut server = Server::new(graph, &cch);
    println!("{:?}", server.td_query(TDQuery { from: 0, to: 1, departure: 42 }).map(|res| res.distance()));

    Ok(())
}
