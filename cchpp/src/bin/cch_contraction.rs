use std::{env, error::Error, path::Path};

use rust_road_router::{
    algo::customizable_contraction_hierarchy::*,
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    io::*,
    report::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("cch_contraction");
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let order = NodeOrder::from_node_order(Vec::load_from(path.join(args.next().unwrap_or("cch_perm".to_string())))?);

    let mut contractions_ctxt = push_collection_context("contractions");

    for _ in 0..100 {
        let _run = contractions_ctxt.push_collection_item();
        CCH::fix_order_and_build(&graph, order.clone());
    }

    Ok(())
}
