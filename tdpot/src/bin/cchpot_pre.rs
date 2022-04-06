use std::{env, error::Error, path::Path};

use rust_road_router::{
    algo::customizable_contraction_hierarchy::*,
    cli::CliErr,
    datastr::{
        graph::{time_dependent::*, *},
        node_order::NodeOrder,
    },
    io::*,
    report::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("cchpot_pre");
    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;
    let lower_bound = (0..graph.num_arcs() as EdgeId)
        .map(|edge_id| graph.travel_time_function(edge_id).lower_bound())
        .collect::<Box<[Weight]>>();
    let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);

    report_time_with_key("preprocessing", "preprocessing", || {
        without_reporting(|| {
            let cch = CCH::fix_order_and_build(&graph, order);
            customize_perfect(customize(&cch, &BorrowedGraph::new(graph.first_out(), graph.head(), &lower_bound)));
        })
    });

    Ok(())
}
