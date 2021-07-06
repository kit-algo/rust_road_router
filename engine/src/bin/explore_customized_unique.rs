// CATCHUp query experiments with pregenerated queries with source and target drawn uniformly at random.
// Takes as input one directory arg which should contain the all data.

use std::{env, error::Error, path::Path};

use rust_road_router::{
    algo::customizable_contraction_hierarchy::*,
    cli::CliErr,
    datastr::{
        graph::floating_time_dependent::{shortcut_graph::CustomizedGraphReconstrctor, *},
        node_order::NodeOrder,
    },
    io::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc"))?;
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time"))?;
    let ipp_travel_time = Vec::<u32>::load_from(path.join("ipp_travel_time"))?;

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time);

    let cch_folder = path.join("cch");
    let node_order = NodeOrder::reconstruct_from(&cch_folder)?;
    let cch = CCHReconstrctor {
        original_graph: &graph,
        node_order,
    }
    .reconstruct_from(&cch_folder)?;

    let customized_folder = path.join("customized");

    let td_cch_graph = CustomizedGraphReconstrctor {
        original_graph: &graph,
        first_out: cch.first_out(),
        head: cch.head(),
    }
    .reconstruct_from(&customized_folder)?;

    let (unique_down, unique_up) = td_cch_graph.unique_path_edges();
    eprintln!("down: {}/{}", unique_down.count_ones(), unique_down.len());
    eprintln!("up: {}/{}", unique_up.count_ones(), unique_up.len());

    Ok(())
}
