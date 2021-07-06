// CATCHUp query experiments with pregenerated queries with source and target drawn uniformly at random.
// Takes as input one directory arg which should contain the all data.

use std::{error::Error, path::Path};

use crate::{
    algo::customizable_contraction_hierarchy::*,
    datastr::{
        graph::{
            floating_time_dependent::{shortcut_graph::CustomizedGraphReconstrctor, *},
            *,
        },
        node_order::NodeOrder,
    },
    io::*,
    report::*,
};

use rand::prelude::*;

pub fn setup(path: &Path, run: impl FnOnce(&TDGraph, &mut StdRng, &CCH, &CustomizedGraph) -> Result<(), Box<dyn Error>>) -> Result<(), Box<dyn Error>> {
    report!("num_threads", rayon::current_num_threads());

    let seed = Default::default();
    report!("seed", seed);
    let mut rng = StdRng::from_seed(seed);

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let first_ipp_of_arc = Vec::load_from(path.join("first_ipp_of_arc"))?;
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time"))?;
    let ipp_travel_time = Vec::<u32>::load_from(path.join("ipp_travel_time"))?;

    report!("unprocessed_graph", { "num_nodes": first_out.len() - 1, "num_arcs": head.len(), "num_ipps": ipp_departure_time.len() });

    let graph = TDGraph::new(first_out, head, first_ipp_of_arc, ipp_departure_time, ipp_travel_time);

    report!("graph", { "num_nodes": graph.num_nodes(), "num_arcs": graph.num_arcs(), "num_ipps": graph.num_ipps(), "num_constant_ttfs": graph.num_constant() });

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

    run(&graph, &mut rng, &cch, &td_cch_graph)
}
