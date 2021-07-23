// CATCHUp query experiments with pregenerated queries with source and target drawn uniformly at random.
// Takes as input one directory arg which should contain the all data.

use std::{error::Error, path::Path};

use crate::{
    algo::customizable_contraction_hierarchy::*,
    datastr::graph::floating_time_dependent::{shortcut_graph::CustomizedGraphReconstrctor, *},
    io::*,
    report::*,
};

use rand::prelude::*;

pub fn setup(path: &Path, run: impl FnOnce(&TDGraph, &mut StdRng, &CCH, &CustomizedGraph) -> Result<(), Box<dyn Error>>) -> Result<(), Box<dyn Error>> {
    report!("num_threads", rayon::current_num_threads());

    let mut rng = super::rng(Default::default());

    let core_ids = core_affinity::get_core_ids().unwrap();
    core_affinity::set_for_current(core_ids[0]);

    let graph = TDGraph::reconstruct_from(&path)?;

    let cch_folder = path.join("cch");
    let cch = CCHReconstrctor(&graph).reconstruct_from(&cch_folder)?;

    let customized_folder = path.join("customized");

    let td_cch_graph = CustomizedGraphReconstrctor(&graph).reconstruct_from(&customized_folder)?;

    run(&graph, &mut rng, &cch, &td_cch_graph)
}
