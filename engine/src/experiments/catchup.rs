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

    affinity::set_thread_affinity(&[0]).unwrap();

    let graph = TDGraph::reconstruct_from(&path)?;

    let cch_folder = path.join("cch");
    let cch = CCHReconstrctor(&graph).reconstruct_from(&cch_folder)?;

    let customized_folder = path.join("customized");

    let td_cch_graph = CustomizedGraphReconstrctor {
        original_graph: &graph,
        first_out: cch.first_out(),
        head: cch.head(),
    }
    .reconstruct_from(&customized_folder)?;

    run(&graph, &mut rng, &cch, &td_cch_graph)
}
