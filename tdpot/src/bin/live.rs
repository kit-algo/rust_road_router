use csv::ReaderBuilder;
use std::{env, error::Error, fs::File, path::Path};

#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::customizable_contraction_hierarchy::*,
    cli::CliErr,
    datastr::graph::{floating_time_dependent::*, *},
    io::*,
    report::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("tdcch_live");
    report!("num_threads", rayon::current_num_threads());

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;

    let _algo_runs_ctxt = push_collection_context("algo_runs");

    let cch_folder = path.join("cch");
    let _cch = CCHReconstrctor(&graph).reconstruct_from(&cch_folder)?;

    let file = File::open(args.next().unwrap()).unwrap();
    let mut reader = ReaderBuilder::new()
        .has_headers(true)
        .delimiter(b';')
        .quoting(false)
        .double_quote(false)
        .escape(None)
        .from_reader(file);

    let mut live = vec![None; graph.num_arcs()];
    let t_live = (7 * 3600 + 47 * 60) * 1000;

    for line in reader.records() {
        let record = line?;
        let from = record[0].parse()?;
        let to = record[1].parse()?;
        let speed: u32 = record[2].parse()?;
        let distance: u32 = record[3].parse()?;
        let duration: u32 = record[4].parse()?;

        if speed == 0 || duration > 3600 * 5 {
            continue;
        }
        if let Some(EdgeIdT(edge_idx)) = graph.edge_indices(from, to).next() {
            let edge_idx = edge_idx as usize;

            let new_tt = 100 * 36 * distance / speed;
            live[edge_idx] = Some((new_tt, t_live + duration * 1000))
        }
    }

    let _live_graph = LiveGraph::new(graph, Timestamp::new(f64::from(t_live / 1000)), &live);

    Ok(())
}
