use rand::prelude::*;
use rust_road_router::{
    cli::CliErr,
    datastr::graph::{time_dependent::*, *},
    io::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No input directory arg given"))?;
    let path = Path::new(arg);

    let graph = TDGraph::reconstruct_from(&path)?;
    let n = graph.num_nodes();

    let mut rng = StdRng::from_entropy();

    let num_queries = args.next().map(|arg| arg.parse().expect("could not parse num_queries")).unwrap_or(10000);
    let uniform_random_sources: Vec<_> = std::iter::repeat_with(|| rng.gen_range(0..n as NodeId)).take(num_queries).collect();
    let uniform_random_targets: Vec<_> = std::iter::repeat_with(|| rng.gen_range(0..n as NodeId)).take(num_queries).collect();
    let uniform_random_departures: Vec<Timestamp> = std::iter::repeat_with(|| rng.gen_range(0..period())).take(num_queries).collect();

    uniform_random_sources.write_to(&path.join("queries/uniform/source"))?;
    uniform_random_targets.write_to(&path.join("queries/uniform/target"))?;
    uniform_random_departures.write_to(&path.join("queries/uniform/departure"))?;

    Ok(())
}
