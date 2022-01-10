use rust_road_router::{
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    io::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No input directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("weight").reconstruct_from(&path)?;
    let order = NodeOrder::from_node_order(Vec::load_from(path.join("order"))?);

    let (forward, backward) = graph.ch_split(&order);
    let forward = OwnedGraph::permutated(&forward, &order);
    let backward = OwnedGraph::permutated(&backward, &order);

    forward.first_out().write_to(&path.join("forward_first_out"))?;
    forward.head().write_to(&path.join("forward_head"))?;
    forward.weight().write_to(&path.join("forward_weight"))?;
    backward.first_out().write_to(&path.join("backward_first_out"))?;
    backward.head().write_to(&path.join("backward_head"))?;
    backward.weight().write_to(&path.join("backward_weight"))?;

    Ok(())
}
