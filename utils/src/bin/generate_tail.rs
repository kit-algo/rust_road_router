use rust_road_router::{cli::CliErr, datastr::graph::*, io::*};

use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = UnweightedOwnedGraph::reconstruct_from(&path)?;

    let mut tail = Vec::with_capacity(graph.num_arcs());
    for node in 0..graph.num_nodes() {
        for _ in 0..graph.degree(node as NodeId) {
            tail.push(node as NodeId);
        }
    }

    tail.write_to(&path.join("tail"))?;

    Ok(())
}
