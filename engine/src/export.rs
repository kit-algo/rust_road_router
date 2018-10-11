use std::fs::File;
use std::io::{Result, Write};
use crate::graph::*;

pub fn write_graph_to_gr<G: for<'a> LinkIterGraph<'a>>(graph: &G, filename: &str) -> Result<()> {
    let mut file  = File::create(filename)?;
    writeln!(&mut file, "p sp {} {}", graph.num_nodes(), graph.num_arcs())?;

    for i in 0..graph.num_nodes() {
        for Link { node, weight } in graph.neighbor_iter(i as NodeId) {
            writeln!(&mut file, "a {} {} {}", i + 1, node + 1, weight)?;
        }
    }

    Ok(())
}

pub fn write_coords_to_co(lat: &[f32], lng: &[f32], filename: &str) -> Result<()> {
    assert_eq!(lat.len(), lng.len());
    let mut file  = File::create(filename)?;
    writeln!(&mut file, "p aux sp co {}", lat.len())?;
    for (i, (lat, lng)) in lat.iter().zip(lng.iter()).enumerate() {
        writeln!(&mut file, "v {} {} {}", i + 1, (lat * 1_000_000.0) as i32, (lng * 1_000_000.0) as i32)?;
    }
    Ok(())
}
