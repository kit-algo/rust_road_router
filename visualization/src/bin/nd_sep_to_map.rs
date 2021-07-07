use std::{env, error::Error, path::Path};

use rust_road_router::{algo::customizable_contraction_hierarchy::*, cli::CliErr, datastr::graph::*, io::*};

const MAX_LEVEL: usize = 20;

fn main() -> Result<(), Box<dyn Error>> {
    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;

    let graph = UnweightedFirstOutGraph::new(first_out, head);

    let cch_folder = path.join("cch");
    let cch = CCHReconstrctor(&graph).reconstruct_from(&cch_folder)?;

    let mut sizes = vec![0; MAX_LEVEL];
    let mut cells = vec![0; MAX_LEVEL];
    level_sizes(&cch.separators(), &cch, 0, &mut sizes, &mut cells, &lat, &lng);

    dbg!(&sizes, cells);

    Ok(())
}

fn level_sizes(sep_tree: &separator_decomposition::SeparatorTree, cch: &CCH, level: usize, sizes: &mut [usize], cells: &mut [usize], lat: &[f32], lng: &[f32]) {
    if level >= MAX_LEVEL {
        return;
    }
    sizes[level] += sep_tree.nodes.len();
    cells[level] += 1;
    for &node in &sep_tree.nodes {
        let node = cch.node_order().node(node) as usize;
        println!(
            "L.marker([{}, {}], {{ icon: L.dataIcon({{ data: {{ level: {} }}, ...blueIconOptions }}) }}).addTo(map);",
            lat[node], lng[node], level
        );
    }
    for child in &sep_tree.children {
        level_sizes(child, cch, level + 1, sizes, cells, lat, lng);
    }
}
