use std::{env, error::Error, path::Path};

use rust_road_router::{
    algo::customizable_contraction_hierarchy::*,
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    io::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;

    let graph = UnweightedFirstOutGraph::new(first_out, head);

    let cch_folder = path.join("cch");
    let node_order = NodeOrder::reconstruct_from(&cch_folder)?;
    let cch = CCHReconstrctor {
        original_graph: &graph,
        node_order,
    }
    .reconstruct_from(&cch_folder)?;

    let mut down_degs = vec![0; graph.num_nodes()];
    for node in 0..graph.num_nodes() {
        for head in cch.neighbor_iter(node as NodeId) {
            down_degs[head as usize] += 1;
        }
    }

    // println!("level,num_sep_nodes,num_cell_nodes,up_deg,down_deg");
    // debug_sep_tree(&cch.separators(), &cch, &down_degs);

    let mut levels = vec![0; graph.num_nodes()];
    for node in 0..graph.num_nodes() {
        for head in cch.neighbor_iter(node as NodeId) {
            levels[head as usize] = std::cmp::max(levels[head as usize], levels[node] + 1);
        }
    }

    println!("level,up_deg,down_deg");
    for node in 0..graph.num_nodes() {
        println!("{},{},{}", levels[node], cch.degree(node as NodeId), down_degs[node as usize]);
    }

    Ok(())
}

#[allow(dead_code)]
fn debug_sep_tree(sep_tree: &separator_decomposition::SeparatorTree, cch: &CCH, down_degs: &Vec<usize>) -> usize {
    let mut level = 0;
    for child in &sep_tree.children {
        level = std::cmp::max(debug_sep_tree(child, cch, down_degs) + 1, level);
    }

    let mut up_deg = 0;
    let mut down_deg = 0;
    for &node in &sep_tree.nodes {
        up_deg += cch.degree(node);
        down_deg += down_degs[node as usize];
    }

    println!("{},{},{},{},{}", level, sep_tree.nodes.len(), sep_tree.num_nodes, up_deg, down_deg);
    level
}
