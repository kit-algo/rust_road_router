#![feature(array_windows)]

use std::{env, error::Error, path::Path};

use rust_road_router::{
    algo::{
        customizable_contraction_hierarchy::{query::Server, *},
        *,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    io::*,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let order = NodeOrder::from_node_order(Vec::load_from(path.join("cch_perm"))?);
    let cch = CCH::fix_order_and_build(&graph, order);

    let mut server = Server::new(customize_perfect(customize(&cch, &graph)));

    let from = Vec::load_from(path.join("test/source"))?;
    let to = Vec::load_from(path.join("test/target"))?;
    let ground_truth = Vec::load_from(path.join("test/travel_time_length"))?;

    let gt_iter = ground_truth.iter().map(|&gt| match gt {
        INFINITY => None,
        val => Some(val),
    });

    for ((&from, &to), gt) in from.iter().zip(to.iter()).zip(gt_iter) {
        let mut res = server.query(Query { from, to });
        let dist = res.distance();
        assert_eq!(dist, gt);
        if let Some(_) = dist {
            let nodes = res.node_path().unwrap();
            let mut dist = 0;
            for &[tail, head] in nodes.array_windows::<2>() {
                dist += graph.edge_indices(tail, head).map(|EdgeIdT(e)| graph.link(e).weight).min().unwrap();
            }

            assert_eq!(dist, gt.unwrap());
        }
    }

    Ok(())
}
