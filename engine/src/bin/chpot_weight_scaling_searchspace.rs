use rust_road_router::{algo::dijkstra::query::dijkstra::Server as DijkServer, cli::CliErr, datastr::graph::*, io::*};

use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let from = args.next().ok_or(CliErr("No from node arg given"))?.parse::<NodeId>()?;
    let to = args.next().ok_or(CliErr("No to node arg given"))?.parse::<NodeId>()?;

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let travel_time = Vec::load_from(path.join("travel_time"))?;

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);
    let reversed = OwnedGraph::reversed(&graph);

    let n = graph.num_nodes();

    let mut forward_dijkstra = DijkServer::new(graph);
    let mut backward_dijkstra = DijkServer::new(reversed);
    let forward_dists = forward_dijkstra.one_to_all(from);
    let backward_dists = backward_dijkstra.one_to_all(to);

    let total_dist = forward_dists.distance(to);

    for node in 0..n as NodeId {
        let forward_dist = forward_dists.distance(node);
        let backward_dist = backward_dists.distance(node);

        if total_dist - forward_dist > 0 {
            println!("{}", backward_dist as f64 / (total_dist as f64 - forward_dist as f64));
        } else {
            println!("{}", 1.0);
        }
    }

    Ok(())
}
