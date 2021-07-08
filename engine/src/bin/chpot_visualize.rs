use rust_road_router::{
    algo::{a_star::*, ch_potentials::query::Server as TopoServer, dijkstra::generic_dijkstra::DefaultOps, *},
    cli::CliErr,
    datastr::graph::*,
    io::*,
    util::NonNan,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let modified_travel_time: Vec<Weight> = graph.weight().iter().map(|&weight| (weight as f64 * 1.5) as Weight).collect();
    let modified_graph = FirstOutGraph::new(graph.first_out(), graph.head(), &modified_travel_time[..]);

    let n = graph.num_nodes();
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;

    // let reversed = OwnedGraph::reversed(&graph);
    let potential = BaselinePotential::new(&graph);
    // let backward_potential = BaselinePotential::new(&reversed);
    let mut topocore: TopoServer<OwnedGraph, _, _> = TopoServer::new(&modified_graph, potential, DefaultOps::default());
    // let mut bidir_dijk_server = BiDirServer::new_with_potentials(modified_graph, potential, backward_potential);

    let from_lat = 49.0138685;
    let from_lng = 8.4173883;
    let to_lat = 49.0019405;
    let to_lng = 8.3814223;

    let from = (0..n)
        .min_by_key(|&node_idx| {
            let dlat = from_lat - lat[node_idx];
            let dlng = from_lng - lng[node_idx];
            NonNan::new(dlat * dlat + dlng * dlng).unwrap()
        })
        .unwrap() as NodeId;

    let to = (0..n)
        .min_by_key(|&node_idx| {
            let dlat = to_lat - lat[node_idx];
            let dlng = to_lng - lng[node_idx];
            NonNan::new(dlat * dlat + dlng * dlng).unwrap()
        })
        .unwrap() as NodeId;

    topocore.visualize_query(Query { from, to }, &lat[..], &lng[..]);
    // bidir_dijk_server.visualize_query(from, to, &lat[..], &lng[..]);

    Ok(())
}
