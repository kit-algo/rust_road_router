use std::{env, error::Error, path::Path};

use rust_road_router::{
    algo::{
        contraction_hierarchy::{self, query::Server as CHServer, ContractionHierarchy},
        dijkstra::{
            query::{bidirectional_dijkstra::Server as BiDijkServer, dijkstra::Server as DijkServer},
            DefaultOps,
        },
        *,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    io::{Load, ReconstructPrepared},
    report::benchmark::report_time,
};

fn main() -> Result<(), Box<dyn Error>> {
    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let from = Vec::load_from(path.join("test/source"))?;
    let to = Vec::load_from(path.join("test/target"))?;
    let ground_truth = Vec::load_from(path.join("test/travel_time_length"))?;

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let mut simple_server = DijkServer::<_, DefaultOps>::new(graph.borrowed());
    let mut bi_dir_server = BiDijkServer::<_, _, _>::new(graph.borrowed());

    let ch_first_out = Vec::load_from(path.join("travel_time_ch/first_out"))?;
    let ch_head = Vec::load_from(path.join("travel_time_ch/head"))?;
    let ch_weight = Vec::load_from(path.join("travel_time_ch/weight"))?;
    let ch_order = NodeOrder::from_node_order(Vec::load_from(path.join("travel_time_ch/order"))?);
    let mut ch_server = CHServer::new(
        ContractionHierarchy::from_contracted_graph(OwnedGraph::new(ch_first_out, ch_head, ch_weight), &ch_order),
        NodeOrder::identity(graph.num_nodes()),
    );
    let mut ch_server_with_own_ch = CHServer::new(contraction_hierarchy::contract(&graph, ch_order.clone()), ch_order);

    for ((&from, &to), &ground_truth) in from.iter().zip(to.iter()).zip(ground_truth.iter()).take(100) {
        let ground_truth = match ground_truth {
            INFINITY => None,
            val => Some(val),
        };

        report_time("simple dijkstra", || {
            assert_eq!(simple_server.query(Query { from, to }).distance(), ground_truth);
        });
        report_time("bidir dijkstra", || {
            assert_eq!(bi_dir_server.query(Query { from, to }).distance(), ground_truth);
        });
        report_time("CH", || {
            assert_eq!(ch_server.query(Query { from, to }).distance(), ground_truth);
        });
        report_time("own CH", || {
            assert_eq!(ch_server_with_own_ch.query(Query { from, to }).distance(), ground_truth);
        });
    }

    Ok(())
}
