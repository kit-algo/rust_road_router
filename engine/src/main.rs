use std::{env, error::Error, path::Path, sync::Arc};

use rust_road_router::{
    algo::{
        contraction_hierarchy::{self, query::Server as CHServer, ContractionHierarchy},
        dijkstra::query::{bidirectional_dijkstra::Server as BiDijkServer, dijkstra::Server as DijkServer},
        *,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    io::Load,
    report::benchmark::report_time,
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Arc::new(Vec::load_from(path.join("first_out"))?);
    let head = Arc::new(Vec::load_from(path.join("head"))?);
    let travel_time = Arc::new(Vec::load_from(path.join("travel_time"))?);

    let from = Vec::load_from(path.join("test/source"))?;
    let to = Vec::load_from(path.join("test/target"))?;
    let ground_truth = Vec::load_from(path.join("test/travel_time_length"))?;

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);
    let mut simple_server = DijkServer::new(graph.clone());
    let mut bi_dir_server = BiDijkServer::new(graph.clone());

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
            assert_eq!(simple_server.query(Query { from, to }).map(|res| res.distance()), ground_truth);
        });
        report_time("bidir dijkstra", || {
            assert_eq!(bi_dir_server.query(Query { from, to }).map(|res| res.distance()), ground_truth);
        });
        report_time("CH", || {
            assert_eq!(ch_server.query(Query { from, to }).map(|res| res.distance()), ground_truth);
        });
        report_time("own CH", || {
            assert_eq!(ch_server_with_own_ch.query(Query { from, to }).map(|res| res.distance()), ground_truth);
        });
    }

    Ok(())
}
