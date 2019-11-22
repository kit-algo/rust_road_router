use std::{env, error::Error, path::Path};

use time::Duration;

use bmw_routing_engine::{
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    io::Load,
    report::benchmark::*,
    shortest_path::{
        customizable_contraction_hierarchy::*,
        query::topocore::Server as TopoServer,
        // query::dijkstra::Server as DijkServer,
        topocore::preprocess,
    },
};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let travel_time = Vec::load_from(path.join("travel_time"))?;
    #[cfg(feature = "chpot_visualize")]
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    #[cfg(feature = "chpot_visualize")]
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;

    let from = Vec::load_from(path.join("test/source"))?;
    let to = Vec::load_from(path.join("test/target"))?;
    let ground_truth = Vec::load_from(path.join("test/travel_time_length"))?;

    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);

    let cch_order = Vec::load_from(path.join("cch_perm"))?;
    let cch_order = NodeOrder::from_node_order(cch_order);

    let cch = contract(&graph, cch_order.clone());
    let cch_order = CCHReordering {
        node_order: cch_order,
        latitude: &[],
        longitude: &[],
    }
    .reorder_for_seperator_based_customization(cch.separators());
    let cch = contract(&graph, cch_order);

    // let mut simple_server = DijkServer::new(graph);

    let topocore = report_time("topocore preprocessing", || preprocess(&graph));
    let mut topocore = TopoServer::new(topocore, &cch, &graph);

    let mut total_query_time = Duration::zero();

    let num_queries = 1000;

    for ((&from, &to), &ground_truth) in from.iter().zip(to.iter()).zip(ground_truth.iter()).take(num_queries) {
        let ground_truth = match ground_truth {
            INFINITY => None,
            val => Some(val),
        };

        let (res, time) = measure(|| {
            // simple_server.distance(from, to)
            #[cfg(feature = "chpot_visualize")]
            {
                topocore.distance(from, to, &lat, &lng)
            }
            #[cfg(not(feature = "chpot_visualize"))]
            {
                topocore.distance(from, to)
            }
        });
        topocore.path();
        if res != ground_truth {
            eprintln!("topo {:?} ground_truth {:?} ({} - {})", res, ground_truth, from, to);
        }
        // assert_eq!(res, ground_truth, "{} - {}", from, to);

        total_query_time = total_query_time + time;
    }

    eprintln!("Avg. query time {}", total_query_time / (num_queries as i32));

    Ok(())
}
