// WIP: CH potentials for live traffic
use bmw_routing_engine::{
    algo::{
        ch_potentials::query::Server as TopoServer,
        customizable_contraction_hierarchy::{query::Server, *},
        *,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder},
    io::*,
    report::benchmark::*,
};
use std::{env, error::Error, path::Path};

use rand::prelude::*;
use time::Duration;

const TUNNEL_BIT: u8 = 1;
const FREEWAY_BIT: u8 = 2;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let first_out = Vec::<NodeId>::load_from(path.join("first_out"))?;
    let head = Vec::<EdgeId>::load_from(path.join("head"))?;
    let mut travel_time = Vec::<EdgeId>::load_from(path.join("travel_time"))?;
    let arc_category = Vec::<u8>::load_from(path.join("arc_category"))?;
    #[cfg(feature = "chpot_visualize")]
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    #[cfg(feature = "chpot_visualize")]
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;
    let mut graph = FirstOutGraph::new(&first_out[..], &head[..], &mut travel_time[..]);
    unify_parallel_edges(&mut graph);
    drop(graph);
    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);
    let mut no_tunnels = travel_time.clone();

    for (weight, &category) in no_tunnels.iter_mut().zip(arc_category.iter()) {
        if (category & TUNNEL_BIT) != 0 {
            *weight = INFINITY;
        }
    }

    // let mut no_freeways = travel_time.clone();
    let no_tunnels = FirstOutGraph::new(&first_out[..], &head[..], &no_tunnels[..]);

    let cch_order = Vec::load_from(path.join("cch_perm"))?;
    let cch_order = NodeOrder::from_node_order(cch_order);

    let cch = contract(&graph, cch_order);
    let cch_order = CCHReordering {
        cch: &cch,
        latitude: &[],
        longitude: &[],
    }
    .reorder_for_seperator_based_customization();
    let cch = contract(&graph, cch_order);

    let mut cch_static_server = Server::new(customize(&cch, &graph));
    let mut cch_live_server = Server::new(customize(&cch, &no_tunnels));

    let mut topocore = {
        #[cfg(feature = "chpot_visualize")]
        {
            TopoServer::new(no_tunnels.clone(), &cch, &graph, &lat, &lng)
        }
        #[cfg(not(feature = "chpot_visualize"))]
        {
            TopoServer::new(no_tunnels.clone(), &cch, &graph)
        }
    };

    let mut query_count = 0;
    let mut live_count = 0;
    let seed = Default::default();
    let mut rng = StdRng::from_seed(seed);
    let mut total_query_time = Duration::zero();
    let mut live_query_time = Duration::zero();

    for i in 0..100 {
        dbg!(i);
        let from: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);
        let to: NodeId = rng.gen_range(0, graph.num_nodes() as NodeId);
        let ground_truth = cch_live_server.query(Query { from, to }).map(|res| res.distance());
        // let ground_truth = cch_static_server.distance(from, to);

        query_count += 1;

        let lower_bound = cch_static_server.query(Query { from, to }).map(|res| res.distance());
        let (mut res, time) = measure(|| topocore.query(Query { from, to }));
        let dist = res.as_ref().map(|res| res.distance());
        res.as_mut().map(|res| res.path());
        let live = lower_bound != ground_truth;
        eprintln!("live: {:?}", live);
        if live {
            if let Some(ground_truth) = ground_truth {
                eprintln!("{}% length of static", ground_truth * 100 / lower_bound.unwrap());
            } else {
                eprintln!("Disconnected");
            }
            live_query_time = live_query_time + time;
            live_count += 1;
        }
        if dist != ground_truth {
            eprintln!("topo {:?} ground_truth {:?} ({} - {})", dist, ground_truth, from, to);
            assert!(ground_truth < dist);
        }

        total_query_time = total_query_time + time;
    }

    if query_count > 0 {
        eprintln!("Avg. query time {}", total_query_time / (query_count as i32))
    };
    if live_count > 0 {
        eprintln!("Avg. live query time {}", live_query_time / (live_count as i32))
    };

    Ok(())
}
