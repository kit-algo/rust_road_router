// Plot a subgraph within given geographic boundaries to a SVG.

use rust_road_router::{
    algo::{
        a_star::*,
        contraction_hierarchy,
        // customizable_contraction_hierarchy::*,
        dijkstra::{gen_topo_dijkstra::*, generic_dijkstra::*, *},
        topocore::*,
        Query,
    },
    cli::CliErr,
    datastr::{graph::*, node_order::*},
    io::*,
    util::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let min_lat = args.next().ok_or(CliErr("No min_lat arg given"))?.parse::<f64>()?;
    let min_lon = args.next().ok_or(CliErr("No min_lon arg given"))?.parse::<f64>()?;
    let max_lat = args.next().ok_or(CliErr("No max_lat arg given"))?.parse::<f64>()?;
    let max_lon = args.next().ok_or(CliErr("No max_lon arg given"))?.parse::<f64>()?;

    let max_x = args.next().ok_or(CliErr("No max_x arg given"))?.parse::<f64>()?;
    let max_y = args.next().ok_or(CliErr("No max_y arg given"))?.parse::<f64>()?;

    let first_out = Vec::load_from(path.join("first_out"))?;
    let head = Vec::load_from(path.join("head"))?;
    let travel_time = Vec::load_from(path.join("travel_time"))?;
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;

    let order = Vec::load_from(path.join("ch_order"))?;
    let node_order = NodeOrder::from_node_order(order);

    let graph = FirstOutGraph::new(first_out, head, travel_time);

    let (up, down) = contraction_hierarchy::overlay(&graph, node_order.clone(), graph.num_nodes());

    let x_coord = |node: usize| (lng[node] as f64 - min_lon) * max_x / (max_lon - min_lon);
    let y_coord = |node: usize| (max_lat - lat[node] as f64) * max_y / (max_lat - min_lat);

    let from = (0..graph.num_nodes())
        .min_by_key(|&n| {
            NonNan::new((lat[n] - 49.00815772031336) * (lat[n] - 49.00815772031336) + (lng[n] - 8.403795863852542) * (lng[n] - 8.403795863852542)).unwrap()
        })
        .unwrap() as NodeId;

    let to = (0..graph.num_nodes())
        .min_by_key(|&n| {
            NonNan::new((lat[n] - 49.013879000705934) * (lat[n] - 49.013879000705934) + (lng[n] - 8.419350046011754) * (lng[n] - 8.419350046011754)).unwrap()
        })
        .unwrap() as NodeId;

    let mut pot = BaselinePotential::new(&graph);

    let mut dijkstra_rank = vec![graph.num_nodes(); graph.num_nodes()];
    let mut discovered = vec![graph.num_nodes(); graph.num_nodes()];
    let mut forward_dijkstra = GenericDijkstra::<OwnedGraph, DefaultOps, &OwnedGraph>::new(&graph);
    forward_dijkstra.initialize_query(Query { from, to: std::u32::MAX });
    pot.init(to);

    let pot_downscale = |est| est * 3 / 5;

    let mut i = 0;
    // while let Some(node) = forward_dijkstra.next_step_with_potential(|node| pot.potential(node).map(pot_downscale)) {
    //     if node == to {
    //         eprintln!("{}", i);
    //     }
    //     dijkstra_rank[node as usize] = i;
    //     discovered[node as usize] = i;
    //     i += 1;
    // }

    // for node in 0..graph.num_nodes() {
    //     for link in LinkIterable::<Link>::link_iter(&graph, node as NodeId) {
    //         discovered[link.node as usize] = std::cmp::min(discovered[link.node as usize], dijkstra_rank[node]);
    //     }
    // }

    // for node in 0..graph.num_nodes() {
    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node\" style=\"--settled: {}; --discovered: {};\" />",
    //         x_coord(node),
    //         y_coord(node),
    //         x_coord(node),
    //         y_coord(node),
    //         dijkstra_rank[node],
    //         discovered[node],
    //     );
    //     for link in LinkIterable::<Link>::link_iter(&graph, node as NodeId) {
    //         println!(
    //             "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" style=\"--tail-settled: {}; --head-settled: {};\" />",
    //             x_coord(node),
    //             y_coord(node),
    //             x_coord(link.node as usize),
    //             y_coord(link.node as usize),
    //             dijkstra_rank[node],
    //             dijkstra_rank[link.node as usize]
    //         );
    //     }
    // }

    let (core, _comp, topocore) = VirtualTopocoreGraph::<OwnedGraph>::new_topo_dijkstra_graphs(&graph);
    let mut data = DijkstraData::new(graph.num_nodes());
    let mut ops = DefaultOps::default();
    let mut dijk_run = TopoDijkstraRun::query(
        &core,
        &mut data,
        &mut ops,
        Query {
            from: topocore.order.rank(from),
            to: std::u32::MAX,
        },
    );

    let mut done_at = graph.num_nodes();
    let mut topo_dijk_visited = vec![false; graph.num_nodes()];
    while let Some(rank) = dijk_run.next_step_with_potential_and_edge_callback(
        |node| pot.potential(topocore.order.node(node)).map(pot_downscale),
        |link| {
            let head = topocore.order.node(link.node) as usize;
            // dijkstra_rank[head] = i;
            discovered[head] = std::cmp::min(i, discovered[head]);
        },
    ) {
        let node = topocore.order.node(rank);
        if (node == to || dijk_run.queue().peek().map(|e| e.key).unwrap_or(INFINITY) >= *dijk_run.tentative_distance(topocore.order.rank(to))) && i < done_at {
            eprintln!("{}", i);
            done_at = i;
        }
        dijkstra_rank[node as usize] = i;
        discovered[node as usize] = std::cmp::min(i, discovered[node as usize]);
        i += 1;
        topo_dijk_visited[node as usize] = true;
    }

    // for node in 0..graph.num_nodes() {
    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node\" style=\"--settled: {}; --discovered: {}; --topo-visited: {};\" />",
    //         x_coord(node),
    //         y_coord(node),
    //         x_coord(node),
    //         y_coord(node),
    //         dijkstra_rank[node],
    //         discovered[node],
    //         if topo_dijk_visited[node] { 1 } else { 0 }
    //     );
    //     for link in LinkIterable::<Link>::link_iter(&core, topocore.order.rank(node as NodeId)) {
    //         let link_node = topocore.order.node(link.node) as usize;
    //         println!(
    //             "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" style=\"--tail-settled: {}; --head-settled: {};\" />",
    //             x_coord(node),
    //             y_coord(node),
    //             x_coord(link_node),
    //             y_coord(link_node),
    //             if topo_dijk_visited[node] { dijkstra_rank[node] } else { discovered[node] },
    //             dijkstra_rank[link_node]
    //         );
    //     }
    // }

    let (up, down) = contraction_hierarchy::overlay(&graph, node_order.clone(), graph.num_nodes());

    let mut ch_for_dijkstra_rank = vec![graph.num_nodes(); graph.num_nodes()];
    let mut ch_for_discovered = vec![graph.num_nodes(); graph.num_nodes()];
    let mut ch_forward_dijkstra = GenericDijkstra::<OwnedGraph, DefaultOps, &OwnedGraph>::new(&up);
    ch_forward_dijkstra.initialize_query(Query {
        from: node_order.rank(from),
        to: std::u32::MAX,
    });
    for (i, node) in ch_forward_dijkstra.enumerate() {
        ch_for_dijkstra_rank[node_order.node(node) as usize] = i;
        ch_for_discovered[node_order.node(node) as usize] = i;
    }

    for node in 0..graph.num_nodes() {
        for link in LinkIterable::<Link>::link_iter(&up, node_order.rank(node as NodeId)) {
            let head = node_order.node(link.node) as usize;
            ch_for_discovered[head] = std::cmp::min(ch_for_discovered[head], ch_for_dijkstra_rank[node]);
        }
    }

    let mut ch_back_dijkstra_rank = vec![graph.num_nodes(); graph.num_nodes()];
    let mut ch_back_discovered = vec![graph.num_nodes(); graph.num_nodes()];
    let mut ch_backward_dijkstra = GenericDijkstra::<OwnedGraph, DefaultOps, &OwnedGraph>::new(&down);
    ch_backward_dijkstra.initialize_query(Query {
        from: node_order.rank(to),
        to: std::u32::MAX,
    });
    for (i, node) in ch_backward_dijkstra.enumerate() {
        ch_back_dijkstra_rank[node_order.node(node) as usize] = i;
        ch_back_discovered[node_order.node(node) as usize] = i;
    }

    for node in 0..graph.num_nodes() {
        for link in LinkIterable::<Link>::link_iter(&down, node_order.rank(node as NodeId)) {
            let head = node_order.node(link.node) as usize;
            ch_back_discovered[head] = std::cmp::min(ch_back_discovered[head], ch_back_dijkstra_rank[node]);
        }
    }

    for rank in 0..graph.num_nodes() {
        let node = node_order.node(rank as NodeId) as usize;
        println!(
            "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node\" style=\"--rank: {}; --forward-settled: {}; --forward-discovered: {}; --backward-settled: {}; --backward-discovered: {};\" />",
            x_coord(node),
            y_coord(node),
            x_coord(node),
            y_coord(node),
            rank,
            ch_for_dijkstra_rank[node],
            ch_for_discovered[node],
            ch_back_dijkstra_rank[node],
            ch_back_discovered[node],
        );
        for link in LinkIterable::<Link>::link_iter(&up, rank as NodeId) {
            println!(
                "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" style=\"--tail-forward-settled: {};\" />",
                x_coord(node),
                y_coord(node),
                x_coord(node_order.node(link.node) as usize),
                y_coord(node_order.node(link.node) as usize),
                ch_for_dijkstra_rank[node],
            );
        }
        for link in LinkIterable::<Link>::link_iter(&down, rank as NodeId) {
            println!(
                "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" style=\"--tail-backward-settled: {};\" />",
                x_coord(node),
                y_coord(node),
                x_coord(node_order.node(link.node) as usize),
                y_coord(node_order.node(link.node) as usize),
                ch_back_dijkstra_rank[node],
            );
        }
    }

    // let mut step = 0;
    // let mut visited = vec![graph.num_nodes(); graph.num_nodes()];
    // let mut edge_visited = vec![graph.num_nodes(); up.num_arcs()];
    // let mut settled = vec![graph.num_nodes(); graph.num_nodes()];

    // step = foo(node_order.rank(from), &up, &mut visited, &mut settled, &mut edge_visited, step);
    // eprintln!("{}", step);
    // step = foo(664, &up, &mut visited, &mut settled, &mut edge_visited, step);
    // eprintln!("{}", step);

    // for node in 0..graph.num_nodes() {
    //     // if dijkstra_rank[node] < dijkstra_rank[to as usize] {
    //     if dijkstra_rank[node] < done_at {
    //         step = foo(node_order.rank(node as NodeId), &up, &mut visited, &mut settled, &mut edge_visited, step);
    //     }
    // }
    // eprintln!("{}", step);

    // for rank in 0..graph.num_nodes() {
    //     let node = node_order.node(rank as NodeId) as usize;
    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node\" style=\"--rank: {}; --forward-settled: {}; --forward-discovered: {}; --backward-settled: {}; --backward-discovered: {};\" />",
    //         x_coord(node),
    //         y_coord(node),
    //         x_coord(node),
    //         y_coord(node),
    //         rank,
    //         settled[node_order.rank(node as NodeId) as usize],
    //         visited[node_order.rank(node as NodeId) as usize],
    //         ch_back_dijkstra_rank[node],
    //         ch_back_discovered[node],
    //     );
    //     for (link, e) in LinkIterable::<Link>::link_iter(&up, rank as NodeId).zip(up.neighbor_edge_indices_usize(rank as NodeId)) {
    //         println!(
    //             "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" style=\"--visited: {};\" />",
    //             x_coord(node),
    //             y_coord(node),
    //             x_coord(node_order.node(link.node) as usize),
    //             y_coord(node_order.node(link.node) as usize),
    //             edge_visited[e],
    //         );
    //     }
    //     for link in LinkIterable::<Link>::link_iter(&down, rank as NodeId) {
    //         println!(
    //             "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" style=\"--tail-backward-settled: {};\" />",
    //             x_coord(node),
    //             y_coord(node),
    //             x_coord(node_order.node(link.node) as usize),
    //             y_coord(node_order.node(link.node) as usize),
    //             ch_back_dijkstra_rank[node],
    //         );
    //     }
    // }

    Ok(())
}

// fn foo(node: NodeId, graph: &OwnedGraph, visited: &mut [usize], settled: &mut [usize], edge_visited: &mut [usize], mut step: usize) -> usize {
//     if step < visited[node as usize] {
//         visited[node as usize] = step;
//         step += 1;

//         for (link, e) in LinkIterable::<Link>::link_iter(graph, node).zip(graph.neighbor_edge_indices_usize(node)) {
//             edge_visited[e] = step;
//             step = foo(link.node, graph, visited, settled, edge_visited, step);
//         }
//         settled[node as usize] = step;
//         step += 1;
//     }
//     step
// }
