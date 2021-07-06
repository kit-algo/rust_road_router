// Plot a subgraph within given geographic boundaries to a SVG.

use rust_road_router::{
    algo::{
        a_star::*,
        contraction_hierarchy,
        customizable_contraction_hierarchy::*,
        dijkstra::{gen_topo_dijkstra::*, *},
        hl::HubLabels,
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
    let mut args = env::args().skip(1);
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

    let graph = FirstOutGraph::new(first_out, head, travel_time);
    let reversed = FirstOutGraph::reversed(&graph);

    let order = Vec::load_from(path.join("ch_order"))?;
    let node_order = NodeOrder::from_node_order(order);
    // let cch_order = Vec::load_from(path.join("cch_perm"))?;
    // let cch_order = NodeOrder::from_node_order(cch_order);
    // let cch = contract(&graph, cch_order);
    // let mut node_cell_levels = vec![0; graph.num_nodes()];
    // let mut cell_paths = vec![Vec::new(); graph.num_nodes()];
    // assign_cell_levels(&cch.separators(), &cch, &mut Vec::new(), &mut node_cell_levels, &mut cell_paths, 0);
    // let max_cell_level = *node_cell_levels.iter().max().unwrap();
    // for l in &mut node_cell_levels {
    //     *l = max_cell_level - *l;
    // }

    // let node_order = CCHReordering {
    //     cch: &cch,
    //     latitude: &[],
    //     longitude: &[],
    // }
    // .reorder_bfs();
    // let cch = contract(&graph, node_order.clone());

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

    // let mut pot = BaselinePotential::new(&graph);
    let mut pot = ZeroPotential();

    let mut dijkstra_rank = vec![graph.num_nodes(); graph.num_nodes()];
    let mut discovered = vec![graph.num_nodes(); graph.num_nodes()];
    let mut fw_ops = DefaultOps();
    let mut fw_data = DijkstraData::new(graph.num_nodes());
    let mut forward_dijkstra = DijkstraRun::query(&graph, &mut fw_data, &mut fw_ops, Query { from, to: std::u32::MAX });
    pot.init(to);

    let pot_downscale = |est| est * 3 / 5;

    let mut i = 0;

    let mut backward_dijkstra_rank = vec![graph.num_nodes(); graph.num_nodes()];
    let mut backward_discovered = vec![graph.num_nodes(); graph.num_nodes()];
    let mut bw_ops = DefaultOps();
    let mut bw_data = DijkstraData::new(graph.num_nodes());
    let mut backward_dijkstra = DijkstraRun::query(&reversed, &mut bw_data, &mut bw_ops, Query { from: to, to: std::u32::MAX });

    // ########################## Dijkstra/A/ ##########################

    while let Some(node) = forward_dijkstra.next_step_with_potential(|node| pot.potential(node).map(pot_downscale)) {
        if node == to {
            eprintln!("{}", i);
        }
        dijkstra_rank[node as usize] = i;
        discovered[node as usize] = i;
        i += 1;
    }

    for node in 0..graph.num_nodes() {
        for link in LinkIterable::<Link>::link_iter(&graph, node as NodeId) {
            discovered[link.node as usize] = std::cmp::min(discovered[link.node as usize], dijkstra_rank[node]);
        }
    }

    let dist = forward_dijkstra.tentative_distance(to);

    i = 0;
    while let Some(node) = backward_dijkstra.next() {
        backward_dijkstra_rank[node as usize] = i;
        backward_discovered[node as usize] = i;
        i += 1;
    }

    for node in 0..graph.num_nodes() {
        for link in LinkIterable::<Link>::link_iter(&reversed, node as NodeId) {
            backward_discovered[link.node as usize] = std::cmp::min(backward_discovered[link.node as usize], backward_dijkstra_rank[node]);
        }
    }

    // ########################## Dijkstra/A* Output ##########################

    for node in 0..graph.num_nodes() {
        println!(
            "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node\" style=\"--settled: {}; --discovered: {}; --backward-settled: {}; --backward-discovered: {};\" />",
            x_coord(node),
            y_coord(node),
            x_coord(node),
            y_coord(node),
            dijkstra_rank[node],
            discovered[node],
            backward_dijkstra_rank[node],
            backward_discovered[node],
        );
        for link in LinkIterable::<Link>::link_iter(&graph, node as NodeId) {
            println!(
                "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" style=\"--tail-settled: {}; --head-settled: {}; --backward-tail-settled: {}; --backward-head-settled: {};\" />",
                x_coord(node),
                y_coord(node),
                x_coord(link.node as usize),
                y_coord(link.node as usize),
                dijkstra_rank[node],
                dijkstra_rank[link.node as usize],
                backward_dijkstra_rank[link.node as usize],
                backward_dijkstra_rank[node],
            );
        }
    }

    // let mut cur_node = to;
    // while cur_node != from {
    //     let pred = forward_dijkstra.predecessor(cur_node);

    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc shortest\" />",
    //         x_coord(cur_node as usize),
    //         y_coord(cur_node as usize),
    //         x_coord(pred as usize),
    //         y_coord(pred as usize),
    //     );

    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node shortest\" />",
    //         x_coord(cur_node as usize),
    //         y_coord(cur_node as usize),
    //         x_coord(cur_node as usize),
    //         y_coord(cur_node as usize),
    //     );

    //     cur_node = pred;
    // }

    // println!(
    //     "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node shortest\" />",
    //     x_coord(cur_node as usize),
    //     y_coord(cur_node as usize),
    //     x_coord(cur_node as usize),
    //     y_coord(cur_node as usize),
    // );

    // ########################## TopoDijkstra ##########################

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

    // ########################## TopoDijkstra Output ##########################

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

    // ########################## Overlay Build ##########################

    // let overlay_min_rank = 3 * graph.num_nodes() / 4;
    let overlay_min_rank = graph.num_nodes();
    let (up, down) = contraction_hierarchy::overlay(&graph, node_order.clone(), overlay_min_rank);

    // let (up, down) = customize(&cch, &graph).into_ch_graphs();

    // let mut first_out = Vec::with_capacity(graph.num_nodes() + 1);
    // let mut head = Vec::new();
    // let mut weight = Vec::new();

    // for rank in 0..graph.num_nodes() {
    //     first_out.push(head.len() as EdgeId);
    //     if rank < overlay_min_rank {
    //         for link in LinkIterable::<Link>::link_iter(&graph, node_order.node(rank as NodeId)) {
    //             head.push(node_order.rank(link.node));
    //             weight.push(link.weight);
    //         }
    //     } else {
    //         for link in LinkIterable::<Link>::link_iter(&up, rank as NodeId) {
    //             head.push(link.node);
    //             weight.push(link.weight);
    //         }
    //     }
    // }
    // first_out.push(head.len() as EdgeId);

    // let up = OwnedGraph::new(first_out, head, weight);

    // let mut first_out = Vec::with_capacity(graph.num_nodes() + 1);
    // let mut head = Vec::new();
    // let mut weight = Vec::new();

    // for rank in 0..graph.num_nodes() {
    //     first_out.push(head.len() as EdgeId);
    //     if rank < overlay_min_rank {
    //         for link in LinkIterable::<Link>::link_iter(&reversed, node_order.node(rank as NodeId)) {
    //             head.push(node_order.rank(link.node));
    //             weight.push(link.weight);
    //         }
    //     } else {
    //         for link in LinkIterable::<Link>::link_iter(&down, rank as NodeId) {
    //             head.push(link.node);
    //             weight.push(link.weight);
    //         }
    //     }
    // }
    // first_out.push(head.len() as EdgeId);

    // let down = OwnedGraph::new(first_out, head, weight);

    // ########################## CH/CCH/MLD Dijkstra ##########################

    let mut ch_for_dijkstra_rank = vec![graph.num_nodes(); graph.num_nodes()];
    let mut ch_for_discovered = vec![graph.num_nodes(); graph.num_nodes()];
    let mut fw_ops = DefaultOps();
    let mut fw_data = DijkstraData::new(graph.num_nodes());
    let mut ch_forward_dijkstra = DijkstraRun::query(
        &up,
        &mut fw_data,
        &mut fw_ops,
        Query {
            from: node_order.rank(from),
            to: std::u32::MAX,
        },
    );
    // let mut ch_forward_dijkstra = GenericDijkstra::<FirstOutGraph<&[_], &[_], Vec<_>>, DefaultOps, &FirstOutGraph<&[_], &[_], Vec<_>>>::new(&up);
    for (i, node) in (&mut ch_forward_dijkstra).enumerate() {
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
    let mut ch_backward_dijkstra = DijkstraRun::query(
        &down,
        &mut bw_data,
        &mut bw_ops,
        Query {
            from: node_order.rank(to),
            to: std::u32::MAX,
        },
    );
    // let mut ch_backward_dijkstra = GenericDijkstra::<FirstOutGraph<&[_], &[_], Vec<_>>, DefaultOps, &FirstOutGraph<&[_], &[_], Vec<_>>>::new(&down);
    for (i, node) in (&mut ch_backward_dijkstra).enumerate() {
        ch_back_dijkstra_rank[node_order.node(node) as usize] = i;
        ch_back_discovered[node_order.node(node) as usize] = i;
    }

    for node in 0..graph.num_nodes() {
        for link in LinkIterable::<Link>::link_iter(&down, node_order.rank(node as NodeId)) {
            let head = node_order.node(link.node) as usize;
            ch_back_discovered[head] = std::cmp::min(ch_back_discovered[head], ch_back_dijkstra_rank[node]);
        }
    }

    let mut _meeting_node = graph.num_nodes() as NodeId;
    for node in 0..graph.num_nodes() {
        let node = node as NodeId;
        if ch_forward_dijkstra.tentative_distance(node) + ch_backward_dijkstra.tentative_distance(node) == *dist {
            _meeting_node = node;
            break;
        }
    }

    // ########################## CCH Elimination Tree ##########################

    // let mut ch_for_dijkstra_rank = vec![graph.num_nodes(); graph.num_nodes()];
    // let mut ch_for_discovered = vec![graph.num_nodes(); graph.num_nodes()];

    // let mut cur = node_order.rank(from) as usize;
    // let mut i = 0;
    // while let Some(par) = cch.elimination_tree()[cur].value() {
    //     ch_for_dijkstra_rank[node_order.node(cur as NodeId) as usize] = i;
    //     ch_for_discovered[node_order.node(cur as NodeId) as usize] = i;
    //     cur = par as usize;
    //     i += 1;
    // }
    // ch_for_dijkstra_rank[node_order.node(cur as NodeId) as usize] = i;
    // ch_for_discovered[node_order.node(cur as NodeId) as usize] = i;

    // for node in 0..graph.num_nodes() {
    //     for link in LinkIterable::<Link>::link_iter(&up, node_order.rank(node as NodeId)) {
    //         let head = node_order.node(link.node) as usize;
    //         ch_for_discovered[head] = std::cmp::min(ch_for_discovered[head], ch_for_dijkstra_rank[node]);
    //     }
    // }

    // let mut ch_back_dijkstra_rank = vec![graph.num_nodes(); graph.num_nodes()];
    // let mut ch_back_discovered = vec![graph.num_nodes(); graph.num_nodes()];

    // let mut cur = node_order.rank(to) as usize;
    // let mut i = 0;
    // while let Some(par) = cch.elimination_tree()[cur].value() {
    //     ch_back_dijkstra_rank[node_order.node(cur as NodeId) as usize] = i;
    //     ch_back_discovered[node_order.node(cur as NodeId) as usize] = i;
    //     cur = par as usize;
    //     i += 1;
    // }
    // ch_back_dijkstra_rank[node_order.node(cur as NodeId) as usize] = i;
    // ch_back_discovered[node_order.node(cur as NodeId) as usize] = i;

    // for node in 0..graph.num_nodes() {
    //     for link in LinkIterable::<Link>::link_iter(&down, node_order.rank(node as NodeId)) {
    //         let head = node_order.node(link.node) as usize;
    //         ch_back_discovered[head] = std::cmp::min(ch_back_discovered[head], ch_back_dijkstra_rank[node]);
    //     }
    // }

    // ########################## CH/CCH/MLD Output ##########################

    // for rank in 0..graph.num_nodes() {
    //     let node = node_order.node(rank as NodeId) as usize;
    //     if ch_for_discovered[node] == graph.num_nodes() && ch_back_discovered[node] == graph.num_nodes() {
    //         continue;
    //     }

    //     if let Some(parent) = cch.elimination_tree()[rank].value() {
    //         let parent = node_order.node(parent) as usize;
    //         println!(
    //             "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" style=\"--tail-rank: {};\" />",
    //             x_coord(node),
    //             y_coord(node),
    //             x_coord(parent),
    //             y_coord(parent),
    //             rank,
    //         );
    //     }

    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node\" style=\"--rank: {}; --cell-level: {};\" data-cell-path=\"{:?}\" />",
    //         x_coord(node),
    //         y_coord(node),
    //         x_coord(node),
    //         y_coord(node),
    //         rank,
    //         node_cell_levels[node],
    //         &cell_paths[node],
    //     );
    // }

    // for rank in 0..graph.num_nodes() {
    //     let node = node_order.node(rank as NodeId) as usize;
    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node\" style=\"--rank: {}; --cell-level: {}; --forward-settled: {}; --forward-discovered: {}; --backward-settled: {}; --backward-discovered: {};\" data-cell-path=\"{:?}\" />",
    //         // "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node\" style=\"--rank: {}; --forward-settled: {}; --forward-discovered: {}; --backward-settled: {}; --backward-discovered: {};\" />",
    //         x_coord(node),
    //         y_coord(node),
    //         x_coord(node),
    //         y_coord(node),
    //         rank,
    //         node_cell_levels[node],
    //         ch_for_dijkstra_rank[node],
    //         ch_for_discovered[node],
    //         ch_back_dijkstra_rank[node],
    //         ch_back_discovered[node],
    //         &cell_paths[node],
    //     );
    //     for link in LinkIterable::<Link>::link_iter(&up, rank as NodeId) {
    //         println!(
    //             "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" style=\"--tail-forward-settled: {}; --tail-rank: {};\" />",
    //             x_coord(node),
    //             y_coord(node),
    //             x_coord(node_order.node(link.node) as usize),
    //             y_coord(node_order.node(link.node) as usize),
    //             ch_for_dijkstra_rank[node],
    //             rank,
    //         );
    //     }
    //     for link in LinkIterable::<Link>::link_iter(&down, rank as NodeId) {
    //         println!(
    //             "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc\" style=\"--tail-backward-settled: {}; --tail-rank: {};\" />",
    //             x_coord(node),
    //             y_coord(node),
    //             x_coord(node_order.node(link.node) as usize),
    //             y_coord(node_order.node(link.node) as usize),
    //             ch_back_dijkstra_rank[node],
    //             rank,
    //         );
    //     }
    // }

    // let mut cur_node = meeting_node;
    // while cur_node != node_order.rank(from) {
    //     let pred = ch_forward_dijkstra.predecessor(cur_node);

    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc shortest\" />",
    //         x_coord(node_order.node(cur_node) as usize),
    //         y_coord(node_order.node(cur_node) as usize),
    //         x_coord(node_order.node(pred) as usize),
    //         y_coord(node_order.node(pred) as usize),
    //     );

    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node shortest\" />",
    //         x_coord(node_order.node(cur_node) as usize),
    //         y_coord(node_order.node(cur_node) as usize),
    //         x_coord(node_order.node(cur_node) as usize),
    //         y_coord(node_order.node(cur_node) as usize),
    //     );

    //     cur_node = pred;
    // }

    // println!(
    //     "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node shortest\" />",
    //     x_coord(node_order.node(cur_node) as usize),
    //     y_coord(node_order.node(cur_node) as usize),
    //     x_coord(node_order.node(cur_node) as usize),
    //     y_coord(node_order.node(cur_node) as usize),
    // );

    // let mut cur_node = meeting_node;
    // while cur_node != node_order.rank(to) {
    //     let pred = ch_backward_dijkstra.predecessor(cur_node);

    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc shortest\" />",
    //         x_coord(node_order.node(cur_node) as usize),
    //         y_coord(node_order.node(cur_node) as usize),
    //         x_coord(node_order.node(pred) as usize),
    //         y_coord(node_order.node(pred) as usize),
    //     );

    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node shortest\" />",
    //         x_coord(node_order.node(cur_node) as usize),
    //         y_coord(node_order.node(cur_node) as usize),
    //         x_coord(node_order.node(cur_node) as usize),
    //         y_coord(node_order.node(cur_node) as usize),
    //     );

    //     cur_node = pred;
    // }

    // println!(
    //     "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node shortest\" />",
    //     x_coord(node_order.node(cur_node) as usize),
    //     y_coord(node_order.node(cur_node) as usize),
    //     x_coord(node_order.node(cur_node) as usize),
    //     y_coord(node_order.node(cur_node) as usize),
    // );

    // ########################## Hub Labeling ##########################

    let hl = HubLabels::new(&up, &down);
    let _best = hl.hub_and_dist(node_order.rank(from), node_order.rank(to)).unwrap().0;

    // for label in &hl.forward_labels()[node_order.rank(from) as usize] {
    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc{}\" />",
    //         x_coord(from as usize),
    //         y_coord(from as usize),
    //         x_coord(node_order.node(label.0) as usize),
    //         y_coord(node_order.node(label.0) as usize),
    //         if best == label.0 { " shortest" } else { "" }
    //     );

    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node hl-forward{}\" style=\"--rank: {};\" />",
    //         x_coord(node_order.node(label.0) as usize),
    //         y_coord(node_order.node(label.0) as usize),
    //         x_coord(node_order.node(label.0) as usize),
    //         y_coord(node_order.node(label.0) as usize),
    //         if best == label.0 { " shortest" } else { "" },
    //         label.0
    //     );
    // }

    // for label in &hl.backward_labels()[node_order.rank(to) as usize] {
    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"arc{}\" />",
    //         x_coord(to as usize),
    //         y_coord(to as usize),
    //         x_coord(node_order.node(label.0) as usize),
    //         y_coord(node_order.node(label.0) as usize),
    //         if best == label.0 { " shortest" } else { "" }
    //     );

    //     println!(
    //         "<line x1=\"{}\" y1=\"{}\" x2=\"{}\" y2=\"{}\" class=\"node hl-backward{}\" style=\"--rank: {};\" />",
    //         x_coord(node_order.node(label.0) as usize),
    //         y_coord(node_order.node(label.0) as usize),
    //         x_coord(node_order.node(label.0) as usize),
    //         y_coord(node_order.node(label.0) as usize),
    //         if best == label.0 { " shortest" } else { "" },
    //         label.0
    //     );
    // }

    // ########################## CH-Pot Output ##########################

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

#[allow(dead_code)]
fn assign_cell_levels(
    sep_tree: &separator_decomposition::SeparatorTree,
    cch: &CCH,
    cell_stack: &mut Vec<usize>,
    node_cell_levels: &mut [usize],
    cell_path: &mut [Vec<usize>],
    level: usize,
) -> usize {
    // let mut level = 0;
    for (i, child) in sep_tree.children.iter().enumerate() {
        cell_stack.push(i);
        // level = std::cmp::max(
        assign_cell_levels(child, cch, cell_stack, node_cell_levels, cell_path, level + 1);
        // + 1, level);
        cell_stack.pop();
    }

    for &node in &sep_tree.nodes {
        node_cell_levels[cch.node_order().node(node) as usize] = level;
        cell_path[cch.node_order().node(node) as usize] = cell_stack.clone();
    }

    level
}
