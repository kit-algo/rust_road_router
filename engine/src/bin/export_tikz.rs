// Plot a subgraph within given geographic boundaries to a SVG.

use rust_road_router::{
    algo::{contraction_hierarchy, dijkstra::*, *},
    cli::CliErr,
    datastr::{graph::*, node_order::NodeOrder, rank_select_map::*},
    io::*,
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
    let order = Vec::load_from(path.join("ch_order"))?;
    let node_order = NodeOrder::from_node_order(order);
    let lat = Vec::<f32>::load_from(path.join("latitude"))?;
    let lng = Vec::<f32>::load_from(path.join("longitude"))?;
    let graph = FirstOutGraph::new(&first_out[..], &head[..], &travel_time[..]);

    let (up, down) = contraction_hierarchy::overlay(&graph, node_order.clone(), graph.num_nodes());

    let in_bounding_box =
        |node: usize| lat[node] as f64 >= min_lat && lat[node] as f64 <= max_lat && lng[node] as f64 >= min_lon && lng[node] as f64 <= max_lon;
    let x_coord = |node: usize| (lng[node] as f64 - min_lon) * max_x / (max_lon - min_lon);
    let y_coord = |node: usize| (lat[node] as f64 - min_lat) * max_y / (max_lat - min_lat);
    let node_to_color = |node: usize, num_nodes: usize| -> String {
        let rank = node_order.rank(node as NodeId) as usize;
        if rank * 125 < num_nodes * 100 {
            format!("kit-blue100!{}!kit-green100", rank * 125 / num_nodes)
        } else {
            format!("kit-red100!{}!kit-blue100", (rank * 500 / num_nodes).saturating_sub(400))
        }
    };

    for &node in node_order.order() {
        let node = node as usize;
        if in_bounding_box(node) {
            println!("\\node [node] at ({},{}) ({}) {{}};", x_coord(node), y_coord(node), node);
        }
    }

    // for node in 0..graph.num_nodes() {
    //     if in_bounding_box(node) {
    //         for link in LinkIterable::<Link>::link_iter(&graph, node as NodeId) {
    //             if in_bounding_box(link.node as usize) {
    //                 println!("\\path ({}) edge[edge] ({});", node, link.node);
    //             }
    //         }
    //     }
    // }
    let start_rank = 0;
    for rank in start_rank..graph.num_nodes() {
        let node = node_order.node(rank as NodeId);
        if in_bounding_box(node as usize) {
            for link in LinkIterable::<Link>::link_iter(&up, rank as NodeId) {
                if in_bounding_box(node_order.node(link.node) as usize) {
                    println!("\\path ({}) edge[edge] ({});", node, node_order.node(link.node));
                }
            }
        }
    }
    for rank in start_rank..graph.num_nodes() {
        let node = node_order.node(rank as NodeId);
        if in_bounding_box(node as usize) {
            for link in LinkIterable::<Link>::link_iter(&down, rank as NodeId) {
                if in_bounding_box(node_order.node(link.node) as usize) {
                    println!("\\path ({}) edge[edge] ({});", node, node_order.node(link.node));
                }
            }
        }
    }
    // for &node in node_order.order() {
    //     let node = node as usize;
    //     if in_bounding_box(node) {
    //         println!(
    //             "\\node [node, fill={}] at ({},{}) ({}) {{}};",
    //             node_to_color(node, graph.num_nodes()),
    //             x_coord(node),
    //             y_coord(node),
    //             node
    //         );
    //     }
    // }

    let mut forward_settled_nodes = BitVec::new(graph.num_nodes());
    let mut fw_ops = DefaultOps();
    let mut fw_data = DijkstraData::new(graph.num_nodes());
    let forward_dijkstra = DijkstraRun::query(&up, &mut fw_data, &mut fw_ops, Query { from: 0, to: std::u32::MAX });
    for node in forward_dijkstra {
        forward_settled_nodes.set(node as usize);
    }

    let mut backward_settled_nodes = BitVec::new(graph.num_nodes());
    let mut bw_ops = DefaultOps();
    let mut bw_data = DijkstraData::new(graph.num_nodes());
    let backward_dijkstra = DijkstraRun::query(&up, &mut bw_data, &mut bw_ops, Query { from: 100, to: std::u32::MAX });
    for node in backward_dijkstra {
        backward_settled_nodes.set(node as usize);
    }

    let start_rank = 0;
    // for rank in start_rank..graph.num_nodes() {
    //     let node = node_order.node(rank as NodeId);
    //     if in_bounding_box(node as usize) {
    //         for link in LinkIterable::<Link>::link_iter(&up, rank as NodeId) {
    //             if in_bounding_box(node_order.node(link.node) as usize) {
    //                 if forward_settled_nodes.get(node_order.rank(node as NodeId) as usize) {
    //                     assert!(forward_settled_nodes.get(link.node as usize));
    //                     println!("\\path ({}) edge[searchspace_edge] ({});", node, node_order.node(link.node));
    //                 }
    //             }
    //         }
    //     }
    // }
    for rank in start_rank..graph.num_nodes() {
        let node = node_order.node(rank as NodeId);
        if in_bounding_box(node as usize) {
            for link in LinkIterable::<Link>::link_iter(&down, rank as NodeId) {
                if in_bounding_box(node_order.node(link.node) as usize) {
                    if backward_settled_nodes.get(node_order.rank(node as NodeId) as usize) {
                        assert!(backward_settled_nodes.get(link.node as usize));
                        println!("\\path<2-> ({}) edge[searchspace_edge] ({});", node, node_order.node(link.node));
                    }
                }
            }
        }
    }

    // for &node in node_order.order() {
    //     let node = node as usize;
    //     if in_bounding_box(node) {
    //         if forward_settled_nodes.get(node_order.rank(node as NodeId) as usize) || backward_settled_nodes.get(node_order.rank(node as NodeId) as usize) {
    //             println!(
    //                 "\\node [searchspace_node, fill={}] at ({},{}) ({}) {{}};",
    //                 node_to_color(node, graph.num_nodes()),
    //                 x_coord(node),
    //                 y_coord(node),
    //                 node
    //             );
    //         }
    //     }
    // }

    for &node in node_order.order() {
        let node = node as usize;
        if in_bounding_box(node) {
            if backward_settled_nodes.get(node_order.rank(node as NodeId) as usize) {
                println!(
                    "\\node<2-> [pot_requested, fill={}] at ({},{}) ({}) {{}};",
                    node_to_color(node, graph.num_nodes()),
                    x_coord(node),
                    y_coord(node),
                    node
                );
            }
        }
    }

    let mut visited = BitVec::new(graph.num_nodes());

    let counter = std::cell::Cell::new(3);

    foo(
        0,
        &up,
        &mut visited,
        &mut |rank| {
            let node = node_order.node(rank);
            if in_bounding_box(node as usize) {
                println!(
                    "\\node<{}-> [pot_requested, fill={}] at ({},{}) ({}) {{}};",
                    counter.get(),
                    node_to_color(node as usize, graph.num_nodes()),
                    x_coord(node as usize),
                    y_coord(node as usize),
                    node
                );
                for link in LinkIterable::<Link>::link_iter(&up, rank) {
                    if in_bounding_box(node_order.node(link.node) as usize) {
                        println!("\\path<{}-> ({}) edge[searchspace_edge] ({});", counter.get(), node, node_order.node(link.node));
                    }
                }
                counter.set(counter.get() + 1);
            }
        },
        &mut |node| {
            let node = node_order.node(node);
            if in_bounding_box(node as usize) {
                println!(
                    "\\node<{}-> [pot_computed, fill={}] at ({},{}) ({}) {{}};",
                    counter.get(),
                    node_to_color(node as usize, graph.num_nodes()),
                    x_coord(node as usize),
                    y_coord(node as usize),
                    node
                );
                counter.set(counter.get() + 1);
            }
        },
    );

    let mut server = Server::<FirstOutGraph<_, _, _>>::new(graph.clone());
    let mut res = server
        .query(Query {
            from: node_order.node(0),
            to: node_order.node(100),
        })
        .unwrap();
    let fifth = node_order.rank(res.path()[5]);

    foo(
        fifth,
        &up,
        &mut visited,
        &mut |rank| {
            let node = node_order.node(rank);
            if in_bounding_box(node as usize) {
                println!(
                    "\\node<{}-> [pot_requested, fill={}] at ({},{}) ({}) {{}};",
                    counter.get(),
                    node_to_color(node as usize, graph.num_nodes()),
                    x_coord(node as usize),
                    y_coord(node as usize),
                    node
                );
                for link in LinkIterable::<Link>::link_iter(&up, rank) {
                    if in_bounding_box(node_order.node(link.node) as usize) {
                        println!("\\path<{}-> ({}) edge[searchspace_edge] ({});", counter.get(), node, node_order.node(link.node));
                    }
                }
                counter.set(counter.get() + 1);
            }
        },
        &mut |node| {
            let node = node_order.node(node);
            if in_bounding_box(node as usize) {
                println!(
                    "\\node<{}-> [pot_computed, fill={}] at ({},{}) ({}) {{}};",
                    counter.get(),
                    node_to_color(node as usize, graph.num_nodes()),
                    x_coord(node as usize),
                    y_coord(node as usize),
                    node
                );
                counter.set(counter.get() + 1);
            }
        },
    );

    println!(
        "\\node [terminal] at ({},{}) ({}) {{}};",
        x_coord(node_order.node(0) as usize),
        y_coord(node_order.node(0) as usize),
        node_order.node(0)
    );
    println!(
        "\\node [terminal] at ({},{}) ({}) {{}};",
        x_coord(node_order.node(100) as usize),
        y_coord(node_order.node(100) as usize),
        node_order.node(100)
    );

    Ok(())
}

fn foo(node: NodeId, graph: &OwnedGraph, visited: &mut BitVec, print_before: &mut impl FnMut(NodeId), print_after: &mut impl FnMut(NodeId)) {
    if !visited.get(node as usize) {
        print_before(node);
        visited.set(node as usize);

        for link in LinkIterable::<Link>::link_iter(graph, node) {
            foo(link.node, graph, visited, print_before, print_after);
        }
        print_after(node);
    }
}
