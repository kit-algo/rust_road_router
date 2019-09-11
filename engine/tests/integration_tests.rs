extern crate bmw_routing_engine;

use bmw_routing_engine::{
    graph::*,
    shortest_path::query::{
        dijkstra::Server as DijkServer,
        bidirectional_dijkstra::Server as BiDijkServer,
    },
};

fn graph() -> OwnedGraph {
    // This is the directed graph we're going to use.
    // The node numbers correspond to the different states,
    // and the edge weights symbolize the cost of moving
    // from one node to another.
    // Note that the edges are one-way.
    //
    //                  7
    //          +-----------------+
    //          |                 |
    //          v   1        2    |  2
    //          0 -----> 1 -----> 3 ---> 4
    //          |        ^        ^      ^
    //          |        | 1      |      |
    //          |        |        | 3    | 1
    //          +------> 2 -------+      |
    //           10      |               |
    //                   +---------------+
    //
    OwnedGraph::new(
        vec![0,      2,  3,        6,    8, 8, 8],
        vec![2,  1,  3,  1, 3, 4,  0, 4],
        vec![10, 1,  2,  1, 3, 1,  7, 2])
}

#[test]
fn simple_dijkstra_correct_distances() {
    let mut server = DijkServer::new(graph());

    assert_eq!(server.distance(0, 1), Some(1));
    assert_eq!(server.distance(0, 3), Some(3));
    assert_eq!(server.distance(3, 0), Some(7));
    assert_eq!(server.distance(0, 4), Some(5));
    assert_eq!(server.distance(4, 0), None);
}

#[test]
fn bidir_dijkstra_correct_distances() {
    let mut server = BiDijkServer::new(graph());

    assert_eq!(server.distance(0, 1), Some(1));
    assert_eq!(server.distance(0, 3), Some(3));
    assert_eq!(server.distance(3, 0), Some(7));
    assert_eq!(server.distance(0, 4), Some(5));
    assert_eq!(server.distance(4, 0), None);
}

#[test]
fn bidir_dijkstra_stopping_crit() {
    let first_out = vec![0,      3, 4, 5, 6, 6];
    let head      = vec![1,3,4,  2, 4, 4,];
    let weight    = vec![4,7,13, 4, 4, 7,];
    let mut server = BiDijkServer::new(OwnedGraph::new(first_out, head, weight));

    assert_eq!(server.distance(0, 4), Some(12));
}
