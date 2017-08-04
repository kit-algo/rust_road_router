extern crate bmw_routing_engine;

use bmw_routing_engine::*;
use shortest_path::ShortestPathServerBiDirDijk;
use graph::Graph;

#[test]
fn it_calculates_correct_shortest_paths() {
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
    let graph = Graph::new(
        vec![0,      2,  3,        6,    8, 8, 8],
        vec![2,  1,  3,  1, 3, 4,  0, 4],
        vec![10, 1,  2,  1, 3, 1,  7, 2]);

    let mut server = ShortestPathServerBiDirDijk::new(graph);

    assert_eq!(server.distance(0, 1), Some(1));
    assert_eq!(server.distance(0, 3), Some(3));
    assert_eq!(server.distance(3, 0), Some(7));
    assert_eq!(server.distance(0, 4), Some(5));
    assert_eq!(server.distance(4, 0), None);
}
