pub mod shortest_path;
pub mod graph;

#[cfg(test)]
mod tests {
    use shortest_path::shortest_path;
    use graph::Graph;

    #[test]
    fn it_works() {
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
        // The graph is represented as an adjacency list where each index,
        // corresponding to a node value, has a list of outgoing edges.
        // Chosen for its efficiency.
        // let graph = vec![
        //     // Node 0
        //     vec![Edge { node: 2, cost: 10 },
        //          Edge { node: 1, cost: 1 }],
        //     // Node 1
        //     vec![Edge { node: 3, cost: 2 }],
        //     // Node 2
        //     vec![Edge { node: 1, cost: 1 },
        //          Edge { node: 3, cost: 3 },
        //          Edge { node: 4, cost: 1 }],
        //     // Node 3
        //     vec![Edge { node: 0, cost: 7 },
        //          Edge { node: 4, cost: 2 }],
        //     // Node 4
        //     vec![]];

        assert_eq!(shortest_path(&graph, 0, 1), Some(1));
        assert_eq!(shortest_path(&graph, 0, 3), Some(3));
        assert_eq!(shortest_path(&graph, 3, 0), Some(7));
        assert_eq!(shortest_path(&graph, 0, 4), Some(5));
        assert_eq!(shortest_path(&graph, 4, 0), None);
    }
}


