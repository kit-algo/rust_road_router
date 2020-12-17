use super::*;
use crate::algo::{
    ch_potentials::Potential,
    dijkstra::{
        generic_dijkstra::{DefaultOps, StandardDijkstra},
        query::dijkstra::Server,
    },
};

#[derive(Debug)]
pub struct ALTPotential {
    landmark_forward_distances: Vec<Vec<Weight>>,
    landmark_backward_distances: Vec<Vec<Weight>>,
    target_forward_distances: Vec<Weight>,
    target_backward_distances: Vec<Weight>,
    num_pot_evals: usize,
}

impl ALTPotential {
    pub fn new<G>(graph: G, landmarks: Vec<NodeId>) -> Self
    where
        G: for<'a> LinkIterable<'a, Link>,
        OwnedGraph: BuildReversed<G>,
    {
        let n = graph.num_nodes() as NodeId;
        let reversed = OwnedGraph::reversed(&graph);
        let mut server = Server::<DefaultOps, _, _>::new(graph);
        let mut reversed_server = Server::<DefaultOps, _, _>::new(reversed);

        let (landmark_forward_distances, landmark_backward_distances) = landmarks
            .iter()
            .map(|&l| {
                let forward = server.one_to_all(l);
                let backward = reversed_server.one_to_all(l);

                (0..n).into_iter().map(|node| (forward.distance(node), backward.distance(node))).unzip()
            })
            .unzip();

        Self {
            landmark_forward_distances,
            landmark_backward_distances,
            target_forward_distances: vec![0; landmarks.len()],
            target_backward_distances: vec![0; landmarks.len()],
            num_pot_evals: 0,
        }
    }

    pub fn farthest_landmarks<G>(graph: G, num_landmarks: usize, initial_landmark: NodeId) -> Vec<NodeId>
    where
        G: for<'a> LinkIterable<'a, Link>,
    {
        let n = graph.num_nodes() as NodeId;
        let mut landmarks = Vec::with_capacity(num_landmarks);
        let mut dijkstra = StandardDijkstra::new(graph);

        dijkstra.initialize_query(Query { from: initial_landmark, to: n });
        let mut last_node = initial_landmark;

        while landmarks.len() < num_landmarks {
            while let Some(node) = dijkstra.next() {
                last_node = node;
            }

            dijkstra.initialize_query(Query { from: last_node, to: n });
            for &l in &landmarks {
                dijkstra.add_start_node(Query { from: l, to: n })
            }
            landmarks.push(last_node);
        }

        landmarks
    }
}

impl Potential for ALTPotential {
    fn init(&mut self, target: NodeId) {
        for (target_dist, distances) in self
            .target_forward_distances
            .iter_mut()
            .zip(self.landmark_forward_distances.iter())
            .chain(self.target_backward_distances.iter_mut().zip(self.landmark_backward_distances.iter()))
        {
            *target_dist = distances[target as usize];
        }
        self.num_pot_evals = 0;
    }

    fn potential(&mut self, node: NodeId) -> Option<Weight> {
        self.num_pot_evals += 1;

        let max_pot = self
            .landmark_forward_distances
            .iter()
            .zip(self.target_forward_distances.iter())
            .map(|(distances, target_dist)| target_dist.saturating_sub(distances[node as usize]))
            .chain(
                self.landmark_backward_distances
                    .iter()
                    .zip(self.target_backward_distances.iter())
                    .map(|(distances, target_dist)| distances[node as usize].saturating_sub(*target_dist)),
            )
            .max()
            .unwrap();

        // TODO mh...........
        if max_pot < INFINITY {
            return Some(max_pot);
        } else {
            None
        }
    }

    fn num_pot_evals(&self) -> usize {
        self.num_pot_evals
    }
}
