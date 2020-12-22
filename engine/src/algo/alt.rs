use super::*;
use crate::report::*;
use crate::{
    algo::{
        ch_potentials::*,
        dijkstra::{generic_dijkstra::*, query::dijkstra::*},
    },
    datastr::rank_select_map::BitVec,
};
use rand::prelude::*;

#[derive(Debug)]
pub struct ALTPotential {
    landmark_forward_distances: Vec<Vec<Weight>>,
    landmark_backward_distances: Vec<Vec<Weight>>,
    target_forward_distances: Vec<Weight>,
    target_backward_distances: Vec<Weight>,
    num_pot_evals: usize,
}

impl ALTPotential {
    pub fn new<G>(graph: &G, landmarks: Vec<NodeId>) -> Self
    where
        G: for<'a> LinkIterable<'a, Link>,
        OwnedGraph: BuildReversed<G>,
    {
        let mut landmark_runs_ctxt = push_collection_context("landmark_dijkstras".to_string());
        let n = graph.num_nodes() as NodeId;
        let reversed = OwnedGraph::reversed(graph);
        let mut server = Server::<G, DefaultOps, _, &G>::new(graph);
        let mut reversed_server = Server::<_, DefaultOps>::new(reversed);

        let (landmark_forward_distances, landmark_backward_distances) = landmarks
            .iter()
            .map(|&l| {
                let ctx = landmark_runs_ctxt.push_collection_item();
                let forward = server.one_to_all(l);
                drop(ctx);
                let ctx = landmark_runs_ctxt.push_collection_item();
                let backward = reversed_server.one_to_all(l);
                drop(ctx);

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

    pub fn new_with_avoid<G>(graph: &G, num_landmarks: usize, rng: &mut StdRng) -> Self
    where
        G: for<'a> LinkIterable<'a, Link>,
        OwnedGraph: BuildReversed<G>,
    {
        report!("algo", "Avoid Landmarks");
        report!("num_landmarks", num_landmarks);
        let mut landmark_runs_ctxt = push_collection_context("landmark_dijkstras".to_string());
        let n = graph.num_nodes() as NodeId;
        let reversed = OwnedGraph::reversed(graph);
        let mut server = Server::<G, DefaultOps, _, &G>::new(graph);
        let mut reversed_server = Server::<_, DefaultOps>::new(reversed);

        let mut landmarks: Vec<NodeId> = Vec::with_capacity(num_landmarks);
        let mut landmark_forward_distances: Vec<Vec<Weight>> = Vec::with_capacity(num_landmarks);
        let mut landmark_backward_distances: Vec<Vec<Weight>> = Vec::with_capacity(num_landmarks);

        let mut landmrk_nodes = BitVec::new(n as usize);
        let mut visited = BitVec::new(n as usize);

        while landmarks.len() < num_landmarks {
            let r = rng.gen_range(0, n);
            let ctx = landmark_runs_ctxt.push_collection_item();
            let dists = server.one_to_all(r);
            drop(ctx);

            let root_forward_distances: Vec<Weight> = landmark_forward_distances.iter().map(|l| l[r as usize]).collect();
            let root_backward_distances: Vec<Weight> = landmark_backward_distances.iter().map(|l| l[r as usize]).collect();

            let mut weights: Vec<u64> = (0..n)
                .map(|node| {
                    (dists.distance(node)
                        - landmark_forward_distances
                            .iter()
                            .zip(root_forward_distances.iter())
                            .map(|(distances, root_dist)| distances[node as usize].saturating_sub(*root_dist))
                            .chain(
                                landmark_backward_distances
                                    .iter()
                                    .zip(root_backward_distances.iter())
                                    .map(|(distances, root_dist)| root_dist.saturating_sub(distances[node as usize])),
                            )
                            .max()
                            .unwrap_or(0)) as u64
                })
                .inspect(|&delta| debug_assert!(delta < INFINITY as u64))
                .collect();

            Self::dfs_set_sizes(graph, r, &mut weights, &mut visited, &landmrk_nodes, &dists);
            visited.clear();

            let mut v = weights.iter().enumerate().max_by_key(|(_, &w)| w).unwrap().0 as NodeId;
            while let Some(next) = graph
                .link_iter(v)
                .map(|l| l.head())
                .filter(|&l| dists.predecessor(l) == v)
                .max_by_key(|&l| weights[l as usize])
            {
                v = next;
            }

            landmarks.push(v);
            landmrk_nodes.set(v as usize);
            let ctx = landmark_runs_ctxt.push_collection_item();
            let forward = server.one_to_all(v);
            drop(ctx);
            let ctx = landmark_runs_ctxt.push_collection_item();
            let backward = reversed_server.one_to_all(v);
            drop(ctx);

            let (forward_distances, backward_distances) = (0..n).into_iter().map(|node| (forward.distance(node), backward.distance(node))).unzip();
            landmark_forward_distances.push(forward_distances);
            landmark_backward_distances.push(backward_distances);
        }

        Self {
            landmark_forward_distances,
            landmark_backward_distances,
            target_forward_distances: vec![0; landmarks.len()],
            target_backward_distances: vec![0; landmarks.len()],
            num_pot_evals: 0,
        }
    }

    fn dfs_set_sizes<G>(
        graph: &G,
        r: NodeId,
        weights: &mut [u64],
        visited: &mut BitVec,
        landmarks: &BitVec,
        sp_tree: &ServerWrapper<G, DefaultOps, ZeroPotential, &G>,
    ) -> bool
    where
        G: for<'a> LinkIterable<'a, Link>,
    {
        visited.set(r as usize);

        if landmarks.get(r as usize) {
            weights[r as usize] = 0;
            return true;
        }

        let mut contains_landmark = false;

        for head in graph.link_iter(r).map(|l| l.head()) {
            if sp_tree.predecessor(head) == r && !visited.get(head as usize) && head != r {
                contains_landmark |= Self::dfs_set_sizes(graph, head, weights, visited, landmarks, sp_tree);
                weights[r as usize] += weights[head as usize];
            }
        }

        if contains_landmark {
            weights[r as usize] = 0;
        }

        contains_landmark
    }

    pub fn farthest_landmarks<G>(graph: &G, num_landmarks: usize, initial_landmark: NodeId) -> Vec<NodeId>
    where
        G: for<'a> LinkIterable<'a, Link>,
    {
        report!("algo", "Farthest Landmarks");
        report!("num_landmarks", num_landmarks);
        let n = graph.num_nodes() as NodeId;
        let mut landmarks = Vec::with_capacity(num_landmarks);
        let mut dijkstra = GenericDijkstra::<G, DefaultOps, &G>::new(graph);

        dijkstra.initialize_query(Query { from: initial_landmark, to: n });
        let mut last_node = initial_landmark;

        report_time("landmark_selection", || {
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
        });

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
