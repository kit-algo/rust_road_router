use crate::{
    algo::{
        ch_potentials::{query::Server as TopoServer, *},
        dijkstra::{query::dijkstra::Server as DijkServer, DefaultOps},
    },
    datastr::graph::*,
    io::*,
    report::*,
    util::{fl_max, fl_min},
};
use std::{error::Error, path::Path};

use rand::prelude::*;

/// Number of queries performed for each experiment.
/// Can be overriden through the CHPOT_NUM_QUERIES env var.
pub fn num_queries() -> usize {
    std::env::var("CHPOT_NUM_QUERIES").map_or(10000, |num| num.parse().unwrap())
}

pub fn run(
    path: &Path,
    modify_travel_time: impl FnOnce(&FirstOutGraph<&[EdgeId], &[NodeId], &[Weight]>, &mut StdRng, &mut [Weight]) -> Result<(), Box<dyn Error>>,
) -> Result<(), Box<dyn Error>> {
    let mut rng = super::rng(Default::default());

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let mut modified_travel_time = graph.weight().to_vec();

    modify_travel_time(&graph.borrowed(), &mut rng, &mut modified_travel_time)?;
    let modified_graph = FirstOutGraph::new(graph.first_out(), graph.head(), &modified_travel_time[..]);

    let mut algo_runs_ctxt = push_collection_context("algo_runs");

    affinity::set_thread_affinity(&[0]).unwrap();

    let potential = CHPotential::reconstruct_from(&path.join("lower_bound_ch"))?;

    let virtual_topocore_ctxt = algo_runs_ctxt.push_collection_item();
    let mut topocore: TopoServer<OwnedGraph, _, _, true, true, true> = TopoServer::new(&modified_graph, potential, DefaultOps::default());
    drop(virtual_topocore_ctxt);

    super::run_random_queries_with_callbacks(
        graph.num_nodes(),
        &mut topocore,
        &mut rng,
        &mut algo_runs_ctxt,
        num_queries(),
        |_, _, _| (),
        // |mut res| {
        //     report!("num_pot_computations", res.data().potential().num_pot_computations());
        //     let from = res.data().query().from();
        //     report!("lower_bound", res.data().lower_bound(from));
        // },
        |_, _| None,
    );

    let mut server = DijkServer::<_, DefaultOps>::new(modified_graph);
    super::run_random_queries(graph.num_nodes(), &mut server, &mut rng, &mut algo_runs_ctxt, super::num_dijkstra_queries());

    Ok(())
}

fn kmh_to_mpms(tt_units_per_s: u32, dist_units_per_m: u32, speed: f64) -> f64 {
    // speed * 1000.0 / 3600.0 / 1000.0
    speed * 1000.0 * dist_units_per_m as f64 / 3600.0 / tt_units_per_s as f64
}

pub struct ProbabilisticSpeedWeightedScaler {
    v_min: f64,
    v_max: f64,
    prob_at_min: f64,
    prob_at_max: f64,
    factor: f64,
}

impl ProbabilisticSpeedWeightedScaler {
    pub fn scale_all(tt_units_per_s: u32, dist_units_per_m: u32, factor: f64) -> Self {
        Self {
            v_min: 0.0,
            v_max: kmh_to_mpms(tt_units_per_s, dist_units_per_m, 300.0),
            prob_at_min: 1.0,
            prob_at_max: 1.0,
            factor,
        }
    }

    pub fn scale_probabilistically(tt_units_per_s: u32, dist_units_per_m: u32, factor: f64, p: f64) -> Self {
        Self {
            v_min: 0.0,
            v_max: kmh_to_mpms(tt_units_per_s, dist_units_per_m, 300.0),
            prob_at_min: p,
            prob_at_max: p,
            factor,
        }
    }

    pub fn speed_cutoff(tt_units_per_s: u32, dist_units_per_m: u32, factor: f64, speed: f64, p_below: f64) -> Self {
        Self {
            v_min: kmh_to_mpms(tt_units_per_s, dist_units_per_m, speed),
            v_max: kmh_to_mpms(tt_units_per_s, dist_units_per_m, speed + 1.0),
            prob_at_min: p_below,
            prob_at_max: 1.0 - p_below,
            factor,
        }
    }

    pub fn scale_with_speed_weighted_prob(&self, rng: &mut StdRng, travel_time: &mut [Weight], geo_distance: &[Weight]) {
        for (weight, &dist) in travel_time.iter_mut().zip(geo_distance.iter()) {
            if *weight == 0 {
                continue;
            }

            let speed = dist as f64 / *weight as f64;
            let speed = fl_max(speed, self.v_min);
            let speed = fl_min(speed, self.v_max);

            let alpha = (speed - self.v_min) / (self.v_max - self.v_min);
            let p = self.prob_at_min + alpha * (self.prob_at_max - self.prob_at_min);

            if rng.gen_bool(fl_max(fl_min(p, 1.0), 0.0)) {
                *weight = (*weight as f64 * self.factor) as Weight;
            }
        }
    }
}

pub struct FakeTraffic {
    v_min: f64,
    prob: f64,
    traffic_speed: f64,
}

impl FakeTraffic {
    pub fn new(tt_units_per_s: u32, dist_units_per_m: u32, v_min: f64, prob: f64, traffic_speed: f64) -> Self {
        Self {
            v_min: kmh_to_mpms(tt_units_per_s, dist_units_per_m, v_min),
            prob,
            traffic_speed: kmh_to_mpms(tt_units_per_s, dist_units_per_m, traffic_speed),
        }
    }

    pub fn simulate(&self, rng: &mut StdRng, travel_time: &mut [Weight], geo_distance: &[Weight]) {
        let mut num_changed = 0;
        for (weight, &dist) in travel_time.iter_mut().zip(geo_distance.iter()) {
            if *weight == 0 {
                continue;
            }

            let speed = dist as f64 / *weight as f64;

            if speed > self.v_min && rng.gen_bool(self.prob) {
                num_changed += 1;
                *weight = (dist as f64 / self.traffic_speed) as Weight;
            }
        }
        report!("num_changed_arcs", num_changed);
    }
}
