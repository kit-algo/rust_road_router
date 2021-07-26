#[macro_use]
extern crate rust_road_router;
use rust_road_router::{
    algo::{
        ch_potentials::*,
        dijkstra::{
            query::{bidirectional_dijkstra::Server as BiDir, dijkstra::Server as UniDir, sym_bidir_astar::Server as SymBiDir},
            AlternatingDirs, DefaultOps,
        },
    },
    cli::CliErr,
    datastr::graph::*,
    experiments,
    io::*,
    report::*,
};
use std::{env, error::Error, path::Path};

use rand::prelude::*;

fn main() -> Result<(), Box<dyn Error>> {
    let _reporter = enable_reporting("bidir_chpot_scaling");

    let arg = &env::args().skip(1).next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let graph = WeightedGraphReconstructor("travel_time").reconstruct_from(&path)?;
    let geo_distance = Vec::<Weight>::load_from(path.join("geo_distance"))?;
    let chpot_data = CHPotLoader::reconstruct_from(&path.join("lower_bound_ch"))?;

    let rng = experiments::rng(Default::default());
    let mut modify_rng = rng.clone();
    let mut exps_ctxt = push_collection_context("experiments".to_string());

    let run = |modified_travel_time: &[Weight]| {
        let mut algo_runs_ctxt = push_collection_context("algo_runs".to_string());
        let modified_graph = FirstOutGraph::new(graph.first_out(), graph.head(), modified_travel_time);

        let mut server = UniDir::<_, DefaultOps, _>::with_potential(modified_graph.clone(), chpot_data.potentials().0);
        experiments::run_random_queries(
            graph.num_nodes(),
            &mut server,
            &mut rng.clone(),
            &mut algo_runs_ctxt,
            experiments::chpot::num_queries(),
        );

        let (forward_pot, backward_pot) = chpot_data.potentials();
        let mut server = BiDir::<_, _, _>::new_with_potentials(modified_graph.clone(), forward_pot, backward_pot);
        experiments::run_random_queries(
            graph.num_nodes(),
            &mut server,
            &mut rng.clone(),
            &mut algo_runs_ctxt,
            experiments::chpot::num_queries(),
        );

        let (forward_pot, backward_pot) = chpot_data.potentials();
        let mut server = SymBiDir::<_, _, _, AlternatingDirs>::new_with_potentials(modified_graph, forward_pot, backward_pot);
        experiments::run_random_queries(
            graph.num_nodes(),
            &mut server,
            &mut rng.clone(),
            &mut algo_runs_ctxt,
            experiments::chpot::num_queries(),
        );
    };

    for factor in [1., 1.04, 1.08, 1.12, 1.16, 1.2] {
        let _exp_ctx = exps_ctxt.push_collection_item();
        report!("experiment", "weight_scale");
        report!("factor", factor);

        let mut modified_travel_time = graph.weight().to_vec();
        ProbabilisticSpeedWeightedScaler::scale_all(factor).scale_with_speed_weighted_prob(&mut modify_rng, &mut modified_travel_time, &geo_distance);
        run(&modified_travel_time);
    }

    for factor in [1., 1.1, 1.2, 1.3, 1.4, 1.5] {
        for p in [0.25, 0.5, 0.75] {
            let _exp_ctx = exps_ctxt.push_collection_item();
            report!("experiment", "probabilistic_weight_scale");
            report!("factor", factor);
            report!("probability", p);

            let mut modified_travel_time = graph.weight().to_vec();
            ProbabilisticSpeedWeightedScaler::scale_probabilistically(factor, p).scale_with_speed_weighted_prob(
                &mut modify_rng,
                &mut modified_travel_time,
                &geo_distance,
            );
            run(&modified_travel_time);
        }
    }

    for factor in [1., 1.25, 1.5, 1.75] {
        for p in [0.01, 0.25, 0.75, 0.99] {
            for speed in [30.0, 70.0, 110.0] {
                let _exp_ctx = exps_ctxt.push_collection_item();
                report!("experiment", "probabilistic_scale_by_speed");
                report!("factor", factor);
                report!("probability", p);
                report!("speed", speed);

                let mut modified_travel_time = graph.weight().to_vec();
                ProbabilisticSpeedWeightedScaler::speed_cutoff(factor, speed, p).scale_with_speed_weighted_prob(
                    &mut modify_rng,
                    &mut modified_travel_time,
                    &geo_distance,
                );
                run(&modified_travel_time);
            }
        }
    }

    Ok(())
}

fn kmh_to_mpms(speed: f64) -> f64 {
    // speed * 1000.0 / 3600.0 / 1000.0
    speed * 1000.0 / 3600.0 / 10.0
}

struct ProbabilisticSpeedWeightedScaler {
    v_min: f64,
    v_max: f64,
    prob_at_min: f64,
    prob_at_max: f64,
    factor: f64,
}

impl ProbabilisticSpeedWeightedScaler {
    fn scale_all(factor: f64) -> Self {
        Self {
            v_min: 0.0,
            v_max: kmh_to_mpms(300.0),
            prob_at_min: 1.0,
            prob_at_max: 1.0,
            factor,
        }
    }

    fn scale_probabilistically(factor: f64, p: f64) -> Self {
        Self {
            v_min: 0.0,
            v_max: kmh_to_mpms(300.0),
            prob_at_min: p,
            prob_at_max: p,
            factor,
        }
    }

    fn speed_cutoff(factor: f64, speed: f64, p_below: f64) -> Self {
        Self {
            v_min: kmh_to_mpms(speed),
            v_max: kmh_to_mpms(speed + 1.0),
            prob_at_min: p_below,
            prob_at_max: 1.0 - p_below,
            factor,
        }
    }

    fn scale_with_speed_weighted_prob(&self, rng: &mut StdRng, travel_time: &mut [Weight], geo_distance: &[Weight]) {
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

fn fl_min(x: f64, y: f64) -> f64 {
    if x < y {
        x
    } else {
        y
    }
}

fn fl_max(x: f64, y: f64) -> f64 {
    if x > y {
        x
    } else {
        y
    }
}
