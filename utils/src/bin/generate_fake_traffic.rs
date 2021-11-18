use rust_road_router::{
    cli::CliErr,
    datastr::graph::*,
    experiments::{chpot::FakeTraffic, *},
    io::*,
};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut rng = rng(Default::default());

    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);
    let live_weight_file = args.next().unwrap();

    let mut travel_time = Vec::<Weight>::load_from(path.join("travel_time"))?;
    let geo_distance = Vec::<Weight>::load_from(path.join("geo_distance"))?;
    let tt_units_per_s = Vec::<u32>::load_from(path.join("tt_units_per_s"))?[0];
    let dist_units_per_m = Vec::<u32>::load_from(path.join("dist_units_per_m"))?[0];

    FakeTraffic::new(tt_units_per_s, dist_units_per_m, 30.0, 0.005, 5.0).simulate(&mut rng, &mut travel_time, &geo_distance);

    Ok(travel_time.write_to(&path.join(live_weight_file))?)
}
