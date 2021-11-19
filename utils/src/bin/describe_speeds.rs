use rust_road_router::{cli::CliErr, datastr::graph::*, io::*};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No graph directory arg given"))?;
    let path = Path::new(arg);

    let travel_time = Vec::<Weight>::load_from(path.join("travel_time"))?;
    let geo_distance = Vec::<Weight>::load_from(path.join("geo_distance"))?;
    let tt_units_per_s = Vec::<u32>::load_from(path.join("tt_units_per_s"))?[0] as f64;
    let dist_units_per_m = Vec::<u32>::load_from(path.join("dist_units_per_m"))?[0] as f64;

    let mut min_speed = f64::INFINITY;
    let mut max_speed = 0.0;
    let mut sum = 0.0;
    for (&tt, &dist) in travel_time.iter().zip(geo_distance.iter()) {
        let speed = (dist as f64 / dist_units_per_m) / (tt as f64 / tt_units_per_s) * 3.6;
        if speed < min_speed {
            min_speed = speed;
        }
        if speed > max_speed {
            max_speed = speed;
        }
        sum += speed;
    }

    eprintln!("Min Speed: {}", min_speed);
    eprintln!("Max Speed: {}", max_speed);
    eprintln!("Avg Speed: {}", sum / travel_time.len() as f64);

    Ok(())
}
