// Program to convert map data from HERE into RoutingKit data structures

use std::{env, error::Error, path::Path, str::FromStr};

use conversion::here::{csv_source::CSVSource, read_graph};
use rust_road_router::{cli::CliErr, io::*};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);

    let in_dir = &args.next().ok_or(CliErr("No input directory arg given"))?;
    let out_dir = &args.next().ok_or(CliErr("No output directory arg given"))?;

    let min_lat = (args.next().as_deref().map(f64::from_str).unwrap_or(Ok(-360.0))? * 100_000.) as i64;
    let min_lon = (args.next().as_deref().map(f64::from_str).unwrap_or(Ok(-360.0))? * 100_000.) as i64;
    let max_lat = (args.next().as_deref().map(f64::from_str).unwrap_or(Ok(360.0))? * 100_000.) as i64;
    let max_lon = (args.next().as_deref().map(f64::from_str).unwrap_or(Ok(360.0))? * 100_000.) as i64;

    let source = CSVSource::new(Path::new(in_dir));
    let data = read_graph(&source, (min_lat, min_lon), (max_lat, max_lon));
    data.graph.deconstruct_to(out_dir)?;
    let out_dir = Path::new(out_dir);

    data.graph.weight().write_to(&out_dir.join("travel_time"))?;
    data.functional_road_classes.write_to(&out_dir.join("functional_road_classes"))?;
    data.lat.write_to(&out_dir.join("latitude"))?;
    data.lng.write_to(&out_dir.join("longitude"))?;
    data.link_id_mapping.write_to(&out_dir.join("link_id_mapping"))?;
    data.here_rank_to_link_id.write_to(&out_dir.join("here_rank_to_link_id"))?;
    data.link_lengths
        .into_iter()
        .map(|d| d as u32)
        .collect::<Box<[_]>>()
        .write_to(&out_dir.join("geo_distance"))?;
    [1000].write_to(&out_dir.join("tt_units_per_s"))?;
    [1].write_to(&out_dir.join("dist_units_per_m"))?;

    Ok(())
}
