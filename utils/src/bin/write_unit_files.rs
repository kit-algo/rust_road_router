use rust_road_router::{cli::CliErr, io::*};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let tt_units_per_s: u32 = args
        .next()
        .map(|arg| arg.parse().expect("could not parse tt_units_per_s"))
        .expect("No tt_units_per_s arg given");
    let dist_units_per_m: u32 = args
        .next()
        .map(|arg| arg.parse().expect("could not parse dist_units_per_m"))
        .expect("No dist_units_per_m arg given");

    Ok(vec![tt_units_per_s]
        .write_to(&path.join("tt_units_per_s"))
        .and(vec![dist_units_per_m].write_to(&path.join("dist_units_per_m")))?)
}
