use std::{
    env,
    error::Error,
    io::{stdout, Write},
    path::Path,
};

use bmw_routing_engine::{cli::CliErr, io::*};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let mut query_dir = None;
    let mut base_dir = Some(path);

    while let Some(base) = base_dir {
        if base.join("source_node").exists() {
            query_dir = Some(base);
            break;
        } else {
            base_dir = base.parent();
        }
    }

    let query_dir = query_dir.ok_or(CliErr("No queries found"))?;

    let from = Vec::<u32>::load_from(query_dir.join("source_node").to_str().unwrap())?;
    let at = Vec::<u32>::load_from(query_dir.join("source_time").to_str().unwrap())?;
    let to = Vec::<u32>::load_from(query_dir.join("target_node").to_str().unwrap())?;

    for ((from, at), to) in from.into_iter().zip(at.into_iter()).zip(to.into_iter()) {
        if writeln!(stdout(), "{} {} {}", from, at, to).is_err() {
            break;
        };
    }

    Ok(())
}
