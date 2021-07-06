// CATCHUp utilitie to write RoutingKit querties to stdout

use std::{
    env,
    error::Error,
    io::{stdout, Write},
    path::Path,
};

use rust_road_router::{cli::CliErr, io::*};

fn main() -> Result<(), Box<dyn Error>> {
    let arg = &env::args().skip(1).next().ok_or(CliErr("No directory arg given"))?;
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

    let from = Vec::<u32>::load_from(query_dir.join("source_node"))?;
    let at = Vec::<u32>::load_from(query_dir.join("source_time"))?;
    let to = Vec::<u32>::load_from(query_dir.join("target_node"))?;

    for ((from, at), to) in from.into_iter().zip(at.into_iter()).zip(to.into_iter()) {
        if writeln!(stdout(), "{} {} {}", from, at, to).is_err() {
            break;
        };
    }

    Ok(())
}
