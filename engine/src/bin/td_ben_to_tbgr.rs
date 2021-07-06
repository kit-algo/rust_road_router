// CATCHUp graph conversion utility from RoutingKit format to tbgr (used by TD-CRP).

use std::{env, error::Error, fs::File, io::prelude::*, path::Path};

use rust_road_router::{cli::CliErr, io::*};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    let arg = &args.next().ok_or(CliErr("No directory arg given"))?;
    let path = Path::new(arg);

    let period = Vec::<u32>::load_from(path.join("period"))?[0] as i32;
    let first_out = Vec::<u32>::load_from(path.join("first_out"))?;
    let head = Vec::<u32>::load_from(path.join("head"))?;
    let first_ipp_of_arc = Vec::<u32>::load_from(path.join("first_ipp_of_arc"))?;
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time"))?;
    let ipp_travel_time = Vec::<u32>::load_from(path.join("ipp_travel_time"))?;

    let output = &args.next().ok_or(CliErr("No output file given"))?;
    let mut output = File::create(output)?;

    output.write_all(&2i32.to_ne_bytes())?;
    output.write_all(&((first_out.len() - 1) as u32).to_ne_bytes())?;
    output.write_all(&(head.len() as u32).to_ne_bytes())?;
    output.write_all(&(ipp_departure_time.len() as u32).to_ne_bytes())?;
    output.write_all(&period.to_ne_bytes())?;

    for node in &first_out[..first_out.len() - 1] {
        output.write_all(&node.to_ne_bytes())?;
    }

    for (head, first_ipp) in head.iter().zip(first_ipp_of_arc.iter()) {
        output.write_all(&head.to_ne_bytes())?;
        output.write_all(&first_ipp.to_ne_bytes())?;
    }

    for (&dept, &tt) in ipp_departure_time.iter().zip(ipp_travel_time.iter()) {
        output.write_all(&(dept as i32).to_ne_bytes())?;
        output.write_all(&(tt as i32).to_ne_bytes())?;
    }

    Ok(())
}
