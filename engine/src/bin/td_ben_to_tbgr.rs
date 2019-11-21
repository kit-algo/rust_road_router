use std::{env, error::Error, fs::File, io::prelude::*, path::Path};

use bmw_routing_engine::{cli::CliErr, io::*};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or_else(|| Box::new(CliErr("No directory arg given")))?;
    let path = Path::new(arg);

    let period = Vec::<u32>::load_from(path.join("period").to_str().unwrap())?[0] as i32;
    let first_out = Vec::<u32>::load_from(path.join("first_out").to_str().unwrap())?;
    let head = Vec::<u32>::load_from(path.join("head").to_str().unwrap())?;
    let first_ipp_of_arc = Vec::<u32>::load_from(path.join("first_ipp_of_arc").to_str().unwrap())?;
    let ipp_departure_time = Vec::<u32>::load_from(path.join("ipp_departure_time").to_str().unwrap())?;
    let ipp_travel_time = Vec::<u32>::load_from(path.join("ipp_travel_time").to_str().unwrap())?;

    let output = &args.next().ok_or_else(|| Box::new(CliErr("No output file given")))?;
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
