use bmw_routing_engine::{cli::CliErr, io::*};
use std::{env, error::Error, path::Path};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();
    let arg = &args.next().ok_or_else(|| Box::new(CliErr("No directory arg given")))?;
    let path = Path::new(arg);

    let mut first_out = Vec::<u32>::load_from(path.join("customized/incoming_first_source").to_str().unwrap())?;
    let mut other_first_out = Vec::<u32>::load_from(path.join("customized/outgoing_first_source").to_str().unwrap())?;
    let first_last = first_out.pop().unwrap();
    for idx in &mut other_first_out {
        *idx += first_last;
    }
    first_out.append(&mut other_first_out);

    println!("Arcs: {}", first_out.len() - 1);
    println!("Average: {}", f64::from(*first_out.last().unwrap()) / (first_out.len() - 1) as f64);
    println!(
        "Unique: {}",
        first_out.windows(2).filter(|w| w[1] - w[0] == 1).count() as f64 / (first_out.len() - 1) as f64
    );
    println!("Max: {}", first_out.windows(2).map(|w| w[1] - w[0]).max().unwrap());

    Ok(())
}
