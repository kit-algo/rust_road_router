// Graph processing utility to scale the weights of a tpgr graph (because KaTCH uses tenths of seconds as the base unit).

use bmw_routing_engine::cli::CliErr;
use std::env;
use std::error::Error;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args();
    args.next();

    let input = &args.next().ok_or(CliErr("No input tpgr given"))?;
    let input = Path::new(input);

    let output = &args.next().ok_or(CliErr("No output tpgr given"))?;
    let output = Path::new(output);

    let input = File::open(input)?;
    let output = File::create(output)?;
    let mut output = BufWriter::new(output);

    let mut lines = BufReader::new(&input).lines();

    let header = lines.next().unwrap()?;
    // let mut header = header.split_whitespace().map(|number| number.parse::<usize>().unwrap());
    writeln!(output, "{}0", header)?;

    for line in lines {
        let line = line.unwrap();
        let mut words = line.split_whitespace();
        let tail = words.next().unwrap();
        let head = words.next().unwrap();
        let ipp_count = words.next().unwrap();

        write!(output, "{} {} {}", tail, head, ipp_count)?;

        for num in words {
            let num = num.parse::<f64>().unwrap() * 10.0;
            write!(output, " {}", num)?;
        }

        writeln!(output)?;
    }

    Ok(())
}
