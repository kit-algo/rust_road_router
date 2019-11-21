use std::env;
use std::fs::File;
use std::io::{BufRead, BufReader, BufWriter, Write};
use std::path::Path;

fn main() {
    let mut args = env::args();
    args.next();

    let input = &args.next().expect("No input tpgr given");
    let input = Path::new(input);

    let output = &args.next().expect("No output tpgr given");
    let output = Path::new(output);

    let input = File::open(input).unwrap();
    let output = File::create(output).unwrap();
    let mut output = BufWriter::new(output);

    let mut lines = BufReader::new(&input).lines();

    let header = lines.next().unwrap().unwrap();
    // let mut header = header.split_whitespace().map(|number| number.parse::<usize>().unwrap());
    writeln!(output, "{}0", header).unwrap();

    for line in lines {
        let line = line.unwrap();
        let mut words = line.split_whitespace();
        let tail = words.next().unwrap();
        let head = words.next().unwrap();
        let ipp_count = words.next().unwrap();

        write!(output, "{} {} {}", tail, head, ipp_count).unwrap();

        for num in words {
            let num = num.parse::<f64>().unwrap() * 10.0;
            write!(output, " {}", num).unwrap();
        }

        writeln!(output).unwrap();
    }
}
