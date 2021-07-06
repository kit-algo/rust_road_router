use std::env::args;
use std::io::{BufRead, Write};

fn main() {
    let mut args = args().skip(1);
    let start = args.next().unwrap().parse::<usize>().unwrap();
    let end = args.next().unwrap().parse::<usize>().unwrap();

    let mut profiles = std::collections::BTreeMap::<(u64, u64), Vec<u8>>::new();

    let stdin = std::io::stdin();
    let mut cin = stdin.lock();
    let mut s = String::new();
    while cin.read_line(&mut s).unwrap() > 0 {
        let mut iter = s.split(',').map(|x| x.parse::<u64>().unwrap());
        let tail = iter.next().unwrap();
        let head = iter.next().unwrap();
        let values = iter.skip(start).take(end - start).map(|speed| speed as u8).collect::<Vec<u8>>();

        let mut constant = true;
        let mut iter = values.iter();
        let val = iter.next().unwrap();
        while let Some(other) = iter.next() {
            if other != val {
                constant = false;
                break;
            }
        }

        if !constant {
            profiles.insert((tail, head), values);
        }

        s.clear();
    }

    let stdout = std::io::stdout();
    let mut stdout = stdout.lock();

    for ((tail, head), values) in profiles {
        write!(&mut stdout, "{},{}", tail, head).unwrap();
        for val in values {
            write!(&mut stdout, ",{}", val).unwrap();
        }
        writeln!(&mut stdout).unwrap();
    }
}
