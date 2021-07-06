use rust_road_router::{cli::CliErr, io::*};
use std::{env, error::Error};

fn main() -> Result<(), Box<dyn Error>> {
    let mut args = env::args().skip(1);
    match (args.next(), args.next()) {
        (Some(data_type), Some(ref output)) => {
            match data_type.as_ref() {
                "i8" | "int8" => parse_input::<i8>()?.write_to(output)?,
                "u8" | "uint8" => parse_input::<u8>()?.write_to(output)?,
                "i16" | "int16" => parse_input::<i16>()?.write_to(output)?,
                "u16" | "uint16" => parse_input::<u16>()?.write_to(output)?,
                "i32" | "int32" => parse_input::<i32>()?.write_to(output)?,
                "u32" | "uint32" => parse_input::<u32>()?.write_to(output)?,
                "i64" | "int64" => parse_input::<i64>()?.write_to(output)?,
                "u64" | "uint64" => parse_input::<u64>()?.write_to(output)?,
                "f32" | "float32" => parse_input::<f32>()?.write_to(output)?,
                "f64" | "float64" => parse_input::<f64>()?.write_to(output)?,
                _ => {
                    print_usage();
                    return Err(Box::new(CliErr("Invalid data type")));
                }
            };
            Ok(())
        }
        _ => {
            print_usage();
            Err(Box::new(CliErr("Invalid arguments")))
        }
    }
}

fn print_usage() {
    eprintln!("Usage: encode_vector data_type output_vector_file

Reads textual data from the standard input and writes it in a binary format to output_vector_file. The input data should be one data element per line. The data is only written once an end of file is encountered on the input. data_type can be one of
* i8
* u8
* i16
* u16
* i32
* u32
* i64
* u64
* f32
* f64

");
}

use std::str::FromStr;

fn parse_input<T>() -> Result<Vec<T>, Box<dyn Error>>
where
    T: FromStr,
    <T as FromStr>::Err: Error + 'static,
{
    use std::io::{stdin, BufRead};

    let mut values = Vec::new();

    let stdin = stdin();
    for line in stdin.lock().lines() {
        values.push(line.unwrap().parse::<T>()?)
    }

    Ok(values)
}
