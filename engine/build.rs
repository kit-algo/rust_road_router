use std::env;
use std::fs::File;
use std::io::Write;
use std::path::Path;

fn main() {
    built::write_built_file().expect("Failed to acquire build-time information");

    let out_dir = env::var("OUT_DIR").unwrap();

    if let Ok(val) = env::var("TDCCH_APPROX_THRESHOLD") {
        let dest_path = Path::new(&out_dir).join("TDCCH_APPROX_THRESHOLD");
        let mut f = File::create(&dest_path).unwrap();
        f.write_all(val.as_bytes()).unwrap();
        println!("cargo:rustc-cfg=override_tdcch_approx_threshold");
    }
    println!("cargo:rerun-if-env-changed=TDCCH_APPROX_THRESHOLD");

    if let Ok(val) = env::var("TDCCH_APPROX") {
        let dest_path = Path::new(&out_dir).join("TDCCH_APPROX");
        let mut f = File::create(&dest_path).unwrap();
        f.write_all(val.as_bytes()).unwrap();
        println!("cargo:rustc-cfg=override_tdcch_approx");
    }
    println!("cargo:rerun-if-env-changed=TDCCH_APPROX");

    println!("cargo:rerun-if-changed=foobaz");
}
