use std::{env, fs::File, io::Write, path::Path};

fn main() {
    // write build time info
    built::write_built_file().expect("Failed to acquire build-time information");
    // unconditionally rerun this build script so build time info is always up to date
    println!("cargo:rerun-if-changed=foobaz");

    // the following lines allow overriding certain experimental params through env vars.
    // If the env var is set, we enable a certain feature.
    // The module where the parameter is used either defines the param with a default value
    // or includes the file created here.
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

    if let Ok(val) = env::var("CHPOT_NUM_QUERIES") {
        let dest_path = Path::new(&out_dir).join("CHPOT_NUM_QUERIES");
        let mut f = File::create(&dest_path).unwrap();
        f.write_all(val.as_bytes()).unwrap();
        println!("cargo:rustc-cfg=override_chpot_num_queries");
    }
    println!("cargo:rerun-if-env-changed=CHPOT_NUM_QUERIES");

    if let Ok(val) = env::var("NUM_DIJKSTRA_QUERIES") {
        let dest_path = Path::new(&out_dir).join("NUM_DIJKSTRA_QUERIES");
        let mut f = File::create(&dest_path).unwrap();
        f.write_all(val.as_bytes()).unwrap();
        println!("cargo:rustc-cfg=override_num_dijkstra_queries");
    }
    println!("cargo:rerun-if-env-changed=NUM_DIJKSTRA_QUERIES");
}
