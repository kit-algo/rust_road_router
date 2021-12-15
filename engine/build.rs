use std::{env, fs::File, io::Write, path::Path};

fn main() {
    // write build time info
    built::write_built_file().expect("Failed to acquire build-time information");
    // unconditionally rerun this build script so build time info is always up to date
    #[cfg(not(debug_assertions))]
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

    if let Ok(val) = env::var("TRAFFIC_MAX_ITERATIONS") {
        let dest_path = Path::new(&out_dir).join("TRAFFIC_MAX_ITERATIONS");
        let mut f = File::create(&dest_path).unwrap();
        f.write_all(val.as_bytes()).unwrap();
        println!("cargo:rustc-cfg=override_traffic_max_iterations");
    }
    println!("cargo:rerun-if-env-changed=TRAFFIC_MAX_ITERATIONS");
}
