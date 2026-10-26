fn main() {
    // Plugin archive names carry the target triple of this build.
    let target = std::env::var("TARGET").expect("cargo sets TARGET");
    println!("cargo:rustc-env=BEACON_TARGET={target}");
    println!("cargo:rerun-if-changed=build.rs");
}
