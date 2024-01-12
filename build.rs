extern crate pkg_config;

fn main() {
    println!("cargo:rustc-link-lib=statgrab");
    println!("cargo:rustc-link-search=/opt/homebrew/include");
    pkg_config::Config::new().probe("libstatgrab").unwrap();
    println!("cargo:rerun-if-changed=build.rs");
}
