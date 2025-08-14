fn main() {
    // Build C ABI wrappers as static libraries using the symlinked wrappers
    let wrappers: &[(&str, &str, &str)] = &[
        (
            "phmap_string_wrapper.cpp",
            "phmap_string_bridge",
            "ffi-include",
        ),
        (
            "boost_string_wrapper.cpp",
            "boost_string_bridge",
            "ffi-include",
        ),
        ("tbb_string_wrapper.cpp", "tbb_string_bridge", "ffi-include"),
        (
            "folly_string_wrapper.cpp",
            "folly_string_bridge",
            "ffi-include",
        ),
        (
            "parlay_string_wrapper.cpp",
            "parlay_string_bridge",
            "ffi-include",
        ),
        (
            "libcuckoo_string_wrapper.cpp",
            "libcuckoo_string_bridge",
            "ffi-include",
        ),
        ("seq_string_wrapper.cpp", "seq_string_bridge", "ffi-include"),
    ];
    for (cpp, lib, include_dir) in wrappers {
        let mut build = cc::Build::new();
        build
            .cpp(true)
            .file(format!("{}/{}", "ffi-wrappers", cpp))
            .flag_if_supported("-O3")
            .flag_if_supported("-std=c++17")
            .include(include_dir)
            .include("/usr/local/include")
            .include("ffi-include");
        build.compile(lib);
    }

    // tbb runtime
    println!("cargo:rustc-link-lib=tbb");
    // folly runtime - using system libraries
    println!("cargo:rustc-link-search=native=/usr/local/lib");
    println!("cargo:rustc-link-lib=static=folly");
    // Folly dependencies
    println!("cargo:rustc-link-lib=glog");
    println!("cargo:rustc-link-lib=gflags");
    println!("cargo:rustc-link-lib=fmt");
    println!("cargo:rustc-link-lib=double-conversion");
    println!("cargo:rustc-link-lib=boost_context");
    println!("cargo:rustc-link-lib=boost_filesystem");
    println!("cargo:rustc-link-lib=boost_program_options");
    println!("cargo:rustc-link-lib=boost_regex");
    println!("cargo:rustc-link-lib=boost_system");
    println!("cargo:rustc-link-lib=boost_thread");
    println!("cargo:rustc-link-lib=event");
}
