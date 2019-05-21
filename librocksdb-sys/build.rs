extern crate cc;
extern crate pkg_config;

use pkg_config::probe_library;
use std::env::{var, VarError::NotPresent};
use std::fs::{create_dir, remove_dir_all};
use std::process::Command;

fn link(name: &str, bundled: bool) {
    let target = var("TARGET").unwrap();
    let target: Vec<_> = target.split('-').collect();
    if target.get(2) == Some(&"windows") {
        println!("cargo:rustc-link-lib=dylib={}", name);
        if bundled && target.get(3) == Some(&"gnu") {
            let dir = var("CARGO_MANIFEST_DIR").unwrap();
            println!("cargo:rustc-link-search=native={}/{}", dir, target[0]);
        }
    }
}

fn build_rocksdb() {
    println!("cargo:rerun-if-changed=build.rs");
    println!("cargo:rerun-if-changed=rocksdb/");

    let mut build = cc::Build::new();
    build.include("rocksdb/include/");
    build.include("rocksdb/");
    build.include("rocksdb/third-party/gtest-1.7.0/fused-src/");
    build.include("snappy/");
    build.include(".");

    build.opt_level(3);

    build.define("NDEBUG", Some("1"));
    build.define("SNAPPY", Some("1"));

    let mut lib_sources = include_str!("rocksdb_lib_sources.txt")
        .split(" ")
        .collect::<Vec<&'static str>>();

    // We have a pregenerated a version of build_version.cc in the local directory
    lib_sources = lib_sources
        .iter()
        .cloned()
        .filter(|file| *file != "util/build_version.cc")
        .collect::<Vec<&'static str>>();

    if cfg!(target_os = "macos") {
        build.define("OS_MACOSX", Some("1"));
        build.define("ROCKSDB_PLATFORM_POSIX", Some("1"));
        build.define("ROCKSDB_LIB_IO_POSIX", Some("1"));
    }
    if cfg!(target_os = "linux") {
        build.define("OS_LINUX", Some("1"));
        build.define("ROCKSDB_PLATFORM_POSIX", Some("1"));
        build.define("ROCKSDB_LIB_IO_POSIX", Some("1"));
        // COMMON_FLAGS="$COMMON_FLAGS -fno-builtin-memcmp"
    }
    if cfg!(target_os = "freebsd") {
        build.define("OS_FREEBSD", Some("1"));
        build.define("ROCKSDB_PLATFORM_POSIX", Some("1"));
        build.define("ROCKSDB_LIB_IO_POSIX", Some("1"));
    }

    if cfg!(windows) {
        link("rpcrt4", false);
        build.define("OS_WIN", Some("1"));
        build.define("NOMINMAX", Some("1"));

        // Remove POSIX-specific sources
        lib_sources = lib_sources
            .iter()
            .cloned()
            .filter(|file| match *file {
                "port/port_posix.cc" | "util/env_posix.cc" | "env/env_posix.cc"
                | "env/io_posix.cc" => false,
                _ => true,
            })
            .collect::<Vec<&'static str>>();

        // Add Windows-specific sources
        lib_sources.push("port/win/port_win.cc");
        lib_sources.push("port/win/env_win.cc");
        lib_sources.push("port/win/env_default.cc");
        lib_sources.push("port/win/win_logger.cc");
        lib_sources.push("port/win/win_thread.cc");
        lib_sources.push("port/win/io_win.cc");
        lib_sources.push("port/win/xpress_win.cc");
    }

    if cfg!(target_env = "msvc") {
        build.flag("-EHsc");
    } else {
        build.flag("-std=c++11");
    }

    for file in lib_sources {
        let file = "rocksdb/".to_string() + file;
        build.file(&file);
    }

    build.file("build_version.cc");
    build.cpp(true);
    build.compile("librocksdb.a");
}

fn build_snappy() {
    let mut build = cc::Build::new();
    build.include("snappy/");
    build.include(".");

    build.define("NDEBUG", Some("1"));

    build.opt_level(3);

    if cfg!(target_env = "msvc") {
        build.flag("-EHsc");
    } else {
        build.flag("-std=c++11");
        build.flag("-fPIC");
    }

    build.flag_if_supported("-Wno-unused-parameter");
    build.flag_if_supported("-Wno-sign-compare");

    build.file("snappy/snappy.cc");
    build.file("snappy/snappy-sinksource.cc");
    build.file("snappy/snappy-c.cc");

    build.cpp(true);
    build.compile("libsnappy.a");
}

/// Returns `true` if `library` was found, or `false` if it should be compiled.
fn try_to_find_lib(library: &str) -> bool {
    let lib_name = match library {
        "librocksdb" => "ROCKSDB",
        "libsnappy" => "SNAPPY",
        _ => "UNKNOWN",
    };

    let should_build = match var(format!("{}_BUILD", lib_name)).ok() {
        None => false,
        Some(ref x) if x == "0" => false,
        _ => true,
    };
    if should_build {
        return false;
    }

    if let Ok(lib_dir) = var(format!("{}_LIB_DIR", lib_name).as_str()) {
        println!("cargo:rustc-link-search=native={}", lib_dir);
        let mode = match var(format!("{}_STATIC", lib_name).as_str()) {
            Ok(_) => {
                if cfg!(target_os = "macos") {
                    println!("cargo:rustc-link-lib=static=lz4");
                    println!("cargo:rustc-link-lib=dylib=c++");
                    println!("cargo:rustc-link-lib=dylib=bz2");
                    println!("cargo:rustc-link-lib=dylib=z");
                }
                "static"
            }
            Err(NotPresent) => "dylib",
            Err(_) => panic!("Wrong value in env variable"),
        };
        println!(
            "cargo:rustc-link-lib={0}={1}",
            mode,
            lib_name.to_lowercase()
        );
        return true;
    }

    probe_library(library).is_ok()
}

fn get_local_src_if(name: &str, repo: &str, sha: &str) {
    let is_to_pull = Command::new("git")
        .arg("rev-parse")
        .arg("HEAD")
        .current_dir(name)
        .output()
        .map(|output| {
            let curren_head = std::str::from_utf8(&output.stdout)
                .expect("UTF-8 output")
                .trim();
            //Assume that if sha is different, we were not able
            //to finish clone/checkout
            curren_head != sha
        })
        .unwrap_or(true);

    if !is_to_pull {
        return;
    }

    let _ = remove_dir_all(name);
    create_dir(name).expect("To create dir");

    Command::new("git")
        .arg("clone")
        .arg(repo)
        .arg(".")
        .current_dir(name)
        .status()
        .map(|status| match status.success() {
            true => (),
            false => panic!("Git: Unable to clone repo"),
        })
        .expect("Failed to run clone command");

    Command::new("git")
        .arg("checkout")
        .arg(sha)
        .current_dir(name)
        .output()
        .map(|output| match output.status.success() {
            true => (),
            false => panic!(
                "Git: Unable to checkout repo: {}",
                String::from_utf8_lossy(&output.stderr)
            ),
        })
        .expect("Failed to run checkout command");
}

fn main() {
    if !try_to_find_lib("libsnappy") {
        get_local_src_if(
            "snappy",
            "https://github.com/google/snappy.git",
            "b02bfa754ebf27921d8da3bd2517eab445b84ff9",
        );
        build_snappy();
    }

    if !try_to_find_lib("librocksdb") {
        get_local_src_if(
            "rocksdb",
            "https://github.com/facebook/rocksdb.git",
            "641fae60f63619ed5d0c9d9e4c4ea5a0ffa3e253",
        );
        build_rocksdb();
    }
}
