fn main() {
    let os = std::env::var("CARGO_CFG_TARGET_OS").unwrap_or_default();
    let abi = std::env::var("CARGO_CFG_TARGET_ENV").unwrap_or_default();
    println!("cargo:rustc-check-cfg=cfg(guarded_kafka_metadata)");
    if os == "linux" || (os == "windows" && abi == "gnu") {
        // Both supported builds use static librdkafka and a GNU-compatible
        // linker. Keep the version floor on automatic metadata refreshes too.
        println!("cargo:rustc-cfg=guarded_kafka_metadata");
        println!("cargo:rustc-link-arg=-Wl,--wrap=rd_kafka_broker_ApiVersion_supported");
    }
    if os == "windows" && abi == "gnu" {
        // GNU ld resolves libraries in order: native Windows system imports must
        // follow the statically linked librdkafka/OpenSSL archives.
        for library in ["ws2_32", "secur32", "crypt32"] {
            println!("cargo:rustc-link-arg=-l{library}");
        }
    }
}
