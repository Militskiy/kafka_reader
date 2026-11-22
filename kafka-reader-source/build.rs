fn main() {
    if std::env::var("CARGO_CFG_TARGET_OS").as_deref() == Ok("windows")
        && std::env::var("CARGO_CFG_TARGET_ENV").as_deref() == Ok("gnu")
    {
        // GNU ld resolves libraries in order: native Windows system imports must
        // follow the statically linked librdkafka/OpenSSL archives.
        for library in ["ws2_32", "secur32", "crypt32"] {
            println!("cargo:rustc-link-arg=-l{library}");
        }
    }
}
