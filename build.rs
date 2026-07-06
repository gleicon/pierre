fn main() {
    println!("cargo:rerun-if-changed=proto/logproto.proto");
    prost_build::compile_protos(&["proto/logproto.proto"], &["proto/"]).expect("failed to compile logproto.proto");
}
