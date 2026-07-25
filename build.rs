fn main() {
    println!("cargo:rerun-if-changed=proto/logproto.proto");
    prost_build::compile_protos(&["proto/logproto.proto"], &["proto/"])
        .expect("failed to compile logproto.proto");

    // OTLP logs (PRD v0.2 Block C): real upstream schema, vendored from
    // open-telemetry/opentelemetry-proto (same reasoning as logproto.proto — field
    // numbers/types determine wire compatibility, not any codegen sugar). Compiled
    // via tonic-prost-build rather than plain prost-build because this set includes
    // a service definition (`LogsService`), which needs the gRPC server/client
    // scaffolding tonic-prost-build generates alongside the plain messages.
    println!("cargo:rerun-if-changed=proto/opentelemetry");
    tonic_prost_build::configure()
        .compile_protos(
            &[
                "proto/opentelemetry/proto/collector/logs/v1/logs_service.proto",
                "proto/opentelemetry/proto/logs/v1/logs.proto",
                "proto/opentelemetry/proto/common/v1/common.proto",
                "proto/opentelemetry/proto/resource/v1/resource.proto",
            ],
            &["proto/"],
        )
        .expect("failed to compile OTLP logs protos");
}
