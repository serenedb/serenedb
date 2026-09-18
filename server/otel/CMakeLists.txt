set(SDB_OTEL_SCHEMA_SQL ${CMAKE_SOURCE_DIR}/resources/otel/otel_schema.sql)
set(SDB_OTEL_GENERATED_DIR ${CMAKE_BINARY_DIR}/generated)
set(SDB_OTEL_SCHEMA_HEADER ${SDB_OTEL_GENERATED_DIR}/otel/schema_sql.h)

add_custom_command(
    OUTPUT ${SDB_OTEL_SCHEMA_HEADER}
    COMMAND ${CMAKE_COMMAND} -E make_directory ${SDB_OTEL_GENERATED_DIR}/otel
    COMMAND
        ${PYTHON_EXECUTABLE} ${CMAKE_SOURCE_DIR}/scripts/otel_schema.py embed
        ${SDB_OTEL_SCHEMA_HEADER}
    DEPENDS ${SDB_OTEL_SCHEMA_SQL} ${CMAKE_SOURCE_DIR}/scripts/otel_schema.py
    COMMENT "Generating embedded OpenTelemetry schema"
    VERBATIM
)

add_custom_target(serene_otel_schema DEPENDS ${SDB_OTEL_SCHEMA_HEADER})

################################################################################
## OTLP messages generated from the vendored opentelemetry-proto
################################################################################

set(SDB_OTLP_PROTO_ROOT ${CMAKE_SOURCE_DIR}/third_party/opentelemetry-proto)
set(SDB_OTLP_PROTO_OUT ${SDB_OTEL_GENERATED_DIR}/otlp)

# Only the three export requests and what they reach; profiles and
# processcontext are not part of the schema SereneDB stores.
set(SDB_OTLP_PROTOS
    opentelemetry/proto/common/v1/common.proto
    opentelemetry/proto/resource/v1/resource.proto
    opentelemetry/proto/logs/v1/logs.proto
    opentelemetry/proto/trace/v1/trace.proto
    opentelemetry/proto/metrics/v1/metrics.proto
    opentelemetry/proto/collector/logs/v1/logs_service.proto
    opentelemetry/proto/collector/trace/v1/trace_service.proto
    opentelemetry/proto/collector/metrics/v1/metrics_service.proto
)

set(SDB_OTLP_GENERATED_SOURCES)
foreach(proto ${SDB_OTLP_PROTOS})
    string(REPLACE ".proto" ".pb.cc" generated_source ${proto})
    string(REPLACE ".proto" ".pb.h" generated_header ${proto})
    list(
        APPEND SDB_OTLP_GENERATED_SOURCES
        ${SDB_OTLP_PROTO_OUT}/${generated_source}
    )
    add_custom_command(
        OUTPUT
            ${SDB_OTLP_PROTO_OUT}/${generated_source}
            ${SDB_OTLP_PROTO_OUT}/${generated_header}
        COMMAND ${CMAKE_COMMAND} -E make_directory ${SDB_OTLP_PROTO_OUT}
        COMMAND
            protobuf::protoc --cpp_out=${SDB_OTLP_PROTO_OUT}
            -I${SDB_OTLP_PROTO_ROOT} ${SDB_OTLP_PROTO_ROOT}/${proto}
        DEPENDS protobuf::protoc ${SDB_OTLP_PROTO_ROOT}/${proto}
        COMMENT "protoc ${proto}"
        VERBATIM
    )
endforeach()

add_library(serene_otlp_proto STATIC ${SDB_OTLP_GENERATED_SOURCES})

target_link_libraries(serene_otlp_proto PUBLIC protobuf::libprotobuf)

# Generated sources are not ours to lint.
target_include_directories(
    serene_otlp_proto
    SYSTEM
    PUBLIC ${SDB_OTLP_PROTO_OUT}
)
target_compile_options(serene_otlp_proto PRIVATE -w)

target_sources(serene_connector PRIVATE mapper.cpp protobuf.cpp protojson.cpp)

target_include_directories(serene_connector PUBLIC ${SDB_OTEL_GENERATED_DIR})

target_link_libraries(serene_connector serene_otlp_proto)

add_dependencies(serene_connector serene_otel_schema)
add_dependencies(sereneserver serene_otel_schema)
