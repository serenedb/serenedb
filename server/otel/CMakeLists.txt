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

target_sources(sereneserver PRIVATE mapper.cpp protobuf.cpp protojson.cpp)

target_include_directories(sereneserver PUBLIC ${SDB_OTEL_GENERATED_DIR})

target_link_libraries(sereneserver PUBLIC protozero)

add_dependencies(sereneserver serene_otel_schema)
