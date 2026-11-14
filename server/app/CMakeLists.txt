add_library(serene_app STATIC process.cpp shell.cpp)
target_link_libraries(serene_app PUBLIC serene_docs_shell)
target_compile_definitions(
    serene_app
    PRIVATE
        SDB_REMAP_EXECUTABLE=$<IF:$<BOOL:${SDB_REMAP_EXECUTABLE}>,true,false>
)
