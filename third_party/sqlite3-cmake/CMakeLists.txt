set(SQLITE3_DIR "${CMAKE_CURRENT_SOURCE_DIR}/../sqlite3")

add_library(_sqlite3 STATIC "${SQLITE3_DIR}/sqlite3.c")

target_include_directories(_sqlite3 SYSTEM PUBLIC "${SQLITE3_DIR}")

target_compile_definitions(
    _sqlite3
    PUBLIC
        SQLITE_USE_URI=1
        SQLITE_ENABLE_COLUMN_METADATA=1
    PRIVATE
        SQLITE_TEMP_STORE=3
        SQLITE_ENABLE_MATH_FUNCTIONS
        SQLITE_THREADSAFE=1
        SQLITE_OMIT_LOAD_EXTENSION=1
)

target_link_libraries(_sqlite3 PUBLIC Threads::Threads m)

add_library(sqlite3 ALIAS _sqlite3)
add_library(SQLite::SQLite3 ALIAS _sqlite3)
add_library(unofficial::sqlite3::sqlite3 ALIAS _sqlite3)

# PROJ's build unconditionally requires a host sqlite3 shell (find_program at
# PROJ/CMakeLists.txt:174). We do not need the proj.db it would generate --
# duckdb_spatial links its own pre-generated proj_db.c -- but the check must be
# satisfied, so build the shell from the same amalgamation instead of relying on
# a system package.
add_executable(sqlite3_shell "${SQLITE3_DIR}/shell.c")
target_link_libraries(sqlite3_shell PRIVATE _sqlite3)
target_compile_definitions(sqlite3_shell PRIVATE SQLITE_OMIT_LOAD_EXTENSION=1)
set_target_properties(sqlite3_shell PROPERTIES OUTPUT_NAME sqlite3)
