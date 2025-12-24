target_sources(
    sereneserver
    PRIVATE
        rocksdb_comparator.cpp
        rocksdb_background_thread.cpp
        rocksdb_settings_manager.cpp
        rocksdb_common.cpp
        rocksdb_key.cpp
        rocksdb_key_bounds.cpp
        rocksdb_types.cpp
        rocksdb_utils.cpp
        rocksdb_value.cpp
        rocksdb_column_family_manager.cpp
        rocksdb_engine_catalog.cpp
        rocksdb_settings_manager.cpp
        rocksdb_sync_thread.cpp
        rocksdb_wal_access.cpp
        rocksdb_recovery_manager.cpp
        rocksdb_option_feature.cpp
        options.cpp
)

## these defines are copied from rocksdb's CMakeLists.txt
##  because we are currently using some of their internal headers
##  in our build
if(CMAKE_SYSTEM_NAME MATCHES "Cygwin")
    target_compile_options(sereneserver PRIVATE -fno-builtin-memcmp -DCYGWIN)
elseif(CMAKE_SYSTEM_NAME MATCHES "Darwin")
    target_compile_options(sereneserver PRIVATE -DOS_MACOSX)
elseif(CMAKE_SYSTEM_NAME MATCHES "Linux")
    target_compile_options(sereneserver PRIVATE -DOS_LINUX)
elseif(CMAKE_SYSTEM_NAME MATCHES "SunOS")
    target_compile_options(sereneserver PRIVATE -DOS_SOLARIS)
elseif(CMAKE_SYSTEM_NAME MATCHES "FreeBSD")
    target_compile_options(sereneserver PRIVATE -DOS_FREEBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "NetBSD")
    target_compile_options(sereneserver PRIVATE -DOS_NETBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "OpenBSD")
    target_compile_options(sereneserver PRIVATE -DOS_OPENBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "DragonFly")
    target_compile_options(sereneserver PRIVATE -DOS_DRAGONFLYBSD)
elseif(CMAKE_SYSTEM_NAME MATCHES "Android")
    target_compile_options(sereneserver PRIVATE -DOS_ANDROID)
elseif(CMAKE_SYSTEM_NAME MATCHES "Windows")
    target_compile_options(
        sereneserver
        PRIVATE -DWIN32 -DOS_WIN -D_MBCS -DWIN64 -DNOMINMAX
    )
    if(MINGW)
        target_compile_options(
            sereneserver
            PRIVATE -D_WIN32_WINNT=_WIN32_WINNT_VISTA
        )
    endif()
endif()

add_subdirectory(listeners)
