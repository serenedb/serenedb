add_executable(
    fuerte-tests
    connection_basic_test.cpp
    connection_concurrent_test.cpp
    connection_failures_test.cpp
    connection_timeouts_test.cpp
    #    test_10000_writes.cpp
    main.cpp
)

target_link_libraries(
    fuerte-tests
    serene
    fuerte
    gtest
    boost_boost
    ${OPENSSL_LIBRARIES}
    ${CMAKE_THREAD_LIBS_INIT}
)

target_include_directories(
    fuerte-tests
    SYSTEM
    PRIVATE ${INCLUDE_DIRECTORIES} ${CMAKE_SOURCE_DIR}/libs
)
