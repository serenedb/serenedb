add_library(vector_distances STATIC vector.cpp)
target_compile_options(
    vector_distances
    PRIVATE
        -fapprox-func
        -fno-math-errno
        -fassociative-math
        -freciprocal-math
        -fno-signed-zeros
        -fno-trapping-math
        -fno-rounding-math
        -ffp-contract=fast
)

target_link_libraries(vector_distances PRIVATE sereneserver)
target_link_libraries(sereneserver PRIVATE vector_distances)

target_sources(sereneserver PRIVATE search.cpp)
