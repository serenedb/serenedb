#
# Copyright (c) 2016-present, Facebook, Inc.
# All rights reserved.
#
# This source code is licensed under the MIT license found in the
# LICENSE file in the root directory of this source tree.
#

add_library(
    fasttext-static
    STATIC
    src/args.cc
    src/autotune.cc
    src/densematrix.cc
    src/dictionary.cc
    src/fasttext.cc
    src/loss.cc
    src/matrix.cc
    src/meter.cc
    src/model.cc
    src/productquantizer.cc
    src/quantmatrix.cc
    src/utils.cc
    src/vector.cc
)
target_include_directories(
    fasttext-static
    PUBLIC ${CMAKE_CURRENT_SOURCE_DIR}/src
)
target_compile_options(fasttext-static PRIVATE -funroll-loops)
