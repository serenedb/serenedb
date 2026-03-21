////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2016 by EMC Corporation, All Rights Reserved
///
/// Licensed under the Apache License, Version 2.0 (the "License");
/// you may not use this file except in compliance with the License.
/// You may obtain a copy of the License at
///
///     http://www.apache.org/licenses/LICENSE-2.0
///
/// Unless required by applicable law or agreed to in writing, software
/// distributed under the License is distributed on an "AS IS" BASIS,
/// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
/// See the License for the specific language governing permissions and
/// limitations under the License.
///
/// Copyright holder is EMC Corporation
///
/// @author Andrey Abramov
/// @author Vasiliy Nabatchikov
////////////////////////////////////////////////////////////////////////////////

#include "bit_packing.hpp"

#include <cstring>

#include "basics/assert.h"

namespace {

template<int N>
void Fastpack(const uint32_t* IRS_RESTRICT in,
              uint32_t* IRS_RESTRICT out) noexcept {
  // 32 == sizeof(uint32_t) * 8
  static_assert(0 < N && N < 32);
  // ensure all computations are constexpr,
  // i.e. no conditional jumps, no loops, no variable increment/decrement
  *out |= ((*in) % (1U << N)) << (N * 0) % 32;
  if constexpr (((N * 1) % 32) < ((N * 0) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 1) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 1) % 32;
  if constexpr (((N * 2) % 32) < ((N * 1) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 2) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 2) % 32;
  if constexpr (((N * 3) % 32) < ((N * 2) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 3) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 3) % 32;
  if constexpr (((N * 4) % 32) < ((N * 3) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 4) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 4) % 32;
  if constexpr (((N * 5) % 32) < ((N * 4) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 5) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 5) % 32;
  if constexpr (((N * 6) % 32) < ((N * 5) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 6) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 6) % 32;
  if constexpr (((N * 7) % 32) < ((N * 6) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 7) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 7) % 32;
  if constexpr (((N * 8) % 32) < ((N * 7) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 8) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 8) % 32;
  if constexpr (((N * 9) % 32) < ((N * 8) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 9) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 9) % 32;
  if constexpr (((N * 10) % 32) < ((N * 9) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 10) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 10) % 32;
  if constexpr (((N * 11) % 32) < ((N * 10) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 11) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 11) % 32;
  if constexpr (((N * 12) % 32) < ((N * 11) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 12) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 12) % 32;
  if constexpr (((N * 13) % 32) < ((N * 12) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 13) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 13) % 32;
  if constexpr (((N * 14) % 32) < ((N * 13) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 14) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 14) % 32;
  if constexpr (((N * 15) % 32) < ((N * 14) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 15) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 15) % 32;
  if constexpr (((N * 16) % 32) < ((N * 15) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 16) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 16) % 32;
  if constexpr (((N * 17) % 32) < ((N * 16) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 17) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 17) % 32;
  if constexpr (((N * 18) % 32) < ((N * 17) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 18) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 18) % 32;
  if constexpr (((N * 19) % 32) < ((N * 18) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 19) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 19) % 32;
  if constexpr (((N * 20) % 32) < ((N * 19) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 20) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 20) % 32;
  if constexpr (((N * 21) % 32) < ((N * 20) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 21) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 21) % 32;
  if constexpr (((N * 22) % 32) < ((N * 21) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 22) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 22) % 32;
  if constexpr (((N * 23) % 32) < ((N * 22) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 23) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 23) % 32;
  if constexpr (((N * 24) % 32) < ((N * 23) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 24) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 24) % 32;
  if constexpr (((N * 25) % 32) < ((N * 24) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 25) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 25) % 32;
  if constexpr (((N * 26) % 32) < ((N * 25) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 26) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 26) % 32;
  if constexpr (((N * 27) % 32) < ((N * 26) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 27) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 27) % 32;
  if constexpr (((N * 28) % 32) < ((N * 27) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 28) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 28) % 32;
  if constexpr (((N * 29) % 32) < ((N * 28) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 29) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 29) % 32;
  if constexpr (((N * 30) % 32) < ((N * 29) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 30) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 30) % 32;
  if constexpr (((N * 31) % 32) < ((N * 30) % 32)) {
    ++out;
    *out |= ((*in) % (1U << N)) >> (N - ((N * 31) % 32));
  }
  ++in;
  *out |= ((*in) % (1U << N)) << (N * 31) % 32;
}

template<>
void Fastpack<32>(const uint32_t* IRS_RESTRICT in,
                  uint32_t* IRS_RESTRICT out) noexcept {
  std::memcpy(out, in, sizeof(uint32_t) * irs::packed::kBlockSize32);
}

template<int N>
void Fastpack(const uint64_t* IRS_RESTRICT in,
              uint64_t* IRS_RESTRICT out) noexcept {
  // 64 == sizeof(uint64_t) * 8
  static_assert(0 < N && N < 64);
  // ensure all computations are constexpr,
  // i.e. no conditional jumps, no loops, no variable increment/decrement
  *out |= ((*in) % (1ULL << N)) << (N * 0) % 64;
  if constexpr (((N * 1) % 64) < ((N * 0) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 1) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 1) % 64;
  if constexpr (((N * 2) % 64) < ((N * 1) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 2) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 2) % 64;
  if constexpr (((N * 3) % 64) < ((N * 2) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 3) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 3) % 64;
  if constexpr (((N * 4) % 64) < ((N * 3) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 4) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 4) % 64;
  if constexpr (((N * 5) % 64) < ((N * 4) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 5) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 5) % 64;
  if constexpr (((N * 6) % 64) < ((N * 5) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 6) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 6) % 64;
  if constexpr (((N * 7) % 64) < ((N * 6) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 7) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 7) % 64;
  if constexpr (((N * 8) % 64) < ((N * 7) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 8) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 8) % 64;
  if constexpr (((N * 9) % 64) < ((N * 8) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 9) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 9) % 64;
  if constexpr (((N * 10) % 64) < ((N * 9) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 10) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 10) % 64;
  if constexpr (((N * 11) % 64) < ((N * 10) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 11) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 11) % 64;
  if constexpr (((N * 12) % 64) < ((N * 11) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 12) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 12) % 64;
  if constexpr (((N * 13) % 64) < ((N * 12) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 13) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 13) % 64;
  if constexpr (((N * 14) % 64) < ((N * 13) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 14) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 14) % 64;
  if constexpr (((N * 15) % 64) < ((N * 14) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 15) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 15) % 64;
  if constexpr (((N * 16) % 64) < ((N * 15) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 16) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 16) % 64;
  if constexpr (((N * 17) % 64) < ((N * 16) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 17) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 17) % 64;
  if constexpr (((N * 18) % 64) < ((N * 17) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 18) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 18) % 64;
  if constexpr (((N * 19) % 64) < ((N * 18) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 19) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 19) % 64;
  if constexpr (((N * 20) % 64) < ((N * 19) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 20) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 20) % 64;
  if constexpr (((N * 21) % 64) < ((N * 20) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 21) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 21) % 64;
  if constexpr (((N * 22) % 64) < ((N * 21) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 22) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 22) % 64;
  if constexpr (((N * 23) % 64) < ((N * 22) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 23) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 23) % 64;
  if constexpr (((N * 24) % 64) < ((N * 23) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 24) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 24) % 64;
  if constexpr (((N * 25) % 64) < ((N * 24) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 25) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 25) % 64;
  if constexpr (((N * 26) % 64) < ((N * 25) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 26) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 26) % 64;
  if constexpr (((N * 27) % 64) < ((N * 26) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 27) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 27) % 64;
  if constexpr (((N * 28) % 64) < ((N * 27) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 28) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 28) % 64;
  if constexpr (((N * 29) % 64) < ((N * 28) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 29) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 29) % 64;
  if constexpr (((N * 30) % 64) < ((N * 29) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 30) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 30) % 64;
  if constexpr (((N * 31) % 64) < ((N * 30) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 31) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 31) % 64;
  if constexpr (((N * 32) % 64) < ((N * 31) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 32) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 32) % 64;
  if constexpr (((N * 33) % 64) < ((N * 32) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 33) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 33) % 64;
  if constexpr (((N * 34) % 64) < ((N * 33) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 34) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 34) % 64;
  if constexpr (((N * 35) % 64) < ((N * 34) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 35) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 35) % 64;
  if constexpr (((N * 36) % 64) < ((N * 35) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 36) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 36) % 64;
  if constexpr (((N * 37) % 64) < ((N * 36) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 37) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 37) % 64;
  if constexpr (((N * 38) % 64) < ((N * 37) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 38) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 38) % 64;
  if constexpr (((N * 39) % 64) < ((N * 38) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 39) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 39) % 64;
  if constexpr (((N * 40) % 64) < ((N * 39) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 40) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 40) % 64;
  if constexpr (((N * 41) % 64) < ((N * 40) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 41) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 41) % 64;
  if constexpr (((N * 42) % 64) < ((N * 41) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 42) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 42) % 64;
  if constexpr (((N * 43) % 64) < ((N * 42) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 43) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 43) % 64;
  if constexpr (((N * 44) % 64) < ((N * 43) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 44) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 44) % 64;
  if constexpr (((N * 45) % 64) < ((N * 44) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 45) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 45) % 64;
  if constexpr (((N * 46) % 64) < ((N * 45) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 46) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 46) % 64;
  if constexpr (((N * 47) % 64) < ((N * 46) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 47) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 47) % 64;
  if constexpr (((N * 48) % 64) < ((N * 47) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 48) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 48) % 64;
  if constexpr (((N * 49) % 64) < ((N * 48) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 49) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 49) % 64;
  if constexpr (((N * 50) % 64) < ((N * 49) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 50) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 50) % 64;
  if constexpr (((N * 51) % 64) < ((N * 50) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 51) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 51) % 64;
  if constexpr (((N * 52) % 64) < ((N * 51) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 52) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 52) % 64;
  if constexpr (((N * 53) % 64) < ((N * 52) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 53) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 53) % 64;
  if constexpr (((N * 54) % 64) < ((N * 53) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 54) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 54) % 64;
  if constexpr (((N * 55) % 64) < ((N * 54) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 55) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 55) % 64;
  if constexpr (((N * 56) % 64) < ((N * 55) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 56) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 56) % 64;
  if constexpr (((N * 57) % 64) < ((N * 56) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 57) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 57) % 64;
  if constexpr (((N * 58) % 64) < ((N * 57) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 58) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 58) % 64;
  if constexpr (((N * 59) % 64) < ((N * 58) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 59) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 59) % 64;
  if constexpr (((N * 60) % 64) < ((N * 59) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 60) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 60) % 64;
  if constexpr (((N * 61) % 64) < ((N * 60) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 61) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 61) % 64;
  if constexpr (((N * 62) % 64) < ((N * 61) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 62) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 62) % 64;
  if constexpr (((N * 63) % 64) < ((N * 62) % 64)) {
    ++out;
    *out |= ((*in) % (1ULL << N)) >> (N - ((N * 63) % 64));
  }
  ++in;
  *out |= ((*in) % (1ULL << N)) << (N * 63) % 64;
}

template<>
IRS_FORCE_INLINE void Fastpack<64>(const uint64_t* IRS_RESTRICT in,
                                   uint64_t* IRS_RESTRICT out) noexcept {
  std::memcpy(out, in, sizeof(uint64_t) * irs::packed::kBlockSize64);
}

template<int N>
void Fastunpack(const uint32_t* IRS_RESTRICT in,
                uint32_t* IRS_RESTRICT out) noexcept {
  // 32 == sizeof(uint32_t) * 8
  static_assert(0 < N && N < 32);
  *out = ((*in) >> (N * 0) % 32) % (1U << N);
  if constexpr (((N * 1) % 32) < ((N * 0) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 1) % 32)) << (N - ((N * 1) % 32));
  }
  out++;
  *out = ((*in) >> (N * 1) % 32) % (1U << N);
  if constexpr (((N * 2) % 32) < ((N * 1) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 2) % 32)) << (N - ((N * 2) % 32));
  }
  out++;
  *out = ((*in) >> (N * 2) % 32) % (1U << N);
  if constexpr (((N * 3) % 32) < ((N * 2) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 3) % 32)) << (N - ((N * 3) % 32));
  }
  out++;
  *out = ((*in) >> (N * 3) % 32) % (1U << N);
  if constexpr (((N * 4) % 32) < ((N * 3) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 4) % 32)) << (N - ((N * 4) % 32));
  }
  out++;
  *out = ((*in) >> (N * 4) % 32) % (1U << N);
  if constexpr (((N * 5) % 32) < ((N * 4) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 5) % 32)) << (N - ((N * 5) % 32));
  }
  out++;
  *out = ((*in) >> (N * 5) % 32) % (1U << N);
  if constexpr (((N * 6) % 32) < ((N * 5) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 6) % 32)) << (N - ((N * 6) % 32));
  }
  out++;
  *out = ((*in) >> (N * 6) % 32) % (1U << N);
  if constexpr (((N * 7) % 32) < ((N * 6) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 7) % 32)) << (N - ((N * 7) % 32));
  }
  out++;
  *out = ((*in) >> (N * 7) % 32) % (1U << N);
  if constexpr (((N * 8) % 32) < ((N * 7) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 8) % 32)) << (N - ((N * 8) % 32));
  }
  out++;
  *out = ((*in) >> (N * 8) % 32) % (1U << N);
  if constexpr (((N * 9) % 32) < ((N * 8) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 9) % 32)) << (N - ((N * 9) % 32));
  }
  out++;
  *out = ((*in) >> (N * 9) % 32) % (1U << N);
  if constexpr (((N * 10) % 32) < ((N * 9) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 10) % 32)) << (N - ((N * 10) % 32));
  }
  out++;
  *out = ((*in) >> (N * 10) % 32) % (1U << N);
  if constexpr (((N * 11) % 32) < ((N * 10) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 11) % 32)) << (N - ((N * 11) % 32));
  }
  out++;
  *out = ((*in) >> (N * 11) % 32) % (1U << N);
  if constexpr (((N * 12) % 32) < ((N * 11) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 12) % 32)) << (N - ((N * 12) % 32));
  }
  out++;
  *out = ((*in) >> (N * 12) % 32) % (1U << N);
  if constexpr (((N * 13) % 32) < ((N * 12) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 13) % 32)) << (N - ((N * 13) % 32));
  }
  out++;
  *out = ((*in) >> (N * 13) % 32) % (1U << N);
  if constexpr (((N * 14) % 32) < ((N * 13) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 14) % 32)) << (N - ((N * 14) % 32));
  }
  out++;
  *out = ((*in) >> (N * 14) % 32) % (1U << N);
  if constexpr (((N * 15) % 32) < ((N * 14) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 15) % 32)) << (N - ((N * 15) % 32));
  }
  out++;
  *out = ((*in) >> (N * 15) % 32) % (1U << N);
  if constexpr (((N * 16) % 32) < ((N * 15) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 16) % 32)) << (N - ((N * 16) % 32));
  }
  out++;
  *out = ((*in) >> (N * 16) % 32) % (1U << N);
  if constexpr (((N * 17) % 32) < ((N * 16) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 17) % 32)) << (N - ((N * 17) % 32));
  }
  out++;
  *out = ((*in) >> (N * 17) % 32) % (1U << N);
  if constexpr (((N * 18) % 32) < ((N * 17) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 18) % 32)) << (N - ((N * 18) % 32));
  }
  out++;
  *out = ((*in) >> (N * 18) % 32) % (1U << N);
  if constexpr (((N * 19) % 32) < ((N * 18) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 19) % 32)) << (N - ((N * 19) % 32));
  }
  out++;
  *out = ((*in) >> (N * 19) % 32) % (1U << N);
  if constexpr (((N * 20) % 32) < ((N * 19) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 20) % 32)) << (N - ((N * 20) % 32));
  }
  out++;
  *out = ((*in) >> (N * 20) % 32) % (1U << N);
  if constexpr (((N * 21) % 32) < ((N * 20) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 21) % 32)) << (N - ((N * 21) % 32));
  }
  out++;
  *out = ((*in) >> (N * 21) % 32) % (1U << N);
  if constexpr (((N * 22) % 32) < ((N * 21) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 22) % 32)) << (N - ((N * 22) % 32));
  }
  out++;
  *out = ((*in) >> (N * 22) % 32) % (1U << N);
  if constexpr (((N * 23) % 32) < ((N * 22) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 23) % 32)) << (N - ((N * 23) % 32));
  }
  out++;
  *out = ((*in) >> (N * 23) % 32) % (1U << N);
  if constexpr (((N * 24) % 32) < ((N * 23) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 24) % 32)) << (N - ((N * 24) % 32));
  }
  out++;
  *out = ((*in) >> (N * 24) % 32) % (1U << N);
  if constexpr (((N * 25) % 32) < ((N * 24) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 25) % 32)) << (N - ((N * 25) % 32));
  }
  out++;
  *out = ((*in) >> (N * 25) % 32) % (1U << N);
  if constexpr (((N * 26) % 32) < ((N * 25) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 26) % 32)) << (N - ((N * 26) % 32));
  }
  out++;
  *out = ((*in) >> (N * 26) % 32) % (1U << N);
  if constexpr (((N * 27) % 32) < ((N * 26) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 27) % 32)) << (N - ((N * 27) % 32));
  }
  out++;
  *out = ((*in) >> (N * 27) % 32) % (1U << N);
  if constexpr (((N * 28) % 32) < ((N * 27) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 28) % 32)) << (N - ((N * 28) % 32));
  }
  out++;
  *out = ((*in) >> (N * 28) % 32) % (1U << N);
  if constexpr (((N * 29) % 32) < ((N * 28) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 29) % 32)) << (N - ((N * 29) % 32));
  }
  out++;
  *out = ((*in) >> (N * 29) % 32) % (1U << N);
  if constexpr (((N * 30) % 32) < ((N * 29) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 30) % 32)) << (N - ((N * 30) % 32));
  }
  out++;
  *out = ((*in) >> (N * 30) % 32) % (1U << N);
  if constexpr (((N * 31) % 32) < ((N * 30) % 32)) {
    ++in;
    *out |= ((*in) % (1U << (N * 31) % 32)) << (N - ((N * 31) % 32));
  }
  out++;
  *out = ((*in) >> (N * 31) % 32) % (1U << N);
}

template<>
IRS_FORCE_INLINE void Fastunpack<32>(const uint32_t* IRS_RESTRICT in,
                                     uint32_t* IRS_RESTRICT out) noexcept {
  std::memcpy(out, in, sizeof(uint32_t) * irs::packed::kBlockSize32);
}

template<int N>
void Fastunpack(const uint64_t* IRS_RESTRICT in,
                uint64_t* IRS_RESTRICT out) noexcept {
  // 64 == sizeof(uint32_t) * 8
  static_assert(0 < N && N < 64);
  *out = ((*in) >> (N * 0) % 64) % (1ULL << N);
  if constexpr (((N * 1) % 64) < ((N * 0) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 1) % 64)) << (N - ((N * 1) % 64));
  }
  out++;
  *out = ((*in) >> (N * 1) % 64) % (1ULL << N);
  if constexpr (((N * 2) % 64) < ((N * 1) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 2) % 64)) << (N - ((N * 2) % 64));
  }
  out++;
  *out = ((*in) >> (N * 2) % 64) % (1ULL << N);
  if constexpr (((N * 3) % 64) < ((N * 2) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 3) % 64)) << (N - ((N * 3) % 64));
  }
  out++;
  *out = ((*in) >> (N * 3) % 64) % (1ULL << N);
  if constexpr (((N * 4) % 64) < ((N * 3) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 4) % 64)) << (N - ((N * 4) % 64));
  }
  out++;
  *out = ((*in) >> (N * 4) % 64) % (1ULL << N);
  if constexpr (((N * 5) % 64) < ((N * 4) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 5) % 64)) << (N - ((N * 5) % 64));
  }
  out++;
  *out = ((*in) >> (N * 5) % 64) % (1ULL << N);
  if constexpr (((N * 6) % 64) < ((N * 5) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 6) % 64)) << (N - ((N * 6) % 64));
  }
  out++;
  *out = ((*in) >> (N * 6) % 64) % (1ULL << N);
  if constexpr (((N * 7) % 64) < ((N * 6) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 7) % 64)) << (N - ((N * 7) % 64));
  }
  out++;
  *out = ((*in) >> (N * 7) % 64) % (1ULL << N);
  if constexpr (((N * 8) % 64) < ((N * 7) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 8) % 64)) << (N - ((N * 8) % 64));
  }
  out++;
  *out = ((*in) >> (N * 8) % 64) % (1ULL << N);
  if constexpr (((N * 9) % 64) < ((N * 8) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 9) % 64)) << (N - ((N * 9) % 64));
  }
  out++;
  *out = ((*in) >> (N * 9) % 64) % (1ULL << N);
  if constexpr (((N * 10) % 64) < ((N * 9) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 10) % 64)) << (N - ((N * 10) % 64));
  }
  out++;
  *out = ((*in) >> (N * 10) % 64) % (1ULL << N);
  if constexpr (((N * 11) % 64) < ((N * 10) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 11) % 64)) << (N - ((N * 11) % 64));
  }
  out++;
  *out = ((*in) >> (N * 11) % 64) % (1ULL << N);
  if constexpr (((N * 12) % 64) < ((N * 11) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 12) % 64)) << (N - ((N * 12) % 64));
  }
  out++;
  *out = ((*in) >> (N * 12) % 64) % (1ULL << N);
  if constexpr (((N * 13) % 64) < ((N * 12) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 13) % 64)) << (N - ((N * 13) % 64));
  }
  out++;
  *out = ((*in) >> (N * 13) % 64) % (1ULL << N);
  if constexpr (((N * 14) % 64) < ((N * 13) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 14) % 64)) << (N - ((N * 14) % 64));
  }
  out++;
  *out = ((*in) >> (N * 14) % 64) % (1ULL << N);
  if constexpr (((N * 15) % 64) < ((N * 14) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 15) % 64)) << (N - ((N * 15) % 64));
  }
  out++;
  *out = ((*in) >> (N * 15) % 64) % (1ULL << N);
  if constexpr (((N * 16) % 64) < ((N * 15) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 16) % 64)) << (N - ((N * 16) % 64));
  }
  out++;
  *out = ((*in) >> (N * 16) % 64) % (1ULL << N);
  if constexpr (((N * 17) % 64) < ((N * 16) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 17) % 64)) << (N - ((N * 17) % 64));
  }
  out++;
  *out = ((*in) >> (N * 17) % 64) % (1ULL << N);
  if constexpr (((N * 18) % 64) < ((N * 17) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 18) % 64)) << (N - ((N * 18) % 64));
  }
  out++;
  *out = ((*in) >> (N * 18) % 64) % (1ULL << N);
  if constexpr (((N * 19) % 64) < ((N * 18) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 19) % 64)) << (N - ((N * 19) % 64));
  }
  out++;
  *out = ((*in) >> (N * 19) % 64) % (1ULL << N);
  if constexpr (((N * 20) % 64) < ((N * 19) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 20) % 64)) << (N - ((N * 20) % 64));
  }
  out++;
  *out = ((*in) >> (N * 20) % 64) % (1ULL << N);
  if constexpr (((N * 21) % 64) < ((N * 20) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 21) % 64)) << (N - ((N * 21) % 64));
  }
  out++;
  *out = ((*in) >> (N * 21) % 64) % (1ULL << N);
  if constexpr (((N * 22) % 64) < ((N * 21) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 22) % 64)) << (N - ((N * 22) % 64));
  }
  out++;
  *out = ((*in) >> (N * 22) % 64) % (1ULL << N);
  if constexpr (((N * 23) % 64) < ((N * 22) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 23) % 64)) << (N - ((N * 23) % 64));
  }
  out++;
  *out = ((*in) >> (N * 23) % 64) % (1ULL << N);
  if constexpr (((N * 24) % 64) < ((N * 23) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 24) % 64)) << (N - ((N * 24) % 64));
  }
  out++;
  *out = ((*in) >> (N * 24) % 64) % (1ULL << N);
  if constexpr (((N * 25) % 64) < ((N * 24) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 25) % 64)) << (N - ((N * 25) % 64));
  }
  out++;
  *out = ((*in) >> (N * 25) % 64) % (1ULL << N);
  if constexpr (((N * 26) % 64) < ((N * 25) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 26) % 64)) << (N - ((N * 26) % 64));
  }
  out++;
  *out = ((*in) >> (N * 26) % 64) % (1ULL << N);
  if constexpr (((N * 27) % 64) < ((N * 26) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 27) % 64)) << (N - ((N * 27) % 64));
  }
  out++;
  *out = ((*in) >> (N * 27) % 64) % (1ULL << N);
  if constexpr (((N * 28) % 64) < ((N * 27) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 28) % 64)) << (N - ((N * 28) % 64));
  }
  out++;
  *out = ((*in) >> (N * 28) % 64) % (1ULL << N);
  if constexpr (((N * 29) % 64) < ((N * 28) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 29) % 64)) << (N - ((N * 29) % 64));
  }
  out++;
  *out = ((*in) >> (N * 29) % 64) % (1ULL << N);
  if constexpr (((N * 30) % 64) < ((N * 29) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 30) % 64)) << (N - ((N * 30) % 64));
  }
  out++;
  *out = ((*in) >> (N * 30) % 64) % (1ULL << N);
  if constexpr (((N * 31) % 64) < ((N * 30) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 31) % 64)) << (N - ((N * 31) % 64));
  }
  out++;
  *out = ((*in) >> (N * 31) % 64) % (1ULL << N);
  if constexpr (((N * 32) % 64) < ((N * 31) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 32) % 64)) << (N - ((N * 32) % 64));
  }
  out++;
  *out = ((*in) >> (N * 32) % 64) % (1ULL << N);
  if constexpr (((N * 33) % 64) < ((N * 32) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 33) % 64)) << (N - ((N * 33) % 64));
  }
  out++;
  *out = ((*in) >> (N * 33) % 64) % (1ULL << N);
  if constexpr (((N * 34) % 64) < ((N * 33) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 34) % 64)) << (N - ((N * 34) % 64));
  }
  out++;
  *out = ((*in) >> (N * 34) % 64) % (1ULL << N);
  if constexpr (((N * 35) % 64) < ((N * 34) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 35) % 64)) << (N - ((N * 35) % 64));
  }
  out++;
  *out = ((*in) >> (N * 35) % 64) % (1ULL << N);
  if constexpr (((N * 36) % 64) < ((N * 35) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 36) % 64)) << (N - ((N * 36) % 64));
  }
  out++;
  *out = ((*in) >> (N * 36) % 64) % (1ULL << N);
  if constexpr (((N * 37) % 64) < ((N * 36) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 37) % 64)) << (N - ((N * 37) % 64));
  }
  out++;
  *out = ((*in) >> (N * 37) % 64) % (1ULL << N);
  if constexpr (((N * 38) % 64) < ((N * 37) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 38) % 64)) << (N - ((N * 38) % 64));
  }
  out++;
  *out = ((*in) >> (N * 38) % 64) % (1ULL << N);
  if constexpr (((N * 39) % 64) < ((N * 38) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 39) % 64)) << (N - ((N * 39) % 64));
  }
  out++;
  *out = ((*in) >> (N * 39) % 64) % (1ULL << N);
  if constexpr (((N * 40) % 64) < ((N * 39) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 40) % 64)) << (N - ((N * 40) % 64));
  }
  out++;
  *out = ((*in) >> (N * 40) % 64) % (1ULL << N);
  if constexpr (((N * 41) % 64) < ((N * 40) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 41) % 64)) << (N - ((N * 41) % 64));
  }
  out++;
  *out = ((*in) >> (N * 41) % 64) % (1ULL << N);
  if constexpr (((N * 42) % 64) < ((N * 41) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 42) % 64)) << (N - ((N * 42) % 64));
  }
  out++;
  *out = ((*in) >> (N * 42) % 64) % (1ULL << N);
  if constexpr (((N * 43) % 64) < ((N * 42) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 43) % 64)) << (N - ((N * 43) % 64));
  }
  out++;
  *out = ((*in) >> (N * 43) % 64) % (1ULL << N);
  if constexpr (((N * 44) % 64) < ((N * 43) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 44) % 64)) << (N - ((N * 44) % 64));
  }
  out++;
  *out = ((*in) >> (N * 44) % 64) % (1ULL << N);
  if constexpr (((N * 45) % 64) < ((N * 44) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 45) % 64)) << (N - ((N * 45) % 64));
  }
  out++;
  *out = ((*in) >> (N * 45) % 64) % (1ULL << N);
  if constexpr (((N * 46) % 64) < ((N * 45) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 46) % 64)) << (N - ((N * 46) % 64));
  }
  out++;
  *out = ((*in) >> (N * 46) % 64) % (1ULL << N);
  if constexpr (((N * 47) % 64) < ((N * 46) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 47) % 64)) << (N - ((N * 47) % 64));
  }
  out++;
  *out = ((*in) >> (N * 47) % 64) % (1ULL << N);
  if constexpr (((N * 48) % 64) < ((N * 47) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 48) % 64)) << (N - ((N * 48) % 64));
  }
  out++;
  *out = ((*in) >> (N * 48) % 64) % (1ULL << N);
  if constexpr (((N * 49) % 64) < ((N * 48) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 49) % 64)) << (N - ((N * 49) % 64));
  }
  out++;
  *out = ((*in) >> (N * 49) % 64) % (1ULL << N);
  if constexpr (((N * 50) % 64) < ((N * 49) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 50) % 64)) << (N - ((N * 50) % 64));
  }
  out++;
  *out = ((*in) >> (N * 50) % 64) % (1ULL << N);
  if constexpr (((N * 51) % 64) < ((N * 50) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 51) % 64)) << (N - ((N * 51) % 64));
  }
  out++;
  *out = ((*in) >> (N * 51) % 64) % (1ULL << N);
  if constexpr (((N * 52) % 64) < ((N * 51) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 52) % 64)) << (N - ((N * 52) % 64));
  }
  out++;
  *out = ((*in) >> (N * 52) % 64) % (1ULL << N);
  if constexpr (((N * 53) % 64) < ((N * 52) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 53) % 64)) << (N - ((N * 53) % 64));
  }
  out++;
  *out = ((*in) >> (N * 53) % 64) % (1ULL << N);
  if constexpr (((N * 54) % 64) < ((N * 53) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 54) % 64)) << (N - ((N * 54) % 64));
  }
  out++;
  *out = ((*in) >> (N * 54) % 64) % (1ULL << N);
  if constexpr (((N * 55) % 64) < ((N * 54) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 55) % 64)) << (N - ((N * 55) % 64));
  }
  out++;
  *out = ((*in) >> (N * 55) % 64) % (1ULL << N);
  if constexpr (((N * 56) % 64) < ((N * 55) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 56) % 64)) << (N - ((N * 56) % 64));
  }
  out++;
  *out = ((*in) >> (N * 56) % 64) % (1ULL << N);
  if constexpr (((N * 57) % 64) < ((N * 56) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 57) % 64)) << (N - ((N * 57) % 64));
  }
  out++;
  *out = ((*in) >> (N * 57) % 64) % (1ULL << N);
  if constexpr (((N * 58) % 64) < ((N * 57) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 58) % 64)) << (N - ((N * 58) % 64));
  }
  out++;
  *out = ((*in) >> (N * 58) % 64) % (1ULL << N);
  if constexpr (((N * 59) % 64) < ((N * 58) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 59) % 64)) << (N - ((N * 59) % 64));
  }
  out++;
  *out = ((*in) >> (N * 59) % 64) % (1ULL << N);
  if constexpr (((N * 60) % 64) < ((N * 59) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 60) % 64)) << (N - ((N * 60) % 64));
  }
  out++;
  *out = ((*in) >> (N * 60) % 64) % (1ULL << N);
  if constexpr (((N * 61) % 64) < ((N * 60) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 61) % 64)) << (N - ((N * 61) % 64));
  }
  out++;
  *out = ((*in) >> (N * 61) % 64) % (1ULL << N);
  if constexpr (((N * 62) % 64) < ((N * 61) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 62) % 64)) << (N - ((N * 62) % 64));
  }
  out++;
  *out = ((*in) >> (N * 62) % 64) % (1ULL << N);
  if constexpr (((N * 63) % 64) < ((N * 62) % 64)) {
    ++in;
    *out |= ((*in) % (1ULL << (N * 63) % 64)) << (N - ((N * 63) % 64));
  }
  out++;
  *out = ((*in) >> (N * 63) % 64) % (1ULL << N);
}

template<>
IRS_FORCE_INLINE void Fastunpack<64>(const uint64_t* IRS_RESTRICT in,
                                     uint64_t* IRS_RESTRICT out) noexcept {
  std::memcpy(out, in, sizeof(uint64_t) * irs::packed::kBlockSize64);
}

template<int N, int I, typename T>
IRS_FORCE_INLINE T FastpackAt(const T* in) noexcept {
  static_assert(std::is_same_v<T, uint32_t> || std::is_same_v<T, uint64_t>);
  static constexpr int kBits = sizeof(T) * 8;
  static_assert(0 < N && N < kBits);
  static_assert(0 <= I && I < kBits);

  // ensure all computations are constexpr,
  // i.e. no conditional jumps, no loops, no variable increment/decrement

  // TODO(mbkkt) misalignment access
  if constexpr ((N * (I + 1) % kBits) < (N * I) % kBits &&
                (1 + N * I / kBits) < N) {
    return ((in[N * I / kBits] >> (N * I % kBits)) % (T{1} << N)) |
           ((in[1 + N * I / kBits] % (T{1} << (N * (I + 1)) % kBits))
            << (N - ((N * (I + 1)) % kBits)));
  } else {
    return ((in[N * I / kBits] >> (N * I % kBits)) % (T{1} << N));
  }
}

template<int N>
uint32_t FastpackAt(const uint32_t* in, const size_t i) noexcept {
  SDB_ASSERT(i < irs::packed::kBlockSize32);

  switch (i) {
    case 0:
      return FastpackAt<N, 0>(in);
    case 1:
      return FastpackAt<N, 1>(in);
    case 2:
      return FastpackAt<N, 2>(in);
    case 3:
      return FastpackAt<N, 3>(in);
    case 4:
      return FastpackAt<N, 4>(in);
    case 5:
      return FastpackAt<N, 5>(in);
    case 6:
      return FastpackAt<N, 6>(in);
    case 7:
      return FastpackAt<N, 7>(in);
    case 8:
      return FastpackAt<N, 8>(in);
    case 9:
      return FastpackAt<N, 9>(in);
    case 10:
      return FastpackAt<N, 10>(in);
    case 11:
      return FastpackAt<N, 11>(in);
    case 12:
      return FastpackAt<N, 12>(in);
    case 13:
      return FastpackAt<N, 13>(in);
    case 14:
      return FastpackAt<N, 14>(in);
    case 15:
      return FastpackAt<N, 15>(in);
    case 16:
      return FastpackAt<N, 16>(in);
    case 17:
      return FastpackAt<N, 17>(in);
    case 18:
      return FastpackAt<N, 18>(in);
    case 19:
      return FastpackAt<N, 19>(in);
    case 20:
      return FastpackAt<N, 20>(in);
    case 21:
      return FastpackAt<N, 21>(in);
    case 22:
      return FastpackAt<N, 22>(in);
    case 23:
      return FastpackAt<N, 23>(in);
    case 24:
      return FastpackAt<N, 24>(in);
    case 25:
      return FastpackAt<N, 25>(in);
    case 26:
      return FastpackAt<N, 26>(in);
    case 27:
      return FastpackAt<N, 27>(in);
    case 28:
      return FastpackAt<N, 28>(in);
    case 29:
      return FastpackAt<N, 29>(in);
    case 30:
      return FastpackAt<N, 30>(in);
    case 31:
      return FastpackAt<N, 31>(in);
    default:
      SDB_ASSERT(false);
      return 0;  // this should never be hit, or algorithm error
  }
}

template<>
uint32_t FastpackAt<32>(const uint32_t* in, const size_t i) noexcept {
  // 32 == sizeof(uint32_t) * 8
  SDB_ASSERT(i < 32);
  return in[i];
}

template<int N>
uint64_t FastpackAt(const uint64_t* in, const size_t i) noexcept {
  SDB_ASSERT(i < irs::packed::kBlockSize64);

  switch (i) {
    case 0:
      return FastpackAt<N, 0>(in);
    case 1:
      return FastpackAt<N, 1>(in);
    case 2:
      return FastpackAt<N, 2>(in);
    case 3:
      return FastpackAt<N, 3>(in);
    case 4:
      return FastpackAt<N, 4>(in);
    case 5:
      return FastpackAt<N, 5>(in);
    case 6:
      return FastpackAt<N, 6>(in);
    case 7:
      return FastpackAt<N, 7>(in);
    case 8:
      return FastpackAt<N, 8>(in);
    case 9:
      return FastpackAt<N, 9>(in);
    case 10:
      return FastpackAt<N, 10>(in);
    case 11:
      return FastpackAt<N, 11>(in);
    case 12:
      return FastpackAt<N, 12>(in);
    case 13:
      return FastpackAt<N, 13>(in);
    case 14:
      return FastpackAt<N, 14>(in);
    case 15:
      return FastpackAt<N, 15>(in);
    case 16:
      return FastpackAt<N, 16>(in);
    case 17:
      return FastpackAt<N, 17>(in);
    case 18:
      return FastpackAt<N, 18>(in);
    case 19:
      return FastpackAt<N, 19>(in);
    case 20:
      return FastpackAt<N, 20>(in);
    case 21:
      return FastpackAt<N, 21>(in);
    case 22:
      return FastpackAt<N, 22>(in);
    case 23:
      return FastpackAt<N, 23>(in);
    case 24:
      return FastpackAt<N, 24>(in);
    case 25:
      return FastpackAt<N, 25>(in);
    case 26:
      return FastpackAt<N, 26>(in);
    case 27:
      return FastpackAt<N, 27>(in);
    case 28:
      return FastpackAt<N, 28>(in);
    case 29:
      return FastpackAt<N, 29>(in);
    case 30:
      return FastpackAt<N, 30>(in);
    case 31:
      return FastpackAt<N, 31>(in);
    case 32:
      return FastpackAt<N, 32>(in);
    case 33:
      return FastpackAt<N, 33>(in);
    case 34:
      return FastpackAt<N, 34>(in);
    case 35:
      return FastpackAt<N, 35>(in);
    case 36:
      return FastpackAt<N, 36>(in);
    case 37:
      return FastpackAt<N, 37>(in);
    case 38:
      return FastpackAt<N, 38>(in);
    case 39:
      return FastpackAt<N, 39>(in);
    case 40:
      return FastpackAt<N, 40>(in);
    case 41:
      return FastpackAt<N, 41>(in);
    case 42:
      return FastpackAt<N, 42>(in);
    case 43:
      return FastpackAt<N, 43>(in);
    case 44:
      return FastpackAt<N, 44>(in);
    case 45:
      return FastpackAt<N, 45>(in);
    case 46:
      return FastpackAt<N, 46>(in);
    case 47:
      return FastpackAt<N, 47>(in);
    case 48:
      return FastpackAt<N, 48>(in);
    case 49:
      return FastpackAt<N, 49>(in);
    case 50:
      return FastpackAt<N, 50>(in);
    case 51:
      return FastpackAt<N, 51>(in);
    case 52:
      return FastpackAt<N, 52>(in);
    case 53:
      return FastpackAt<N, 53>(in);
    case 54:
      return FastpackAt<N, 54>(in);
    case 55:
      return FastpackAt<N, 55>(in);
    case 56:
      return FastpackAt<N, 56>(in);
    case 57:
      return FastpackAt<N, 57>(in);
    case 58:
      return FastpackAt<N, 58>(in);
    case 59:
      return FastpackAt<N, 59>(in);
    case 60:
      return FastpackAt<N, 60>(in);
    case 61:
      return FastpackAt<N, 61>(in);
    case 62:
      return FastpackAt<N, 62>(in);
    case 63:
      return FastpackAt<N, 63>(in);
    default:
      SDB_ASSERT(false);
      return 0;  // this should never be hit, or algorithm error
  }
}

template<>
uint64_t FastpackAt<64>(const uint64_t* in, const size_t i) noexcept {
  // 64 == sizeof(uint64_t) * 8
  SDB_ASSERT(i < 64);
  return in[i];
}

}  // namespace
namespace irs::packed {

void PackBlock(const uint32_t* IRS_RESTRICT in, uint32_t* IRS_RESTRICT out,
               const uint32_t bit) noexcept {
  switch (bit) {
    case 1:
      Fastpack<1>(in, out);
      break;
    case 2:
      Fastpack<2>(in, out);
      break;
    case 3:
      Fastpack<3>(in, out);
      break;
    case 4:
      Fastpack<4>(in, out);
      break;
    case 5:
      Fastpack<5>(in, out);
      break;
    case 6:
      Fastpack<6>(in, out);
      break;
    case 7:
      Fastpack<7>(in, out);
      break;
    case 8:
      Fastpack<8>(in, out);
      break;
    case 9:
      Fastpack<9>(in, out);
      break;
    case 10:
      Fastpack<10>(in, out);
      break;
    case 11:
      Fastpack<11>(in, out);
      break;
    case 12:
      Fastpack<12>(in, out);
      break;
    case 13:
      Fastpack<13>(in, out);
      break;
    case 14:
      Fastpack<14>(in, out);
      break;
    case 15:
      Fastpack<15>(in, out);
      break;
    case 16:
      Fastpack<16>(in, out);
      break;
    case 17:
      Fastpack<17>(in, out);
      break;
    case 18:
      Fastpack<18>(in, out);
      break;
    case 19:
      Fastpack<19>(in, out);
      break;
    case 20:
      Fastpack<20>(in, out);
      break;
    case 21:
      Fastpack<21>(in, out);
      break;
    case 22:
      Fastpack<22>(in, out);
      break;
    case 23:
      Fastpack<23>(in, out);
      break;
    case 24:
      Fastpack<24>(in, out);
      break;
    case 25:
      Fastpack<25>(in, out);
      break;
    case 26:
      Fastpack<26>(in, out);
      break;
    case 27:
      Fastpack<27>(in, out);
      break;
    case 28:
      Fastpack<28>(in, out);
      break;
    case 29:
      Fastpack<29>(in, out);
      break;
    case 30:
      Fastpack<30>(in, out);
      break;
    case 31:
      Fastpack<31>(in, out);
      break;
    case 32:
      Fastpack<32>(in, out);
      break;
    default:
      SDB_ASSERT(false);
      break;
  }
}

void PackBlock(const uint64_t* IRS_RESTRICT in, uint64_t* IRS_RESTRICT out,
               const uint32_t bit) noexcept {
  switch (bit) {
    case 1:
      Fastpack<1>(in, out);
      break;
    case 2:
      Fastpack<2>(in, out);
      break;
    case 3:
      Fastpack<3>(in, out);
      break;
    case 4:
      Fastpack<4>(in, out);
      break;
    case 5:
      Fastpack<5>(in, out);
      break;
    case 6:
      Fastpack<6>(in, out);
      break;
    case 7:
      Fastpack<7>(in, out);
      break;
    case 8:
      Fastpack<8>(in, out);
      break;
    case 9:
      Fastpack<9>(in, out);
      break;
    case 10:
      Fastpack<10>(in, out);
      break;
    case 11:
      Fastpack<11>(in, out);
      break;
    case 12:
      Fastpack<12>(in, out);
      break;
    case 13:
      Fastpack<13>(in, out);
      break;
    case 14:
      Fastpack<14>(in, out);
      break;
    case 15:
      Fastpack<15>(in, out);
      break;
    case 16:
      Fastpack<16>(in, out);
      break;
    case 17:
      Fastpack<17>(in, out);
      break;
    case 18:
      Fastpack<18>(in, out);
      break;
    case 19:
      Fastpack<19>(in, out);
      break;
    case 20:
      Fastpack<20>(in, out);
      break;
    case 21:
      Fastpack<21>(in, out);
      break;
    case 22:
      Fastpack<22>(in, out);
      break;
    case 23:
      Fastpack<23>(in, out);
      break;
    case 24:
      Fastpack<24>(in, out);
      break;
    case 25:
      Fastpack<25>(in, out);
      break;
    case 26:
      Fastpack<26>(in, out);
      break;
    case 27:
      Fastpack<27>(in, out);
      break;
    case 28:
      Fastpack<28>(in, out);
      break;
    case 29:
      Fastpack<29>(in, out);
      break;
    case 30:
      Fastpack<30>(in, out);
      break;
    case 31:
      Fastpack<31>(in, out);
      break;
    case 32:
      Fastpack<32>(in, out);
      break;
    case 33:
      Fastpack<33>(in, out);
      break;
    case 34:
      Fastpack<34>(in, out);
      break;
    case 35:
      Fastpack<35>(in, out);
      break;
    case 36:
      Fastpack<36>(in, out);
      break;
    case 37:
      Fastpack<37>(in, out);
      break;
    case 38:
      Fastpack<38>(in, out);
      break;
    case 39:
      Fastpack<39>(in, out);
      break;
    case 40:
      Fastpack<40>(in, out);
      break;
    case 41:
      Fastpack<41>(in, out);
      break;
    case 42:
      Fastpack<42>(in, out);
      break;
    case 43:
      Fastpack<43>(in, out);
      break;
    case 44:
      Fastpack<44>(in, out);
      break;
    case 45:
      Fastpack<45>(in, out);
      break;
    case 46:
      Fastpack<46>(in, out);
      break;
    case 47:
      Fastpack<47>(in, out);
      break;
    case 48:
      Fastpack<48>(in, out);
      break;
    case 49:
      Fastpack<49>(in, out);
      break;
    case 50:
      Fastpack<50>(in, out);
      break;
    case 51:
      Fastpack<51>(in, out);
      break;
    case 52:
      Fastpack<52>(in, out);
      break;
    case 53:
      Fastpack<53>(in, out);
      break;
    case 54:
      Fastpack<54>(in, out);
      break;
    case 55:
      Fastpack<55>(in, out);
      break;
    case 56:
      Fastpack<56>(in, out);
      break;
    case 57:
      Fastpack<57>(in, out);
      break;
    case 58:
      Fastpack<58>(in, out);
      break;
    case 59:
      Fastpack<59>(in, out);
      break;
    case 60:
      Fastpack<60>(in, out);
      break;
    case 61:
      Fastpack<61>(in, out);
      break;
    case 62:
      Fastpack<62>(in, out);
      break;
    case 63:
      Fastpack<63>(in, out);
      break;
    case 64:
      Fastpack<64>(in, out);
      break;
    default:
      SDB_ASSERT(false);
      break;
  }
}

void UnpackBlock(const uint32_t* IRS_RESTRICT in, uint32_t* IRS_RESTRICT out,
                 const uint32_t bit) noexcept {
  switch (bit) {
    case 1:
      Fastunpack<1>(in, out);
      break;
    case 2:
      Fastunpack<2>(in, out);
      break;
    case 3:
      Fastunpack<3>(in, out);
      break;
    case 4:
      Fastunpack<4>(in, out);
      break;
    case 5:
      Fastunpack<5>(in, out);
      break;
    case 6:
      Fastunpack<6>(in, out);
      break;
    case 7:
      Fastunpack<7>(in, out);
      break;
    case 8:
      Fastunpack<8>(in, out);
      break;
    case 9:
      Fastunpack<9>(in, out);
      break;
    case 10:
      Fastunpack<10>(in, out);
      break;
    case 11:
      Fastunpack<11>(in, out);
      break;
    case 12:
      Fastunpack<12>(in, out);
      break;
    case 13:
      Fastunpack<13>(in, out);
      break;
    case 14:
      Fastunpack<14>(in, out);
      break;
    case 15:
      Fastunpack<15>(in, out);
      break;
    case 16:
      Fastunpack<16>(in, out);
      break;
    case 17:
      Fastunpack<17>(in, out);
      break;
    case 18:
      Fastunpack<18>(in, out);
      break;
    case 19:
      Fastunpack<19>(in, out);
      break;
    case 20:
      Fastunpack<20>(in, out);
      break;
    case 21:
      Fastunpack<21>(in, out);
      break;
    case 22:
      Fastunpack<22>(in, out);
      break;
    case 23:
      Fastunpack<23>(in, out);
      break;
    case 24:
      Fastunpack<24>(in, out);
      break;
    case 25:
      Fastunpack<25>(in, out);
      break;
    case 26:
      Fastunpack<26>(in, out);
      break;
    case 27:
      Fastunpack<27>(in, out);
      break;
    case 28:
      Fastunpack<28>(in, out);
      break;
    case 29:
      Fastunpack<29>(in, out);
      break;
    case 30:
      Fastunpack<30>(in, out);
      break;
    case 31:
      Fastunpack<31>(in, out);
      break;
    case 32:
      Fastunpack<32>(in, out);
      break;
    default:
      SDB_ASSERT(false);
      break;
  }
}

void UnpackBlock(const uint64_t* IRS_RESTRICT in, uint64_t* IRS_RESTRICT out,
                 const uint32_t bit) noexcept {
  switch (bit) {
    case 1:
      Fastunpack<1>(in, out);
      break;
    case 2:
      Fastunpack<2>(in, out);
      break;
    case 3:
      Fastunpack<3>(in, out);
      break;
    case 4:
      Fastunpack<4>(in, out);
      break;
    case 5:
      Fastunpack<5>(in, out);
      break;
    case 6:
      Fastunpack<6>(in, out);
      break;
    case 7:
      Fastunpack<7>(in, out);
      break;
    case 8:
      Fastunpack<8>(in, out);
      break;
    case 9:
      Fastunpack<9>(in, out);
      break;
    case 10:
      Fastunpack<10>(in, out);
      break;
    case 11:
      Fastunpack<11>(in, out);
      break;
    case 12:
      Fastunpack<12>(in, out);
      break;
    case 13:
      Fastunpack<13>(in, out);
      break;
    case 14:
      Fastunpack<14>(in, out);
      break;
    case 15:
      Fastunpack<15>(in, out);
      break;
    case 16:
      Fastunpack<16>(in, out);
      break;
    case 17:
      Fastunpack<17>(in, out);
      break;
    case 18:
      Fastunpack<18>(in, out);
      break;
    case 19:
      Fastunpack<19>(in, out);
      break;
    case 20:
      Fastunpack<20>(in, out);
      break;
    case 21:
      Fastunpack<21>(in, out);
      break;
    case 22:
      Fastunpack<22>(in, out);
      break;
    case 23:
      Fastunpack<23>(in, out);
      break;
    case 24:
      Fastunpack<24>(in, out);
      break;
    case 25:
      Fastunpack<25>(in, out);
      break;
    case 26:
      Fastunpack<26>(in, out);
      break;
    case 27:
      Fastunpack<27>(in, out);
      break;
    case 28:
      Fastunpack<28>(in, out);
      break;
    case 29:
      Fastunpack<29>(in, out);
      break;
    case 30:
      Fastunpack<30>(in, out);
      break;
    case 31:
      Fastunpack<31>(in, out);
      break;
    case 32:
      Fastunpack<32>(in, out);
      break;
    case 33:
      Fastunpack<33>(in, out);
      break;
    case 34:
      Fastunpack<34>(in, out);
      break;
    case 35:
      Fastunpack<35>(in, out);
      break;
    case 36:
      Fastunpack<36>(in, out);
      break;
    case 37:
      Fastunpack<37>(in, out);
      break;
    case 38:
      Fastunpack<38>(in, out);
      break;
    case 39:
      Fastunpack<39>(in, out);
      break;
    case 40:
      Fastunpack<40>(in, out);
      break;
    case 41:
      Fastunpack<41>(in, out);
      break;
    case 42:
      Fastunpack<42>(in, out);
      break;
    case 43:
      Fastunpack<43>(in, out);
      break;
    case 44:
      Fastunpack<44>(in, out);
      break;
    case 45:
      Fastunpack<45>(in, out);
      break;
    case 46:
      Fastunpack<46>(in, out);
      break;
    case 47:
      Fastunpack<47>(in, out);
      break;
    case 48:
      Fastunpack<48>(in, out);
      break;
    case 49:
      Fastunpack<49>(in, out);
      break;
    case 50:
      Fastunpack<50>(in, out);
      break;
    case 51:
      Fastunpack<51>(in, out);
      break;
    case 52:
      Fastunpack<52>(in, out);
      break;
    case 53:
      Fastunpack<53>(in, out);
      break;
    case 54:
      Fastunpack<54>(in, out);
      break;
    case 55:
      Fastunpack<55>(in, out);
      break;
    case 56:
      Fastunpack<56>(in, out);
      break;
    case 57:
      Fastunpack<57>(in, out);
      break;
    case 58:
      Fastunpack<58>(in, out);
      break;
    case 59:
      Fastunpack<59>(in, out);
      break;
    case 60:
      Fastunpack<60>(in, out);
      break;
    case 61:
      Fastunpack<61>(in, out);
      break;
    case 62:
      Fastunpack<62>(in, out);
      break;
    case 63:
      Fastunpack<63>(in, out);
      break;
    case 64:
      Fastunpack<64>(in, out);
      break;
    default:
      SDB_ASSERT(false);
      break;
  }
}

uint32_t FastpackAt(const uint32_t* in, const size_t i,
                    const uint32_t bits) noexcept {
  SDB_ASSERT(i < kBlockSize32);

  switch (bits) {
    case 1:
      return ::FastpackAt<1>(in, i);
    case 2:
      return ::FastpackAt<2>(in, i);
    case 3:
      return ::FastpackAt<3>(in, i);
    case 4:
      return ::FastpackAt<4>(in, i);
    case 5:
      return ::FastpackAt<5>(in, i);
    case 6:
      return ::FastpackAt<6>(in, i);
    case 7:
      return ::FastpackAt<7>(in, i);
    case 8:
      return ::FastpackAt<8>(in, i);
    case 9:
      return ::FastpackAt<9>(in, i);
    case 10:
      return ::FastpackAt<10>(in, i);
    case 11:
      return ::FastpackAt<11>(in, i);
    case 12:
      return ::FastpackAt<12>(in, i);
    case 13:
      return ::FastpackAt<13>(in, i);
    case 14:
      return ::FastpackAt<14>(in, i);
    case 15:
      return ::FastpackAt<15>(in, i);
    case 16:
      return ::FastpackAt<16>(in, i);
    case 17:
      return ::FastpackAt<17>(in, i);
    case 18:
      return ::FastpackAt<18>(in, i);
    case 19:
      return ::FastpackAt<19>(in, i);
    case 20:
      return ::FastpackAt<20>(in, i);
    case 21:
      return ::FastpackAt<21>(in, i);
    case 22:
      return ::FastpackAt<22>(in, i);
    case 23:
      return ::FastpackAt<23>(in, i);
    case 24:
      return ::FastpackAt<24>(in, i);
    case 25:
      return ::FastpackAt<25>(in, i);
    case 26:
      return ::FastpackAt<26>(in, i);
    case 27:
      return ::FastpackAt<27>(in, i);
    case 28:
      return ::FastpackAt<28>(in, i);
    case 29:
      return ::FastpackAt<29>(in, i);
    case 30:
      return ::FastpackAt<30>(in, i);
    case 31:
      return ::FastpackAt<31>(in, i);
    case 32:
      return ::FastpackAt<32>(in, i);
    default:
      SDB_ASSERT(false);
      return 0;  // this should never be hit, or algorithm error
  }
}

uint64_t FastpackAt(const uint64_t* in, const size_t i,
                    const uint32_t bits) noexcept {
  SDB_ASSERT(i < kBlockSize64);

  switch (bits) {
    case 1:
      return ::FastpackAt<1>(in, i);
    case 2:
      return ::FastpackAt<2>(in, i);
    case 3:
      return ::FastpackAt<3>(in, i);
    case 4:
      return ::FastpackAt<4>(in, i);
    case 5:
      return ::FastpackAt<5>(in, i);
    case 6:
      return ::FastpackAt<6>(in, i);
    case 7:
      return ::FastpackAt<7>(in, i);
    case 8:
      return ::FastpackAt<8>(in, i);
    case 9:
      return ::FastpackAt<9>(in, i);
    case 10:
      return ::FastpackAt<10>(in, i);
    case 11:
      return ::FastpackAt<11>(in, i);
    case 12:
      return ::FastpackAt<12>(in, i);
    case 13:
      return ::FastpackAt<13>(in, i);
    case 14:
      return ::FastpackAt<14>(in, i);
    case 15:
      return ::FastpackAt<15>(in, i);
    case 16:
      return ::FastpackAt<16>(in, i);
    case 17:
      return ::FastpackAt<17>(in, i);
    case 18:
      return ::FastpackAt<18>(in, i);
    case 19:
      return ::FastpackAt<19>(in, i);
    case 20:
      return ::FastpackAt<20>(in, i);
    case 21:
      return ::FastpackAt<21>(in, i);
    case 22:
      return ::FastpackAt<22>(in, i);
    case 23:
      return ::FastpackAt<23>(in, i);
    case 24:
      return ::FastpackAt<24>(in, i);
    case 25:
      return ::FastpackAt<25>(in, i);
    case 26:
      return ::FastpackAt<26>(in, i);
    case 27:
      return ::FastpackAt<27>(in, i);
    case 28:
      return ::FastpackAt<28>(in, i);
    case 29:
      return ::FastpackAt<29>(in, i);
    case 30:
      return ::FastpackAt<30>(in, i);
    case 31:
      return ::FastpackAt<31>(in, i);
    case 32:
      return ::FastpackAt<32>(in, i);
    case 33:
      return ::FastpackAt<33>(in, i);
    case 34:
      return ::FastpackAt<34>(in, i);
    case 35:
      return ::FastpackAt<35>(in, i);
    case 36:
      return ::FastpackAt<36>(in, i);
    case 37:
      return ::FastpackAt<37>(in, i);
    case 38:
      return ::FastpackAt<38>(in, i);
    case 39:
      return ::FastpackAt<39>(in, i);
    case 40:
      return ::FastpackAt<40>(in, i);
    case 41:
      return ::FastpackAt<41>(in, i);
    case 42:
      return ::FastpackAt<42>(in, i);
    case 43:
      return ::FastpackAt<43>(in, i);
    case 44:
      return ::FastpackAt<44>(in, i);
    case 45:
      return ::FastpackAt<45>(in, i);
    case 46:
      return ::FastpackAt<46>(in, i);
    case 47:
      return ::FastpackAt<47>(in, i);
    case 48:
      return ::FastpackAt<48>(in, i);
    case 49:
      return ::FastpackAt<49>(in, i);
    case 50:
      return ::FastpackAt<50>(in, i);
    case 51:
      return ::FastpackAt<51>(in, i);
    case 52:
      return ::FastpackAt<52>(in, i);
    case 53:
      return ::FastpackAt<53>(in, i);
    case 54:
      return ::FastpackAt<54>(in, i);
    case 55:
      return ::FastpackAt<55>(in, i);
    case 56:
      return ::FastpackAt<56>(in, i);
    case 57:
      return ::FastpackAt<57>(in, i);
    case 58:
      return ::FastpackAt<58>(in, i);
    case 59:
      return ::FastpackAt<59>(in, i);
    case 60:
      return ::FastpackAt<60>(in, i);
    case 61:
      return ::FastpackAt<61>(in, i);
    case 62:
      return ::FastpackAt<62>(in, i);
    case 63:
      return ::FastpackAt<63>(in, i);
    case 64:
      return ::FastpackAt<64>(in, i);
    default:
      SDB_ASSERT(false);
      return 0;  // this should never be hit, or algorithm error
  }
}

}  // namespace irs::packed
