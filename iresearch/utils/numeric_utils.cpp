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
////////////////////////////////////////////////////////////////////////////////

#include "numeric_utils.hpp"

namespace irs::numeric_utils {
namespace {

static_assert(static_cast<uint32_t>(INT32_C(-1) >> 31) == UINT32_C(0xFFFFFFFF));
static_assert(static_cast<uint64_t>(INT64_C(-1) >> 63) ==
              UINT64_C(0xFFFFFFFFFFFFFFFF));

constexpr int32_t MakeSortable32(int32_t value) noexcept {
  return value ^ ((value >> 31) & INT32_C(0x7FFFFFFF));
}

constexpr int64_t MakeSortable64(int64_t value) noexcept {
  return value ^ ((value >> 63) & INT64_C(0x7FFFFFFFFFFFFFFF));
}

}  // namespace

int32_t Ftoi32(float_t value) {
  static_assert(std::numeric_limits<float_t>::is_iec559,
                "compiler does not support ieee754 (float)");

  union {
    float_t in;
    int32_t out;
  } conv;

  conv.in = value;
  return MakeSortable32(conv.out);
}

float_t I32tof(int32_t value) {
  static_assert(std::numeric_limits<float_t>::is_iec559,
                "compiler does not support ieee754 (float)");

  union {
    float_t out;
    int32_t in;
  } conv;

  conv.in = MakeSortable32(value);
  return conv.out;
}

int64_t Dtoi64(double_t value) {
  static_assert(std::numeric_limits<double_t>::is_iec559,
                "compiler does not support ieee754 (double)");

  union {
    double_t in;
    int64_t out;
  } conv;

  conv.in = value;
  return MakeSortable64(conv.out);
}

double_t I64tod(int64_t value) {
  static_assert(std::numeric_limits<double_t>::is_iec559,
                "compiler does not support ieee754 (double)");

  union {
    double_t out;
    int64_t in;
  } conv;

  conv.in = MakeSortable64(value);
  return conv.out;
}

}  // namespace irs::numeric_utils
