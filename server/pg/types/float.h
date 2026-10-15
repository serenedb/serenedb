////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2026 SereneDB GmbH, Berlin, Germany
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
/// Copyright holder is SereneDB GmbH, Berlin, Germany
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include <cstddef>
#include <cstdint>
#include <duckdb/function/cast/cast_function_set.hpp>

namespace sdb::pg {

inline constexpr size_t kFloatText = sizeof("-2.2250738585072014e-308") - 1;

size_t WriteFloatShortest(float value, char* buffer);
size_t WriteFloatShortest(double value, char* buffer);
size_t WriteFloatRounded(float value, int8_t extra_float_digits, char* buffer);
size_t WriteFloatRounded(double value, int8_t extra_float_digits, char* buffer);

[[noreturn]] void ThrowFloatOverflow();
[[noreturn]] void ThrowFloatUnderflow();

void RegisterFloat(duckdb::CastFunctionSet& casts);

}  // namespace sdb::pg
