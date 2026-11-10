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

#include <iresearch/types.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <iresearch/utils/string.hpp>
#include <string_view>
#include <type_traits>

namespace tests {

constexpr std::string_view kTrueTerm{"\xFF", 1};
constexpr std::string_view kFalseTerm{"\x00", 1};

constexpr std::string_view BooleanTerm(bool value) noexcept {
  return value ? kTrueTerm : kFalseTerm;
}

constexpr std::string_view kNullTerm{"\x00", 0};

template<typename T>
irs::bstring NumericTerm(T value) {
  irs::byte_type buf[irs::numeric_utils::kNumericTermMaxSize];
  return irs::bstring{irs::numeric_utils::EncodeNumericTerm(buf, value)};
}

template<typename T>
constexpr irs::field_id TypedFieldId(irs::field_id id) noexcept {
  constexpr irs::field_id kStride = 1024;
  if constexpr (std::is_same_v<T, int32_t>) {
    return id + kStride;
  } else if constexpr (std::is_same_v<T, int64_t>) {
    return id + 2 * kStride;
  } else if constexpr (std::is_same_v<T, float_t>) {
    return id + 3 * kStride;
  } else {
    static_assert(std::is_same_v<T, double_t>);
    return id;
  }
}

}  // namespace tests
