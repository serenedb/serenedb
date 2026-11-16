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

#pragma once

#include <cstdint>
#include <duckdb/common/radix.hpp>
#include <type_traits>

#include "iresearch/types.hpp"
#include "iresearch/utils/assert.hpp"
#include "iresearch/utils/string.hpp"

namespace irs::numeric_utils {

template<typename T>
inline constexpr byte_type kNumericTermMagic = [] {
  if constexpr (std::is_same_v<T, int32_t>) {
    return byte_type{0x00};
  } else if constexpr (std::is_same_v<T, float>) {
    return byte_type{0x20};
  } else if constexpr (std::is_same_v<T, int64_t>) {
    return byte_type{0x60};
  } else {
    static_assert(std::is_same_v<T, double>);
    return byte_type{0xA0};
  }
}();

template<typename T>
inline constexpr size_t kNumericTermSize = 1 + sizeof(T);

inline constexpr size_t kNumericTermMaxSize = kNumericTermSize<int64_t>;

template<typename T>
bytes_view EncodeNumericTerm(byte_type* buf, T value) {
  buf[0] = kNumericTermMagic<T>;
  duckdb::Radix::EncodeData(buf + 1, value);
  return {buf, kNumericTermSize<T>};
}

template<typename T>
T DecodeNumericTerm(bytes_view term) {
  SDB_ASSERT(term.size() == kNumericTermSize<T>);
  SDB_ASSERT(term[0] == kNumericTermMagic<T>);
  return duckdb::Radix::DecodeData<T>(term.data() + 1);
}

}  // namespace irs::numeric_utils
