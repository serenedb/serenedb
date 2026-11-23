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

#include <algorithm>
#include <bit>
#include <cstdint>
#include <cstring>
#include <duckdb/common/radix.hpp>
#include <duckdb/common/types/string_type.hpp>
#include <span>
#include <type_traits>

#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/utils/numeric_utils.hpp"

namespace irs {

template<typename T, typename ValueAt>
void AppendNumericTermsBlock(duckdb::string_t* out, size_t count,
                             ValueAt&& value_at) {
  using Key =
    std::conditional_t<sizeof(T) == sizeof(uint64_t), uint64_t, uint32_t>;
  static_assert(sizeof(Key) == sizeof(T));
  static_assert(std::endian::native == std::endian::little);
  static_assert(sizeof(duckdb::string_t) == 2 * sizeof(uint64_t));
  static_assert(numeric_utils::kNumericTermSize<T> <=
                duckdb::string_t::INLINE_LENGTH);
  constexpr uint64_t kHead = numeric_utils::kNumericTermSize<T> |
                             uint64_t{numeric_utils::kNumericTermMagic<T>}
                               << 32;
  Key keys[TokenBatch::kCapacity];
  for (size_t base = 0; base < count; base += TokenBatch::kCapacity) {
    const auto n = std::min(count - base, TokenBatch::kCapacity);
    for (size_t i = 0; i < n; ++i) {
      duckdb::Radix::EncodeData<T>(
        reinterpret_cast<duckdb::data_ptr_t>(keys + i), value_at(base + i));
    }
    for (size_t i = 0; i < n; ++i) {
      const uint64_t key = keys[i];
      const uint64_t words[] = {kHead | key << 40, key >> 24};
      std::memcpy(static_cast<void*>(out + base + i), words, sizeof(words));
    }
  }
}

template<typename T>
void AppendNumericTermsBlock(duckdb::string_t* out, std::span<const T> values) {
  AppendNumericTermsBlock<T>(out, values.size(),
                             [values](size_t i) noexcept { return values[i]; });
}

template<typename T>
void AppendNumericTermsBlock(TokenBatch& batch, std::span<const T> values) {
  SDB_ASSERT(batch.count + values.size() <= TokenBatch::kCapacity);
  AppendNumericTermsBlock<T>(batch.terms + batch.count, values);
  batch.count += static_cast<uint32_t>(values.size());
}

}  // namespace irs
