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

#include <span>

#include "iresearch/analysis/token_batch.hpp"
#include "iresearch/utils/numeric_utils.hpp"

namespace irs {

template<typename T>
inline constexpr bool kInlineNumericTerm =
  numeric_utils::kNumericTermSize<T> <= duckdb::string_t::INLINE_LENGTH;

template<typename T, typename ValueAt>
void AppendNumericTermsBlock(duckdb::string_t* out, size_t count,
                             ValueAt&& value_at, byte_type* keys = nullptr) {
  constexpr auto kSize =
    static_cast<uint32_t>(numeric_utils::kNumericTermSize<T>);
  for (size_t i = 0; i < count; ++i) {
    if constexpr (kInlineNumericTerm<T>) {
      out[i] = duckdb::string_t{kSize};
      numeric_utils::EncodeNumericTerm<T>(
        reinterpret_cast<byte_type*>(out[i].GetDataWriteable()), value_at(i));
    } else {
      SDB_ASSERT(keys);
      auto* bytes = keys + i * kSize;
      numeric_utils::EncodeNumericTerm<T>(bytes, value_at(i));
      out[i] = duckdb::string_t{reinterpret_cast<const char*>(bytes), kSize};
    }
  }
}

template<typename T>
void AppendNumericTermsBlock(duckdb::string_t* out, std::span<const T> values) {
  static_assert(kInlineNumericTerm<T>);
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
