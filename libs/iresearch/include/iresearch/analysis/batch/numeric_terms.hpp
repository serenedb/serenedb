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

#include <iresearch/analysis/batch/token_batch.hpp>
#include <iresearch/utils/numeric_utils.hpp>
#include <span>

namespace irs {

using numeric_utils::NumericTermCount;

// Appends the precision-step trie terms of every value straight into the
// batch's inline string_t slots. Terms layout only (trie terms of one value
// share a position). The term count per value is a compile-time constant, so
// slot indices and term sizes are all statically known: the inner loop unrolls
// and the outer loop is branch-free, auto-vectorizable straight-line stores.
template<typename T, uint32_t Step = numeric_utils::kPrecisionStepDef>
void AppendNumericTermsBlock(duckdb::string_t* out, std::span<const T> values) {
  namespace nu = numeric_utils;
  using Enc = typename nu::NumericEncodeOf<T>::Traits;
  using U = typename Enc::type;
  constexpr uint32_t kBits = BitsRequired<U>();
  constexpr uint32_t kTerms = (kBits + Step - 1) / Step;

  for (size_t i = 0; i < values.size(); ++i) {
    const U payload =
      nu::NumericEncodeOf<T>::Integral(values[i]) ^ (U{1} << (kBits - 1));
    for (uint32_t s = 0; s < kTerms; ++s) {
      const uint32_t shift = s * Step;
      byte_type buf[1 + sizeof(U)];
      buf[0] = static_cast<byte_type>(shift) + Enc::TYPE_MAGIC;
      const U be = absl::big_endian::FromHost(
        payload & (std::numeric_limits<U>::max() ^ ((U{1} << shift) - U{1})));
      std::memcpy(buf + 1, &be, sizeof(U));
      out[i * kTerms + s] =
        duckdb::string_t{reinterpret_cast<const char*>(buf),
                         static_cast<uint32_t>(nu::EncodedSize<U>(shift) + 1)};
    }
  }
}

template<typename T, uint32_t Step = numeric_utils::kPrecisionStepDef>
void AppendNumericTermsBlock(TokenBatch& batch, std::span<const T> values) {
  constexpr uint32_t kTerms = NumericTermCount<T>(Step);
  SDB_ASSERT(batch.count + values.size() * kTerms <= TokenBatch::kCapacity);
  AppendNumericTermsBlock<T, Step>(batch.terms + batch.count, values);
  batch.count += static_cast<uint32_t>(values.size()) * kTerms;
}

template<typename T>
uint32_t AppendNumericTerms(TokenBatch& batch, T value) {
  AppendNumericTermsBlock(batch, std::span<const T>{&value, 1});
  return NumericTermCount<T>();
}

}  // namespace irs
