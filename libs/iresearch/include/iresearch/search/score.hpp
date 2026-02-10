////////////////////////////////////////////////////////////////////////////////
/// DISCLAIMER
///
/// Copyright 2017 ArangoDB GmbH, Cologne, Germany
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
/// Copyright holder is ArangoDB GmbH, Cologne, Germany
///
/// @author Andrey Abramov
////////////////////////////////////////////////////////////////////////////////

#pragma once

#include "basics/shared.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attributes.hpp"

namespace irs {

using ScoreFunctions = sdb::containers::SmallVector<ScoreFunction, 1>;

// Prepare empty collectors, i.e. call collect(...) on each of the
// buckets without explicitly collecting field or term statistics,
// e.g. for 'all' filter.
void PrepareCollectors(std::span<const ScorerBucket> order, byte_type* stats);

template<ScoreMergeType MergeType>
IRS_FORCE_INLINE void Merge(score_t& bucket, score_t arg) noexcept {
  if constexpr (MergeType == ScoreMergeType::Sum) {
    bucket += arg;
  } else if constexpr (MergeType == ScoreMergeType::Max) {
    bucket = std::max(bucket, arg);
  } else {
    static_assert(MergeType == ScoreMergeType::Noop);
  }
}

template<typename T>
IRS_FORCE_INLINE score_t* GetScoreValue(T& v) noexcept {
  if constexpr (std::is_same_v<T, score_t>) {
    return std::addressof(v);
  } else {
    static_assert(false);
  }
}

template<ScoreMergeType MergeType, typename T>
IRS_FORCE_INLINE void Merge(T* IRS_RESTRICT res,
                            const score_t* IRS_RESTRICT args,
                            size_t n) noexcept {
  for (size_t i = 0; i < n; ++i) {
    Merge<MergeType>(*GetScoreValue(res[i]), args[i]);
  }
}

template<ScoreMergeType MergeType, typename T, typename I>
IRS_FORCE_INLINE void Merge(T* IRS_RESTRICT res, const I* IRS_RESTRICT hits,
                            const score_t* IRS_RESTRICT args,
                            size_t n) noexcept {
  for (size_t i = 0; i < n; ++i) {
    const auto bucket_index = hits[i];
    Merge<MergeType>(res[bucket_index], args[i]);
  }
}

template<ScoreMergeType MergeType, typename T, typename I>
IRS_FORCE_INLINE void Merge(T* IRS_RESTRICT res, const I* IRS_RESTRICT hits,
                            I base, const score_t* IRS_RESTRICT args,
                            size_t n) noexcept {
  for (size_t i = 0; i < n; ++i) {
    const auto bucket_index = hits[i] - base;
    Merge<MergeType>(res[bucket_index], args[i]);
  }
}

template<ScoreMergeType MergeType, typename T, size_t N>
IRS_FORCE_INLINE void Merge(score_t* res, std::span<score_t, N> args) noexcept {
  Merge<MergeType>(res, args.data(), args.size());
}

template<ScoreMergeType MergeType, typename T, typename I, size_t N>
IRS_FORCE_INLINE void Merge(T* res, std::span<I, N> hits,
                            std::span<score_t, N> args) noexcept {
  SDB_ASSERT(hits.size() <= args.size());
  Merge<MergeType>(res, hits.data(), args.data(), hits.size());
}

}  // namespace irs
