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

#include <iresearch/index/field_meta.hpp>

#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/attributes.hpp"

namespace irs {

// Represents a score related for the particular document
// min score set by document consumers
// max score set by document producers
struct ScoreAttr : Attribute, ScoreFunction {
  static const ScoreAttr kNoScore;

  static constexpr std::string_view type_name() noexcept { return "score"; }

  template<typename Provider>
  static const ScoreAttr& get(const Provider& attrs) {
    const auto* score = irs::get<ScoreAttr>(attrs);
    return score ? *score : kNoScore;
  }

  using ScoreFunction::operator=;

  // For disjunction/conjunction it's just sum of sub-iterators max score
  // For iterator without score it depends on count of documents in iterator
  // For wanderator it's max score for whole skip-list
  // TODO(mbkkt) tail better here and not affect correctness
  //  but to support it we need to know max value in the tail blocks.
  //  Open question: how do it without read next blocks?
  // TODO(mbkkt) At least when iterator exhausted, we could set it to zero.
  struct UpperBounds {
    score_t tail = std::numeric_limits<score_t>::max();
    score_t leaf = std::numeric_limits<score_t>::max();
#ifdef SDB_GTEST
    std::span<const score_t> levels;  // levels.back() == leaf
#endif
  } max;
};

using ScoreFunctions = sdb::containers::SmallVector<ScoreFunction, 1>;

// Prepare empty collectors, i.e. call collect(...) on each of the
// buckets without explicitly collecting field or term statistics,
// e.g. for 'all' filter.
void PrepareCollectors(std::span<const ScorerBucket> order, byte_type* stats);

template<ScoreMergeType MergeType>
void Merge(score_t& bucket, score_t arg) noexcept {
  if constexpr (MergeType == ScoreMergeType::Sum) {
    bucket += arg;
  } else if constexpr (MergeType == ScoreMergeType::Max) {
    bucket = std::max(bucket, arg);
  } else {
    static_assert(MergeType == ScoreMergeType::Noop);
  }
}

template<ScoreMergeType MergeType>
void Merge(score_t* IRS_RESTRICT res, const score_t* IRS_RESTRICT args,
           size_t n) noexcept {
  for (size_t i = 0; i < n; ++i) {
    Merge<MergeType>(res[i], args[i]);
  }
}

template<ScoreMergeType MergeType, typename I>
void Merge(score_t* IRS_RESTRICT res, const I* IRS_RESTRICT hits,
           const score_t* IRS_RESTRICT args, size_t n) noexcept {
  for (size_t i = 0; i < n; ++i) {
    const auto bucket_index = hits[i];
    Merge<MergeType>(res[bucket_index], args[i]);
  }
}

template<ScoreMergeType MergeType, size_t N>
void Merge(score_t* res, std::span<score_t, N> args) noexcept {
  Merge<MergeType>(res, args.data(), args.size());
}

template<ScoreMergeType MergeType, typename I, size_t N>
void Merge(score_t* res, std::span<I, N> hits,
           std::span<score_t, N> args) noexcept {
  SDB_ASSERT(hits.size() <= args.size());
  Merge<MergeType>(res, hits.data(), args.data(), hits.size());
}

template<typename T>
class FixedBuffer {
 public:
  void Realloc(uint16_t size) {
    _buf = std::make_unique<T[]>(size);
    _capacity = size;
  }
  void Resize(uint16_t size) {
    SDB_ASSERT(size <= _capacity);
    _size = size;
  }
  size_t Capacity() const noexcept { return _capacity; }
  void Clear() noexcept { _size = 0; }
  void PushBack(T value) noexcept {
    SDB_ASSERT(_size < _capacity);
    _buf[_size++] = value;
  }
  auto& Back(this auto& self) noexcept {
    SDB_ASSERT(self._size > 0);
    return self._buf[self._size - 1];
  }
  size_t Size() const noexcept { return _size; }
  auto Data() noexcept { return _buf.get(); }
  auto begin(this auto& self) noexcept { return self._buf.get(); }
  auto end(this auto& self) noexcept { return self.begin() + self.Size(); }

 private:
  std::unique_ptr<T[]> _buf;
  uint32_t _size = 0;
  uint32_t _capacity = 0;
};

}  // namespace irs
