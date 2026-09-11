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
#include <vector>

#include "basics/bit_utils.hpp"
#include "iresearch/formats/posting/skip_list.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/common/posting_leaf.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename InputType>
class PostingProbeScored : public PostingLeaf<InputType, kProbeScoredShape> {
  using Base = PostingLeaf<InputType, kProbeScoredShape>;

  using Base::_cursor;
  using Base::_doc;
  using Base::_docs;
  using Base::_freqs;
  using Base::_gather;
  using Base::_last;
  using Base::kBits;
  using Base::kBlock;
  using Base::ReadLeafFill;
  using Base::SeekToLeaf;

 public:
  PostingProbeScored() = default;

  PostingProbeScored(const PostingMeta& meta, const IndexInput& doc_in,
                     const SubReader& segment, const TermReader& field,
                     const ScoreArgs& args) {
    Prepare(meta, doc_in, segment, field, args);
  }

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               const SubReader& segment, const TermReader& field,
               const ScoreArgs& args) {
    SDB_ASSERT(meta.docs_count != 0);
    SDB_ASSERT(FeaturesHaveFreq(field.meta().index_features));
    this->SetRecipe(segment, field, args);

    if (meta.docs_count == 1) {
      const auto doc = this->SetSingle(meta);
      _cursor.base = doc - 1;
      _len = 1;
      _at = kBlock - 1;
      _packed = true;
      return;
    }

    const auto bounds = field.HasScoreBounds();
    this->OpenInput(meta, doc_in, bounds);
    this->ArmWalk(meta, field.meta().index_features, bounds);
  }

  ScoreFunction PrepareScore() { return this->MakeDeferredScore(); }

  void CollectScorers(std::vector<ScoreFunction>& out) {
    AppendScorer(out, PrepareScore());
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t slot) noexcept {
    _gather.data[slot] = _freqs.data[_index];
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    if (target <= _doc) [[unlikely]] {
      return _doc;
    }

    if (_last < target && !ReadTo(target)) [[unlikely]] {
      return _doc = doc_limits::eof();
    }

    if (_packed) [[likely]] {
      if (_len == kBlock) [[likely]] {
        const auto* const it = BranchlessLowerBound<doc_limits::kBlockSize>(
          std::begin(_docs), target);
        _index = static_cast<uint32_t>(it - std::cbegin(_docs));
        return _doc = *it;
      }
      const auto* const end = std::cend(_docs);
      for (const auto* it = std::cbegin(_docs) + _at; it != end; ++it) {
        if (target <= *it) {
          _at = static_cast<uint32_t>(it - std::cbegin(_docs));
          _index = _at;
          return _doc = *it;
        }
      }
      _at = kBlock;
      return _doc = doc_limits::eof();
    }

    return _doc = ProbeMasked(target);
  }

 private:
  IRS_NO_INLINE doc_id_t ProbeMasked(doc_id_t target) noexcept {
    const auto first = _cursor.base + 1;
    if (target < first) {
      target = first;
    }

    if (_run) {
      _index = kBlock - _len + (target - first);
      return target;
    }

    auto bit = static_cast<uint64_t>(target) - _cursor.base;
    for (auto w = bit / kBits; w != _words; ++w) {
      const auto word = _bitset[w] & (~uint64_t{0} << (bit % kBits));
      if (word != 0) {
        const auto tz = static_cast<uint32_t>(std::countr_zero(word));
        for (; _prefix_word != w; ++_prefix_word) {
          _prefix_bits +=
            static_cast<uint32_t>(std::popcount(_bitset[_prefix_word]));
        }
        _index = kBlock - _len + _prefix_bits +
                 static_cast<uint32_t>(
                   std::popcount(_bitset[w] & ((uint64_t{1} << tz) - 1)));
        return static_cast<doc_id_t>(_cursor.base + w * kBits + tz);
      }
      bit = (w + 1) * kBits;
    }
    return doc_limits::eof();
  }

  void ReadLeaf(doc_id_t prev) {
    const auto read = ReadLeafFill(prev);
    _bitset = read.bitset;
    _len = read.len;
    _at = kBlock - read.len;
    _words = read.leaf.words;
    _run = read.leaf.IsRun();
    _prefix_word = 0;
    _prefix_bits = 0;
    _packed = !read.leaf.Maskable();
  }

  IRS_FORCE_INLINE bool ReadTo(doc_id_t target) {
    return SeekToLeaf(
      target, [this](doc_id_t prev) IRS_FORCE_INLINE { ReadLeaf(prev); });
  }

  const uint64_t* _bitset = nullptr;
  uint32_t _words = 0;
  uint32_t _len = 0;
  uint32_t _at = doc_limits::kBlockSize;
  uint32_t _index = doc_limits::kBlockSize - 1;
  uint64_t _prefix_word = 0;
  uint32_t _prefix_bits = 0;
  bool _run = false;
  bool _packed = true;
};

}  // namespace irs::search
