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

#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/search/detail/posting_leaf.hpp"
#include "iresearch/store/data_input.hpp"
#include "iresearch/utils/lower_bound.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::detail {

template<typename InputType, ScoreMergeType MergeType, bool Counts>
class PostingWindowScored : public PostingLeaf<InputType, kWindowScoredShape> {
  static_assert(
    MergeType != ScoreMergeType::Noop,
    "a clause of a disjunction is merged into what the others said");

  using Base = PostingLeaf<InputType, kWindowScoredShape>;

  using Base::_doc;
  using Base::_docs;
  using Base::_last;
  using Base::_left_in_leaf;
  using Base::_left_in_list;
  using Base::kBlock;
  using Base::ReadLeafBelow;
  using Base::ScoresAt;

  static constexpr bool kCounts = Counts;

 public:
  PostingWindowScored() = default;

  PostingWindowScored(const PostingMeta& meta, const IndexInput& doc_in,
                      bool has_score_bounds, const SubReader& segment,
                      const TermReader& field, const ScoreArgs& args) {
    Prepare(meta, doc_in, has_score_bounds, segment, field, args);
  }

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               bool has_score_bounds, const SubReader& segment,
               const TermReader& field, const ScoreArgs& args) {
    this->MakeScore(segment, field, args);
    if (!this->OpenList(meta, doc_in, field.meta().index_features,
                        has_score_bounds)) {
      _doc = this->SetSingle(meta);
      this->ScoreLeaf(kBlock - 1, 1);
    }
  }

  doc_id_t Count(doc_id_t min, doc_id_t max, uint32_t* IRS_RESTRICT counts,
                 uint64_t* IRS_RESTRICT mask, score_t* IRS_RESTRICT window)
    requires Counts
  {
    return Add(min, max, counts, mask, window);
  }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                score_t* IRS_RESTRICT window)
    requires(!Counts)
  {
    return Add(min, max, nullptr, mask, window);
  }

  doc_id_t AddMasked(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                     score_t* IRS_RESTRICT window)
    requires(!Counts)
  {
    SDB_ASSERT(min < max);
    if (!this->Start(min, max)) {
      return _doc;
    }
    const auto* const end = std::cend(_docs);
    if (_left_in_leaf != 0) {
      const auto* const begin = ScanNotBelow(end - _left_in_leaf, end, min);
      const auto* const it = this->template ScoreMasked<MergeType>(
        begin, end, min, max, mask, window);
      _left_in_leaf = static_cast<uint32_t>(end - it);
      if (it != end) {
        return _doc = *it;
      }
    }
    for (;;) {
      if (_left_in_list == 0) {
        return _doc = doc_limits::eof();
      }
      const auto len = std::min(_left_in_list, kBlock);
      _left_in_list -= len;
      if (!this->template ReadLeafBelow<false>(len, min)) {
        continue;
      }
      const auto* const it = this->template ScoreMasked<MergeType>(
        ScanNotBelow(end - len, end, min), end, min, max, mask, window);
      if (it != end) {
        _left_in_leaf = static_cast<uint32_t>(end - it);
        return _doc = *it;
      }
    }
  }

 private:
  IRS_FORCE_INLINE doc_id_t Add(doc_id_t min, doc_id_t max,
                                uint32_t* IRS_RESTRICT counts,
                                uint64_t* IRS_RESTRICT mask,
                                score_t* IRS_RESTRICT window) {
    SDB_ASSERT(min < max);
    if (!this->Start(min, max)) {
      return _doc;
    }
    const auto* const end = std::cend(_docs);

    if (_left_in_leaf != 0) {
      const auto* const begin = ScanNotBelow(end - _left_in_leaf, end, min);
      if (begin == end) {
        _left_in_leaf = 0;
      } else if (_last >= max) {
        const auto* const it = AddUntil<MergeType, kCounts>(
          begin, end, ScoresAt(begin), min, max, counts, mask, window);
        _left_in_leaf = static_cast<uint32_t>(end - it);
        return _doc = *it;
      } else {
        AddWhole<MergeType, kCounts>(begin, end, ScoresAt(begin), min, counts,
                                     mask, window);
        _left_in_leaf = 0;
      }
    }

    for (;;) {
      if (_left_in_list == 0) {
        return _doc = doc_limits::eof();
      }
      const auto len = std::min(_left_in_list, kBlock);
      _left_in_list -= len;
      if (!ReadLeafBelow(len, min)) {
        continue;
      }

      const auto* const begin = ScanNotBelow(end - len, end, min);
      if (_last < max) {
        AddWhole<MergeType, kCounts>(begin, end, ScoresAt(begin), min, counts,
                                     mask, window);
        continue;
      }
      const auto* const it = AddUntil<MergeType, kCounts>(
        begin, end, ScoresAt(begin), min, max, counts, mask, window);
      _left_in_leaf = static_cast<uint32_t>(end - it);
      return _doc = *it;
    }
  }
};

template<typename InputType, ScoreMergeType MergeType>
using PostingCountScored = PostingWindowScored<InputType, MergeType, true>;

template<typename InputType, ScoreMergeType MergeType>
using PostingFillScored = PostingWindowScored<InputType, MergeType, false>;

}  // namespace irs::detail
