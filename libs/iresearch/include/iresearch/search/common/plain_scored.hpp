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

#include <bit>
#include <utility>
#include <vector>

#include "basics/bit_utils.hpp"
#include "iresearch/formats/posting_meta.hpp"
#include "iresearch/search/common/all_docs_score.hpp"
#include "iresearch/search/common/posting_count.hpp"
#include "iresearch/search/common/posting_fill.hpp"
#include "iresearch/search/common/posting_probe.hpp"
#include "iresearch/search/common/score_args.hpp"
#include "iresearch/search/common/window.hpp"
#include "iresearch/search/lead/posting_docs.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/utils/type_limits.hpp"

namespace irs::search {

template<typename InputType>
class PlainFillScored {
 public:
  PlainFillScored() = default;

  PlainFillScored(const PostingMeta& meta, const IndexInput& doc_in,
                  bool has_score_bounds, bool has_freq) {
    Prepare(meta, doc_in, has_score_bounds, has_freq);
  }

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               bool has_score_bounds, bool has_freq) {
    _leaf.Prepare(meta, doc_in, has_score_bounds, has_freq);
  }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                score_t* IRS_RESTRICT) {
    return _leaf.FillOr(min, max, mask);
  }

 private:
  PostingFill<InputType> _leaf;
};

template<typename InputType, ScoreMergeType MergeType>
class ConstFillScored {
  static_assert(
    MergeType != ScoreMergeType::Noop,
    "a clause of a disjunction is merged into what the others said");

 public:
  ConstFillScored() = default;

  ConstFillScored(const PostingMeta& meta, const IndexInput& doc_in,
                  bool has_score_bounds, const SubReader& segment,
                  const TermReader& field, const ScoreArgs& args) {
    Prepare(meta, doc_in, has_score_bounds, segment, field, args);
  }

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               bool has_score_bounds, const SubReader& segment,
               const TermReader& field, const ScoreArgs& args) {
    _value = ConstantTermOf(segment, field, args);
    _leaf.Prepare(meta, doc_in, has_score_bounds,
                  FeaturesHaveFreq(field.meta().index_features));
  }

  doc_id_t Fill(doc_id_t min, doc_id_t max, uint64_t* IRS_RESTRICT mask,
                score_t* IRS_RESTRICT window) {
    if constexpr (MergeType == ScoreMergeType::Max) {
      return _leaf.FillMax(min, max, mask, window, _value);
    } else {
      return _leaf.FillSum(min, max, mask, window, _value);
    }
  }

 private:
  PostingFill<InputType> _leaf;
  score_t _value = 0;
};

template<typename InputType>
class PlainCountScored {
 public:
  PlainCountScored() = default;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               bool has_score_bounds, bool has_freq) {
    _leaf.Prepare(meta, doc_in, has_score_bounds, has_freq);
  }

  doc_id_t Count(doc_id_t min, doc_id_t max, uint32_t* IRS_RESTRICT counts,
                 uint64_t* IRS_RESTRICT mask, score_t* IRS_RESTRICT) {
    const auto words = WindowWords(min, max);
    Clear(_own.data(), words);
    const auto next = _leaf.FillOr(min, max, _own.data());
    for (size_t w = 0; w != words; ++w) {
      auto word = _own[w];
      if (word == 0) {
        continue;
      }
      mask[w] |= word;
      auto* const base = counts + w * kWindowBits;
      do {
        ++base[static_cast<uint32_t>(std::countr_zero(word))];
        word = PopBit(word);
      } while (word != 0);
    }
    return next;
  }

 private:
  Scratch _own{};
  PostingFill<InputType> _leaf;
};

template<typename InputType, ScoreMergeType MergeType>
class ConstCountScored {
  static_assert(
    MergeType != ScoreMergeType::Noop,
    "a clause of a disjunction is merged into what the others said");

 public:
  ConstCountScored() = default;

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               bool has_score_bounds, const SubReader& segment,
               const TermReader& field, const ScoreArgs& args) {
    _value = ConstantTermOf(segment, field, args);
    _leaf.Prepare(meta, doc_in, has_score_bounds,
                  FeaturesHaveFreq(field.meta().index_features));
  }

  doc_id_t Count(doc_id_t min, doc_id_t max, uint32_t* IRS_RESTRICT counts,
                 uint64_t* IRS_RESTRICT mask, score_t* IRS_RESTRICT window) {
    const auto words = WindowWords(min, max);
    Clear(_own.data(), words);
    const auto next = _leaf.FillOr(min, max, _own.data());
    for (size_t w = 0; w != words; ++w) {
      auto word = _own[w];
      if (word == 0) {
        continue;
      }
      mask[w] |= word;
      auto* const tally = counts + w * kWindowBits;
      auto* const scores = window + w * kWindowBits;
      do {
        const auto slot = static_cast<uint32_t>(std::countr_zero(word));
        ++tally[slot];
        irs::Merge<MergeType>(scores[slot], _value);
        word = PopBit(word);
      } while (word != 0);
    }
    return next;
  }

 private:
  Scratch _own{};
  PostingFill<InputType> _leaf;
  score_t _value = 0;
};

template<typename InputType>
class PlainProbeScored {
 public:
  PlainProbeScored() = default;

  PlainProbeScored(const PostingMeta& meta, const IndexInput& doc_in,
                   IndexFeatures layout, bool bounds) {
    Prepare(meta, doc_in, layout, bounds);
  }

  void Prepare(const PostingMeta& meta, const IndexInput& doc_in,
               IndexFeatures layout, bool bounds) {
    _docs_count = meta.docs_count;
    _leaf.Prepare(meta, doc_in, layout, bounds);
  }

  IRS_FORCE_INLINE doc_id_t Probe(doc_id_t target) {
    return _leaf.Probe(target);
  }

  IRS_FORCE_INLINE void FetchScoreArgs(uint32_t) noexcept {}

  ScoreFunction PrepareScore() { return ScoreFunction::Default(); }

  void CollectScorers(std::vector<ScoreFunction>&) const noexcept {}

 private:
  PostingProbe<InputType> _leaf;
  uint32_t _docs_count = 0;
};

}  // namespace irs::search
