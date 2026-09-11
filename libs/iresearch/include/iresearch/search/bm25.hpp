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

#include "iresearch/index/field_meta.hpp"
#include "iresearch/search/scorer.hpp"

namespace irs {

struct BM25Stats {
  score_t idf;
  score_t norm_const;
  score_t norm_length;
};

class BM25 final : public irs::ScorerBase<BM25, BM25Stats> {
 public:
  static constexpr std::string_view type_name() noexcept { return "bm25"; }

  static constexpr score_t K() noexcept { return 1.2f; }

  static constexpr score_t B() noexcept { return 0.75f; }

  static constexpr bool BOOST_AS_SCORE() noexcept { return false; }

  struct Options {
    using Owner = BM25;
    float k1 = K();
    float b = B();
    bool boost_as_score = BOOST_AS_SCORE();
    bool approximate = true;
    bool operator==(const Options&) const = default;
  };

  static ScoreBoundType BoundTypeOf(const Options& opts) noexcept {
    if (opts.k1 == 0.f) {
      return ScoreBoundType::None;
    }
    if (opts.b == 0.f) {
      return ScoreBoundType::MaxFreq;
    }
    if (opts.b == 1.f) {
      return ScoreBoundType::DivNorm;
    }
    return ScoreBoundType::MinNorm;
  }

  static std::unique_ptr<BM25> Make(const Options& opts) {
    return std::make_unique<BM25>(opts.k1, opts.b, opts.boost_as_score,
                                  opts.approximate);
  }

  BM25(score_t k = K(), score_t b = B(), bool boost_as_score = BOOST_AS_SCORE(),
       bool approximate = true) noexcept
    : _k{k},
      _b{b},
      _boost_as_score{boost_as_score},
      _approximate{approximate} {}

  void collect(byte_type* stats_buf, const irs::FieldCollector* field,
               const irs::TermCollector* term) const final;

  bool ScoresPerDoc() const noexcept final { return !IsBM1(); }

  IndexFeatures GetIndexFeatures() const noexcept final {
    if (IsBM1()) {
      return IndexFeatures::None;
    }

    if (NeedsNorm()) {
      return IndexFeatures::Freq | IndexFeatures::Norm;
    }

    return IndexFeatures::Freq;
  }

  ScoreFunction PrepareScorer(const ScoreContext& ctx) const final;

  ScoreBoundWriter::ptr PrepareScoreBoundWriter(size_t max_levels) const final;

  ScoreBoundSource::ptr PrepareScoreBoundSource() const final;

  bool HasScoreBounds() const noexcept final { return !IsBM1(); }

  bool Compatible(const ScorerOptions& persisted) const noexcept final;

  bool equals(const Scorer& other) const noexcept final;

  std::string ToString() const final;

  Options GetOptions() const noexcept {
    return {.k1 = _k,
            .b = _b,
            .boost_as_score = _boost_as_score,
            .approximate = _approximate};
  }

  score_t k() const noexcept { return _k; }

  score_t b() const noexcept { return _b; }

  bool use_boost_as_score() const noexcept { return _boost_as_score; }

  bool IsBM15() const noexcept { return _b == 0.f; }

  bool IsBM11() const noexcept { return _b == 1.f; }

  bool IsBM1() const noexcept { return _k == 0.f; }

  bool NeedsNorm() const noexcept { return !IsBM1() && !IsBM15(); }

 private:
  score_t _k;
  score_t _b;
  bool _boost_as_score;
  bool _approximate;
};

}  // namespace irs
