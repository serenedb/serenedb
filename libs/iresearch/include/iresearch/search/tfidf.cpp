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

#include "tfidf.hpp"

#include <absl/container/inlined_vector.h>

#include <cmath>
#include <cstddef>
#include <string_view>

#include "basics/down_cast.h"
#include "basics/empty.hpp"
#include "basics/misc.hpp"
#include "basics/shared.hpp"
#include "iresearch/analysis/token_attributes.hpp"
#include "iresearch/formats/posting/wand_writer.hpp"
#include "iresearch/index/field_meta.hpp"
#include "iresearch/index/index_reader.hpp"
#include "iresearch/index/norm.hpp"
#include "iresearch/search/collectors.hpp"
#include "iresearch/search/column_collector.hpp"
#include "iresearch/search/score_function.hpp"
#include "iresearch/search/scorer.hpp"
#include "iresearch/search/scorers.hpp"

namespace irs {
namespace {

template<typename T>
constexpr const T* TryGetValue(const T* value) noexcept {
  return value;
}

constexpr std::nullptr_t TryGetValue(utils::Empty /*value*/) noexcept {
  return nullptr;
}

// Helper functions

IRS_FORCE_INLINE score_t TfIdf(uint32_t freq, score_t idf) noexcept {
  // TODO(gnusi): do we need sqrt?
  return std::sqrt(static_cast<score_t>(freq)) * idf;
}

template<ScoreMergeType MergeType, bool HasNorm, bool HasBoost>
IRS_FORCE_INLINE void TfIdf(score_t* IRS_RESTRICT res, scores_size_t n,
                            const uint32_t* IRS_RESTRICT freq,
                            [[maybe_unused]] const uint32_t* IRS_RESTRICT norm,
                            [[maybe_unused]] const score_t* IRS_RESTRICT boost,
                            score_t idf) noexcept {
  for (scores_size_t i = 0; i != n; ++i) {
    const auto r = [&] IRS_FORCE_INLINE {
      if constexpr (HasNorm && HasBoost) {
        return boost[i] * TfIdf(freq[i], idf) /
               std::sqrt(static_cast<score_t>(norm[i]));
      } else if constexpr (HasNorm) {
        return TfIdf(freq[i], idf) / std::sqrt(static_cast<score_t>(norm[i]));
      } else if constexpr (HasBoost) {
        return boost[i] * TfIdf(freq[i], idf);
      } else {
        return TfIdf(freq[i], idf);
      }
    }();
    Merge<MergeType>(res[i], r);
  }
}

template<bool HasNorm, bool HasFilterBoost>
struct TfIdfScore : public ScoreOperator {
  TfIdfScore(const uint32_t* norm, score_t boost, TFIDFStats idf,
             const FreqBlockAttr* freq,
             const score_t* filter_boost = nullptr) noexcept
    : freq{freq},
      filter_boost{filter_boost},
      norm{norm},
      idf{boost * idf.value} {}

  template<ScoreMergeType MergeType = ScoreMergeType::Noop>
  IRS_FORCE_INLINE void ScoreImpl(score_t* IRS_RESTRICT res,
                                  scores_size_t n) const noexcept {
    TfIdf<MergeType, HasNorm, HasFilterBoost>(
      res, n, freq->value, TryGetValue(norm), TryGetValue(filter_boost), idf);
  }

  score_t Score() const noexcept final {
    score_t res{};
    ScoreImpl(&res, 1);
    return res;
  }

  void Score(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl(res, n);
  }
  void ScoreSum(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, n);
  }
  void ScoreMax(score_t* res, scores_size_t n) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, n);
  }

  void ScoreBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kScoreBlock);
  }
  void ScoreSumBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Sum>(res, kScoreBlock);
  }
  void ScoreMaxBlock(score_t* res) const noexcept final {
    ScoreImpl<ScoreMergeType::Max>(res, kScoreBlock);
  }

  void ScorePostingBlock(score_t* res) const noexcept final {
    ScoreImpl(res, kPostingBlock);
  }

  const FreqBlockAttr* freq;
  [[no_unique_address]] utils::Need<HasFilterBoost, const score_t*>
    filter_boost;
  [[no_unique_address]] utils::Need<HasNorm, const uint32_t*> norm;
  score_t idf;  // precomputed : boost * idf
};

}  // namespace

void TFIDF::collect(byte_type* stats_buf, const FieldCollector* field,
                    const TermCollector* term) const {
  const auto docs_with_field = field ? field->docs_with_field : 0;
  const auto docs_with_term = term ? term->docs_with_term : 0;

  auto* idf = stats_cast(stats_buf);
  idf->value += static_cast<score_t>(
    std::log1p((docs_with_field + 1.0) / (docs_with_term + 1.0)));
  // TODO(mbkkt) SDB_ASSERT(idf.value >= 0.f);
}

ScoreFunction TFIDF::PrepareScorer(const ScoreContext& ctx) const {
  auto* freq = irs::get<FreqBlockAttr>(ctx.doc_attrs);

  if (!freq) {
    if (!_boost_as_score || 0.f == ctx.boost) {
      return ScoreFunction::Default();
    }

    // if there is no frequency then all the
    // scores will be the same (e.g. filter irs::all)
    return ScoreFunction::Constant(ctx.boost);
  }

  auto* filter_boost = [&] {
    auto* attr = irs::get<BoostBlockAttr>(ctx.doc_attrs);
    return attr ? attr->value : nullptr;
  }();

  const uint32_t* norm = nullptr;
  if (_normalize) {
    norm = [&] {
      auto* attr = irs::get<Norm>(ctx.doc_attrs);
      return attr ? &attr->value : nullptr;
    }();

    // Fallback to reading from columnstore
    if (!norm && ctx.fetcher) {
      norm = ctx.fetcher->AddNorms(ctx.field.norm,
                                   ctx.segment.norms(ctx.field.norm));
    }
  }

  return ResolveBool(norm != nullptr, [&]<bool HasNorms>() {
    return ResolveBool(filter_boost != nullptr, [&]<bool HasBoost>() {
      const auto* stats = stats_cast(ctx.stats);
      return ScoreFunction::Make<TfIdfScore<HasNorms, HasBoost>>(
        norm, ctx.boost, *stats, freq, filter_boost);
    });
  });
}

WandWriter::ptr TFIDF::prepare_wand_writer(size_t max_levels) const {
  if (_normalize) {
    // idf * sqrt(tf) / sqrt(dl)
    // sqrt(tf) / sqrt(dl)
    // tf / dl
    return std::make_unique<FreqNormWriter<kWandTagDivNorm>>(max_levels);
  }
  return std::make_unique<FreqNormWriter<kWandTagMaxFreq>>(max_levels);
}

WandSource::ptr TFIDF::prepare_wand_source() const {
  if (_normalize) {
    return std::make_unique<FreqNormSource<kWandTagNorm>>();
  }
  return std::make_unique<FreqNormSource<kWandTagFreq>>();
}

Scorer::WandType TFIDF::wand_type() const noexcept {
  if (_normalize) {
    return WandType::DivNorm;
  }
  return WandType::MaxFreq;
}

bool TFIDF::equals(const Scorer& other) const noexcept {
  if (!Scorer::equals(other)) {
    return false;
  }
  const auto& p = sdb::basics::downCast<TFIDF>(other);
  return p._normalize == _normalize;
}

}  // namespace irs
